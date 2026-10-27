<!--
Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
Licensed under the Apache License, Version 2.0 (the "License").
-->

# Kafka proxy authentication

KafScale primarily runs in Kubernetes. With proxy mTLS enabled, clients use TLS
passthrough at the load balancer and authenticate at the Kafka entry proxy. The
proxy forwards requests to brokers over plaintext TCP with a connection-bound,
authenticated principal. Broker ACLs enforce that principal for topic, group, and
administrative operations. `client.id` remains a diagnostic value.

## Upgrading an existing installation

Helm does not upgrade schemas stored under the chart's `crds/` directory. Apply
the CRDs before installing the new operator or adding `spec.security`:

```sh
kubectl apply --server-side --field-manager=kafscale-crd-upgrade -f deploy/helm/kafscale/crds/
kubectl wait --for=condition=Established crd/kafscaleclusters.kafscale.io
kubectl explain kafscalecluster.spec.security --api-version=kafscale.io/v1alpha1
```

Then upgrade the Helm release and apply the cluster manifest with
`kubectl apply --validate=strict -f cluster.yaml`. Reapply manifests that were
submitted before the schema update: Kubernetes may have pruned `spec.security`.
Do not infer that authentication is enabled from a successful Helm upgrade;
verify the stored cluster spec and projected security files in both workloads.

## Secrets and configuration

Create three Secrets in the KafscaleCluster namespace:

| Secret | Required keys | Purpose |
| --- | --- | --- |
| `kafka-proxy-tls` | `tls.crt`, `tls.key`, `ca.crt`, `principals.json` | Server certificate chain and key, trusted client CAs, URI SAN mapping |
| `kafka-proxy-identity` | `key` | At least 32 cryptographically random raw bytes, shared only by proxies and brokers |
| `kafka-acl` | `acl.json` | Existing KafScale ACL configuration |

Issue a server certificate for the proxy's advertised DNS name, with server
authentication usage. Issue each client a certificate with client authentication
usage and a URI SAN. Use your Kubernetes certificate issuer or existing PKI.
The private key must match the certificate. The CA file can contain a bundle.
Generate the forwarding key with `openssl rand -out identity-key 32`, create the
Secret from that file, and remove the local file after securely storing the key.
Never commit certificates, private keys, or identity keys.

`principals.json` explicitly maps URI SANs to service principals:

```json
{
  "spiffe://example.org/services/analytics": "analytics-service",
  "spiffe://example.org/services/ingestion": "ingestion-service"
}
```

No CN, subject DN, address, or `client.id` fallback is used. Certificates with no
mapped URI, or URIs mapping to different principals, are rejected. Principals
must contain 1 to 256 UTF-8 bytes without whitespace or control characters.
`__kafscale_proxy_probe` and `__kafscale_lfs_service` are reserved. Client
certificate mappings cannot use either principal.

For an operator-managed proxy, configure the cluster:

```yaml
spec:
  security:
    tlsSecretRef: kafka-proxy-tls
    identitySecretRef: kafka-proxy-identity
    aclSecretRef: kafka-acl
    maxHandshakes: 64
    maxHandshakesPerIP: 8
    handshakeTimeoutMs: 1000
    handshakeQueueMs: 100
  lfsProxy:
    enabled: true
    advertisedHost: kafka.example.org
    http:
      enabled: false
```

For the standalone Helm proxy, configure the same TLS and identity Secrets:

```yaml
proxy:
  enabled: true
  advertisedHost: kafka.example.org
  security:
    tlsSecretRef: kafka-proxy-tls
    identitySecretRef: kafka-proxy-identity
    aclSecretRef: kafka-acl  # Required if enabling the LFS module
```

Also configure `spec.security.identitySecretRef` and `aclSecretRef` on the
KafscaleCluster. Allow the Helm proxy through the operator's broker NetworkPolicy:

```yaml
spec:
  security:
    identitySecretRef: kafka-proxy-identity
    aclSecretRef: kafka-acl
    proxyPodSelector:
      app.kubernetes.io/component: proxy
      app.kubernetes.io/instance: kafscale
```

Use the actual Helm release instance label. Peers are limited to the same
namespace. The policy permits Kafka port 9092 only from the managed proxy or the
configured proxy selector, and permits metrics port 9093. Other broker ingress
is denied. Your CNI must enforce NetworkPolicies. Kubernetes policies are
additive: an existing broad allow policy can reopen access. Restrict permission
to create pods, change labels, read Secrets, and modify NetworkPolicies.

When authentication is enabled, the operator overrides inherited ACL settings
with enabled, fail-closed ACLs loaded from the configured Secret. Brokers reject
connections that do not prove possession of the forwarding key. Direct legacy
Kafka clients cannot use this listener. Intentional direct access requires a
separate deployment with its own explicit authentication and network controls.

The optional LFS HTTP API uses its own API key and the broker principal
`__kafscale_lfs_service`; grant that service only the permissions it needs.
It does not inherit a Kafka client's certificate identity. Permission is checked
before a direct upload or multipart operation writes to S3. HTTP success requires
a matching Kafka Produce response with no partition error. Authorization failures
return HTTP 403; other broker or malformed-response failures return HTTP 502.
If permission is revoked during a multipart upload, a denied part or completion
request aborts the S3 upload and removes its session. Cleanup uses a bounded
context independent of request cancellation. Failed aborts retain the session for
a retry until it expires or the proxy restarts; an explicit abort returns HTTP 502
on cleanup failure. Expiry does not abort S3 uploads. Configure a bucket lifecycle
rule to clean up incomplete multipart uploads as described in
[operations](operations.md#lfs-proxy).
Objects created before a later Kafka failure are recorded as orphans. In
authenticated mode, startup rejects an enabled LFS HTTP listener without an API key. Secure the HTTP
endpoint independently and configure `KAFSCALE_LFS_PROXY_HTTP_API_KEY` before
exposing it. Kafka produce and fetch requests retain their certificate identity.
The LFS module loads the same ACL configuration as brokers and checks produce permission
before uploading records to S3. Denied topics pass through unchanged for broker
rejection. The operator watches Secret metadata and queues only clusters referencing
the changed ACL Secret. Secret values are read directly from the API when needed,
and are never stored in its informer cache. Cached Secret metadata excludes
annotations, labels, and managed fields, including last-applied Secret contents.
The operator updates a checksum annotation
on both pod templates, triggering a rollout when `acl.json` changes. For the standalone
Helm proxy, run `helm upgrade` after changing the external ACL Secret; Helm
recomputes its checksum annotation from the live Secret. A Secret update alone
does not rerender a Helm-managed Deployment.

## Runtime settings

| Component | Environment variable | Meaning |
| --- | --- | --- |
| Proxy | `KAFSCALE_PROXY_TLS_CERT_FILE` | Server certificate chain |
| Proxy | `KAFSCALE_PROXY_TLS_KEY_FILE` | Server private key |
| Proxy | `KAFSCALE_PROXY_TLS_CLIENT_CA_FILE` | Client CA bundle |
| Proxy | `KAFSCALE_PROXY_TLS_PRINCIPALS_FILE` | JSON URI SAN mapping |
| Both | `KAFSCALE_PROXY_IDENTITY_KEY_FILE` | Shared forwarding key file |

Supplying a server certificate and key alone enables server TLS. mTLS requires
both the client CA and principal mapping files, plus the forwarding key. Invalid
or incomplete configuration fails startup. TLS 1.3 is required. Set Kafka clients
to `security.protocol=SSL`, configure the CA truststore and client keystore, and
keep hostname verification enabled.

Certificates, client CAs, mappings, and the forwarding key are read for each new
connection. Mount whole Secret volumes without `subPath`; Kubernetes must be able
to update the projected files. Invalid rotations reject new connections. Existing
connections retain their original identity and trust decision until disconnected.
TLS session tickets are disabled, so reconnects revalidate certificates and
mappings. Revoking an active client requires closing its existing connections.
Shared-key changes must be coordinated between proxies and brokers; a temporary
mismatch rejects new backend connections. Existing authenticated connections
continue. A metadata request that encounters a stale pooled backend connection
retries once on a fresh connection authenticated with the same principal. New
connection failures and Kafka response errors are not retried. ACLs load at startup. Operator-managed brokers and proxies roll
automatically when the referenced ACL Secret changes. Standalone Helm proxies require the
Helm upgrade described above.

The health endpoint exposes `/metrics` with connection count, authentication
failure count with bounded reason labels, and handshake duration. Authentication
failure logs include the socket remote address and error reason. Broker
authorization denial metrics remain available through the broker metrics endpoint. Metrics have no principal or
certificate labels. The proxy starts a one-second total deadline before admission
and limits pending connections to eight per observed source IP. It reads the first ClientHello record
before taking a cryptographic slot, so idle sockets do not consume those slots.
A connection with a ClientHello waits up to 100 ms for one of 64 slots. The total
handshake budget includes this wait. Connections that exceed a deadline or limit
are rejected.

Tune these values through `spec.security` for operator-managed proxies or
`proxy.security` in Helm. Direct runtime settings are
`KAFSCALE_PROXY_TLS_MAX_HANDSHAKES`, `KAFSCALE_PROXY_TLS_MAX_HANDSHAKES_PER_IP`,
`KAFSCALE_PROXY_TLS_HANDSHAKE_TIMEOUT_MS`, and
`KAFSCALE_PROXY_TLS_HANDSHAKE_QUEUE_MS`. Each accepts an integer from 1 to 60000.
Raise the deadline for high-latency clients. If a load balancer hides client IPs,
configure source IP preservation or size the per-IP limit for the shared address.
At startup, the proxy warns that source-rewriting load balancers make clients
share the per-IP quota. PROXY protocol is not supported.
Peers that send a ClientHello and then stall can still occupy all cryptographic
slots until the handshake deadline. These controls bound handshake work; edge
connection rate limiting is still needed against distributed attacks.

## Internal identity protocol and trust boundary

Before Kafka frames, a broker sends eight ASCII bytes `KSAUTH01` followed by a
fresh 32-byte random challenge. The proxy replies with a two-byte big-endian
principal byte length, the principal, and HMAC-SHA-256 of the complete broker
challenge (including the magic bytes) followed by the principal. The shared key
never crosses the network. The broker validates the length, principal, and MAC
with the Go standard library and replies with byte `1` before accepting Kafka
frames. A proof from one connection cannot be replayed against another challenge.
There is no fallback to unauthenticated framing on this listener.

Each backend connection has one principal. Pools are scoped to one frontend
connection, including partition routing, retries, and reconnects. Authenticated
metadata requests go through broker ACL checks before the proxy rewrites broker
addresses. They reuse the frontend connection's backend pool. Named denials clear
topic IDs and internal-topic flags; unauthorized ID lookups return the same
unknown-ID response as absent IDs without revealing the name. Listing metadata omits unauthorized topics; explicit requests return
an authorization error. Unauthorized metadata requests cannot auto-create topics. Broker auto-create
retains its existing behavior for older clients and clients that omit the
AllowAutoTopicCreation flag; the produce ACL is checked before creation.

This handshake authenticates the proxy's identity assertion. It does not encrypt
or authenticate subsequent Kafka frames, and it does not authenticate the broker
to the proxy. The internal network must prevent traffic interception and
modification. Use a service mesh or CNI encryption where that trust assumption is
not acceptable. A compromised proxy or leaked forwarding key can assert any
principal. Isolate workloads and restrict Secret access accordingly.

Existing external TLS termination remains supported for legacy deployments.
External gateway mTLS alone provides admission, without per-client broker
identity. No gateway identity-forwarding integration is provided by this change;
use TLS passthrough to the native proxy listener for per-client ACLs.

## Validation and benchmarks

`make test-auth-coverage` runs the authentication, proxy, broker, and operator
suites with the race detector and requires 100% statement coverage in the
authentication and identity implementation files, the existing ACL engine, and
the connection identity context. The general repository coverage gate remains
separate. Coverage does not establish that a security design is correct; the
suites also test certificate rejection and rotation, identity replay and forgery,
ACL impersonation, concurrent client isolation, and Kubernetes reconciliation.

Run `go test ./pkg/authn -run '^$' -bench . -benchmem` to measure fresh TLS,
mTLS, and internal identity handshakes. These use in-memory connections. TLS
benchmarks include reads of certificate, trust, and mapping files; the identity
benchmark uses an already-loaded key. They do not predict Kubernetes network
latency or production capacity. For a deployment benchmark, compare sustained traffic
using persistent clients with controlled reconnect bursts, recording proxy CPU,
handshake latency, failures, and broker CPU from the same workload and hardware.
Delegation tokens and SASL/SCRAM are outside this change.

### Measured local costs

One run on an Apple M1 with Go 1.27.2, using `-benchtime=500ms -benchmem`:

| Operation | Measured result |
| --- | --- |
| Internal identity handshake, key already loaded | 6.93 microseconds/op |
| Read and validate the identity key file | 14.7 microseconds/op |
| Fresh server TLS handshake | 0.471 milliseconds/op |
| Fresh mTLS handshake | 2.027 milliseconds/op |
| Concurrent mTLS reconnects, eight workers | 0.247 milliseconds/op aggregate, about 4,055 handshakes/second |
| Established mTLS connection, 64 KiB records with echo | 58.5 microseconds/op; 1,119.6 MB/second payload rate |

TLS figures include loading and parsing the server certificate, client CAs, and
principal mapping. Files remain reloaded on each new connection so trust and key
changes take effect without a stale cache. There is no certificate parsing per
Kafka data request. The identity key is read once per new backend connection;
pooled metadata requests reuse that connection and its authenticated principal.
The reconnect result is aggregate throughput, not individual request latency.
These in-memory transport microbenchmarks do not measure Kafka or Kubernetes
throughput, network RTT, p99 latency, broker CPU, or a production capacity limit.
