<!--
Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
Licensed under the Apache License, Version 2.0 (the "License").
-->

# Kafka proxy authentication

KafScale can authenticate Kafka clients with mTLS at the Kafka entry proxy.
Each client certificate maps to a principal, and broker ACLs enforce that principal for topic, group and admin operations.
The trust boundary is the proxy: traffic from the proxy to brokers is plaintext inside the cluster, protected by an HMAC identity proof and NetworkPolicies.

```text
Kafka client
    |  TLS 1.3, client certificate (mTLS)
    v
KafScale proxy  -- maps the certificate URI SAN to a principal
    |  plaintext TCP inside the cluster
    |  + per-connection challenge and HMAC principal proof
    v
Brokers  -- enforce ACLs on the forwarded principal
    |
    +--> etcd (inside the cluster)    S3 (data)
```

## How it works

- Clients use TLS passthrough at the load balancer and connect to the proxy.
- The proxy terminates TLS 1.3 and verifies the client certificate.
- It maps a URI SAN on the certificate to a principal through `principals.json`.
- It forwards requests to brokers over plaintext TCP with that principal bound to the connection.
- Brokers check the proof, then enforce ACLs on the principal.
- `client.id` remains a diagnostic value. It has no effect on authorization.

## What you need

| Item | What it is |
| --- | --- |
| Server certificate and key | For the proxy's advertised DNS name, with server authentication usage. The key must match the certificate |
| Client CA | The CAs that issue client certificates. The file can contain a bundle |
| Client certificates | One per client, with client authentication usage and a URI SAN |
| `principals.json` | Maps each URI SAN to a principal |
| Forwarding key | At least 32 cryptographically random raw bytes, shared only by proxies and brokers |
| ACL configuration | Your existing KafScale `acl.json` |

Use your Kubernetes certificate issuer or existing PKI.
Never commit certificates, private keys or identity keys.

### Principal mapping

`principals.json` maps URI SANs to service principals explicitly:

```json
{
  "spiffe://example.org/services/analytics": "analytics-service",
  "spiffe://example.org/services/ingestion": "ingestion-service"
}
```

Rules:

- No CN, subject DN, address or `client.id` fallback is used.
- Certificates with no mapped URI are rejected.
- Certificates whose URIs map to different principals are rejected.
- Principals must be 1 to 256 UTF-8 bytes, without whitespace or control characters.
- `__kafscale_proxy_probe` and `__kafscale_lfs_service` are reserved. Client certificate mappings cannot use either.

## Setup step by step

### 1. Upgrade the CRDs (existing installations)

Helm does not upgrade schemas stored under the chart's `crds/` directory.
Apply the CRDs before you install the new operator or add `spec.security`:

```sh
kubectl apply --server-side --field-manager=kafscale-crd-upgrade -f deploy/helm/kafscale/crds/
kubectl wait --for=condition=Established crd/kafscaleclusters.kafscale.io
kubectl explain kafscalecluster.spec.security --api-version=kafscale.io/v1alpha1
```

Then:

- Upgrade the Helm release.
- Apply the cluster manifest with `kubectl apply --validate=strict -f cluster.yaml`.
- Reapply manifests submitted before the schema update. Kubernetes may have pruned `spec.security`.
- Do not assume authentication is on because the Helm upgrade succeeded. Check the stored cluster spec and the projected security files in both workloads.

### 2. Create the Secrets

Create three Secrets in the KafscaleCluster namespace:

| Secret | Required keys | Purpose |
| --- | --- | --- |
| `kafka-proxy-tls` | `tls.crt`, `tls.key`, `ca.crt`, `principals.json` | Server certificate chain and key, trusted client CAs, URI SAN mapping |
| `kafka-proxy-identity` | `key` | Forwarding key shared by proxies and brokers |
| `kafka-acl` | `acl.json` | Existing KafScale ACL configuration |

Generate the forwarding key with `openssl rand -out identity-key 32`.
Create the Secret from that file.
Remove the local file after you store the key securely.

### 3a. Operator-managed proxy: `spec.security`

```yaml
spec:
  security:
    tlsSecretRef: kafka-proxy-tls          # certificates and principals.json
    identitySecretRef: kafka-proxy-identity # forwarding key
    aclSecretRef: kafka-acl                 # acl.json
    maxHandshakes: 64                       # handshake limits, see below
    maxHandshakesPerIP: 8
    handshakeTimeoutMs: 1000
    handshakeQueueMs: 100
  lfsProxy:
    enabled: true
    advertisedHost: kafka.example.org
    http:
      enabled: false
```

### 3b. Standalone Helm proxy: `proxy.security`

```yaml
proxy:
  enabled: true
  advertisedHost: kafka.example.org
  security:
    tlsSecretRef: kafka-proxy-tls
    identitySecretRef: kafka-proxy-identity
    aclSecretRef: kafka-acl  # Required if enabling the LFS module
```

Also set `identitySecretRef` and `aclSecretRef` on the KafscaleCluster.
Then let the Helm proxy through the operator's broker NetworkPolicy:

```yaml
spec:
  security:
    identitySecretRef: kafka-proxy-identity
    aclSecretRef: kafka-acl
    proxyPodSelector:                          # use your Helm release instance label
      app.kubernetes.io/component: proxy
      app.kubernetes.io/instance: kafscale
```

### 4. Configure clients

- Set `security.protocol=SSL`.
- Configure the CA truststore and the client keystore.
- Keep hostname verification enabled.

## What the operator sets up

### NetworkPolicy

- Peers are limited to the same namespace.
- Kafka port 9092 is allowed only from the managed proxy or the configured proxy selector.
- Metrics port 9093 is allowed.
- Other broker ingress is denied.

Limits:

- Your CNI must enforce NetworkPolicies.
- Kubernetes policies are additive. An existing broad allow policy can reopen access.
- Restrict permission to create pods, change labels, read Secrets and modify NetworkPolicies.

### ACLs on brokers

- When authentication is enabled, the operator overrides inherited ACL settings.
- ACLs are enabled and fail closed, loaded from the configured Secret.
- Brokers reject connections that do not prove possession of the forwarding key.
- Direct legacy Kafka clients cannot use this listener.
- Intentional direct access needs a separate deployment with its own explicit authentication and network controls.

### Secret watches

- The operator watches Secret metadata and queues only clusters that reference the changed ACL Secret.
- Secret values are read directly from the API when needed. They are never stored in its informer cache.
- Cached Secret metadata excludes annotations, labels and managed fields, including last-applied Secret contents.

## Settings reference

| Environment variable | Component | Helm value / CRD field | Default | Meaning |
| --- | --- | --- | --- | --- |
| `KAFSCALE_PROXY_TLS_CERT_FILE` | Proxy | `tls.crt` in `tlsSecretRef` | none | Server certificate chain |
| `KAFSCALE_PROXY_TLS_KEY_FILE` | Proxy | `tls.key` in `tlsSecretRef` | none | Server private key |
| `KAFSCALE_PROXY_TLS_CLIENT_CA_FILE` | Proxy | `ca.crt` in `tlsSecretRef` | none | Client CA bundle |
| `KAFSCALE_PROXY_TLS_PRINCIPALS_FILE` | Proxy | `principals.json` in `tlsSecretRef` | none | JSON URI SAN mapping |
| `KAFSCALE_PROXY_IDENTITY_KEY_FILE` | Proxy and brokers | `key` in `identitySecretRef` | none | Shared forwarding key file |
| `KAFSCALE_PROXY_TLS_MAX_HANDSHAKES` | Proxy | `maxHandshakes` | 64 | Concurrent TLS handshake slots, 1 to 60000 |
| `KAFSCALE_PROXY_TLS_MAX_HANDSHAKES_PER_IP` | Proxy | `maxHandshakesPerIP` | 8 | Pending connections per source IP, 1 to 60000 |
| `KAFSCALE_PROXY_TLS_HANDSHAKE_TIMEOUT_MS` | Proxy | `handshakeTimeoutMs` | 1000 | Total handshake deadline in ms, 1 to 60000 |
| `KAFSCALE_PROXY_TLS_HANDSHAKE_QUEUE_MS` | Proxy | `handshakeQueueMs` | 100 | Wait for a handshake slot in ms, 1 to 60000 |

Helm values live under `proxy.security`. CRD fields live under `spec.security`.

How the settings combine:

- A server certificate and key alone enable server TLS.
- mTLS needs the client CA and principal mapping files, plus the forwarding key.
- Invalid or incomplete configuration fails startup.
- TLS 1.3 is required.

## Rotation and reload

What is reloaded:

- Certificates, client CAs, mappings and the forwarding key are read for each new connection.
- Mount whole Secret volumes without `subPath`, so Kubernetes can update the projected files.
- TLS session tickets are disabled, so reconnects revalidate certificates and mappings.

What happens during a rotation:

- An invalid rotation rejects new connections.
- Existing connections keep their original identity and trust decision until they disconnect.
- To revoke an active client, close its existing connections.

Forwarding key changes:

- Coordinate changes between proxies and brokers.
- A temporary mismatch rejects new backend connections. Existing authenticated connections continue.
- A metadata request that hits a stale pooled backend connection retries once, on a fresh connection with the same principal.
- New connection failures and Kafka response errors are not retried.

ACL changes:

- ACLs load at startup.
- Operator-managed brokers and proxies roll automatically when the referenced ACL Secret changes. The operator updates a checksum annotation on both pod templates when `acl.json` changes.
- Standalone Helm proxy: run `helm upgrade` after you change the external ACL Secret. Helm recomputes its checksum annotation from the live Secret.
- A Secret update alone does not rerender a Helm-managed Deployment.

## Handshake limits

| Limit | Default | Setting |
| --- | --- | --- |
| Total handshake deadline, started before admission | 1000 ms | `handshakeTimeoutMs` |
| Pending connections per observed source IP | 8 | `maxHandshakesPerIP` |
| Concurrent cryptographic handshake slots | 64 | `maxHandshakes` |
| Wait for a slot after the ClientHello | 100 ms | `handshakeQueueMs` |

How admission works:

- The proxy reads the first ClientHello record before it takes a cryptographic slot. Idle sockets do not use slots.
- The total handshake budget includes the queue wait.
- Connections that exceed a deadline or limit are rejected.

Tuning:

- Raise the deadline for high-latency clients.
- If a load balancer hides client IPs, configure source IP preservation or size the per-IP limit for the shared address.
- At startup, the proxy warns that source-rewriting load balancers make clients share the per-IP quota.
- PROXY protocol is not supported.

Limits of these controls:

- Peers that send a ClientHello and then stall can hold all slots until the handshake deadline.
- These controls bound handshake work. You still need edge connection rate limiting against distributed attacks.

## Internal identity protocol

The handshake between proxy and broker, before any Kafka frames:

1. The broker sends eight ASCII bytes `KSAUTH01` and a fresh 32-byte random challenge.
2. The proxy replies with a two-byte big-endian principal length, the principal, and HMAC-SHA-256 of the full broker challenge (magic bytes included) followed by the principal.
3. The broker validates the length, principal and MAC with the Go standard library.
4. The broker replies with byte `1`, then accepts Kafka frames.

Properties:

- The shared key never crosses the network.
- A proof from one connection cannot be replayed against another challenge.
- This listener has no fallback to unauthenticated framing.

### Connections and metadata

- Each backend connection has one principal.
- Pools are scoped to one frontend connection, including partition routing, retries and reconnects.
- Authenticated metadata requests pass broker ACL checks before the proxy rewrites broker addresses. They reuse the frontend connection's backend pool.
- Named denials clear topic IDs and internal-topic flags.
- Unauthorized ID lookups return the same unknown-ID response as absent IDs, without revealing the name.
- Listing metadata omits unauthorized topics. Explicit requests return an authorization error.
- Unauthorized metadata requests cannot auto-create topics.
- Broker auto-create keeps its existing behavior for older clients and for clients that omit the AllowAutoTopicCreation flag. The produce ACL is checked before creation.

## Trust boundary and what is not covered

- The identity handshake authenticates the proxy's identity assertion.
- It does not encrypt or authenticate the Kafka frames that follow.
- It does not authenticate the broker to the proxy.
- Proxy-to-broker traffic is plaintext. The internal network must prevent interception and modification. Use a service mesh or CNI encryption where that is not acceptable.
- NetworkPolicy enforcement depends on your CNI.
- A compromised proxy or a leaked forwarding key can assert any principal. Isolate workloads and restrict Secret access.
- etcd is not part of this change. It runs inside the cluster.
- SASL, SASL/SCRAM and delegation tokens are not covered.

### External TLS termination

- External TLS termination remains supported for legacy deployments.
- External gateway mTLS alone gives admission, without per-client broker identity.
- No gateway identity-forwarding integration is provided.
- For per-client ACLs, use TLS passthrough to the native proxy listener.

## Other endpoints

| Endpoint | Auth | Setting |
| --- | --- | --- |
| Kafka produce and fetch through the proxy | Client certificate identity | `spec.security` / `proxy.security` |
| LFS HTTP API | Its own API key, broker principal `__kafscale_lfs_service` | `KAFSCALE_LFS_PROXY_HTTP_API_KEY` |

### LFS HTTP API

Identity:

- The LFS HTTP API does not inherit a Kafka client's certificate identity.
- It acts as `__kafscale_lfs_service`. Grant that principal only the permissions it needs.
- In authenticated mode, startup rejects an enabled LFS HTTP listener without an API key.
- Secure the HTTP endpoint on its own, and set `KAFSCALE_LFS_PROXY_HTTP_API_KEY` before you expose it.

Authorization and writes:

- Permission is checked before a direct upload or multipart operation writes to S3.
- HTTP success requires a matching Kafka Produce response with no partition error.
- Authorization failures return HTTP 403. Other broker or malformed-response failures return HTTP 502.

Revoked permission during a multipart upload:

- A denied part or completion request aborts the S3 upload and removes its session.
- Cleanup uses a bounded context, independent of request cancellation.
- A failed abort keeps the session for a retry until it expires or the proxy restarts.
- An explicit abort returns HTTP 502 if cleanup fails.
- Expiry does not abort S3 uploads. Configure a bucket lifecycle rule for incomplete multipart uploads, as described in [operations](operations.md#lfs-proxy).
- Objects created before a later Kafka failure are recorded as orphans.

### LFS module on the Kafka path

- Kafka produce and fetch requests keep their certificate identity.
- The LFS module loads the same ACL configuration as brokers.
- It checks produce permission before it uploads records to S3.
- Denied topics pass through unchanged, so the broker rejects them.

## Troubleshooting

| Symptom | Where to look |
| --- | --- |
| Proxy does not start | Invalid or incomplete TLS configuration fails startup. Check that the certificate, key, client CA, mapping and forwarding key are all present |
| Client handshake rejected | Authentication failure logs include the socket remote address and the error reason. Check the URI SAN mapping and the client CA |
| New connections fail after a rotation | An invalid rotation rejects new connections. A forwarding key mismatch between proxies and brokers rejects new backend connections |
| Many clients rejected behind one load balancer | They share the per-IP quota. Preserve source IPs or raise `maxHandshakesPerIP` |
| ACL change not applied on a Helm proxy | Run `helm upgrade` after changing the external ACL Secret |
| `spec.security` missing after upgrade | Apply the CRDs, then reapply the cluster manifest |

Metrics on the proxy health endpoint `/metrics`:

- Connection count.
- Authentication failure count, with bounded reason labels.
- Handshake duration.

Metrics have no principal or certificate labels.
Broker authorization denial metrics stay on the broker metrics endpoint.

## Validation and benchmarks

### Tests

- `make test-auth-coverage` runs the authentication, proxy, broker and operator suites with the race detector.
- It requires 100% statement coverage in the authentication and identity implementation files, the existing ACL engine and the connection identity context.
- The general repository coverage gate is separate.
- Coverage does not prove a security design is correct. The suites also test certificate rejection and rotation, identity replay and forgery, ACL impersonation, concurrent client isolation and Kubernetes reconciliation.

### Running the benchmarks

- Run `go test ./pkg/authn -run '^$' -bench . -benchmem` to measure fresh TLS, mTLS and internal identity handshakes.
- They use in-memory connections.
- TLS benchmarks include reads of the certificate, trust and mapping files. The identity benchmark uses an already-loaded key.
- They do not predict Kubernetes network latency or production capacity.
- For a deployment benchmark, compare sustained traffic using persistent clients with controlled reconnect bursts. Record proxy CPU, handshake latency, failures and broker CPU from the same workload and hardware.

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

How to read these numbers:

- TLS figures include loading and parsing the server certificate, client CAs and principal mapping.
- Files are reloaded on each new connection, so trust and key changes take effect without a stale cache.
- There is no certificate parsing per Kafka data request.
- The identity key is read once per new backend connection. Pooled metadata requests reuse that connection and its principal.
- The reconnect result is aggregate throughput, not individual request latency.
- These in-memory microbenchmarks do not measure Kafka or Kubernetes throughput, network RTT, p99 latency, broker CPU or a production capacity limit.
