<!--
Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
This project is supported and financed by Scalytics, Inc. (www.scalytics.io).

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Security Overview

Kafscale is a kubernetes native platform focused on Kafka protocol parity and
operational stability. This document summarizes the current security posture
and the boundaries of what is and is not supported in v1.

## Current Security Posture

- **Authentication**: optional mTLS at the Kafka entry proxy, with authenticated
  principal forwarding to brokers. See [Kafka proxy authentication](kafka-authentication.md) for installation,
  CRD upgrades, admission limits, ACL rollouts, and measured costs.
  Legacy deployments accept unauthenticated Kafka connections. The console UI
  supports basic auth via
  `KAFSCALE_UI_USERNAME` / `KAFSCALE_UI_PASSWORD`.
- **Authorization**: optional in v1.5. When ACLs are enabled, broker APIs are
  authorized by the configured rules; when disabled, all broker APIs are
  implicitly allowed (including admin APIs like CreatePartitions/DeleteGroups).
- **Transport Security**: TLS can terminate at the Kafka proxy, ingress, or
  mesh. Internal broker connections remain plaintext unless the infrastructure
  encrypts them. The console speaks plaintext by default.
- **Secrets Handling**: S3 credentials are read from Kubernetes secrets and are
  not written to etcd or source control. The operator projects secrets into pods.
- **Data at Rest**: data is stored in S3 and etcd; encryption at rest depends on
  your infrastructure provider (bucket policies, KMS, disk encryption).
- **Network Trust**: the deployment assumes a private network or cluster-level
  controls (SecurityGroups, NetworkPolicies, ingress rules).

## Operational Guidance

- Deploy brokers and the console behind private networking or VPNs.
- Enable TLS for broker and console endpoints in production.
- Restrict ingress to only trusted clients and operator components.
- Use least-privilege IAM roles for S3 access and restrict etcd endpoints.
- Treat the console as privileged; do not expose it publicly without auth.

## v1.5 Auth (Basic ACLs)

Kafscale supports optional, basic ACL enforcement at the broker. TLS
can terminate at the native Kafka proxy listener or your load balancer / service mesh.

### What Is Supported

- **ACL enforcement** for topic, group, and admin operations.
- **Principal identity** from authenticated proxy connections when enabled;
  legacy connections use Kafka `client.id` (ClientID) or network identity.
- **Allow/Deny rules** with wildcard topic/group names (prefix `*`).
- **Proxy protocol identity** when deployed behind a trusted LB/sidecar.

### Enabling ACLs

Set the following environment variables on broker pods:

```bash
KAFSCALE_ACL_ENABLED=true
KAFSCALE_ACL_JSON='{
  "default_policy": "deny",
  "principals": [
    {
      "name": "analytics-service",
      "allow": [
        {"action": "fetch", "resource": "topic", "name": "orders-*"},
        {"action": "group_read", "resource": "group", "name": "analytics-*"}
      ]
    },
    {
      "name": "ops-admin",
      "allow": [
        {"action": "admin", "resource": "cluster", "name": "*"}
      ]
    }
  ]
}'
```

You can also supply `KAFSCALE_ACL_FILE=/path/to/acl.json` instead of inline JSON.
Set `KAFSCALE_ACL_FAIL_OPEN=true` to allow traffic if the ACL config is missing
or invalid. Default is fail-closed (deny).

### Actions and Resources

- **Actions**: `produce`, `fetch`, `group_read`, `group_write`, `group_admin`, `admin`
- **Resources**: `topic`, `group`, `cluster`

OffsetFetch requires `group_read` on the group and `fetch` on each topic. A null
topic list returns all committed partitions the principal may fetch. An empty
topic list returns no partitions. Explicitly named denied topics return
`TOPIC_AUTHORIZATION_FAILED` without committed offsets or metadata.

### Client Configuration

In legacy mode, set `client.id` to the principal name used in ACLs. This value
is client-controlled and provides no authentication. With proxy mTLS enabled,
the certificate mapping determines the principal and `client.id` has no effect
on authorization. You can also derive principals from network identity when the proxy
protocol is enabled (see Operations docs for `KAFSCALE_PRINCIPAL_SOURCE`). Only
enable proxy-derived identity when brokers are reachable solely through a
trusted proxy/LB. PROXY v1 headers are capped at 256 bytes; oversized headers
are rejected.

## Known Gaps

- No SASL authentication or delegation tokens for Kafka protocol clients.
- External TLS termination alone does not forward certificate identities.
- Legacy ACL mode relies on client-controlled or network-derived identities.
  Strong client authentication requires proxy mTLS and authenticated identity
  forwarding.
- No multi-tenant isolation.
- Admin APIs are writable without auth if ACLs are disabled; UI is read-only by
  policy, not enforcement.

## Security model

- Kafka clients connect to the KafScale proxy over TLS 1.3. With a client CA,
  a principal mapping and a forwarding key configured, the proxy requires
  client certificates (mTLS) and maps each certificate URI SAN to a principal.
  See [Kafka proxy authentication](kafka-authentication.md).
- Traffic from the proxy to brokers is plaintext inside the cluster. When
  `spec.security` is configured, the operator creates a NetworkPolicy that
  allows broker port 9092 only from the proxy and allows metrics port 9093.
  Enforcement depends on the CNI.
- The proxy forwards the principal to brokers with a per-connection challenge
  and HMAC proof. Brokers reject connections without a valid proof and
  enforce ACLs on the forwarded principal.
- etcd runs inside the Kubernetes cluster. The operator-managed etcd listens on
  plain HTTP. Components connect with an optional username and password
  (`KAFSCALE_ETCD_USERNAME`, `KAFSCALE_ETCD_PASSWORD`). There is no etcd client
  TLS setting.
- SASL is not supported.

## Reporting Security Issues

If you believe you have found a security vulnerability, please follow the
process in `SECURITY.md`.

## Secure Development Practices

Kafscale is maintained by primary developers who design for secure systems and
regularly review common classes of vulnerabilities in brokered network services
(input validation, request smuggling, SSRF, unsafe deserialization, authN/authZ
gaps, secrets handling, and data integrity). Changes that touch the protocol,
storage, or operator reconciliation paths require explicit review and tests.

## Cryptography Practices

Kafscale does not implement custom cryptography. When cryptographic primitives
are required (TLS, SASL, token validation), we rely on standard Go libraries and
well‑maintained FLOSS dependencies. We do not ship or require broken algorithms
(e.g., MD5, RC4, single DES). Where TLS is enabled, operators are expected to
use modern ciphers and key lengths that meet NIST 2030 minimums.

Kafscale does not store end‑user passwords. Console authentication is backed by
Kubernetes secrets managed by operators. When stronger auth is introduced, we
will rely on standard key‑stretching schemes (e.g., bcrypt/argon2) and secure
randomness from the Go standard library.

## Supply Chain and Delivery

Releases are tagged in Git, and GitHub Actions publishes artifacts over HTTPS.
We do not distribute unsigned artifacts over HTTP. Container images are built
from pinned base images and published to GHCR.

## Static and Dynamic Analysis

We run static analysis as part of CI and before releases:

- CodeQL (GitHub default setup) for vulnerability‑focused static analysis.
- `go vet` on every CI run (`make test`).
- Optional `golangci-lint` via `make lint`.

Dynamic analysis is performed via fuzzing:

- Go fuzz tests run in CI on a schedule (`.github/workflows/fuzz.yml`).
- Fuzz findings are triaged and fixed promptly when confirmed.

We address medium‑and‑higher severity issues discovered by static or dynamic
analysis as quickly as possible after validation.

## Vulnerabilities

We aim to resolve known vulnerabilities quickly. If a runtime vulnerability is
fixed in a release, the associated CVE is documented in `docs/releases/`.

## Operator-managed workload security

The operator runs broker, optional LFS proxy, and managed etcd containers, snapshot download/restore
init containers, and snapshot/maintenance jobs as UID/GID 10001 with
`runAsNonRoot: true`, `seccompProfile: RuntimeDefault`,
`allowPrivilegeEscalation: false`, all capabilities dropped, and a read-only root
filesystem. The shipped broker, proxy, and etcd-tools images declare `USER 10001`;
the pod settings also override the root default of the third-party etcd and AWS
CLI images. Custom images must support this UID and these restrictions.

Only etcd data (`/var/lib/etcd`), snapshot staging (`/snapshots`), and AWS CLI
scratch space (`/tmp`, also its home and working directory), and LFS verification
scratch space (`/tmp`) are writable. The
broker and maintenance containers need no writable root filesystem mounts.
Read-only root filesystems are additional hardening beyond PSA `restricted`.

Etcd and snapshot volumes use `fsGroup: 10001` and
`fsGroupChangePolicy: Always`. For existing root-owned etcd PVCs, kubelet (or a
CSI driver implementing volume ownership) must make all existing data accessible
to that group before the non-root process starts. This can add startup time on
large volumes. Back up etcd before upgrading, and verify your storage driver
supports `fsGroup`; hostPath volumes and storage drivers that ignore group
ownership require an administrator to prepare permissions before rollout.
A privileged permission-repair init container cannot run under `restricted`.

`go test ./pkg/operator -run TestOperatorRestricted` checks every
operator-created pod template, including reconciliation of pre-hardening
resources and both PVC and memory-backed etcd. CI's **Operator Restricted
Workloads** job additionally runs the reconciled templates with real images in
kind: an existing root-owned etcd PVC is upgraded, insecure admission is rejected,
and produce/consume through the optional LFS proxy, snapshot upload, fresh-volume restore, and both maintenance
jobs run under an enforced Kubernetes 1.35 restricted policy.

To repeat the runtime test, build and load `kafscale-broker:security`,
`kafscale-etcd-tools:security`, `kafscale-e2e-client:security`, `kafscale-proxy:security`, and `kafscale-minio:security` into a kind
cluster, building the MinIO fixture from `test/e2e/minio.Dockerfile` as in `.github/workflows/ci.yml`, then run:

```sh
KAFSCALE_E2E_KIND_SECURITY=1 KAFSCALE_SECURITY_CONTEXT=kind-<cluster-name> \
  go test -tags=e2e ./pkg/operator -run '^TestRestrictedWorkloadsKind$' -v -count=1 -timeout=20m
```

The test creates an isolated namespace. Its MinIO fixture and legacy root-running
etcd pod start before enforcement; all upgraded workload pods and jobs must pass
restricted admission. The test requests local-volume provisioning for its etcd
PVC so kubelet can exercise ownership migration.

The optional LFS proxy defaults to the shipped combined `kafscale-proxy` image
with its LFS module enabled. Core listener, etcd, backend, and health settings
are emitted as `KAFSCALE_PROXY_*`; the previous `KAFSCALE_LFS_PROXY_*` names are
retained for custom legacy images. LFS-specific settings keep their existing
names. Set `spec.lfsProxy.advertisedHost` to an address reachable by clients.
