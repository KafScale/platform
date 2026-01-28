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

## Current Security Posture (v1)

- **Authentication**: none at the Kafka protocol layer. Brokers accept any
  client connection. The console UI supports basic auth via
  `KAFSCALE_UI_USERNAME` / `KAFSCALE_UI_PASSWORD`.
- **Authorization**: optional in v1.5. When ACLs are enabled, broker APIs are
  authorized by the configured rules; when disabled, all broker APIs are
  implicitly allowed (including admin APIs like CreatePartitions/DeleteGroups).
- **Transport Security**: TLS termination is expected at the ingress or mesh
  layer in v1; brokers and the console speak plaintext by default.
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

Kafscale v1.5 introduces optional, basic ACL enforcement at the broker. TLS is
still expected to terminate at your load balancer / ingress or service mesh.

### What Is Supported

- **ACL enforcement** for topic, group, and admin operations.
- **Principal identity** derived from the Kafka `client.id` (ClientID) until
  SASL auth is introduced.
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

### Client Configuration

Set `client.id` in your Kafka clients to the principal name used in ACLs.
Until SASL is implemented, this is the default identity Kafscale uses for ACL
checks. You can also derive principals from network identity when the proxy
protocol is enabled (see Operations docs for `KAFSCALE_PRINCIPAL_SOURCE`). Only
enable proxy-derived identity when brokers are reachable solely through a
trusted proxy/LB.

## Known Gaps

- No SASL or mTLS authentication for Kafka protocol clients.
- ACLs are optional and rely on `client.id` or network-derived identity; no
  strong client auth yet.
- No multi-tenant isolation.
- Admin APIs are writable without auth if ACLs are disabled; UI is read-only by
  policy, not enforcement.

## Roadmap

Planned security milestones (order may change as requirements evolve):

- TLS enabled by default in production templates.
- SASL/PLAIN and SASL/SCRAM for Kafka client authentication.
- Authorization / ACL layer for broker admin and data plane APIs.
- Optional mTLS for broker and console endpoints.
- MCP services (if deployed) must be secured with strong auth, RBAC, and audit
  logging; see `docs/mcp.md`.

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
