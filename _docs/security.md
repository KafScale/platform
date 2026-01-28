---
layout: doc
title: Security Overview
description: Current security posture, hardening guidance, and roadmap for KafScale.
permalink: /security/
nav_title: Security
nav_order: 7
---

# Security Overview

KafScale is a Kubernetes-native platform focused on Kafka protocol parity and operational stability. This document summarizes the current security posture and the boundaries of what is and is not supported.

---

## Current security posture (v1.5)

| Area | Status |
|------|--------|
| Authentication | None at Kafka protocol layer; console supports basic auth |
| Authorization | Optional ACLs (v1.5); disabled by default |
| Transport | TLS termination expected at ingress/mesh; broker/console plaintext by default |
| Secrets | S3 credentials via K8s secrets, not stored in etcd |
| Data at rest | Depends on S3/etcd provider encryption |
| Network | Assumes private network or cluster-level controls |

### Details

- **Authentication**: None at the Kafka protocol layer. Brokers accept any client connection. The console UI supports basic auth via `KAFSCALE_UI_USERNAME` / `KAFSCALE_UI_PASSWORD`.

- **Authorization**: Optional in v1.5. When ACLs are enabled, broker APIs are authorized by the configured rules; when disabled, all broker APIs are implicitly allowed (including admin APIs like CreatePartitions/DeleteGroups).

- **Transport security**: TLS termination is expected at the ingress or service mesh layer; brokers and the console speak plaintext by default.

- **Secrets handling**: S3 credentials are read from Kubernetes secrets and are never written to etcd or logged.

- **Data at rest**: Stored in S3 and etcd. Encryption depends on your provider configuration (S3 SSE, etcd encryption at rest).

---

## v1.5 Auth (Basic ACLs)

KafScale v1.5 introduces optional, basic ACL enforcement at the broker. TLS is still expected to terminate at your load balancer, ingress, or service mesh.

### What is supported

- **ACL enforcement** for topic, group, and admin operations.
- **Principal identity** derived from the Kafka `client.id` until SASL auth is introduced.
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

Set `KAFSCALE_ACL_FAIL_OPEN=true` to allow traffic if the ACL config is missing or invalid. Default is fail-closed (deny).

### Actions and resources

| Actions | Resources |
|---------|-----------|
| `produce`, `fetch`, `group_read`, `group_write`, `group_admin`, `admin` | `topic`, `group`, `cluster` |

### Client configuration

Set `client.id` in your Kafka clients to the principal name used in ACLs. Until SASL is implemented, this is the default identity KafScale uses for ACL checks.

You can also derive principals from network identity when the proxy protocol is enabled (see [Operations Guide](/operations/) for `KAFSCALE_PRINCIPAL_SOURCE`). Only enable proxy-derived identity when brokers are reachable solely through a trusted proxy/LB.

PROXY v1 headers are capped at 256 bytes; oversized headers are rejected.

---

## Operational guidance

### Network isolation

Deploy brokers and console behind private networking:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: kafscale-broker
  namespace: kafscale
spec:
  podSelector:
    matchLabels:
      app: kafscale-broker
  policyTypes:
    - Ingress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: allowed-namespace
      ports:
        - protocol: TCP
          port: 9092
```

### TLS termination

Terminate TLS at your ingress controller or service mesh, and restrict access to broker and console services so only trusted clients can reach them.

### S3 IAM least privilege

Use least-privilege IAM roles for S3 access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::kafscale-data",
        "arn:aws:s3:::kafscale-data/*"
      ]
    }
  ]
}
```

### etcd access control

- Use separate etcd credentials for KafScale
- Enable etcd authentication and RBAC
- Restrict etcd endpoints to broker and operator pods only

### Console protection

Treat the console as privileged; do not expose it publicly without auth:

- Use ingress with authentication (OAuth proxy, basic auth)
- Consider disabling the console in production if not needed

---

## Known gaps

| Gap | Impact | Mitigation |
|-----|--------|------------|
| No SASL or mTLS authentication | Cannot cryptographically verify client identity | Network isolation, VPN |
| ACLs rely on `client.id` or network identity | No strong client auth yet | Enable ACLs + network controls |
| No multi-tenant isolation | All clients see all topics (unless ACLs configured) | Separate clusters per tenant, or ACLs |
| Admin APIs writable without auth if ACLs disabled | Anyone with network access can modify | Enable ACLs or network isolation |
| UI read-only by policy, not enforcement | Console could be modified to write | Network isolation, auth proxy |

---

## Roadmap

Planned security milestones (order may change as requirements evolve):

- TLS enabled by default in production templates.
- SASL/PLAIN and SASL/SCRAM for Kafka client authentication.
- Enhanced ACL layer with SASL-derived principals.
- Optional mTLS for broker and console endpoints.
- MCP services (if deployed) must be secured with strong auth, RBAC, and audit logging; see [MCP](/mcp/).

---

## Secure development practices

KafScale is maintained by primary developers who design for secure systems and regularly review common classes of vulnerabilities in brokered network services (input validation, request smuggling, SSRF, unsafe deserialization, authN/authZ gaps, secrets handling, and data integrity). Changes that touch the protocol, storage, or operator reconciliation paths require explicit review and tests.

## Cryptography practices

KafScale does not implement custom cryptography. When cryptographic primitives are required (TLS, SASL, token validation), we rely on standard Go libraries and well‑maintained FLOSS dependencies. We do not ship or require broken algorithms (e.g., MD5, RC4, single DES). Where TLS is enabled, operators are expected to use modern ciphers and key lengths that meet NIST 2030 minimums.

KafScale does not store end‑user passwords. Console authentication is backed by Kubernetes secrets managed by operators. When stronger auth is introduced, we will rely on standard key‑stretching schemes (e.g., bcrypt/argon2) and secure randomness from the Go standard library.

## Supply chain and delivery

Releases are tagged in Git, and GitHub Actions publishes artifacts over HTTPS. We do not distribute unsigned artifacts over HTTP. Container images are built from pinned base images and published to GHCR.

## Static and dynamic analysis

We run static analysis as part of CI and before releases:

- CodeQL (GitHub default setup) for vulnerability‑focused static analysis.
- `go vet` on every CI run (`make test`).
- Optional `golangci-lint` via `make lint`.

Dynamic analysis is performed via fuzzing:

- Go fuzz tests run in CI on a schedule (`.github/workflows/fuzz.yml`).
- Fuzz findings are triaged and fixed promptly when confirmed.

We address medium‑and‑higher severity issues discovered by static or dynamic analysis as quickly as possible after validation.

## Vulnerabilities

We aim to resolve known vulnerabilities quickly. If a runtime vulnerability is fixed in a release, the associated CVE is documented in `docs/releases/`.

## Reporting security issues

If you discover a security vulnerability, please report it responsibly:

1. **Do not** open a public GitHub issue
2. Email security@novatechflow.com with details
3. Include steps to reproduce if possible
4. We will respond within 48 hours

See [SECURITY.md](https://github.com/KafScale/platform/blob/main/SECURITY.md) for full details.