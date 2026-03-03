<!--
Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

# Security Tasks

This file tracks security hardening work for recent LFS proxy additions and provides a phased plan.

## Phase 0 - Baseline & Scope ✅ COMPLETE

### Current Defaults (values.yaml)

| Setting | Default | Risk | Location |
|---------|---------|------|----------|
| `lfsProxy.enabled` | `false` | ✅ Safe | values.yaml:142 |
| `lfsProxy.service.type` | `ClusterIP` | ✅ Internal | values.yaml:199 |
| `lfsProxy.http.enabled` | `false` | ✅ HTTP off | values.yaml:168 |
| `lfsProxy.http.apiKey` | `""` (empty) | ⚠️ Required when HTTP enabled | values.yaml:170 |
| `lfsProxy.s3.existingSecret` | `""` | ⚠️ Must be set for production | values.yaml:185 |
| `lfsProxy.s3.accessKey` | `""` (deprecated) | ⚠️ Plaintext if used | values.yaml:186 |
| `lfsProxy.s3.secretKey` | `""` (deprecated) | ⚠️ Plaintext if used | values.yaml:187 |
| `lfsProxy.etcd.existingSecret` | `""` | ⚠️ Must be set for production | values.yaml:176 |
| `lfsProxy.etcd.username/password` | `""` | ⚠️ Plaintext if used | values.yaml:177-178 |

### Ports Exposed

| Port | Purpose | Exposed via Service |
|------|---------|---------------------|
| 9092 | Kafka protocol | Yes (ClusterIP) |
| 8080 | HTTP /lfs/produce | No (disabled by default) |
| 9094 | Health (/livez, /readyz) | No |
| 9095 | Metrics (/metrics) | No |

### Credential Injection

| Source | Method | Secure? |
|--------|--------|---------|
| Helm values.yaml | Plaintext `s3.accessKey`, `s3.secretKey`, `etcd.username`, `etcd.password` | ❌ No |
| Helm values.yaml | `existingSecret` for S3/etcd | ✅ Yes |
| Demo script (lfs-demo.sh) | Kubernetes Secret with `secretKeyRef` | ✅ Yes |

### HTTP Server Security

| Feature | Status | Risk |
|---------|--------|------|
| Read/Write timeouts | Configured | ✅ Mitigated |
| API key comparison | Constant-time | ✅ Mitigated |
| Topic header validation | Enforced | ✅ Mitigated |

Acceptance criteria:
- [x] Documented current defaults for HTTP/metrics/health and service type.
- [x] Confirmed how credentials are injected (values vs Secret) in Helm and demo.

## Phase 1 - High Priority (Default Hardening) ✅ COMPLETE

- [x] Require auth for HTTP LFS endpoint by default (disable HTTP or require `apiKey` when enabled).
- [x] Change default Service type to `ClusterIP` for LFS proxy (avoid public exposure).
- [x] Store S3/etcd credentials in Kubernetes Secrets and use `valueFrom` (avoid plaintext Helm values/env).

**Changes Made:**
- `values.yaml`: `lfsProxy.http.enabled` changed from `true` → `false`
- `values.yaml`: `lfsProxy.service.type` changed from `LoadBalancer` → `ClusterIP`
- `values.yaml`: Added `lfsProxy.s3.existingSecret` and `lfsProxy.etcd.existingSecret` for Secret-based credentials
- `lfs-proxy-deployment.yaml`: Added `secretKeyRef` support when `existingSecret` is set (S3 + etcd)

Acceptance criteria:
- [x] Helm defaults: `lfsProxy.http.enabled=false` or enforce non-empty `apiKey`.
- [x] Helm defaults: `lfsProxy.service.type=ClusterIP`.
- [x] Helm templates support `existingSecret` for S3 + etcd creds.
- [x] Demo script uses Secret for credentials, not inline values.

## Phase 2 - Medium Priority (Runtime Hardening) ✅ COMPLETE

- [x] Add HTTP server timeouts (read, header, write, idle) to mitigate slowloris.
- [x] Validate `X-Kafka-Topic` header (length + allowed charset) before building S3 key.
- [x] Delete objects on checksum mismatch; track orphan if delete fails.

**Changes Made (http.go):**
- Added `ReadTimeout: 30s`, `WriteTimeout: 5m`, `IdleTimeout: 60s`, `MaxHeaderBytes: 1MB`
- Added `isValidTopicName()` function validating: 1-249 chars, alphanumeric/dots/underscores/hyphens only
- Returns 400 "invalid topic name" for malformed topics
- Checksum mismatch deletes uploaded object; orphan tracked if delete fails

Acceptance criteria:
- [x] HTTP server timeouts configured (sane defaults + env overrides).
- [x] Invalid topic header returns 400 with clear error message.
- [x] Checksum mismatch deletes object; failed delete is tracked as orphan.

## Phase 2.5 - Integrity Options ✅ COMPLETE

- [x] Add configurable checksum algorithm (sha256/md5/crc32/none) for LFS validation.
- [x] Support per-request checksum algorithm override via headers.
- [x] Extend envelope schema to include `checksum_alg` and `checksum` while preserving `sha256` for compatibility.

Acceptance criteria:
- [x] Default behavior remains sha256 (no breaking change).
- [x] HTTP: `X-LFS-Checksum-Alg` honored when provided.
- [x] Kafka: `LFS_BLOB_ALG` honored when provided.
- [x] Consumers can verify using `checksum_alg` if present.

## Phase 3 - Low Priority (Data Hygiene) ✅ COMPLETE

- [x] Implement header allowlist for `OriginalHeaders` in the envelope.
- [x] Use constant-time compare for API key validation.

**Changes Made:**
- `http.go`: `validateHTTPAPIKey()` now uses `subtle.ConstantTimeCompare()` instead of `==`
- `handler.go`: Added `safeHeaderAllowlist` to filter sensitive headers from envelope
  - Allowed: `content-type`, `content-encoding`, `correlation-id`, `message-id`, `x-correlation-id`, `x-request-id`, `traceparent`, `tracestate`
  - Redacted: All other headers (prevents leaking auth tokens, API keys, cookies)

Acceptance criteria:
- [x] Envelope header policy documented and enforced (allowlist or redaction).
- [x] API key comparison uses constant-time function.

## Phase 4 - Future Enhancements ✅ COMPLETE

- [x] Add TLS/SASL options for Kafka backend connections.
- [x] Support TLS for HTTP endpoint (or enforce ingress termination).

Acceptance criteria:
- [x] Documented TLS/SASL config options and examples.
- [x] Integration test or manual recipe confirming TLS endpoint works.
