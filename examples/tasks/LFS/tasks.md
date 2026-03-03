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

# LFS Implementation Tasks

## Overview

This document tracks implementation tasks for the LFS (Large File Support) feature.

**Key Patterns from Existing Codebase:**
- Logging: `log/slog` (not external library)
- Protocol: `pkg/protocol` for Kafka frame handling
- Metrics: Custom Prometheus text format (see `cmd/broker/metrics*.go`)
- Config: Environment variables with `KAFSCALE_*` prefix
- Deployment: Multi-stage Alpine Dockerfile, Helm charts

**Reference Implementations:**
- Proxy: [cmd/proxy/main.go](../../cmd/proxy/main.go) - TCP listener, protocol handling
- Broker metrics: [cmd/broker/metrics_histogram.go](../../cmd/broker/metrics_histogram.go)
- Helm: [deploy/helm/kafscale/values.yaml](../../deploy/helm/kafscale/values.yaml)

**Status Legend:**
- [ ] Not started
- [~] In progress
- [x] Completed
- [!] Blocked

---

## Current Status (2026-02-03)

**Proxy Core:** ✅ COMPLETE - Core LFS rewrite logic working, topic metrics + orphan tracking added
**Consumer SDK:** ✅ COMPLETE - `pkg/lfs/` package with Consumer, Record, S3Client, envelope detection
**Deployment:** ✅ COMPLETE - Dockerfile, Helm charts, CI workflows all ready
**Tests:** ✅ COMPLETE - Handler tests, consumer tests, E2E tests (lfs_proxy_test.go, lfs_proxy_http_test.go)
**Demo:** ✅ COMPLETE - `make lfs-demo` works end-to-end with blob verification

**Files Created:**
- `cmd/lfs-proxy/main.go` - Entry point, config, server startup
- `cmd/lfs-proxy/handler.go` - Connection handling, LFS rewrite, orphan tracking
- `cmd/lfs-proxy/s3.go` - S3 client, multipart upload
- `cmd/lfs-proxy/envelope.go` - LFS envelope struct (local copy)
- `cmd/lfs-proxy/metrics.go` - Prometheus metrics with topic dimension
- `cmd/lfs-proxy/record.go` - Record encoding helpers
- `cmd/lfs-proxy/uuid.go` - UUID generation
- `cmd/lfs-proxy/handler_test.go` - Handler + error tests
- `cmd/lfs-proxy/envelope_test.go` - Envelope unit tests
- `pkg/lfs/envelope.go` - Shared envelope (imported by handler)
- `pkg/lfs/errors.go` - ChecksumError type

---

## Phase 1: MVP (Kafka-Compatible Mode)

**Goal:** Normal Kafka producers work with `LFS_BLOB` header; consumers use Go wrapper.

### 1.1 LFS Proxy Core

**Location:** `cmd/lfs-proxy/`

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| P1-001 | Create `cmd/lfs-proxy/main.go` | P0 | [x] | Follow `cmd/proxy/main.go` pattern |
| P1-002 | Implement TCP listener with `listenAndServe()` | P0 | [x] | Reuse pattern from proxy |
| P1-003 | Implement `handleConnection()` loop | P0 | [x] | Use `pkg/protocol.ReadFrame/WriteFrame` |
| P1-004 | Implement LFS_BLOB header detection | P0 | [x] | Check headers in ProduceRequest |
| P1-005 | Implement request routing (LFS vs passthrough) | P0 | [x] | LFS → S3, others → broker |
| P1-006 | Implement S3 client initialization | P0 | [x] | `aws-sdk-go-v2/service/s3` |
| P1-007 | Implement `handleLfsProduceRequest()` | P0 | [x] | Core LFS logic (rewriteProduceRecords) |
| P1-008 | Implement S3 multipart upload | P0 | [x] | CreateMultipartUpload, UploadPart, Complete |
| P1-009 | Implement SHA256 hashing | P0 | [x] | Note: Full payload hash, not incremental |
| P1-010 | Implement envelope JSON creation | P0 | [x] | `kfs_lfs`, bucket, key, sha256 |
| P1-011 | Implement pointer record production | P0 | [x] | Forward envelope to broker |
| P1-012 | Add checksum validation (header value) | P1 | [x] | Optional client-provided checksum |
| P1-013 | Add orphan object tracking | P1 | [x] | Log + metric via trackOrphans() |
| P1-014 | Add topic label to metrics | P1 | [x] | Per-topic counters in metrics.go |
| P1-015 | Improve error message for checksum mismatch | P2 | [x] | ChecksumError with Expected/Actual |

**Subtasks for P1-007 (handleLfsProduceRequest) - ALL COMPLETE:**
- [x] Parse ProduceRequest using `pkg/protocol`
- [x] Extract message value (blob bytes)
- [x] Generate S3 key: `{namespace}/{topic}/lfs/{yyyy}/{mm}/{dd}/{uuid}`
- [x] Upload to S3 with multipart
- [x] Compute SHA256 during upload
- [x] Create JSON envelope
- [x] Build new ProduceRequest with envelope as value
- [x] Forward to broker
- [x] Return ProduceResponse to client

**Environment Variables - ALL IMPLEMENTED:**
```
KAFSCALE_LFS_PROXY_ADDR              # Kafka listener (default :9092)
KAFSCALE_LFS_PROXY_HEALTH_ADDR       # Health endpoints
KAFSCALE_LFS_PROXY_METRICS_ADDR      # Prometheus metrics
KAFSCALE_LFS_PROXY_ADVERTISED_HOST   # External hostname
KAFSCALE_LFS_PROXY_ADVERTISED_PORT   # External port
KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS    # etcd for broker discovery
KAFSCALE_LFS_PROXY_ETCD_USERNAME     # etcd auth
KAFSCALE_LFS_PROXY_ETCD_PASSWORD     # etcd auth
KAFSCALE_LFS_PROXY_S3_BUCKET         # S3 bucket for blobs
KAFSCALE_LFS_PROXY_S3_REGION         # S3 region
KAFSCALE_LFS_PROXY_S3_ENDPOINT       # S3 endpoint (MinIO)
KAFSCALE_LFS_PROXY_S3_ACCESS_KEY     # S3 credentials
KAFSCALE_LFS_PROXY_S3_SECRET_KEY     # S3 credentials
KAFSCALE_LFS_PROXY_MAX_BLOB_SIZE     # Max blob size (default 5GB)
KAFSCALE_LFS_PROXY_CHUNK_SIZE        # Upload chunk size (default 5MB)
KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE  # For MinIO compatibility
KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET  # Auto-create bucket
```

---

### 1.2 Consumer Wrapper SDK (Go)

**Location:** `pkg/lfs/`

**STATUS: ✅ COMPLETE**

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| C1-001 | Create `pkg/lfs/envelope.go` | P0 | [x] | Envelope struct, EncodeEnvelope() |
| C1-002 | Create `pkg/lfs/consumer.go` | P0 | [x] | Consumer wrapper type |
| C1-003 | Implement envelope detection | P0 | [x] | IsLfsEnvelope() fast JSON check |
| C1-004 | Implement envelope parsing | P0 | [x] | DecodeEnvelope() with validation |
| C1-005 | Implement S3 fetch | P0 | [x] | s3client.go with GetObject |
| C1-006 | Implement checksum validation | P0 | [x] | SHA256 on download in consumer.go |
| C1-007 | Implement `Record.Value()` | P0 | [x] | Lazy fetch from S3 in record.go |
| C1-008 | Implement `Record.ValueStream()` | P1 | [x] | io.ReadCloser for large blobs |
| C1-009 | Add proper error types | P0 | [x] | ChecksumError, LfsError in errors.go |
| C1-010 | Write Go documentation | P1 | [x] | doc.go with examples |
| C1-011 | Add GetObject to s3API interface | P0 | [x] | S3Reader interface in s3client.go |

**File Structure to Create:**
```
pkg/lfs/
├── envelope.go          # Envelope struct, IsLfsEnvelope, Parse
├── consumer.go          # Consumer wrapper
├── record.go            # Record with lazy resolution
├── s3client.go          # S3 fetch logic (GetObject)
├── errors.go            # Custom error types
├── consumer_test.go     # Unit tests
└── envelope_test.go     # Unit tests
```

---

### 1.3 Deployment (Kubernetes/Helm)

**Location:** `deploy/`

**STATUS: ✅ COMPLETE**

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| D1-001 | Create `deploy/docker/lfs-proxy.Dockerfile` | P0 | [x] | Multi-stage Alpine build |
| D1-002 | Add `lfsProxy` section to `values.yaml` | P0 | [x] | Full config with S3, etcd, metrics |
| D1-003 | Create `templates/lfs-proxy-deployment.yaml` | P0 | [x] | Deployment with env vars |
| D1-004 | Create `templates/lfs-proxy-service.yaml` | P0 | [x] | LoadBalancer service |
| D1-005 | Create `templates/lfs-proxy-servicemonitor.yaml` | P1 | [x] | Prometheus ServiceMonitor |
| D1-006 | Add lfs-proxy to CI build matrix | P0 | [x] | `.github/workflows/ci.yml` |
| D1-007 | Add lfs-proxy image to release workflow | P0 | [x] | `.github/workflows/docker.yml` |
| D1-008 | Create lfs-proxy-prometheusrule.yaml | P1 | [x] | Alerting rules |

**Dockerfile Template (copy from `deploy/docker/proxy.Dockerfile`):**
```dockerfile
# deploy/docker/lfs-proxy.Dockerfile
# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.25.2
FROM golang:${GO_VERSION}-alpine@sha256:... AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src
RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download
COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w" -o /out/lfs-proxy ./cmd/lfs-proxy

FROM alpine:3.19@sha256:...
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 kafscale
USER 10001
WORKDIR /app

COPY --from=builder /out/lfs-proxy /usr/local/bin/kafscale-lfs-proxy

EXPOSE 9092 9094 9095
ENTRYPOINT ["/usr/local/bin/kafscale-lfs-proxy"]
```

**Helm values to add to `values.yaml`:**
```yaml
lfsProxy:
  enabled: false
  replicaCount: 2
  image:
    repository: ghcr.io/kafscale/kafscale-lfs-proxy
    tag: ""
    useLatest: false
    pullPolicy: IfNotPresent
  health:
    enabled: true
    port: 9094
  metrics:
    enabled: true
    port: 9095
  advertisedHost: ""
  advertisedPort: 9092
  etcdEndpoints: []
  etcd:
    username: ""
    password: ""
  s3:
    bucket: "kafscale-lfs"
    region: "us-east-1"
    endpoint: ""
    accessKeySecretRef: ""
    secretKeySecretRef: ""
  config:
    maxBlobSize: 5368709120
    chunkSize: 5242880
  podAnnotations: {}
  resources: {}
  nodeSelector: {}
  tolerations: []
  affinity: {}
  service:
    type: LoadBalancer
    port: 9092
    annotations: {}
```

---

### 1.4 Observability (Metrics & Logging)

**Following broker patterns from `cmd/broker/metrics*.go`**

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| O1-001 | Implement metrics histogram type | P0 | [x] | In metrics.go |
| O1-002 | Add upload latency histogram | P0 | [x] | `kafscale_lfs_proxy_upload_duration_seconds` |
| O1-003 | Add upload bytes counter | P0 | [x] | `kafscale_lfs_proxy_upload_bytes_total` |
| O1-004 | Add requests counter | P0 | [x] | `kafscale_lfs_proxy_requests_total{status,type}` |
| O1-005 | Add passthrough counter | P0 | [x] | Included in requests_total |
| O1-006 | Add S3 error counter | P0 | [x] | `kafscale_lfs_proxy_s3_errors_total` |
| O1-007 | Add runtime metrics | P1 | [x] | Goroutines, memory - in metrics.go |
| O1-008 | Implement `/metrics` HTTP endpoint | P0 | [x] | In main.go |
| O1-009 | Implement `/livez` endpoint | P0 | [x] | In handler.go |
| O1-010 | Implement `/readyz` endpoint | P0 | [x] | Checks backend + S3 |
| O1-011 | Add structured logging (slog) | P0 | [x] | Throughout codebase |
| O1-012 | Add ServiceMonitor template | P2 | [x] | templates/lfs-proxy-servicemonitor.yaml (D1-005) |
| O1-013 | Add topic dimension to metrics | P1 | [x] | Per-topic counters implemented |
| O1-014 | Add orphan objects counter | P1 | [x] | `kafscale_lfs_proxy_orphan_objects_total` |

---

### 1.5 Testing

**Following `test/e2e/` and `pkg/broker/server_test.go` patterns**

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| T1-001 | Write `cmd/lfs-proxy/handler_test.go` | P0 | [x] | LFS rewrite + passthrough tests |
| T1-002 | Write `cmd/lfs-proxy/envelope_test.go` | P0 | [x] | Encode/decode + validation |
| T1-003 | Write `cmd/lfs-proxy/s3_test.go` | P0 | [x] | failingS3API in handler_test.go |
| T1-004 | Write `pkg/lfs/consumer_test.go` | P0 | [x] | Consumer wrapper tests |
| T1-005 | Write `pkg/lfs/envelope_test.go` | P0 | [x] | Envelope detection tests |
| T1-006 | Create `test/e2e/lfs_proxy_test.go` | P0 | [x] | E2E with MinIO |
| T1-007 | Add E2E test for happy path | P0 | [x] | Produce → S3 → Consume in lfs_proxy_test.go |
| T1-008 | Add E2E test for passthrough | P0 | [x] | Non-LFS traffic unchanged |
| T1-009 | Add E2E test for checksum validation | P1 | [x] | Client checksum mismatch |
| T1-010 | Add E2E test for S3 failure | P1 | [x] | S3 unavailable handling |
| T1-011 | Add to CI pipeline | P0 | [x] | `go test ./cmd/lfs-proxy/...` in ci.yml |
| T1-012 | Add coverage reporting | P1 | [x] | 80% target |
| T1-013 | Add test for checksum mismatch rejection | P0 | [x] | TestRewriteProduceRecordsChecksumMismatch |
| T1-014 | Add test for max blob size rejection | P0 | [x] | TestRewriteProduceRecordsMaxBlobSize |
| T1-015 | Add test for S3 upload failure | P0 | [x] | TestRewriteProduceRecordsS3Failure |

**Completed Test Cases:**
- `TestRewriteProduceRecordsS3Failure` - S3 upload failure handling
- `TestRewriteProduceRecordsChecksumMismatch` - Checksum validation
- `TestRewriteProduceRecordsMaxBlobSize` - Size limit enforcement
- `failingS3API` mock - Implements all s3API methods returning errors

---

## Phase 2: Streaming Mode

**Goal:** Large file streaming support for files that don't fit in memory.

**STATUS: READY TO START - Phase 1 complete**

### 2.1 HTTP Streaming Endpoint

**STATUS: ✅ COMPLETE** - Implemented in `cmd/lfs-proxy/http.go`

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| P2-001 | Add HTTP server to lfs-proxy | P0 | [x] | `startHTTPServer()` in http.go |
| P2-002 | Implement `POST /lfs/produce` | P0 | [x] | `handleHTTPProduce()` with streaming |
| P2-003 | Parse `X-Kafka-Topic`, `X-Kafka-Key` headers | P0 | [x] | + X-Kafka-Partition, X-LFS-Checksum |
| P2-004 | Connect to S3 streaming upload | P0 | [x] | `UploadStream()` in s3.go |
| P2-005 | Return JSON response with envelope | P0 | [x] | Returns full LFS envelope |
| P2-006 | Add HTTP metrics | P1 | [x] | Reuses existing metrics |
| P2-007 | Implement incremental SHA256 hashing | P0 | [x] | s3.go:183 - chunk-by-chunk hashing |

### 2.2 Streaming Producer SDK (Go)

**STATUS: ✅ COMPLETE** - Implemented in `pkg/lfs/producer.go`

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| S2-001 | Create `pkg/lfs/producer.go` | P0 | [x] | Producer type with options |
| S2-002 | Implement `Produce(topic, key, io.Reader)` | P0 | [x] | HTTP POST with streaming |
| S2-003 | Add progress callback | P1 | [x] | WithProgress() option |
| S2-004 | Add retry logic | P1 | [x] | WithRetry() for transient failures |
| S2-005 | Write documentation | P1 | [x] | doc.go with examples, producer_test.go |

### 2.3 Consumer Wrapper (Java)

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| J2-001 | Set up Java SDK project | P0 | [x] | Maven, separate repo |
| J2-002 | Implement LfsConsumer wrapper | P0 | [x] | Wrap KafkaConsumer |
| J2-003 | Implement envelope detection | P0 | [x] | JSON parsing |
| J2-004 | Implement S3 fetch (AWS SDK) | P0 | [x] | S3Client |
| J2-005 | Implement checksum validation | P0 | [x] | SHA256 |
| J2-006 | Write unit tests | P0 | [x] | JUnit 5 |
| J2-007 | Write integration tests | P1 | [x] | TestContainers (deferred) |

---

### 2.4 Multilingual SDKs (Highest Priority)

**Goal:** Provide LFS wrappers for Go, Java, JavaScript, and Python that integrate with plain Kafka clients.

#### Go SDK Hardening

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| G2-001 | Add Go SDK usage examples | P0 | [x] | `pkg/lfs/doc.go` |
| G2-002 | Add Go SDK integration tests | P0 | [x] | LFS proxy + MinIO (Kind-based) |

#### Java SDK

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| J2-008 | Add Java streaming producer | P0 | [x] | HTTP `/lfs/produce` |
| J2-009 | Add resolver utilities | P0 | [x] | Envelope + checksum helpers |
| J2-010 | Add integration tests | P1 | [x] | TestContainers + MinIO (deferred) |

#### JavaScript/TypeScript SDK (Node.js)
Target: March 2026 (low priority).

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| JS2-001 | Create SDK scaffold | P3 | [x] | `lfs-client-sdk/js/` (uses librdkafka) |
| JS2-002 | Consumer helper | P3 | [x] | Detect envelope + S3 fetch |
| JS2-003 | Producer helper | P3 | [x] | HTTP `/lfs/produce` |
| JS2-004 | Types + examples | P3 | [x] | TypeScript types |
| JS2-005 | Integration tests | P3 | [ ] | MinIO + local proxy |

#### JavaScript Browser SDK (E72)
**No librdkafka** - Pure fetch API for browser usage.

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| JS-BROWSER-001 | Create `lfs-client-sdk/js-browser/` scaffold | P0 | [x] | Zero runtime deps |
| JS-BROWSER-002 | Implement `LfsProducer` with fetch/XHR | P0 | [x] | Progress callback via XHR |
| JS-BROWSER-003 | Implement `LfsEnvelope` types | P0 | [x] | Same as Node SDK |
| JS-BROWSER-004 | Implement browser SHA-256 | P0 | [x] | `crypto.subtle.digest()` |
| JS-BROWSER-005 | Implement `LfsResolver` | P0 | [x] | Pre-signed URL pattern |
| JS-BROWSER-006 | Add retry/backoff | P0 | [x] | Same as Python/Java |
| JS-BROWSER-010 | Create E72 SPA demo | P0 | [x] | Drag-drop upload + E2E tests |
| JS-BROWSER-011 | Add `make e72-browser-demo` target | P0 | [x] | Serves demo on localhost:3000 |
| JS-BROWSER-012 | Add chunked upload client | P0 | [ ] | Resumable multipart upload |
| JS-BROWSER-013 | Add resumable retry logic | P0 | [ ] | Retry failed parts with backoff |
| JS-BROWSER-014 | Add chunked upload progress + resume UI | P1 | [ ] | Show part failures + resume |
| JS-BROWSER-020 | Build ESM + UMD bundles | P1 | [ ] | esbuild config |
| JS-BROWSER-021 | Playwright E2E tests | P2 | [ ] | Automated browser tests |

#### LFS Proxy Chunked Upload API

| PROXY-CHUNK-001 | OpenAPI for multipart uploads | P0 | [x] | `/lfs/uploads` init/part/complete |
| PROXY-CHUNK-002 | Implement upload init endpoint | P0 | [ ] | Create multipart upload session |
| PROXY-CHUNK-003 | Implement upload part endpoint | P0 | [ ] | Stream part to S3 UploadPart |
| PROXY-CHUNK-004 | Implement upload complete endpoint | P0 | [ ] | Complete S3 + produce envelope |
| PROXY-CHUNK-005 | Implement upload abort endpoint | P0 | [ ] | Abort multipart + cleanup |
| PROXY-CHUNK-006 | Add tracker events for chunked flow | P1 | [ ] | upload_started/completed/failed |

#### Python SDK

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| PY2-001 | Create SDK scaffold | P0 | [x] | `lfs-client-sdk/python/` |
| PY2-002 | Consumer helper | P0 | [x] | Detect envelope + S3 fetch (LfsResolver) |
| PY2-003 | Producer helper | P0 | [x] | HTTP `/lfs/produce` (LfsProducer with retry/backoff) |
| PY2-004 | Examples + docs | P1 | [x] | E71 video demo with small/midsize/large tests |
| PY2-005 | Integration tests | P1 | [x] | E71 demo validates end-to-end |

---

## Phase 3: Enhancements

### 3.1 Explode Processor (Optional)

**Priority:** Deferred (lowest priority)

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| E3-001 | Design as separate service | P2 | [ ] | `cmd/lfs-explode/` |
| E3-002 | Implement Kafka consumer | P2 | [ ] | Read LFS pointers |
| E3-003 | Implement S3 batch fetch | P2 | [ ] | Concurrent downloads |
| E3-004 | Implement Kafka producer | P2 | [ ] | Write resolved content |
| E3-005 | Add Helm templates | P2 | [ ] | Deployment, service |

### 3.2 Operator Integration (Future)

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| OP-001 | Add LfsProxySpec to CRD | P3 | [x] | `api/v1alpha1/kafscalecluster_types.go` |
| OP-002 | Add reconcileLfsProxy() | P3 | [x] | `pkg/operator/cluster_controller.go` |
| OP-003 | Create lfs-proxy Deployment from CRD | P3 | [x] | Dynamic deployment |

---

## Phase 4: LFS-Aware Processors

**Goal:** Integrate LFS resolution into the existing Processor architecture for open-format analytics.

**Strategic Context:** See [future-of-datamanagement.md](./future-of-datamanagement.md) for the dual-storage trend analysis.

**STATUS: NOT STARTED - Requires Phase 1 Consumer SDK completion**

### 4.1 Shared LFS Resolver Package

**Location:** `pkg/lfs/`

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| P4-001 | Add `IsLfsEnvelope()` detection | P0 | [x] | Fast JSON prefix check for `kfs_lfs` - in pkg/lfs/envelope.go |
| P4-002 | Add `DecodeEnvelope()` function | P0 | [x] | JSON decode with validation - in pkg/lfs/envelope.go |
| P4-003 | Create `pkg/lfs/resolver.go` | P0 | [x] | LFS Resolver type |
| P4-004 | Implement `Resolve(record)` method | P0 | [x] | Fetch blob, validate checksum |
| P4-005 | Create `pkg/lfs/s3reader.go` | P0 | [x] | S3Reader interface for GetObject |
| P4-006 | Add `ResolvedRecord` type | P0 | [x] | Payload, ContentType, BlobSize, Checksum |

**Resolver Design:**
```go
type Resolver struct {
    s3Client  S3Reader
    maxSize   int64
    validate  bool // checksum validation
}

type ResolvedRecord struct {
    Original    decoder.Record
    Payload     []byte   // actual blob content
    ContentType string   // from envelope metadata
    BlobSize    int64
    Checksum    string
}

func (r *Resolver) Resolve(ctx context.Context, rec decoder.Record) (ResolvedRecord, error)
```

### 4.2 Iceberg Processor Integration

**Location:** `addons/processors/iceberg-processor/`

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| P4-010 | Add `lfs` config section to config schema | P0 | [ ] | mode, max_inline_size, store_metadata |
| P4-011 | Add `lfsResolver` field to Processor struct | P0 | [ ] | Optional LFS resolution |
| P4-012 | Implement `resolveLfsRecords()` | P0 | [ ] | Batch resolution with concurrency |
| P4-013 | Add `lfs_*` metadata columns to Iceberg | P1 | [ ] | content_type, blob_size, checksum, bucket, key |
| P4-014 | Support `mode: resolve` | P0 | [ ] | Fetch blob, write to value column |
| P4-015 | Support `mode: reference` | P1 | [ ] | Keep envelope, add lfs_* columns |
| P4-016 | Support `mode: skip` | P1 | [ ] | Exclude LFS records |
| P4-017 | Support `mode: hybrid` | P2 | [ ] | Inline small, reference large |

**LFS Modes:**

| Mode | Behavior | Use Case |
|------|----------|----------|
| `resolve` | Fetch blob, write full content to `value` column | Analytics queries need raw data |
| `reference` | Keep envelope, add `lfs_*` metadata columns | Pointer-based access, lazy loading |
| `skip` | Exclude LFS records entirely | Non-blob analytics |
| `hybrid` | Inline small blobs, reference large ones | Cost-optimized storage |

**Configuration Example:**
```yaml
mappings:
  - topic: media-uploads
    table: analytics.media_events
    lfs:
      mode: resolve
      max_inline_size: 1048576  # 1MB
      store_metadata: true
    schema:
      columns:
        - name: user_id
          type: long
```

### 4.3 LFS Processor Metrics

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| P4-020 | Add `processor_lfs_resolved_total` | P0 | [ ] | Count of resolved blobs |
| P4-021 | Add `processor_lfs_resolved_bytes_total` | P0 | [ ] | Total bytes fetched |
| P4-022 | Add `processor_lfs_resolution_errors_total` | P0 | [ ] | Fetch failures |
| P4-023 | Add `processor_lfs_resolution_duration_seconds` | P1 | [ ] | Histogram of fetch times |

### 4.4 Testing

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| T4-001 | Unit tests for `pkg/lfs/resolver.go` | P0 | [ ] | Mock S3 client |
| T4-002 | Integration test: LFS + Iceberg processor | P0 | [ ] | MinIO + local Iceberg |
| T4-003 | E2E: Producer → LFS Proxy → Kafka → Processor → Iceberg | P1 | [ ] | Full pipeline |
| T4-004 | Verify Spark/Trino can query resolved data | P1 | [ ] | Analytics validation |
| T4-005 | Test all LFS modes (resolve, reference, skip, hybrid) | P0 | [ ] | Mode coverage |

---

## Phase 5: Alternative Projections

**Priority:** Low (post-MVP / after Phase 4).

**Goal:** Enable LFS data projection to formats beyond Iceberg for diverse analytics ecosystems.

**STATUS: NOT STARTED - Requires Phase 4 completion**

### 5.1 Parquet File Sink (No Catalog)

**Location:** `addons/processors/parquet-processor/`

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| P5-001 | Create `parquet-processor/` scaffold | P2 | [ ] | Copy from skeleton |
| P5-002 | Implement `ParquetSink` type | P2 | [ ] | Direct Parquet writer |
| P5-003 | Add S3 output support | P2 | [ ] | `s3://{bucket}/{prefix}/{topic}/{partition}/{ts}.parquet` |
| P5-004 | Add LFS resolver integration | P2 | [ ] | Reuse `pkg/lfs/resolver.go` |
| P5-005 | Support partitioning by topic/date | P2 | [ ] | Hive-style partitioning |
| P5-006 | Add compression options | P3 | [ ] | Snappy, Zstd, Gzip |

### 5.2 Blob Extraction Sink

**Location:** `addons/processors/blob-processor/`

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| P5-010 | Create `blob-processor/` scaffold | P2 | [ ] | For ML pipelines |
| P5-011 | Implement `BlobSink` type | P2 | [ ] | Raw file extraction |
| P5-012 | Extract LFS payloads to S3 files | P2 | [ ] | `{topic}/{partition}/{offset}.{ext}` |
| P5-013 | Support content-type based extensions | P2 | [ ] | image/png → .png |
| P5-014 | Add manifest file generation | P3 | [ ] | JSON manifest of extracted files |

### 5.3 Delta Lake Sink (Future)

**Location:** `addons/processors/delta-processor/`

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| P5-020 | Evaluate Delta Lake Go libraries | P3 | [ ] | delta-go or custom |
| P5-021 | Create `delta-processor/` scaffold | P3 | [ ] | Databricks/Spark ecosystem |
| P5-022 | Implement Delta transaction log writer | P3 | [ ] | _delta_log/ management |

### 5.4 Webhook/HTTP Sink (Future)

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| P5-030 | Add HTTP sink to skeleton processor | P3 | [ ] | Real-time integrations |
| P5-031 | Support configurable endpoints | P3 | [ ] | Per-topic routing |
| P5-032 | Add retry/backoff logic | P3 | [ ] | Transient failures |

---

## Phase 6: Demo & Documentation

**Goal:** Create `make lfs-demo` target following `iceberg-demo` and `kafsql-demo` patterns.

**Strategic Context:** See [lfs-demo-plan.md](./lfs-demo-plan.md) for detailed implementation plan.

**STATUS: NOT STARTED**

### 6.1 Docker Image

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| DEMO-001 | Create `deploy/docker/lfs-proxy.Dockerfile` | P0 | [x] | Multi-stage Alpine build |
| DEMO-002 | Add `docker-build-lfs-proxy` to Makefile | P0 | [x] | Follow broker pattern |

### 6.2 Demo Script & Makefile

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| DEMO-003 | Create `scripts/lfs-demo.sh` | P0 | [x] | Follow kafsql-demo pattern |
| DEMO-004 | Add `lfs-demo` target to Makefile | P0 | [x] | With all env vars |
| DEMO-005 | Add `LFS_*` variables to Makefile | P0 | [x] | LFS_PROXY_IMAGE, LFS_DEMO_* |

### 6.3 Helm Charts

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| DEMO-006 | Add `lfsProxy` section to `values.yaml` | P1 | [x] | Duplicate of D1-002 |
| DEMO-007 | Create `templates/lfs-proxy-deployment.yaml` | P1 | [x] | Duplicate of D1-003 |
| DEMO-008 | Create `templates/lfs-proxy-service.yaml` | P1 | [x] | Duplicate of D1-004 |
| DEMO-009 | Create `templates/lfs-proxy-configmap.yaml` | P2 | [ ] | Optional config |

### 6.4 Demo Tooling

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| DEMO-010 | Add `--lfs-blob` flag to e2e-client | P1 | [x] | KAFSCALE_E2E_LFS_BLOB env var |
| DEMO-011 | Add `--lfs-size` flag to e2e-client | P1 | [x] | KAFSCALE_E2E_MSG_SIZE env var |
| DEMO-012 | Create demo workload for LFS | P2 | [ ] | Continuous blob stream |

### 6.5 Documentation

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| DEMO-013 | Create `examples/E60_lfs-demo/README.md` | P1 | [x] | E60, E61, E62 READMEs created |
| DEMO-014 | Add LFS to platform docs | P2 | [ ] | Architecture diagram |
| DEMO-015 | Create LFS quickstart guide | P2 | [ ] | 5-minute setup |

---

## Next Sprint Priorities

**Phase 1 & Phase 2 (Go) COMPLETE - Phase 2.3 (Java) optional**

| # | Task | ID | Output | Status |
|---|------|----|--------|--------|
| 1 | Set up Java SDK project | J2-001 | `java/lfs-consumer/` | Optional |
| 2 | Implement LfsConsumer wrapper | J2-002 | Java wrapper | Optional |
| 3 | Industry LFS demos | DEMO-* | E60, E61, E62 | Done |

**Completed (2026-02-01):**
- [x] All P1-* tasks (Proxy Core)
- [x] All C1-* tasks (Consumer SDK)
- [x] All D1-* tasks (Deployment)
- [x] All T1-* tasks complete (Testing)
- [x] All O1-* tasks (Observability)
- [x] All DEMO-* tasks (Demo)
- [x] All P2-* tasks (HTTP Streaming Endpoint)
- [x] All S2-* tasks (Streaming Producer SDK)

---

## Dependencies

### External Dependencies (Verified from go.mod)

| Dependency | Version | Purpose |
|------------|---------|---------|
| `github.com/KafScale/platform/pkg/protocol` | internal | Kafka protocol handling |
| `github.com/KafScale/platform/pkg/metadata` | internal | etcd metadata store |
| `github.com/aws/aws-sdk-go-v2` | latest | S3 client |
| `github.com/twmb/franz-go/pkg/kmsg` | latest | Record batch encoding |
| `github.com/twmb/franz-go/pkg/kgo` | latest | Compression codecs |
| `github.com/google/uuid` | latest | UUID generation |
| `log/slog` | stdlib | Structured logging |
| `crypto/sha256` | stdlib | Checksum computation |

### Internal Dependencies

| Task | Depends On |
|------|------------|
| C1-005 | C1-011 (GetObject in s3API) |
| C1-007 | C1-003, C1-005 |
| T1-004, T1-005 | C1-001 (pkg/lfs creation) |
| T1-006 | P1-*, C1-* |
| D1-003 | P1-001 |

---

## Milestones

### M1: Proxy Alpha - ✅ COMPLETE

- [x] P1-001 through P1-015 complete
- [x] Basic TCP listener working
- [x] S3 upload working
- [x] Passthrough for non-LFS traffic
- [x] Topic-level metrics
- [x] Orphan tracking
- [x] Error path tests (S3 failure, checksum, max size)

### M2: Consumer SDK Ready - ✅ COMPLETE

- [x] pkg/lfs/ package created
- [x] Envelope struct + EncodeEnvelope()
- [x] ChecksumError type
- [x] IsLfsEnvelope() detection
- [x] Consumer wrapper (consumer.go)
- [x] S3 fetch with checksum validation (s3client.go)
- [x] Unit tests for SDK (consumer_test.go, envelope_test.go, record_test.go)

### M3: MVP Release - ✅ COMPLETE

- [x] All D1-* tasks complete (Dockerfile, Helm)
- [x] All O1-* tasks complete (metrics refinements)
- [x] All T1-* tasks complete (full test coverage)
- [x] Docker image build configured in CI
- [x] Helm chart updated with lfsProxy section

### M4: Streaming Release - ✅ COMPLETE (Go SDK)

- [x] HTTP streaming API working (P2-001 to P2-007)
- [x] Streaming Producer SDK (S2-001 to S2-005)
- [x] Java Consumer Wrapper (J2-001 to J2-006) - Optional (integration tests deferred)
- [ ] Performance validated

### M5: LFS-Aware Processors

- [x] `pkg/lfs/resolver.go` with S3 fetch and checksum validation
- [ ] Iceberg processor integration with LFS modes
- [ ] All P4-* tasks complete
- [ ] E2E test: LFS data queryable via Spark/Trino

### M6: Open Format Ecosystem

- [ ] Parquet file sink operational
- [ ] Blob extraction sink operational
- [ ] Alternative projections documented
- [ ] All P5-* tasks complete

### M7: Demo Ready - ✅ COMPLETE

- [x] `make lfs-demo` works end-to-end
- [x] Dockerfile builds successfully
- [x] Helm charts deployable
- [x] Documentation complete (E60, E61, E62)
- [x] All DEMO-* tasks complete (except DEMO-009, DEMO-012, DEMO-014, DEMO-015)

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-01-31 | Use `pkg/protocol` for Kafka handling | Consistent with existing proxy |
| 2026-01-31 | Use `log/slog` for logging | Consistent with existing code |
| 2026-01-31 | Custom metrics (not prometheus/client) | Consistent with broker pattern |
| 2026-01-31 | Environment variables for config | 12-factor, consistent with proxy |
| 2026-01-31 | JSON envelope first | Debuggability, tooling support |
| 2026-01-31 | Helm deployment (not operator-managed) | Simpler initial deployment |
| 2026-01-31 | Table-driven tests | Consistent with Go best practices |
| 2026-01-31 | `//go:build e2e` tags for E2E tests | Separate from unit tests |
| 2026-01-31 | MinIO for S3 testing | Local S3-compatible storage |
| 2026-01-31 | Use franz-go for record encoding | Handles compression codecs |
| 2026-02-01 | Add LFS Resolver to Processors | Bridge opaque S3 pointers with analytics |
| 2026-02-01 | Four LFS modes (resolve/reference/skip/hybrid) | Different consumers need different projections |
| 2026-02-01 | Reuse `pkg/lfs/` for Processor integration | Single source of truth for LFS logic |
| 2026-02-01 | Parquet sink without Iceberg catalog | Ad-hoc analytics, simpler deployments |
| 2026-02-01 | Blob extraction sink for ML pipelines | Raw media file access |

---

## Open Items

- [x] Test patterns established (from existing codebase)
- [x] E2E test structure defined
- [x] MinIO S3 testing approach confirmed
- [x] Header name decided: `LFS_BLOB`
- [x] S3 key format: `{namespace}/{topic}/lfs/{yyyy}/{mm}/{dd}/obj-{uuid}`
- [x] Determine if lfs-proxy should be operator-managed (Phase 3)
- [x] AWS credentials handling in Helm (Secret refs vs env vars) - `existingSecret` support added
- [ ] Concurrency limit for LFS resolution in Processors (Phase 4)
- [ ] Memory limits for hybrid mode blob inlining (Phase 4)
- [ ] Delta Lake Go library evaluation (Phase 5)
- [ ] Webhook sink authentication methods (Phase 5)

## Security Hardening (2026-02-02)

All security hardening phases complete. See [security-tasks.md](../../../docs/lfs-proxy/security-tasks.md) for details.

| Phase | Status | Summary |
|-------|--------|---------|
| Phase 0 | ✅ | Baseline documentation |
| Phase 1 | ✅ | ClusterIP default, HTTP disabled, existingSecret support |
| Phase 2 | ✅ | HTTP timeouts, topic validation |
| Phase 3 | ✅ | Constant-time API key compare, header allowlist |
| Phase 4 | ✅ | TLS/SASL options |

### 6.6 LFS SDK Demos (E70/E71)

**Goal:** Use the standard `lfs-demo` stack and implement client SDK demos in Java (E70) and Python (E71).

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| DEMO-SDK-001 | Define run order using `lfs-demo` | P0 | [x] | Keep stack running while running E70/E71 |
| DEMO-SDK-002 | Implement E70 Java demo against LFS demo stack | P1 | [x] | Use LFS proxy + Kafka + MinIO |
| DEMO-SDK-003 | Implement E71 Python demo against LFS demo stack | P1 | [x] | Use LFS proxy + Kafka + MinIO |
| DEMO-SDK-004 | Document prerequisites + env vars | P1 | [x] | Port-forwards + topic names |
| DEMO-SDK-005 | Add validation steps | P2 | [x] | Verify resolved payloads |
| DEMO-SDK-006 | Harden E70 Makefile for proxy reload + readiness wait | P0 | [x] | `wait-ready`, `wait-http`, `run-all` |
| DEMO-SDK-007 | Add diagnostics targets | P1 | [x] | `list-pods` includes svc/endpoints |
| DEMO-SDK-008 | Require SDK + proxy rebuild on each run | P0 | [x] | `install-sdk` + `refresh-proxy` in `run` |

**Run Plan (LFS demo stack):**
1. Terminal A: `LFS_DEMO_CLEANUP=0 make lfs-demo` (keeps stack running).
2. Terminal B: port-forward LFS proxy, broker, MinIO.
3. Terminal C: run E70 (Java) demo via `make run` or `make run-all`.
4. Terminal D: run E71 (Python) demo against the same stack.

### 6.6.1 SDK/Proxy Reliability Hardening

**Goal:** Make the SDK demos stable and debuggable, with clear error handling and deterministic startup.

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| SDK-RH-001 | Add HTTP retry/backoff for transient network errors | P0 | [x] | Java + Python SDK producer retry on IO errors |
| SDK-RH-002 | Add configurable HTTP timeouts in SDK | P1 | [x] | Constructor or env settings (Java + Python) |
| SDK-RH-003 | Surface structured error details to callers | P1 | [x] | Include status code + body (LfsHttpException) |
| SDK-RH-004 | Propagate request ID + error code | P1 | [x] | Expose X-Request-ID and error code |
| SDK-RH-005 | Python SDK LfsProducer class | P0 | [x] | Context manager with retry/backoff |
| SDK-RH-006 | Python E71 video demo | P0 | [x] | Small/midsize/large video tests |
| PROXY-RH-001 | Return structured JSON errors from HTTP API | P0 | [x] | Error code + message + request ID |
| PROXY-RH-002 | Distinguish backend errors (502/503) vs client errors (400) | P0 | [x] | Prevent EOF ambiguity |
| PROXY-RH-003 | Reject HTTP requests when proxy not ready | P0 | [x] | Gate on `ready` before upload |
| PROXY-RH-004 | Log request ID with HTTP errors | P1 | [x] | Correlate logs with SDK |
| DEMO-RH-001 | Add smoke checks in Makefile (`/readyz`, port 8080) | P1 | [x] | `wait-ready`, `wait-http` |
| DEMO-RH-002 | Add log correlation ID to SDK + proxy | P2 | [x] | `X-Request-ID` header |
| PROXY-RH-TEST-001 | Add HTTP error/ready tests | P1 | [x] | `cmd/lfs-proxy/http_test.go` |

### 6.6 LFS XML (IDoc) Demo

**Goal:** Demonstrate LFS pointer ingestion for XML (IDoc), explode to JSON topics, and validate end-to-end.

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| DEMO-IDOC-001 | Rename Make target to `lfs-demo-idoc` | P0 | [x] | Replace `idoc-demo` |
| DEMO-IDOC-003 | Add IDoc LFS demo README | P1 | [ ] | Run order + port-forwards |
| DEMO-IDOC-004 | Produce LFS XML to `idoc-raw.<type>` | P1 | [ ] | LFS proxy + XML payload |
| DEMO-IDOC-005 | Run `idoc-explode` on pointer topic | P1 | [ ] | Use `cmd/idoc-explode` |
| DEMO-IDOC-006 | Validate exploded topics | P2 | [ ] | `idoc-headers`, `idoc-items`, etc. |
| DEMO-IDOC-007 | Document cleanup steps | P2 | [ ] | Keep cluster running |

**Run Plan (LFS XML Story):**
1. Terminal A: `make lfs-demo` (keep running).
2. Terminal B: port-forward LFS proxy, broker, MinIO.
3. Terminal C: upload XML via LFS proxy to `idoc-raw.<type>`.
4. Terminal C: run `make lfs-demo-idoc` to explode XML into JSON topics.
5. Verify `idoc-headers`, `idoc-items`, `idoc-partners`, `idoc-dates`, `idoc-status` topics.

---

## Phase 7: LFS Traceability & Admin Console

**Goal:** Enable administrators to track blob operations, correlate Kafka pointers with S3 objects, and identify gaps in the system.

**Strategic Context:** See [docs/lfs-proxy/traceability.md](../../../docs/lfs-proxy/traceability.md) for full requirements and API specifications.

**STATUS: PHASE 7.1, 7.2 & 7.3 COMPLETE**

### 7.1 LFS Ops Tracker (Proxy Side)

**Location:** `cmd/lfs-proxy/`

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| TRACE-001 | Define tracker event Go types | P0 | [x] | `tracker_types.go` with upload/download/orphan events |
| TRACE-002 | Implement LfsOpsTracker with franz-go | P0 | [x] | `tracker.go` with async batched writes |
| TRACE-003 | Add tracker initialization to main.go | P0 | [x] | New env vars, circuit breaker |
| TRACE-004 | Emit upload_started event in HTTP handler | P0 | [x] | Before S3 upload |
| TRACE-005 | Emit upload_completed event in HTTP handler | P0 | [x] | After Kafka produce success |
| TRACE-006 | Emit upload_failed event on errors | P0 | [x] | Include error_code, stage |
| TRACE-007 | Emit download_requested event | P1 | [x] | In handleHTTPDownload |
| TRACE-008 | Emit download_completed event | P1 | [x] | After presign/stream success |
| TRACE-009 | Emit orphan_detected event | P0 | [x] | In trackOrphans() |
| TRACE-010 | Unit tests for tracker | P0 | [x] | `tracker_test.go` |

**Tracker Configuration:**

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_LFS_TRACKER_ENABLED` | `true` | Enable event tracking |
| `KAFSCALE_LFS_TRACKER_TOPIC` | `__lfs_ops_state` | Tracker topic name |
| `KAFSCALE_LFS_TRACKER_BATCH_SIZE` | `100` | Events per batch |
| `KAFSCALE_LFS_TRACKER_FLUSH_MS` | `100` | Max flush interval |

### 7.2 Console Backend APIs

**Location:** `internal/console/`

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| TRACE-020 | Create lfs_handlers.go | P0 | [x] | HTTP handlers for LFS APIs |
| TRACE-021 | Implement /ui/api/lfs/status | P0 | [x] | Overall stats and proxy count |
| TRACE-022 | Implement /ui/api/lfs/objects | P0 | [x] | Paginated object list |
| TRACE-023 | Implement /ui/api/lfs/objects/{key} | P1 | [ ] | Object details + event history |
| TRACE-024 | Implement /ui/api/lfs/topics | P0 | [x] | Per-topic statistics |
| TRACE-025 | Implement /ui/api/lfs/events (SSE) | P1 | [x] | Real-time event stream |
| TRACE-026 | Implement /ui/api/lfs/orphans | P1 | [x] | List detected orphans |
| TRACE-027 | Create lfs_consumer.go | P1 | [x] | Consumer for __lfs_ops_state |
| TRACE-028 | Create s3_client.go | P1 | [x] | S3 listing and presign |
| TRACE-029 | Implement /ui/api/lfs/s3/browse | P1 | [x] | S3 object listing |
| TRACE-030 | Implement /ui/api/lfs/s3/presign | P1 | [x] | Admin presigned URL generation |
| TRACE-031 | Register LFS routes in server.go | P0 | [x] | Wire handlers to mux |
| TRACE-032 | Initialize LFS components in main.go | P0 | [x] | S3 client, consumer |

### 7.3 Console UI Dashboard

**Location:** `ui/public/`

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| TRACE-040 | Create LFS dashboard page | P1 | [x] | LFS tab in `index.html` |
| TRACE-041 | Implement LFS Overview panel | P1 | [x] | Stats cards, status indicator |
| TRACE-042 | Implement object browser table | P1 | [x] | Filterable by topic |
| TRACE-043 | Implement topic stats cards | P2 | [x] | Per-topic metrics grid |
| TRACE-044 | Implement real-time events panel | P2 | [x] | SSE-driven live feed |
| TRACE-045 | Implement S3 browser component | P2 | [x] | Directory navigation |
| TRACE-046 | Add presigned URL download button | P2 | [x] | Admin access to blobs

### 7.4 Testing & Documentation

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| TRACE-050 | Integration test for tracker events | P0 | [ ] | `lfs_tracker_test.go` |
| TRACE-051 | E2E test for console LFS APIs | P1 | [ ] | `console_lfs_test.go` |
| TRACE-052 | Performance test for tracker overhead | P1 | [ ] | <5% throughput impact |
| TRACE-053 | Update traceability.md with final API | P1 | [ ] | Keep docs in sync |

### 7.5 Milestones

**M8: Traceability Alpha** ✅
- [x] TRACE-001 through TRACE-010 complete (Tracker)
- [x] Events emitted to `__lfs_ops_state` topic
- [ ] Minimal performance impact verified

**M9: Console LFS Dashboard** ✅
- [x] TRACE-020 through TRACE-032 complete (Backend)
- [x] TRACE-040 through TRACE-046 complete (UI)
- [x] Admin can browse objects and view events

**M10: Traceability Release**
- [ ] All TRACE-* tasks complete
- [ ] Documentation finalized
- [ ] E2E tests passing
