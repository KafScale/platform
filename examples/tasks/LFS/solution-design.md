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

# LFS (Large File Support) Solution Design

## Overview

This document describes the technical solution for implementing Large File Support (LFS) in KafScale using a proxy-based architecture.

---

## Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              PRODUCER LAYER                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────┐         ┌──────────────────────┐                  │
│  │  Normal Kafka Client │         │  Streaming SDK       │                  │
│  │  + LFS_BLOB header   │         │  (large files)       │                  │
│  └──────────┬───────────┘         └──────────┬───────────┘                  │
│             │ Kafka Protocol                  │ HTTP/gRPC                    │
│             └─────────────┬───────────────────┘                              │
│                           ▼                                                  │
└───────────────────────────┼──────────────────────────────────────────────────┘
                            │
┌───────────────────────────┼──────────────────────────────────────────────────┐
│                           ▼                                                  │
│                    ┌──────────────┐                                          │
│                    │  LFS PROXY   │◄─────────────────────────────────────────┤
│                    │              │                         PROXY LAYER      │
│                    │  ┌────────┐  │      ┌─────────────┐                     │
│                    │  │ LFS    │──┼─────▶│ S3 Bucket   │                     │
│                    │  │ Handler│  │      │             │                     │
│                    │  └────────┘  │      └─────────────┘                     │
│                    │              │                                          │
│                    └──────┬───────┘                                          │
│                           │ Kafka Protocol (pointer only)                    │
└───────────────────────────┼──────────────────────────────────────────────────┘
                            │
┌───────────────────────────┼──────────────────────────────────────────────────┐
│                           ▼                                                  │
│                    ┌──────────────┐                                          │
│                    │   BROKER     │◄─────────────────────────────────────────┤
│                    │  (unchanged) │                        BROKER LAYER      │
│                    └──────┬───────┘                                          │
│                           │                                                  │
│                           ▼                                                  │
│                    ┌──────────────┐                                          │
│                    │    TOPIC     │                                          │
│                    │  (pointers)  │                                          │
│                    └──────────────┘                                          │
└──────────────────────────────────────────────────────────────────────────────┘
                            │
┌───────────────────────────┼──────────────────────────────────────────────────┐
│                           ▼                                                  │
│  ┌──────────────────────────────────────────┐                               │
│  │         CONSUMER WRAPPER (SDK)           │◄──────────────────────────────┤
│  │                                          │               CONSUMER LAYER  │
│  │  1. Detect LFS envelope                  │      ┌─────────────┐          │
│  │  2. Fetch from S3 ───────────────────────┼─────▶│ S3 Bucket   │          │
│  │  3. Validate checksum                    │      └─────────────┘          │
│  │  4. Return resolved bytes                │                               │
│  └──────────────────────────────────────────┘                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Specifications

### 1. LFS Proxy

**Purpose:** Intercept LFS-marked produce requests, upload payloads to S3, and forward pointer records to the broker.

**Technology:** Go service using franz-go for Kafka protocol handling

**Interfaces:**

| Interface | Protocol | Purpose |
|-----------|----------|---------|
| Kafka Listener | TCP/Kafka | Receive produce requests from clients |
| Kafka Producer | TCP/Kafka | Forward pointer records to broker |
| S3 Client | HTTPS | Upload blobs to S3 |
| HTTP Listener | HTTP/1.1 | Receive streaming uploads (Mode B) |
| Metrics | HTTP/Prometheus | Expose operational metrics |

**Configuration:**

```yaml
proxy:
  kafka:
    listen_address: ":9092"
    broker_address: "broker:9093"

  http:
    listen_address: ":8080"

  lfs:
    enabled: true
    header_name: "LFS_BLOB"
    detection_mode: "header_only"  # header_only | auto_detect | both
    auto_detect_threshold: 8388608  # 8MB
    max_blob_size: 5368709120       # 5GB

  s3:
    bucket: "kafscale-lfs"
    region: "us-east-1"
    prefix_template: "{namespace}/{topic}/lfs/{yyyy}/{mm}/{dd}"

  checksum:
    algorithm: "sha256"
    validation: "required"  # required | optional | none
```

**Internal Flow:**

```
┌─────────────────────────────────────────────────────────────────┐
│                        LFS PROXY                                 │
│                                                                  │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│  │ Kafka    │───▶│ Request  │───▶│ LFS      │───▶│ Kafka    │  │
│  │ Listener │    │ Router   │    │ Handler  │    │ Producer │  │
│  └──────────┘    └──────────┘    └────┬─────┘    └──────────┘  │
│                        │              │                         │
│                        │         ┌────▼─────┐                   │
│                        │         │ S3       │                   │
│                        │         │ Uploader │                   │
│                        │         └────┬─────┘                   │
│                        │              │                         │
│                        ▼              ▼                         │
│                  ┌──────────┐   ┌──────────┐                   │
│                  │Passthrough│   │ Envelope │                   │
│                  │ (non-LFS)│   │ Creator  │                   │
│                  └──────────┘   └──────────┘                   │
└─────────────────────────────────────────────────────────────────┘
```

---

### 2. Consumer Wrapper SDK

**Purpose:** Wrap standard Kafka consumers to transparently resolve LFS pointers.

**Languages:** Go (Phase 1), Java (Phase 2), Python (Phase 3)

**API Design (Go):**

```go
package lfs

// Config for LFS consumer wrapper
type Config struct {
    S3Bucket    string
    S3Region    string
    S3Endpoint  string  // For MinIO compatibility
    CacheDir    string  // Optional local cache
    CacheSize   int64   // Max cache size in bytes
}

// Consumer wraps a Kafka consumer with LFS resolution
type Consumer[K, V any] struct {
    inner  *kafka.Consumer[K, V]
    config Config
    s3     *s3.Client
}

// NewConsumer creates an LFS-aware consumer wrapper
func NewConsumer[K, V any](inner *kafka.Consumer[K, V], config Config) (*Consumer[K, V], error)

// Poll returns records with LFS pointers transparently resolved
func (c *Consumer[K, V]) Poll(timeout time.Duration) ([]Record[K, V], error)

// Record represents a Kafka record with LFS support
type Record[K, V] struct {
    Topic     string
    Partition int32
    Offset    int64
    Key       K
    Timestamp time.Time
    Headers   []Header

    // Internal: may be LFS pointer or actual value
    rawValue  []byte
    resolved  V
    isLfs     bool
}

// Value returns the resolved value (fetches from S3 if LFS)
func (r *Record[K, V]) Value() (V, error)

// ValueStream returns a streaming reader for large values
func (r *Record[K, V]) ValueStream() (io.ReadCloser, error)

// IsLfs returns true if this record is an LFS pointer
func (r *Record[K, V]) IsLfs() bool

// LfsMetadata returns the LFS envelope metadata (nil if not LFS)
func (r *Record[K, V]) LfsMetadata() *LfsEnvelope
```

---

### 3. Streaming Producer SDK (Mode B)

**Purpose:** Enable streaming uploads for files too large to fit in memory.

**Protocol:** HTTP chunked transfer to proxy

**API Design (Go):**

```go
package lfs

// StreamProducer for large file uploads
type StreamProducer struct {
    proxyEndpoint string
    httpClient    *http.Client
}

// NewStreamProducer creates a streaming producer
func NewStreamProducer(proxyEndpoint string) *StreamProducer

// Produce streams a file to the LFS proxy
func (p *StreamProducer) Produce(ctx context.Context, req ProduceRequest) (*ProduceResult, error)

// ProduceRequest for streaming upload
type ProduceRequest struct {
    Topic       string
    Key         []byte
    Body        io.Reader       // Streaming body
    ContentType string
    Headers     map[string]string
}

// ProduceResult after successful upload
type ProduceResult struct {
    Topic     string
    Partition int32
    Offset    int64
    S3Key     string
    Size      int64
    SHA256    string
}
```

**HTTP Endpoint (Proxy):**

```
POST /lfs/v1/produce

Headers:
  X-Kafka-Topic: <topic-name>
  X-Kafka-Key: <base64-encoded-key>
  X-Kafka-Headers: <json-encoded-headers>
  Content-Type: <mime-type>
  Content-Length: <optional, for progress>
  X-Checksum-SHA256: <optional, for validation>

Body: <streaming bytes>

Response (200 OK):
{
  "topic": "my-topic",
  "partition": 0,
  "offset": 12345,
  "s3_key": "namespace/topic/lfs/2026/01/31/abc123",
  "size": 1073741824,
  "sha256": "a1b2c3..."
}
```

---

### 4. LFS Envelope Schema

**JSON Format (v1):**

```json
{
  "kfs_lfs": 1,
  "bucket": "kafscale-lfs",
  "key": "namespace/topic/lfs/2026/01/31/obj-uuid",
  "size": 262144000,
  "sha256": "a1b2c3d4e5f6789...",
  "content_type": "application/octet-stream",
  "created_at": "2026-01-31T12:34:56Z"
}
```

**Detection Algorithm:**

```go
func IsLfsEnvelope(value []byte) bool {
    if len(value) < 15 || len(value) > 1024 {
        return false  // Envelopes are small
    }
    if value[0] != '{' {
        return false
    }
    return bytes.Contains(value[:min(50, len(value))], []byte(`"kfs_lfs"`))
}
```

---

## Data Flow Diagrams

### Mode A: Kafka-Compatible Producer Flow

```
┌──────────┐    ┌───────────┐    ┌───────────┐    ┌────────┐    ┌───────┐
│ Producer │    │ LFS Proxy │    │    S3     │    │ Broker │    │ Topic │
└────┬─────┘    └─────┬─────┘    └─────┬─────┘    └───┬────┘    └───┬───┘
     │                │                │              │             │
     │ Produce(LFS_BLOB, blob)         │              │             │
     │───────────────▶│                │              │             │
     │                │                │              │             │
     │                │ CreateMultipart│              │             │
     │                │───────────────▶│              │             │
     │                │                │              │             │
     │                │ UploadParts    │              │             │
     │                │───────────────▶│              │             │
     │                │                │              │             │
     │                │ CompleteMultipart             │             │
     │                │───────────────▶│              │             │
     │                │                │              │             │
     │                │ CreateEnvelope │              │             │
     │                │────────┐       │              │             │
     │                │        │       │              │             │
     │                │◀───────┘       │              │             │
     │                │                │              │             │
     │                │ Produce(envelope)             │             │
     │                │───────────────────────────────▶│            │
     │                │                │              │             │
     │                │                │              │ Write       │
     │                │                │              │────────────▶│
     │                │                │              │             │
     │ ACK            │                │              │             │
     │◀───────────────│                │              │             │
     │                │                │              │             │
```

### Consumer Flow with LFS Resolution

```
┌──────────┐    ┌────────────┐    ┌────────┐    ┌───────┐    ┌─────┐
│ App      │    │ LFS Wrapper│    │ Broker │    │ Topic │    │ S3  │
└────┬─────┘    └──────┬─────┘    └───┬────┘    └───┬───┘    └──┬──┘
     │                 │              │             │            │
     │ Poll()          │              │             │            │
     │────────────────▶│              │             │            │
     │                 │              │             │            │
     │                 │ Fetch        │             │            │
     │                 │─────────────▶│             │            │
     │                 │              │ Read        │            │
     │                 │              │────────────▶│            │
     │                 │              │◀────────────│            │
     │                 │◀─────────────│             │            │
     │                 │              │             │            │
     │                 │ IsLfsEnvelope?             │            │
     │                 │──────┐       │             │            │
     │                 │      │ Yes   │             │            │
     │                 │◀─────┘       │             │            │
     │                 │              │             │            │
     │                 │ GetObject    │             │            │
     │                 │─────────────────────────────────────────▶│
     │                 │◀─────────────────────────────────────────│
     │                 │              │             │            │
     │                 │ Validate SHA256            │            │
     │                 │──────┐       │             │            │
     │                 │      │       │             │            │
     │                 │◀─────┘       │             │            │
     │                 │              │             │            │
     │ Records (resolved)             │             │            │
     │◀────────────────│              │             │            │
     │                 │              │             │            │
```

---

## Error Handling

### Proxy Error Responses

| Error | HTTP Status | Kafka Error Code | Recovery |
|-------|-------------|------------------|----------|
| Blob too large | 413 | MESSAGE_TOO_LARGE | Client reduces size |
| S3 upload failed | 503 | UNKNOWN_SERVER_ERROR | Client retries |
| Checksum mismatch | 400 | CORRUPT_MESSAGE | Client re-sends |
| Broker unavailable | 503 | UNKNOWN_SERVER_ERROR | Client retries |
| Invalid header | 400 | INVALID_REQUEST | Client fixes header |

### Consumer Error Handling

| Error | Behavior | Application Action |
|-------|----------|-------------------|
| S3 fetch failed | Return error | Retry or skip |
| Checksum mismatch | Return error | Log, alert, skip |
| Invalid envelope | Return raw value | Process as non-LFS |
| S3 object missing | Return error | Log, alert, skip |

---

## Security Considerations

### Authentication & Authorization

1. **Proxy to S3**: IAM role or access keys (never exposed to clients)
2. **Client to Proxy**: Kafka SASL (passed through to broker)
3. **Consumer to S3**: IAM role or access keys in SDK config

### Data Protection

1. **In transit**: TLS for all connections
2. **At rest**: S3 server-side encryption (SSE-S3 or SSE-KMS)
3. **Checksums**: SHA256 validation on upload and download

### Credential Management

- Proxy holds S3 credentials (not clients)
- Consumer SDK requires S3 credentials for resolution
- Never embed credentials in LFS envelopes

---

## Deployment Topology

### Recommended Production Setup

```
┌─────────────────────────────────────────────────────────────────┐
│                        Load Balancer                             │
│                    (TCP passthrough :9092)                       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
            ┌───────────────┼───────────────┐
            ▼               ▼               ▼
     ┌──────────┐    ┌──────────┐    ┌──────────┐
     │LFS Proxy │    │LFS Proxy │    │LFS Proxy │
     │    #1    │    │    #2    │    │    #3    │
     └────┬─────┘    └────┬─────┘    └────┬─────┘
          │               │               │
          └───────────────┼───────────────┘
                          │
                          ▼
                   ┌──────────────┐
                   │   KafScale   │
                   │   Brokers    │
                   └──────────────┘
```

### Proxy Scaling

- Stateless: Scale horizontally
- Memory: ~2x largest expected blob size per concurrent request
- CPU: Checksum computation scales with throughput

---

## Performance Considerations

### Proxy Memory Usage

```
Memory per request = chunk_size + envelope overhead
                   = 1MB + ~500 bytes

Concurrent requests = available_memory / memory_per_request
Example: 8GB RAM → ~8000 concurrent LFS requests
```

### Latency Impact

| Operation | Expected Latency |
|-----------|------------------|
| Passthrough (non-LFS) | <1ms added |
| LFS upload (100MB) | S3 upload time + ~10ms |
| LFS resolution (100MB) | S3 download time + ~10ms |

### Throughput

- Proxy throughput limited by S3 upload bandwidth
- Consumer throughput limited by S3 download bandwidth
- Non-LFS traffic: minimal proxy overhead

---

## Monitoring & Alerting

### Key Metrics

| Metric | Alert Threshold | Meaning |
|--------|-----------------|---------|
| `lfs_proxy_errors_total` | >10/min | Proxy failures |
| `lfs_proxy_orphan_objects_total` | >0 | Cleanup needed |
| `lfs_proxy_upload_duration_p99` | >30s | S3 slowdown |
| `lfs_consumer_checksum_failures` | >0 | Data corruption |

### Dashboards

1. **Proxy Overview**: Requests, bytes, errors, latency
2. **S3 Operations**: Uploads, downloads, failures
3. **Consumer Resolution**: Hit rate, latency, errors

---

## Appendix: Configuration Reference

### Proxy Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LFS_KAFKA_LISTEN` | `:9092` | Kafka listener address |
| `LFS_KAFKA_BROKER` | `localhost:9093` | Upstream broker address |
| `LFS_HTTP_LISTEN` | `:8080` | HTTP listener for streaming |
| `LFS_S3_BUCKET` | - | S3 bucket name (required) |
| `LFS_S3_REGION` | `us-east-1` | S3 region |
| `LFS_S3_ENDPOINT` | - | Custom S3 endpoint (MinIO) |
| `LFS_MAX_BLOB_SIZE` | `5368709120` | Max blob size (5GB) |
| `LFS_CHUNK_SIZE` | `1048576` | Upload chunk size (1MB) |

### Consumer SDK Configuration

```go
config := lfs.Config{
    S3Bucket:   "kafscale-lfs",
    S3Region:   "us-east-1",
    S3Endpoint: "",              // Empty for AWS, set for MinIO
    CacheDir:   "/tmp/lfs-cache", // Optional
    CacheSize:  1073741824,       // 1GB cache
}
```
