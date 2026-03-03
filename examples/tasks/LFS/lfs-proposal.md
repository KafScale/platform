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

LFS (Large File Support) Design
Summary
KafScale will support per-topic Large File Support (LFS) by storing large payloads in S3 and writing a small pointer record to Kafka. Classic Kafka consumers will receive the pointer record; KafScale LFS consumer wrappers can resolve the pointer to stream the object directly from S3.

**Revised Approach (v2):** Uploads are handled by a dedicated **LFS Proxy** that intercepts produce requests, streams payload bytes directly to S3, validates checksums, and emits pointer records to Kafka. The broker remains unchanged. Classic Kafka clients work transparently by setting a header flag.

This design avoids streaming huge payloads through the broker memory, keeps Kafka compatibility, and enables seamless adoption without client SDK changes for producers.

---

## Design Challenge: Client-Owned vs Proxy-Owned Upload

### Problems with Client-Owned Upload (Original Proposal)

The original design proposed client-owned uploads where the SDK uploads to S3 first, then produces a pointer record. This has several issues:

1. **Two-phase commit problem**: If S3 upload succeeds but Kafka produce fails, orphan objects accumulate in S3.
2. **SDK requirement for producers**: Classic Kafka clients cannot produce LFS messages without adopting the LFS SDK.
3. **Coordination complexity**: Client must manage S3 credentials, compute checksums, and coordinate two separate operations.
4. **No atomic guarantee**: The upload and pointer produce are not transactional.

### Proxy-Owned Upload (Revised Design)

The revised design introduces an **LFS Proxy** that sits between clients and the broker:

1. **Transparent to producers**: Classic Kafka clients produce normally; the proxy intercepts and handles LFS.
2. **Atomic operation**: Proxy uploads to S3, validates checksum, then produces pointer—all in one flow.
3. **Broker unchanged**: No modifications to the KafScale broker required.
4. **Header-based signaling**: Producers indicate LFS intent via Kafka header (e.g., `LFS_BLOB`).

---

## Critical Design Challenge: What "Works with Normal Producer" Actually Means

### The Honest Assessment

**Claim:** "Normal Kafka producer works with just a header"

**Reality:** Yes, BUT the large payload still travels over the Kafka protocol to the proxy:

```
Producer (100MB in memory) → Kafka Protocol → Proxy (receives 100MB) → S3
```

This is **not true streaming**. The proxy must receive the full Kafka produce request before extracting the blob. Memory pressure is moved from broker to proxy, not eliminated.

### Two Distinct Producer Modes

| Mode | Payload Size | SDK Required | Protocol | True Streaming |
|------|--------------|--------------|----------|----------------|
| **A: Kafka-Compatible** | Up to `max.message.bytes` | ❌ No | Kafka | ❌ No (proxy buffers) |
| **B: Stream-Compatible** | Unlimited | ✅ Yes | HTTP/gRPC | ✅ Yes |

### Mode A: Normal Kafka Producer (No SDK)

For blobs that fit in a Kafka message (typically up to 100MB with tuned configs):

```java
// Standard Kafka producer - NO SDK
ProducerRecord<String, byte[]> record = new ProducerRecord<>("topic", key, blobBytes);
record.headers().add("LFS_BLOB", "".getBytes());
producer.send(record);
```

**What happens:**
1. Producer sends full payload via Kafka protocol
2. Proxy receives complete Kafka request (buffers full payload)
3. Proxy uploads to S3
4. Proxy forwards pointer record to broker

**Limitation:** Payload must fit in memory on both producer and proxy.

### Mode B: Streaming Producer (SDK Required)

For files too large to fit in memory (e.g., 1GB+ video files):

```java
// SDK required for true streaming
LfsProducer producer = new LfsProducer(proxyEndpoint, kafkaConfig);
producer.streamFile("topic", key, new FileInputStream("/path/to/large-file.mp4"));
```

**What happens:**
1. SDK opens HTTP/gRPC connection to proxy
2. SDK streams file chunks (never loads full file in memory)
3. Proxy streams chunks directly to S3 multipart upload
4. Proxy produces pointer record to Kafka

**This is non-Kafka compatible by necessity** - the Kafka protocol cannot stream.

### Summary: Where SDK is Required

| Scenario | Normal Producer | SDK Required |
|----------|-----------------|--------------|
| Blob fits in memory (<100MB typical) | ✅ Works | ❌ Not needed |
| Large file, can load in memory | ✅ Works | ❌ Not needed |
| Large file, cannot fit in memory | ❌ Impossible | ✅ Required |
| True streaming from disk/network | ❌ Impossible | ✅ Required |

**Conclusion:** SDK is only needed when the Kafka protocol itself cannot handle the use case (files too large for memory). This is an inherent limitation of Kafka, not our design.

---

Goals
Per-topic opt-in LFS with minimal impact on existing Kafka clients.
**Proxy-owned upload flow (proxy streams payload to S3, broker unchanged).**
Transparent LFS for classic Kafka producers via header signaling.
Pointer records that are small, stable, and extensible.
Consumer wrappers can transparently resolve pointer records into byte streams.
Clear security posture (authz, S3 permissions, and pointer validation).
Observability of LFS usage and failures.

Non-goals (initial)
~~Server-side chunking or streaming of large payloads through Kafka.~~ (Now supported via proxy)
Transparent delivery of raw file bytes to classic Kafka consumers (requires consumer wrapper).
~~Server-managed upload flow (broker does not receive the file).~~ (Now handled by proxy)
S3 lifecycle automation beyond baseline retention defaults.
Background: Why LFS
The current broker path reads full Kafka frames into memory and buffers record batches before S3 upload. Large message values can cause high memory pressure and slow the broker. LFS avoids this by moving payload bytes directly to S3 and keeping Kafka records small.

Today, large Kafka produce requests are not streamed end-to-end:

The broker reads the full Kafka frame into memory.
Produce parsing materializes record sets as []byte.
Record batches are copied and buffered before flush.
Segments are built fully in memory before S3 upload.
So while KafScale may accept large messages, they are currently buffered in RAM multiple times. LFS is intended to remove this buffering for large payloads by moving the bytes off the Kafka path.

---

## Architecture: LFS Proxy

### Component Overview

```
                                                    ┌─────────────┐
                                               ┌───▶│   Topic A   │
                                               │    │ (pointers)  │
┌─────────────────┐   ┌─────────────────┐   ┌──┴──────────┐    └──────┬──────┘
│  Kafka Client   │   │  Kafka Message  │   │             │           │
│  Producer       │──▶│  Header or Value│──▶│ LFS Proxy   │           │
└─────────────────┘   │  (with BLOB)    │   │             │           ▼
                      └─────────────────┘   └──┬──────────┘    ┌─────────────────┐
                                               │               │   Processor     │
                                               │               │   (Explode)     │
                                               ▼               └────────┬────────┘
                                        ┌─────────────┐                 │
                                        │             │                 │
                                        │  S3 Bucket  │◀────────────────┘
                                        │   (File)    │        fetch
                                        │             │                 │
                                        └─────────────┘                 │
                                               ▲                        ▼
                                               │               ┌─────────────┐
                                               │               │   Topic B   │
                                         upload│               │ (resolved)  │
                                               │               └─────────────┘
                                        ┌──────┴──────┐
                                        │  LFS Proxy  │
                                        │  S3 Stream  │
                                        └─────────────┘
```

**Flow Summary:**

1. **Producer** → sends message with BLOB (Header or Value mode)
2. **LFS Proxy** → detects LFS, streams to S3, creates envelope
3. **Broker** → receives pointer record, writes to **Topic A**
4. **Processor (Explode)** → reads pointers, fetches from S3, writes resolved to **Topic B**
5. **Consumers** → choose: read pointers (Topic A + wrapper) OR resolved (Topic B)

### LFS Proxy Responsibilities

1. **Intercept produce requests** with `LFS_BLOB` header
2. **Stream payload bytes** directly to S3 (no full buffering)
3. **Compute checksum** during streaming (SHA256)
4. **Validate checksum** against client-provided value (if present)
5. **Create envelope** with pointer metadata and append to message value
6. **Forward pointer record** to broker after successful S3 upload
7. **Passthrough** for non-LFS messages (no overhead)

### LFS Detection Modes: Header vs Value

The proxy can detect LFS intent in two ways (configurable):

#### Mode 1: Header-Based (Recommended Default)

Producer explicitly sets `LFS_BLOB` header to signal LFS handling.

| Aspect | Assessment |
|--------|------------|
| Kafka compatibility | ✅ Headers are standard Kafka protocol |
| Client simplicity | ✅ Just add one header to existing produce call |
| Backward compatible | ✅ Clients without header work normally |
| Detection overhead | ✅ O(1) header lookup, minimal |
| Predictability | ✅ Explicit intent, no surprises |

**Header schema:**

```
Header: LFS_BLOB
Value: <optional-checksum-sha256-hex> or empty

Examples:
  LFS_BLOB: ""                    # Proxy computes checksum
  LFS_BLOB: "a1b2c3d4..."         # Client-provided checksum for validation
```

#### Mode 2: Value-Based Auto-Detection (Optional)

Proxy auto-detects large values and converts to LFS without header.

| Aspect | Assessment |
|--------|------------|
| Client simplicity | ✅ Zero changes required |
| Predictability | ⚠️ May trigger unexpectedly |
| Control | ⚠️ No per-message opt-out |
| Use case | Good for "always LFS" topics |

**Configuration:**

```yaml
lfs:
  detection_mode: header_only  # header_only | auto_detect | both
  auto_detect_threshold: 8388608  # 8MB - only for auto_detect mode
```

#### Mode 3: Both (Header OR Size)

Combine both modes: header takes precedence, size triggers fallback.

```
If LFS_BLOB header present → LFS mode
Else if value.size >= threshold AND topic.lfs_auto_detect=true → LFS mode
Else → passthrough
```

**Recommendation:** Start with `header_only` for explicit control. Add `auto_detect` as opt-in per-topic feature for convenience.

---

High-Level Flow (Revised)

**Producer Flow:**
1. Producer sends Kafka produce request with `LFS_BLOB` header to LFS Proxy.
2. Proxy detects header, streams payload bytes directly to S3.
3. Proxy computes SHA256 checksum during streaming.
4. Proxy validates checksum (against header value if provided).
5. On success: Proxy creates pointer envelope, appends to message value.
6. Proxy forwards the (now small) pointer record to KafScale broker.
7. Broker processes pointer record as a normal Kafka message.

**Consumer Flow:**
1. Classic consumer receives pointer record from broker (unchanged).
2. Consumer wrapper detects pointer envelope (via `kfs_lfs` marker).
3. Consumer wrapper fetches blob from S3 using pointer metadata.
4. Consumer wrapper validates checksum on download.
5. Application receives original payload bytes transparently.
Topic Configuration
LFS is enabled per topic (admin-configurable):

kafscale.lfs.enabled (bool, default false)
kafscale.lfs.min_bytes (int, default 8MB)
If a producer uses the LFS SDK and payload exceeds this threshold, upload to S3 and emit a pointer record.
kafscale.lfs.bucket (string, optional override; defaults to cluster S3 bucket)
kafscale.lfs.prefix (string, optional key prefix override)
kafscale.lfs.require_sdk (bool, default false)
If true, reject oversized produce requests without valid LFS pointer.
Note: These configs are intended for the admin API. They may map to internal metadata stored in etcd.

Pointer Envelope Schema (v2)

### Envelope Concept

The proxy creates an **envelope** that replaces the original message value. This envelope contains metadata needed to resolve the actual file from S3.

**Key design decision:** The envelope is appended as the message value, making it transparent to brokers and classic consumers. The consumer wrapper detects and resolves it.

### Envelope Format (JSON v1)

```json
{
  "kfs_lfs": 1,
  "bucket": "kafscale-lfs",
  "key": "namespace/topic/lfs/2026/01/28/obj-<uuid>",
  "size": 262144000,
  "sha256": "a1b2c3d4e5f6...",
  "content_type": "application/octet-stream",
  "original_headers": {
    "LFS_BLOB": "",
    "custom-header": "value"
  },
  "created_at": "2026-01-28T12:34:56Z",
  "proxy_id": "lfs-proxy-01"
}
```

### Envelope Fields

| Field | Required | Description |
|-------|----------|-------------|
| `kfs_lfs` | ✅ | Version discriminator (always check first) |
| `bucket` | ✅ | S3 bucket containing the blob |
| `key` | ✅ | S3 object key |
| `size` | ✅ | Blob size in bytes (for validation, progress) |
| `sha256` | ✅ | Hex-encoded SHA256 checksum |
| `content_type` | ❌ | MIME type (optional, for client hints) |
| `original_headers` | ❌ | Preserved Kafka headers from producer |
| `created_at` | ❌ | ISO timestamp of upload |
| `proxy_id` | ❌ | Proxy instance ID (for debugging) |

### Envelope Detection

Consumer wrapper detects LFS envelope via:

1. **Magic bytes prefix** (optional, for binary detection): `0x4B 0x46 0x53 0x4C` ("KFSL")
2. **JSON detection**: Parse and check for `kfs_lfs` field
3. **Fast path**: First byte is `{` and contains `"kfs_lfs"` within first 20 bytes

```go
func IsLfsEnvelope(value []byte) bool {
    if len(value) < 15 {
        return false
    }
    // Fast check: JSON object with kfs_lfs near start
    if value[0] == '{' && bytes.Contains(value[:min(50, len(value))], []byte(`"kfs_lfs"`)) {
        return true
    }
    return false
}
```

### Alternative: Binary Envelope (v2, future)

For reduced overhead, a binary format can be introduced:

```
[4 bytes: magic "KFSL"]
[1 byte:  version]
[2 bytes: bucket name length]
[N bytes: bucket name]
[2 bytes: key length]
[N bytes: key]
[8 bytes: size (uint64)]
[32 bytes: sha256]
```

Total overhead: ~60 bytes + bucket/key length (vs ~200+ bytes for JSON).

LFS Client Behavior (Simplified)

## SDK Requirement Summary

| Component | SDK Required? | Reason |
|-----------|---------------|--------|
| **Producer (normal blobs)** | ❌ No | Just add header to normal Kafka produce |
| **Producer (streaming large files)** | ✅ Yes | Kafka protocol cannot stream |
| **Consumer** | ✅ Yes (wrapper) | Needed to resolve pointers from S3 |

---

### Producer: Mode A (No SDK - Kafka Compatible)

**Normal Kafka producer with one header addition:**

```java
// Standard Kafka producer - NO SDK REQUIRED
ProducerRecord<String, byte[]> record = new ProducerRecord<>(
    "my-topic",
    key,
    blobBytes  // Must fit in memory
);
record.headers().add("LFS_BLOB", "".getBytes());
producer.send(record);
```

**Optional checksum for validation:**

```java
record.headers().add("LFS_BLOB", sha256Hex.getBytes());
```

**Works with:** Any Kafka client (Java, Go, Python, librdkafka, etc.)

---

### Producer: Mode B (SDK Required - Streaming)

**For files that cannot fit in memory:**

```java
// SDK required - non-Kafka protocol (HTTP streaming to proxy)
try (LfsStreamProducer producer = new LfsStreamProducer(proxyHttpEndpoint)) {
    producer.produce("my-topic", key,
        Files.newInputStream(Path.of("/data/large-video.mp4")),
        "video/mp4");
}
```

**SDK streams via HTTP POST:**

```
POST /lfs/produce
Headers:
  X-Kafka-Topic: my-topic
  X-Kafka-Key: <base64-key>
  Content-Type: video/mp4
Body: <streaming file bytes>
```

**Why SDK here?** The Kafka protocol requires knowing message size upfront and sends complete messages. True streaming requires HTTP chunked transfer or gRPC streaming.

---

### Consumer: SDK Wrapper Required

**Consumer must use our wrapper to resolve LFS pointers:**

```java
// Wrap standard Kafka consumer with LFS resolution
KafkaConsumer<String, byte[]> baseConsumer = new KafkaConsumer<>(props);
LfsConsumer<String, byte[]> consumer = new LfsConsumer<>(baseConsumer, s3Config);

for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(100))) {
    // Automatically resolved: if LFS pointer, fetched from S3
    // If normal message, returned unchanged
    byte[] data = record.value();
    process(data);
}
```

**Why SDK required for consumer?**

Without wrapper, consumer receives raw pointer JSON:
```json
{"kfs_lfs":1,"bucket":"...","key":"...","sha256":"..."}
```

The wrapper transparently:
1. Detects LFS envelope
2. Fetches blob from S3
3. Validates checksum
4. Returns original bytes

**Consumer wrapper options:**

| Method | Use Case |
|--------|----------|
| `record.value()` | Load full blob into memory |
| `record.valueAsStream()` | Stream large blobs without full memory load |
| `record.isLfs()` | Check if record is LFS (for custom handling) |
| `record.lfsMetadata()` | Access pointer metadata directly |
S3 Object Layout
Default layout:
s3://{bucket}/{namespace}/{topic}/lfs/{yyyy}/{mm}/{dd}/{uuid}

Rationale:

Keeps LFS objects scoped to topic and date for lifecycle/cleanup.
UUID ensures uniqueness and avoids collisions.
Upload Approach

~~Preferred: pre-signed S3 PUT or multipart upload.~~ (Original client-owned approach)

### Revised: Proxy-Managed Streaming Upload

The LFS Proxy handles uploads internally. No external upload API is exposed to clients.

**Proxy streaming behavior:**

1. **Chunked streaming**: Proxy reads request body in chunks (e.g., 1MB), writes to S3 multipart upload.
2. **Memory efficiency**: Only one chunk buffered at a time, not full payload.
3. **Checksum computation**: SHA256 computed incrementally during streaming.
4. **Atomic commit**: S3 multipart upload completed only after all chunks received and validated.
5. **Rollback on failure**: Abort multipart upload if checksum fails or client disconnects.

**Proxy configuration:**

```yaml
lfs:
  enabled: true
  chunk_size: 1048576        # 1MB streaming chunks
  max_blob_size: 5368709120  # 5GB max (S3 single object limit consideration)
  s3_bucket: "kafscale-lfs"
  s3_prefix: "{namespace}/{topic}/lfs/{yyyy}/{mm}/{dd}"
  checksum_validation: required  # required | optional | none
```

### Why Not Client-Owned Upload?

| Concern | Client-Owned | Proxy-Owned |
|---------|--------------|-------------|
| Orphan objects | ⚠️ Likely (two-phase) | ✅ Atomic |
| Client complexity | ⚠️ SDK required | ✅ Just add header |
| Credential exposure | ⚠️ Pre-signed URLs | ✅ Proxy holds creds |
| Checksum guarantee | ⚠️ Client-computed | ✅ Proxy-verified |
| Classic client support | ❌ No | ✅ Yes |

Validation and Safety

### Proxy-Side Validation (Revised)

The LFS Proxy performs validation **before** forwarding to the broker:

1. **Size validation**: Reject if payload exceeds `max_blob_size`.
2. **Checksum computation**: SHA256 computed during streaming.
3. **Checksum verification**: If client provided checksum in `LFS_BLOB` header, compare.
4. **S3 upload validation**: Verify ETag/checksum returned by S3 matches computed value.
5. **Atomic commit**: Only produce pointer record after all validations pass.

**Validation flow:**

```
Client ──▶ Proxy: Produce with LFS_BLOB header
                    │
                    ▼
              Check size limit
                    │ FAIL → Return error to client
                    ▼
              Stream to S3 (multipart)
              Compute SHA256 incrementally
                    │ FAIL → Abort multipart, return error
                    ▼
              Validate checksum (if provided)
                    │ FAIL → Abort multipart, return error
                    ▼
              Complete S3 multipart upload
                    │ FAIL → Return error (S3 handles cleanup)
                    ▼
              Create envelope, produce to broker
                    │ FAIL → Log orphan for cleanup
                    ▼
              Return success to client
```

### Broker-Side (No Changes Required)

The broker sees only small pointer records. No LFS-specific validation needed. This is a key benefit of the proxy approach.

### Consumer-Side Validation

Consumer wrapper validates on download:

1. **Envelope parsing**: Validate JSON structure and required fields.
2. **S3 fetch**: Download blob from bucket/key.
3. **Size check**: Verify downloaded size matches envelope `size`.
4. **Checksum verification**: Compute SHA256 of downloaded bytes, compare to `sha256`.
5. **Error handling**: Surface validation failures to application.
Failure Modes (Revised)

| Failure | Impact | Mitigation |
|---------|--------|------------|
| S3 upload fails mid-stream | No pointer produced | Proxy aborts multipart, returns error to client |
| Checksum mismatch | No pointer produced | Proxy aborts multipart, returns error with details |
| S3 upload succeeds but broker produce fails | Orphan object | Proxy logs orphan key; S3 lifecycle cleanup |
| Broker unavailable | Client retries | Standard Kafka retry behavior via proxy |
| Consumer S3 fetch fails | Application error | Consumer wrapper surfaces error, can retry |
| Consumer checksum mismatch | Data corruption detected | Consumer wrapper rejects, logs error |
| Proxy crash mid-upload | Partial multipart | S3 multipart abort timeout cleans up |

### Orphan Object Handling

The only scenario producing orphans: S3 upload completes but broker produce fails. Mitigation:

1. **Proxy tracks pending uploads**: Log S3 key before broker produce.
2. **Cleanup on produce failure**: Proxy can issue S3 DeleteObject.
3. **Fallback lifecycle**: S3 lifecycle policy deletes objects older than N days without matching Kafka record.

**Orphan rate expectation:** Very low—broker produce failures are rare after successful S3 upload.
Observability

### Proxy Metrics

```
# Upload handling
kafscale_lfs_proxy_requests_total{topic, status}
kafscale_lfs_proxy_upload_bytes_total{topic}
kafscale_lfs_proxy_upload_duration_seconds{topic, quantile}

# Validation
kafscale_lfs_proxy_checksum_validations_total{topic, result}
kafscale_lfs_proxy_size_rejections_total{topic}

# S3 operations
kafscale_lfs_proxy_s3_uploads_total{topic, status}
kafscale_lfs_proxy_s3_upload_duration_seconds{topic, quantile}
kafscale_lfs_proxy_s3_multipart_aborts_total{topic, reason}

# Errors
kafscale_lfs_proxy_errors_total{topic, error_type}
kafscale_lfs_proxy_orphan_objects_total{topic}

# Passthrough (non-LFS traffic)
kafscale_lfs_proxy_passthrough_requests_total{topic}
```

### Consumer Wrapper Metrics

```
kafscale_lfs_consumer_resolutions_total{topic, status}
kafscale_lfs_consumer_s3_fetch_duration_seconds{topic, quantile}
kafscale_lfs_consumer_checksum_failures_total{topic}
kafscale_lfs_consumer_bytes_resolved_total{topic}
```

---

## Extension: LFS Explode Processor

### Concept

A server-side processor that reads LFS pointer records and "explodes" them into resolved content on a target topic.

```
┌─────────────┐     ┌─────────────────────┐     ┌─────────────┐
│ LFS Topic   │────▶│ Processor (Explode) │────▶│ Target Topic│
│ (pointers)  │     │                     │     │ (resolved)  │
└─────────────┘     │    ┌───────────┐    │     └─────────────┘
                    │    │ S3 Fetch  │    │
                    │    └─────┬─────┘    │
                    │          │          │
                    └──────────┼──────────┘
                               ▼
                    ┌─────────────────────┐
                    │      S3 Bucket      │
                    └─────────────────────┘
```

### Use Cases

1. **Stream processing pipelines**: Kafka Streams/Flink jobs that need actual bytes
2. **Legacy consumer support**: Materialize resolved content for clients that can't use wrapper
3. **Data lake ingestion**: Resolve and forward to downstream systems
4. **Format conversion**: Explode + transform in one step

### Explode Processor Behavior

1. Consume from source topic (LFS pointers)
2. Detect LFS envelope in message value
3. Fetch blob from S3 using pointer metadata
4. Validate checksum
5. Produce resolved bytes to target topic
6. Preserve original key, headers (minus LFS markers)

### Configuration

```yaml
explode_processor:
  source_topic: "uploads-lfs"
  target_topic: "uploads-resolved"
  consumer_group: "lfs-explode-001"
  concurrency: 4
  max_blob_size: 104857600  # 100MB limit for explode
  s3_fetch_timeout: 30s
```

### Considerations

- **Memory pressure**: Explode processor holds full blob in memory briefly
- **Throughput**: Limited by S3 fetch latency and blob size
- **Target topic sizing**: Resolved topic will have large messages (configure broker accordingly)
- **Selective explode**: Could filter by header/size to only explode certain records

### Logging

- Proxy logs: Upload start/complete, validation failures, S3 errors, orphan objects.
- Consumer wrapper logs: Resolution failures, checksum mismatches, S3 fetch errors.
Security
Pre-signed URLs should be short-lived and scoped to a specific key/prefix.
Enforce per-topic prefix policies on the server side.
Credentials should never be embedded in pointer records.
Consider server-side KMS encryption via configured KMS key.
Compatibility and Migration
Classic Kafka clients receive the pointer record unchanged.
LFS SDKs can be introduced incrementally per topic.
Topic config can be toggled on/off without broker restarts.
Test Plan (Revised)

Validation should cover proxy behavior, consumer wrapper, and end-to-end flows.

### Unit Tests (Proxy)

1. **Envelope creation**
   - JSON envelope encode/decode round-trips
   - All required fields present
   - S3 key generation follows pattern

2. **Streaming upload**
   - Chunked streaming with configurable chunk size
   - SHA256 computed correctly across chunks
   - Multipart upload assembly

3. **Header detection**
   - `LFS_BLOB` header presence detection
   - Checksum extraction from header value
   - Passthrough for non-LFS requests

4. **Validation**
   - Size limit enforcement
   - Checksum mismatch rejection
   - S3 error handling

### Unit Tests (Consumer Wrapper)

1. **Envelope detection**
   - Fast path detection of `kfs_lfs` marker
   - Non-LFS passthrough
   - Malformed envelope handling

2. **S3 resolution**
   - Fetch blob from S3
   - Checksum validation on download
   - Size validation

### Integration / E2E Tests (MinIO)

1. **Happy path**
   - Start proxy + broker + MinIO
   - Classic Kafka producer sends 250MB file with `LFS_BLOB` header
   - Verify S3 object created with correct checksum
   - Verify pointer record in Kafka topic
   - Consumer wrapper resolves and returns original bytes

2. **Client-provided checksum**
   - Producer provides SHA256 in `LFS_BLOB` header
   - Proxy validates and succeeds
   - Proxy rejects on checksum mismatch

3. **Classic consumer**
   - Classic Kafka consumer (no wrapper) receives pointer record
   - Verify envelope is valid JSON with expected fields

4. **Failure cases**
   - S3 unavailable: proxy returns error, no Kafka record
   - Checksum mismatch: proxy rejects, no Kafka record
   - Broker unavailable after S3 upload: orphan logged
   - Consumer wrapper: S3 fetch failure surfaces error

5. **Metrics validation**
   - Proxy metrics increment on upload
   - Consumer wrapper metrics increment on resolution

### Performance / Load Tests

1. **Proxy throughput**
   - High concurrency uploads (100+ concurrent)
   - Measure latency percentiles
   - Verify streaming keeps memory stable

2. **Consumer wrapper throughput**
   - High concurrency resolutions
   - S3 fetch parallelization

3. **Passthrough overhead**
   - Measure latency added by proxy for non-LFS traffic
   - Target: <1ms added latency for passthrough
## Simplicity Assessment

### Does This Make Applications Easier?

| Stakeholder | Before LFS | After LFS (This Design) | Verdict |
|-------------|------------|-------------------------|---------|
| **Producer (normal blobs)** | Send to Kafka | Add 1 header, send to proxy | ✅ Minimal change |
| **Producer (large files)** | Can't do it | Use SDK for streaming | ✅ New capability |
| **Consumer** | Consume from Kafka | Use wrapper library | ⚠️ Requires SDK |
| **Broker** | Handle large messages | No changes | ✅ Zero impact |
| **Ops** | Tune broker memory | Deploy proxy, configure S3 | ⚠️ New component |

### Honest Trade-offs

| Benefit | Cost |
|---------|------|
| Broker unchanged | Proxy is new component to operate |
| Normal producers work | Proxy still buffers full Kafka requests |
| Large file support | SDK required for true streaming |
| Decoupled storage | S3 dependency, network hops |

### Simplicity Ranking

1. **Simplest:** Producer (normal) - just add header
2. **Simple:** Consumer - add wrapper library
3. **Moderate:** Producer (streaming) - use SDK
4. **Complex:** Operations - deploy/monitor proxy + S3

---

Open Questions (Simplified)

| Question | Decision Needed |
|----------|-----------------|
| Header name | `LFS_BLOB` vs `X-KafScale-LFS` |
| Auto-detect by size? | Start with header-only (explicit) |
| Consumer SDK languages | Go first, then Java |
| Streaming SDK protocol | HTTP chunked vs gRPC |
| Explode processor | Nice-to-have, defer to v2 |

---

Next Steps (Prioritized)

### Phase 1: MVP (Kafka-Compatible Mode)

| Task | Component | SDK Required |
|------|-----------|--------------|
| 1. Implement LFS Proxy | Proxy | - |
| 2. Header detection (`LFS_BLOB`) | Proxy | - |
| 3. S3 upload + envelope creation | Proxy | - |
| 4. Consumer wrapper (Go) | SDK | ✅ |
| 5. E2E test with MinIO | Test | - |

**Deliverable:** Normal Kafka producers work with header; consumers use Go wrapper.

### Phase 2: Streaming Mode

| Task | Component | SDK Required |
|------|-----------|--------------|
| 6. HTTP streaming endpoint in proxy | Proxy | - |
| 7. Streaming producer SDK (Go) | SDK | ✅ |
| 8. Consumer wrapper (Java) | SDK | ✅ |

**Deliverable:** Large file streaming support for Go producers.

### Phase 3: Enhancements

| Task | Component |
|------|-----------|
| 9. Explode processor | Optional |
| 10. Auto-detect by size | Optional |
| 11. Binary envelope format | Optimization |
| 12. Consumer SDK (Python, etc.) | Ecosystem |

---

## Summary: Impact on Each Layer

```
┌─────────────────────────────────────────────────────────────────┐
│ PRODUCER                                                         │
│  • Normal Kafka: Add 1 header (LFS_BLOB) ──────────── NO SDK    │
│  • Large file streaming: Use SDK ──────────────────── SDK       │
├─────────────────────────────────────────────────────────────────┤
│ PROXY (NEW COMPONENT)                                           │
│  • Intercept LFS requests                                        │
│  • Upload to S3                                                  │
│  • Forward pointer to broker                                     │
├─────────────────────────────────────────────────────────────────┤
│ BROKER                                                          │
│  • NO CHANGES ─────────────────────────────────────── ZERO      │
├─────────────────────────────────────────────────────────────────┤
│ CONSUMER                                                        │
│  • Use wrapper library to resolve pointers ────────── SDK       │
└─────────────────────────────────────────────────────────────────┘
```