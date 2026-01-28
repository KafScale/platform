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

# LFS (Large File Support) Design

## Summary

KafScale will support per-topic Large File Support (LFS) by storing large payloads in S3 and writing a small pointer record to Kafka. Classic Kafka consumers will receive the pointer record; KafScale LFS SDKs can resolve the pointer to stream the object directly from S3. Uploads are owned by clients using pre-signed S3 URLs or multipart uploads.

This design avoids streaming huge payloads through the Kafka protocol and broker memory, keeps Kafka compatibility, and enables a gradual migration path for clients.

## Goals

- Per-topic opt-in LFS with minimal impact on existing Kafka clients.
- Client-owned upload flow (S3 direct, broker does not proxy payload bytes).
- Pointer records that are small, stable, and extensible.
- LFS SDKs can transparently resolve pointer records into byte streams.
- Clear security posture (authz, S3 permissions, and pointer validation).
- Observability of LFS usage and failures.

## Non-goals (initial)

- Server-side chunking or streaming of large payloads through Kafka.
- Transparent delivery of raw file bytes to classic Kafka consumers.
- Server-managed upload flow (broker does not receive the file).
- S3 lifecycle automation beyond baseline retention defaults.

## Background: Why LFS

The current broker path reads full Kafka frames into memory and buffers record batches before S3 upload. Large message values can cause high memory pressure and slow the broker. LFS avoids this by moving payload bytes directly to S3 and keeping Kafka records small.

Today, large Kafka produce requests are not streamed end-to-end:
- The broker reads the full Kafka frame into memory.
- Produce parsing materializes record sets as `[]byte`.
- Record batches are copied and buffered before flush.
- Segments are built fully in memory before S3 upload.

So while KafScale may accept large messages, they are currently buffered in RAM multiple times. LFS is intended to remove this buffering for large payloads by moving the bytes off the Kafka path.

## High-Level Flow

1) Producer uploads the file directly to S3 (pre-signed URL or multipart upload).
2) Producer emits a Kafka record with a pointer to the uploaded object.
3) Classic consumers receive the pointer as regular Kafka data.
4) LFS-aware consumers resolve the pointer and stream the object from S3.

## Topic Configuration

LFS is enabled per topic (admin-configurable):

- `kafscale.lfs.enabled` (bool, default false)
- `kafscale.lfs.min_bytes` (int, default 8MB)  
  If a producer uses the LFS SDK and payload exceeds this threshold, upload to S3 and emit a pointer record.
- `kafscale.lfs.bucket` (string, optional override; defaults to cluster S3 bucket)
- `kafscale.lfs.prefix` (string, optional key prefix override)
- `kafscale.lfs.require_sdk` (bool, default false)  
  If true, reject oversized produce requests without valid LFS pointer.

Note: These configs are intended for the admin API. They may map to internal metadata stored in etcd.

## Pointer Record Schema (v1)

Pointer records are normal Kafka records (value bytes) with a small JSON or binary payload. We propose a versioned, compact JSON for readability and tooling:

```json
{
  "kfs_lfs": 1,
  "bucket": "kafscale-data",
  "key": "namespace/topic/lfs/2026/01/28/obj-<uuid>",
  "size": 262144000,
  "sha256": "<hex>",
  "content_type": "application/octet-stream",
  "created_at": "2026-01-28T12:34:56Z"
}
```

Schema notes:
- `kfs_lfs` is the version discriminator.
- `bucket` and `key` are mandatory.
- `size` and `sha256` are mandatory for validation and partial reads.
- `content_type` is optional.
- `created_at` is optional; if omitted, broker time is used for observability.

Alternative: a binary schema can be introduced later for smaller pointer records once the JSON format is stable.

## LFS SDK Behavior

Producer SDK:
- Checks topic LFS config (from metadata or out-of-band config).
- If payload size >= `kafscale.lfs.min_bytes`, upload to S3 and emit pointer record.
- If upload fails, either:
  - return error (default), or
  - optionally fallback to normal Kafka produce when size <= Kafka max, if allowed.

Consumer SDK:
- Detects pointer record via `kfs_lfs` field.
- Validates size + checksum on download.
- Streams from S3 using range reads where supported.
- Exposes original record metadata (topic, partition, offset, timestamp).

## S3 Object Layout

Default layout:
`s3://{bucket}/{namespace}/{topic}/lfs/{yyyy}/{mm}/{dd}/{uuid}`

Rationale:
- Keeps LFS objects scoped to topic and date for lifecycle/cleanup.
- UUID ensures uniqueness and avoids collisions.

## Upload Approach

Preferred: pre-signed S3 PUT or multipart upload.

API endpoints (broker or sidecar service):
- `POST /lfs/uploads` -> returns upload session (presigned URL or multipart parts)
- `POST /lfs/uploads/{id}/complete` -> validates and returns final object key

The upload service should not store object bytes. It only brokers credentials and writes metadata if needed.

## Validation and Safety

Broker-side (optional, recommended for `kafscale.lfs.require_sdk=true`):
- On produce, validate pointer records:
  - format + version
  - bucket/key allowed by policy
  - size <= configured max
  - object exists (HEAD) and size matches
- Reject produce if validation fails.

Client-side:
- Producer SDK computes hash and includes it in pointer record.
- Consumer SDK validates hash on download.

## Failure Modes

- Upload succeeds but pointer produce fails: orphan object in S3 (cleanup via lifecycle).
- Pointer produce succeeds but upload fails: consumer sees dangling pointer; SDK should surface error.
- S3 unavailable: SDK errors; broker should not accept pointer if validation is enabled.

## Observability

Metrics (broker or upload service):
- `kafscale_lfs_upload_requests_total`
- `kafscale_lfs_upload_bytes_total`
- `kafscale_lfs_pointer_records_total`
- `kafscale_lfs_validation_errors_total`
- `kafscale_lfs_head_latency_ms`

Logging:
- Log pointer validation failures with topic, partition, key, and error code.

## Security

- Pre-signed URLs should be short-lived and scoped to a specific key/prefix.
- Enforce per-topic prefix policies on the server side.
- Credentials should never be embedded in pointer records.
- Consider server-side KMS encryption via configured KMS key.

## Compatibility and Migration

- Classic Kafka clients receive the pointer record unchanged.
- LFS SDKs can be introduced incrementally per topic.
- Topic config can be toggled on/off without broker restarts.

## Test Plan

Validation should cover SDK behavior, broker validation, and end-to-end flows.

### Unit Tests (SDK)
- Pointer schema encode/decode round-trips (JSON + version).
- Upload flow:
  - generates correct S3 key/prefix per topic
  - handles multipart sessions
  - computes size + SHA256 correctly
- Consumer resolution:
  - detects pointer record
  - performs HEAD + range/GET
  - validates checksum on download

### Broker Validation Tests
- Pointer record acceptance when `kafscale.lfs.enabled=true`.
- Rejection when:
  - invalid schema/version
  - bucket/key outside topic prefix policy
  - size mismatch vs S3 object
  - object missing (HEAD 404)
- Behavior when `kafscale.lfs.require_sdk=true` and a large raw payload is produced.

### Integration / E2E Tests (MinIO)
1) Start broker + MinIO with LFS enabled for a topic.
2) SDK producer uploads 250MB file to MinIO and produces pointer.
3) Classic Kafka consumer reads pointer record unchanged.
4) SDK consumer resolves pointer and streams the full payload; verify SHA256.
5) Failure cases:
   - upload succeeds but pointer produce fails (ensure object left for lifecycle cleanup)
   - pointer produce succeeds but object missing (consumer error surfaced)
6) Validate broker metrics counters increment for pointer records and validation errors.

### Performance / Load Tests
- High concurrency uploads and pointer produces (throughput + latency).
- Broker memory profile under large LFS traffic (should remain stable).

## Open Questions

- Should pointer records be JSON or a compact binary schema?
- Should broker require object HEAD validation by default?
- Do we want per-topic max object size or global limit?
- Should producer SDK allow fallback to non-LFS produce for midsize payloads?

## Next Steps (post-review)

1) Finalize schema and API endpoints for upload flow.
2) Add topic config plumbing + validation hooks in broker.
3) Implement LFS SDK (producer + consumer) with S3 integration.
4) Add metrics, tests, and sample guides in docs.
