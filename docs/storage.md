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

# Storage Layer Plan

Milestone 2 introduces the storage package that translates Kafka record batches into immutable segments living in S3. This document outlines the Go APIs we will implement so downstream milestones can rely on stable interfaces.

## Package Structure

```
pkg/storage/
├── segment.go        // Segment metadata + serialization helpers
├── writer.go         // SegmentWriter (produce path)
├── reader.go         // SegmentReader (fetch path)
├── buffer.go         // Write buffer that batches messages prior to flush
├── index.go          // Sparse index representation
├── s3client.go       // Thin wrapper over AWS SDK for uploads/downloads
└── log.go            // Partition log orchestration (ties everything together)
```

## Core Interfaces

```go
// SegmentWriter accumulates batches until thresholds are met, then finalizes segment/index bytes.
type SegmentWriter interface {
    Append(batch *protocol.RecordBatch) error
    ShouldFlush() bool
    Finalize(ctx context.Context) (*SegmentArtifact, error)
}

// SegmentReader serves fetch requests by locating the right segment + offset.
type SegmentReader interface {
    Fetch(ctx context.Context, topic string, partition int32, offset int64, maxBytes int32) (*FetchResult, error)
    WarmSegment(ctx context.Context, topic string, partition int32, baseOffset int64) error // triggers cache read-ahead
}

// Log is owned per-partition by the broker. It coordinates the writer/reader/cache and etcd metadata.
type Log interface {
    Append(ctx context.Context, req *protocol.ProduceRequest) (*ProduceResult, error)
    Fetch(ctx context.Context, req *protocol.FetchRequest) (*protocol.FetchResponse, error)
    HighWatermark() int64
}

// S3Client hides AWS SDK plumbing and enforces retries + metrics.
type S3Client interface {
    UploadSegment(ctx context.Context, key string, payload []byte) error
    DownloadSegment(ctx context.Context, key string, rng *ByteRange) ([]byte, error)
}
```

### Supporting Types

- `SegmentArtifact`: contains serialized `segment.kfs`, `segment.index`, and metadata (base offset, first/last timestamps, message count, CRC32).
- `FetchResult`: includes marshalled record batches to return over Kafka plus new cache hints.
- `ByteRange`: start/end offsets used for HTTP range reads.

## Flush and Buffering Policy

- `buffer.go` exposes a `WriteBuffer` struct with thresholds for size, message count, and time since last flush. The broker’s partition writer goroutine feeds batches into the buffer, and when `ShouldFlush` returns true it hands the accumulated batches to a `SegmentWriter`.
- `SegmentWriter` calculates CRC, builds headers/footers, and emits both the segment and sparse index (every N bytes as configured).

## Cache Interaction & Read-Ahead

- `log.go` (acting as both writer and reader) talks to `pkg/cache` via:

```go
type SegmentCache interface {
    GetSegment(topic string, partition int32, baseOffset int64) ([]byte, bool)
    PutSegment(topic string, partition int32, baseOffset int64, data []byte)
}
```

- When a segment flushes or a fetch loads a segment, the log optionally prefetches the next `ReadAheadSegments` segments from S3 and seeds the cache so consumers hit memory rather than S3 on sequential reads.
- `PartitionLogConfig` now includes `ReadAheadSegments` and `CacheEnabled` flags so operators can tune prefetch depth per topic/partition profile.

## Observability Hooks

- All key operations accept contexts for cancellation + tracing.
- We will expose Prometheus metrics in storage (segment flush latency, S3 bytes uploaded, cache hit rates). Instrumentation entry points will live in `pkg/storage/metrics.go`.

## Next Steps

1. Implement `SegmentWriter` + serialization (Milestone 2 core).
2. Add the S3 client wrapper with retry + metrics.
3. Flesh out the per-partition log manager that wires in metadata updates and the control plane hooks needed for draining.
