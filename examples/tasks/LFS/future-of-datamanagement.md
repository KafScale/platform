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

# Future of Data Management: LFS + Open Formats

## Industry Context: The Dual-Storage Trend

Modern databases are adopting **dual-storage** architectures:

1. **Proprietary hot-tier** — Optimized internal formats for performance (the "product")
2. **Open cold-tier** — Standard formats like Apache Iceberg for interoperability (the "escape hatch")

This pattern delivers:
- **Vendor independence** — Data remains accessible via open standards
- **Analytics interoperability** — Spark, Flink, Trino, DuckDB can directly query
- **Cold-tier economics** — Object storage (S3) for cost efficiency

---

## How KafScale LFS Fits This Model

| Layer | KafScale Implementation | Dual-Storage Analogy |
|-------|------------------------|---------------------|
| **Hot Path** | Kafka protocol → LFS Proxy → S3 + Pointer Records | Proprietary format (optimized writes) |
| **Cold Path** | Processors → Iceberg/Parquet | Open format (analytics-ready) |
| **Blob Storage** | S3 with `kfs_lfs` envelope | Object-tier for large payloads |

### The Gap

LFS blobs stored in S3 are **opaque to Processors**. The Iceberg processor currently writes the raw `value` field, which for LFS records is just a JSON envelope pointer—not the actual payload.

---

## Architecture: LFS-Aware Processors

### Pipeline Design

Add an **LFS Resolver** step in the Processor pipeline:

```
S3 Segments (KafScale format)
    ↓
Decoder (Binary KafScale parsing)
    ↓
Record[] (with LFS envelope in value)
    ↓
★ LFS Resolver (NEW) ★  ← Fetches blob from S3, replaces value
    ↓
Record[] (with actual payload in value)
    ↓
Sink Writer (Iceberg / SQL / Custom)
```

### Implementation: `pkg/lfs/resolver.go`

```go
// pkg/lfs/resolver.go
package lfs

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

// Resolve detects LFS envelopes and fetches the blob
func (r *Resolver) Resolve(ctx context.Context, rec decoder.Record) (ResolvedRecord, error) {
    if !IsLfsEnvelope(rec.Value) {
        return ResolvedRecord{Original: rec, Payload: rec.Value}, nil
    }

    env, err := DecodeEnvelope(rec.Value)
    if err != nil {
        return ResolvedRecord{}, err
    }

    blob, err := r.s3Client.GetObject(ctx, env.Bucket, env.Key)
    if err != nil {
        return ResolvedRecord{}, err
    }

    if r.validate {
        if err := ValidateChecksum(blob, env.Sha256); err != nil {
            return ResolvedRecord{}, err
        }
    }

    return ResolvedRecord{
        Original:    rec,
        Payload:     blob,
        ContentType: env.ContentType,
        BlobSize:    int64(len(blob)),
        Checksum:    env.Sha256,
    }, nil
}
```

### Iceberg Processor Integration

In `iceberg-processor/internal/processor/processor.go`:

```go
type Processor struct {
    // ... existing fields
    lfsResolver *lfs.Resolver  // NEW: optional LFS resolution
}

func (p *Processor) processSegment(ctx context.Context, seg discovery.SegmentRef) error {
    records, err := p.decode.Decode(ctx, seg.SegmentKey, seg.IndexKey, seg.Topic, seg.Partition)
    if err != nil {
        return err
    }

    // NEW: Resolve LFS envelopes if enabled
    if p.lfsResolver != nil {
        records, err = p.resolveLfsRecords(ctx, records)
        if err != nil {
            return err
        }
    }

    // ... existing filtering and sink writing
}
```

### Extended Iceberg Schema for LFS Metadata

```go
// In iceberg.go - extend baseFields
var lfsFields = []iceberg.NestedField{
    {ID: 100, Name: "lfs_content_type", Type: iceberg.PrimitiveTypes.String, Required: false},
    {ID: 101, Name: "lfs_blob_size", Type: iceberg.PrimitiveTypes.Int64, Required: false},
    {ID: 102, Name: "lfs_checksum", Type: iceberg.PrimitiveTypes.String, Required: false},
    {ID: 103, Name: "lfs_bucket", Type: iceberg.PrimitiveTypes.String, Required: false},
    {ID: 104, Name: "lfs_key", Type: iceberg.PrimitiveTypes.String, Required: false},
}
```

---

## Processor Modes for Different Audiences

Different consumers need different projections. Configuration-driven mode system:

### Configuration (`config.yaml`)

```yaml
mappings:
  - topic: media-uploads
    table: analytics.media_events
    lfs:
      mode: resolve       # resolve | reference | skip | hybrid
      max_inline_size: 1048576  # 1MB - inline smaller blobs
      store_metadata: true      # add lfs_* columns
    schema:
      columns:
        - name: user_id
          type: long
        - name: media_type
          type: string
```

### LFS Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `resolve` | Fetch blob, write full content to `value` column | Analytics queries need raw data |
| `reference` | Keep envelope, add `lfs_*` metadata columns | Pointer-based access, lazy loading |
| `skip` | Exclude LFS records entirely | Non-blob analytics |
| `hybrid` | Inline small blobs, reference large ones | Cost-optimized storage |

---

## Beyond SQL: Non-Tabular Projections

### 1. Parquet File Sink (No Iceberg Catalog)

For direct Parquet files without Iceberg metadata—useful for ad-hoc analytics:

```go
// addons/processors/parquet-processor/internal/sink/parquet.go
type ParquetSink struct {
    s3Client    S3Writer
    bucket      string
    prefix      string
    compression parquet.Compression
}

func (s *ParquetSink) Write(ctx context.Context, records []Record) error {
    // Group by topic/partition, write to S3 as Parquet files
    // Path: s3://{bucket}/{prefix}/{topic}/{partition}/{timestamp}.parquet
}
```

### 2. Delta Lake Sink

For Databricks/Spark ecosystems:

```go
// addons/processors/delta-processor/internal/sink/delta.go
type DeltaSink struct {
    // Delta Lake transaction log writer
}
```

### 3. Object Storage Sink (Raw Blob Extraction)

For ML pipelines that need raw media files:

```go
// addons/processors/blob-processor/internal/sink/blob.go
type BlobSink struct {
    s3Client S3Writer
    bucket   string
}

func (s *BlobSink) Write(ctx context.Context, records []ResolvedRecord) error {
    for _, rec := range records {
        key := fmt.Sprintf("%s/%d/%d.bin", rec.Original.Topic, rec.Original.Partition, rec.Original.Offset)
        s.s3Client.PutObject(ctx, s.bucket, key, rec.Payload)
    }
    return nil
}
```

### 4. Webhook/HTTP Sink

For real-time integrations:

```go
type WebhookSink struct {
    endpoint string
    client   *http.Client
}
```

---

## Implementation Roadmap

### Phase 4: LFS-Aware Processors

| ID | Task | Output |
|----|------|--------|
| P4-001 | Create shared LFS resolver package | `pkg/lfs/resolver.go` |
| P4-002 | Add `IsLfsEnvelope()` detection | `pkg/lfs/envelope.go` |
| P4-003 | Add `DecodeEnvelope()` for JSON parsing | `pkg/lfs/envelope.go` |
| P4-004 | Create `pkg/lfs/s3reader.go` interface | `pkg/lfs/s3reader.go` |
| P4-005 | Integrate LFS resolver into Iceberg processor | `iceberg-processor/internal/processor/processor.go` |
| P4-006 | Add `lfs` config section to `config.yaml` schema | `iceberg-processor/internal/config/config.go` |
| P4-007 | Add `lfs_*` metadata columns to Iceberg schema | `iceberg-processor/internal/sink/iceberg.go` |
| P4-008 | Support `mode: resolve | reference | skip | hybrid` | `iceberg-processor/internal/processor/processor.go` |

### Phase 4 Metrics

| Metric | Description |
|--------|-------------|
| `processor_lfs_resolved_total` | Count of resolved blobs |
| `processor_lfs_resolved_bytes_total` | Total bytes fetched |
| `processor_lfs_resolution_errors_total` | Fetch failures |

### Phase 5: Alternative Projections

| ID | Task | Output |
|----|------|--------|
| P5-001 | Create Parquet file sink (no catalog) | `addons/processors/parquet-processor/` |
| P5-002 | Write Parquet files directly to S3 | Support partitioning by topic/date |
| P5-003 | Create blob extraction sink | `addons/processors/blob-processor/` |
| P5-004 | Extract LFS payloads to raw files in S3 | Support content-type based file extensions |

### E2E Testing

| ID | Task |
|----|------|
| T4-001 | Producer → LFS Proxy → Kafka → Processor → Iceberg table |
| T4-002 | Verify Spark/Trino can query resolved data |
| T4-003 | Test all LFS modes (resolve, reference, skip, hybrid) |

---

## Summary: Strategic Alignment

The LFS feature positions KafScale perfectly for the dual-storage trend:

| Trend Requirement | KafScale Solution |
|-------------------|-------------------|
| **Proprietary hot-tier** | Kafka protocol with LFS proxy (optimized write path) |
| **Open cold-tier** | Iceberg processor with LFS resolution |
| **Blob storage economics** | S3 storage for large payloads |
| **Analytics interop** | Iceberg tables queryable by Spark/Flink/Trino |
| **Non-SQL projections** | Pluggable sink architecture (Parquet, Delta, Blobs) |

The key integration point is the **LFS Resolver** component that bridges the opaque S3 pointers with downstream analytical formats. This allows KafScale to offer the same "dual-storage" value proposition that enterprise databases are adopting—without locking users into proprietary formats.

---

## References

- Apache Iceberg: https://iceberg.apache.org/
- Delta Lake: https://delta.io/
- Apache Parquet: https://parquet.apache.org/
- KafScale LFS Tasks: [tasks.md](./tasks.md)
