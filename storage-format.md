---
layout: doc
title: Storage Format
description: Segment file format, index layout, cache architecture, and S3 key structure for KafScale storage.
---

# Storage Format

KafScale stores all message data in S3 as immutable segment files. This page covers the binary formats, caching strategy, and retention configuration.

## S3 key layout

```
s3://{bucket}/{namespace}/{topic}/{partition}/segment-{base_offset}.kfs
s3://{bucket}/{namespace}/{topic}/{partition}/segment-{base_offset}.index
```

Example:

```
s3://kafscale-data/production/orders/0/segment-00000000000000000000.kfs
s3://kafscale-data/production/orders/0/segment-00000000000000000000.index
```

The 20-digit zero-padded offset ensures lexicographic sorting matches offset order.

## Segment file format

Each `.kfs` segment is a self-contained file with header, message batches, and footer.

<div class="format-card">
  <div class="format-row header">
    <span>Field</span>
    <span>Size</span>
    <span>Description</span>
  </div>
</div>

### Segment header (32 bytes)

<div class="format-card">
  <div class="format-row"><span>Magic number</span><span>4 bytes</span><span><code>0x4B414653</code> ("KAFS")</span></div>
  <div class="format-row"><span>Version</span><span>2 bytes</span><span>Format version (1)</span></div>
  <div class="format-row"><span>Flags</span><span>2 bytes</span><span>Compression codec, etc.</span></div>
  <div class="format-row"><span>Base offset</span><span>8 bytes</span><span>First offset in segment</span></div>
  <div class="format-row"><span>Message count</span><span>4 bytes</span><span>Number of messages</span></div>
  <div class="format-row"><span>Created timestamp</span><span>8 bytes</span><span>Unix milliseconds</span></div>
  <div class="format-row"><span>Reserved</span><span>4 bytes</span><span>Future use</span></div>
</div>

### Segment body (variable)

<div class="format-card">
  <div class="format-row"><span>Message batch 1</span><span>variable</span><span>Kafka RecordBatch format</span></div>
  <div class="format-row"><span>Message batch 2</span><span>variable</span><span>Kafka RecordBatch format</span></div>
  <div class="format-row"><span>...</span><span></span><span>More batches until segment sealed</span></div>
</div>

### Segment footer (16 bytes)

<div class="format-card">
  <div class="format-row"><span>CRC32</span><span>4 bytes</span><span>Checksum of all batches</span></div>
  <div class="format-row"><span>Last offset</span><span>8 bytes</span><span>Last offset in segment</span></div>
  <div class="format-row"><span>Footer magic</span><span>4 bytes</span><span><code>0x454E4421</code> ("END!")</span></div>
</div>

## Message batch format

Batches are Kafka-compatible (magic byte 2) for client interoperability.

### Batch header (49 bytes)

<div class="format-card">
  <div class="format-row"><span>Base offset</span><span>8 bytes</span><span>First offset in batch</span></div>
  <div class="format-row"><span>Batch length</span><span>4 bytes</span><span>Total bytes in batch</span></div>
  <div class="format-row"><span>Partition leader epoch</span><span>4 bytes</span><span>Leader epoch</span></div>
  <div class="format-row"><span>Magic</span><span>1 byte</span><span><code>2</code> (Kafka v2 format)</span></div>
  <div class="format-row"><span>CRC32</span><span>4 bytes</span><span>Checksum of batch</span></div>
  <div class="format-row"><span>Attributes</span><span>2 bytes</span><span>Compression, timestamp type</span></div>
  <div class="format-row"><span>Last offset delta</span><span>4 bytes</span><span>Last record offset - base</span></div>
  <div class="format-row"><span>First timestamp</span><span>8 bytes</span><span>Timestamp of first record</span></div>
  <div class="format-row"><span>Max timestamp</span><span>8 bytes</span><span>Max timestamp in batch</span></div>
  <div class="format-row"><span>Producer ID</span><span>8 bytes</span><span><code>-1</code> (no idempotence)</span></div>
  <div class="format-row"><span>Producer epoch</span><span>2 bytes</span><span><code>-1</code></span></div>
  <div class="format-row"><span>Base sequence</span><span>4 bytes</span><span><code>-1</code></span></div>
  <div class="format-row"><span>Record count</span><span>4 bytes</span><span>Number of records</span></div>
</div>

### Individual record format

Each record within a batch uses varint encoding for compactness.

<div class="format-card">
  <div class="format-row"><span>Length</span><span>varint</span><span>Total record size</span></div>
  <div class="format-row"><span>Attributes</span><span>1 byte</span><span>Unused (0)</span></div>
  <div class="format-row"><span>Timestamp delta</span><span>varint</span><span>Delta from batch first timestamp</span></div>
  <div class="format-row"><span>Offset delta</span><span>varint</span><span>Delta from batch base offset</span></div>
  <div class="format-row"><span>Key length</span><span>varint</span><span><code>-1</code> for null, else byte count</span></div>
  <div class="format-row"><span>Key</span><span>bytes</span><span>Message key (optional)</span></div>
  <div class="format-row"><span>Value length</span><span>varint</span><span>Message value byte count</span></div>
  <div class="format-row"><span>Value</span><span>bytes</span><span>Message payload</span></div>
  <div class="format-row"><span>Headers count</span><span>varint</span><span>Number of headers</span></div>
  <div class="format-row"><span>Headers</span><span>bytes</span><span>Key-value header pairs</span></div>
</div>

## Index file format

Sparse index for fast offset-to-position lookups. One entry per N messages.

### Index header (16 bytes)

<div class="format-card">
  <div class="format-row"><span>Magic</span><span>4 bytes</span><span><code>0x494458</code> ("IDX")</span></div>
  <div class="format-row"><span>Version</span><span>2 bytes</span><span><code>1</code></span></div>
  <div class="format-row"><span>Entry count</span><span>4 bytes</span><span>Number of index entries</span></div>
  <div class="format-row"><span>Interval</span><span>4 bytes</span><span>Messages between entries</span></div>
  <div class="format-row"><span>Reserved</span><span>2 bytes</span><span>Future use</span></div>
</div>

### Index entries (12 bytes each)

<div class="format-card">
  <div class="format-row"><span>Offset</span><span>8 bytes</span><span>Message offset</span></div>
  <div class="format-row"><span>Position</span><span>4 bytes</span><span>Byte position in segment file</span></div>
</div>

To locate offset N: binary search index entries, then scan forward from nearest position.

## Cache architecture

<div class="diagram">
  <div class="diagram-label">Multi-layer cache</div>
  <svg viewBox="0 0 700 200" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="KafScale cache architecture">
    <defs>
      <marker id="ac" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="var(--diagram-stroke)"/></marker>
    </defs>

    <!-- L1 Cache -->
    <rect x="20" y="30" width="200" height="90" rx="10" fill="rgba(56, 189, 248, 0.15)" stroke="#38bdf8" stroke-width="1.5"/>
    <text x="120" y="55" font-size="12" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">L1: Hot Segment Cache</text>
    <text x="120" y="75" font-size="10" fill="var(--diagram-label)" text-anchor="middle">Last N segments per partition</text>
    <text x="120" y="92" font-size="10" fill="var(--diagram-label)" text-anchor="middle">LRU eviction · 1-4 GB</text>
    <text x="120" y="108" font-size="9" fill="#38bdf8" text-anchor="middle">&lt;1ms latency</text>

    <!-- L2 Cache -->
    <rect x="260" y="30" width="200" height="90" rx="10" fill="rgba(52, 211, 153, 0.15)" stroke="#34d399" stroke-width="1.5"/>
    <text x="360" y="55" font-size="12" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">L2: Index Cache</text>
    <text x="360" y="75" font-size="10" fill="var(--diagram-label)" text-anchor="middle">All indexes for assigned partitions</text>
    <text x="360" y="92" font-size="10" fill="var(--diagram-label)" text-anchor="middle">Refreshed on segment roll · 100-500 MB</text>
    <text x="360" y="108" font-size="9" fill="#34d399" text-anchor="middle">&lt;1ms latency</text>

    <!-- S3 -->
    <rect x="500" y="30" width="180" height="90" rx="12" fill="rgba(255, 179, 71, 0.12)" stroke="#ffb347" stroke-width="2"/>
    <circle cx="590" cy="65" r="22" fill="rgba(255, 179, 71, 0.3)" stroke="#ffb347" stroke-width="1"/>
    <text x="590" y="70" font-size="12" font-weight="700" fill="#ffb347" text-anchor="middle">S3</text>
    <text x="590" y="100" font-size="10" fill="var(--diagram-label)" text-anchor="middle">Source of truth · ∞ capacity</text>
    <text x="590" y="115" font-size="9" fill="#ffb347" text-anchor="middle">50-100ms latency</text>

    <!-- Arrows -->
    <path d="M220,75 L255,75" stroke="var(--diagram-stroke)" stroke-width="1.5" fill="none" marker-end="url(#ac)"/>
    <text x="237" y="68" font-size="9" fill="var(--diagram-label)">miss</text>

    <path d="M460,75 L495,75" stroke="var(--diagram-stroke)" stroke-width="1.5" fill="none" marker-end="url(#ac)"/>
    <text x="477" y="68" font-size="9" fill="var(--diagram-label)">miss</text>

    <!-- Flow description -->
    <text x="350" y="175" font-size="10" fill="var(--diagram-label)" text-anchor="middle" font-style="italic">
      Check L1 → Check L2 → Fetch from S3 → Populate caches → Return to client
    </text>
  </svg>
</div>

### Cache configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_CACHE_SIZE` | `1GB` | L1 hot segment cache size |
| `KAFSCALE_INDEX_CACHE_SIZE` | `256MB` | L2 index cache size |
| `KAFSCALE_READAHEAD_SEGMENTS` | `2` | Segments to prefetch |

## Flush triggers

Segments are sealed and flushed to S3 when **any** condition is met:

<div class="format-card">
  <div class="format-row header">
    <span>Trigger</span>
    <span>Default</span>
    <span>Variable</span>
  </div>
  <div class="format-row">
    <span>Buffer size threshold</span>
    <span>4 MB</span>
    <span><code>KAFSCALE_SEGMENT_BYTES</code></span>
  </div>
  <div class="format-row">
    <span>Time since last flush</span>
    <span>500 ms</span>
    <span><code>KAFSCALE_FLUSH_INTERVAL_MS</code></span>
  </div>
  <div class="format-row">
    <span>Explicit flush request</span>
    <span>—</span>
    <span>Admin API or graceful shutdown</span>
  </div>
</div>

### Flush sequence

1. **Seal** current buffer (no more writes accepted)
2. **Compress** batches (Snappy by default)
3. **Build** sparse index file
4. **Upload** segment + index to S3 (both must succeed)
5. **Update** etcd with new segment metadata
6. **Ack** waiting producers (if `acks=all`)
7. **Clear** flushed data from buffer

## S3 lifecycle configuration

Use bucket lifecycle rules to automatically expire old segments. Align with your topic retention settings.

### Example: 7-day retention

```json
{
  "Rules": [
    {
      "ID": "kafscale-retention-7d",
      "Filter": {
        "Prefix": "production/"
      },
      "Status": "Enabled",
      "Expiration": {
        "Days": 7
      }
    }
  ]
}
```

### AWS CLI setup

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket kafscale-data \
  --lifecycle-configuration file://lifecycle.json
```

### Terraform example

```hcl
resource "aws_s3_bucket_lifecycle_configuration" "kafscale" {
  bucket = aws_s3_bucket.kafscale_data.id

  rule {
    id     = "kafscale-retention"
    status = "Enabled"

    filter {
      prefix = "production/"
    }

    expiration {
      days = 7
    }
  }
}
```

### Per-topic retention

For different retention per topic, use prefix-based rules:

```json
{
  "Rules": [
    {
      "ID": "logs-1d",
      "Filter": { "Prefix": "production/logs/" },
      "Status": "Enabled",
      "Expiration": { "Days": 1 }
    },
    {
      "ID": "events-30d",
      "Filter": { "Prefix": "production/events/" },
      "Status": "Enabled",
      "Expiration": { "Days": 30 }
    },
    {
      "ID": "default-7d",
      "Filter": { "Prefix": "production/" },
      "Status": "Enabled",
      "Expiration": { "Days": 7 }
    }
  ]
}
```

Rules are evaluated in order; most specific prefix wins.

## Compression

KafScale supports batch-level compression using Kafka-compatible codecs.

| Codec | ID | Notes |
|-------|-----|-------|
| None | 0 | No compression |
| Snappy | 1 | **Default** — fast, moderate ratio |
| LZ4 | 3 | Faster decompression |
| ZSTD | 4 | Best ratio, slower |

Set via `KAFSCALE_COMPRESSION_CODEC` or per-topic in CRD:

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafscaleTopic
metadata:
  name: logs
spec:
  partitions: 6
  retention: 24h
  compression: zstd  # Better ratio for logs
```