# LFS Proxy Traceability

This document describes the traceability feature for the KafScale LFS Proxy, enabling administrators to track blob operations, correlate Kafka pointers with S3 objects, and identify gaps in the system.

## Overview

The LFS Traceability system provides:

1. **Operation Tracking** - Every upload/download operation is recorded
2. **Blob Correlation** - Link Kafka topic pointers to S3 objects
3. **Real-time Monitoring** - Live event stream for operations
4. **Orphan Detection** - Identify S3 objects without Kafka pointers
5. **S3 Browser** - Admin interface to browse and verify storage

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         LFS Proxy                                        │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────────┐ │
│  │ HTTP Handler│───▶│ S3 Uploader │───▶│ LFS Ops Tracker             │ │
│  │ /lfs/produce│    └─────────────┘    │ (emits events to            │ │
│  │ /lfs/download    ┌─────────────┐    │  __lfs_ops_state topic)     │ │
│  └─────────────┘───▶│Kafka Producer    └─────────────────────────────┘ │
└─────────────────────┴─────────────┬─────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
            ┌───────────┐   ┌───────────┐   ┌───────────────┐
            │ S3 Bucket │   │User Topics│   │__lfs_ops_state│
            │ (blobs)   │   │(pointers) │   │(audit events) │
            └─────┬─────┘   └───────────┘   └───────┬───────┘
                  │                                 │
                  └────────────────┬────────────────┘
                                   ▼
                    ┌──────────────────────────────┐
                    │      KafScale Console        │
                    │  ┌────────────────────────┐  │
                    │  │ LFS Admin Dashboard    │  │
                    │  └────────────────────────┘  │
                    └──────────────────────────────┘
```

## Requirements

### REQ-TRACE-001: Operation Event Tracking ✅

**Priority:** P0
**Description:** The LFS Proxy MUST emit events for all blob operations to enable audit and debugging.

**Acceptance Criteria:**
- [x] Upload start events emitted with topic, s3_key, content_type, client_ip
- [x] Upload complete events emitted with size, sha256, kafka_offset, duration_ms
- [x] Upload failure events emitted with error_code, stage, partial_size
- [x] Download request events emitted with s3_key, mode, client_ip
- [x] Download complete events emitted with duration_ms, size
- [x] Events partitioned by topic name for query efficiency

### REQ-TRACE-002: Tracker Topic Configuration ✅

**Priority:** P0
**Description:** The tracker topic MUST be configurable and have appropriate retention.

**Acceptance Criteria:**
- [x] Topic name configurable via `KAFSCALE_LFS_TRACKER_TOPIC` (default: `__lfs_ops_state`)
- [x] Tracking can be disabled via `KAFSCALE_LFS_TRACKER_ENABLED`
- [ ] 7 days default retention for audit trail
- [x] Partitioned by topic name (3 partitions default)

### REQ-TRACE-003: Minimal Performance Impact ✅

**Priority:** P0
**Description:** Event emission MUST NOT significantly impact upload/download performance.

**Acceptance Criteria:**
- [x] Async event emission using buffered channel
- [x] Batch writes every 100ms or 100 events (configurable)
- [x] Circuit breaker disables tracking if topic unavailable
- [ ] Less than 5% throughput reduction measured in benchmarks

### REQ-TRACE-004: Orphan Detection (Partial)

**Priority:** P1
**Description:** The system MUST detect and track orphaned S3 objects.

**Acceptance Criteria:**
- [x] Orphan event emitted when S3 upload succeeds but Kafka produce fails
- [x] Orphan events include original request_id, topic, reason
- [ ] Console endpoint lists all detected orphans
- [x] Orphan count exposed in Prometheus metrics

### REQ-TRACE-005: Console Status API ✅

**Priority:** P1
**Description:** The Console MUST expose an LFS status endpoint with aggregate statistics.

**Acceptance Criteria:**
- [x] `/ui/api/lfs/status` returns total objects, bytes, 24h stats
- [x] Includes proxy count and S3 connection status
- [x] Lists topics with LFS objects
- [x] Returns orphan count and alerts

### REQ-TRACE-006: Object Browser API ✅

**Priority:** P1
**Description:** The Console MUST provide a paginated object listing API.

**Acceptance Criteria:**
- [x] `/ui/api/lfs/objects` returns paginated list
- [x] Filterable by topic, date range, size range
- [ ] Includes Kafka offset correlation
- [ ] Supports cursor-based pagination

### REQ-TRACE-007: Topic Statistics API ✅

**Priority:** P1
**Description:** The Console MUST provide per-topic LFS statistics.

**Acceptance Criteria:**
- [x] `/ui/api/lfs/topics` returns per-topic stats
- [x] Includes object count, total bytes, avg size
- [x] Includes 24h upload/error counts
- [x] Includes first/last object timestamps

### REQ-TRACE-008: Real-time Event Stream ✅

**Priority:** P2
**Description:** The Console MUST provide a real-time event stream.

**Acceptance Criteria:**
- [x] `/ui/api/lfs/events` returns SSE stream
- [x] Filterable by event type
- [x] Includes all tracker event fields
- [ ] Supports backpressure for slow clients

### REQ-TRACE-009: S3 Browser Integration ✅

**Priority:** P2
**Description:** The Console MUST allow administrators to browse S3 storage.

**Acceptance Criteria:**
- [x] `/ui/api/lfs/s3/browse` lists objects with prefix
- [x] `/ui/api/lfs/s3/presign` generates admin download URLs
- [ ] Objects enriched with tracker metadata when available
- [x] Supports directory-style navigation

### REQ-TRACE-010: Admin Dashboard UI ✅

**Priority:** P2
**Description:** The Console MUST provide a visual dashboard for LFS operations.

**Acceptance Criteria:**
- [x] Overview panel with key metrics
- [x] Searchable object browser table
- [x] Real-time events panel
- [x] Topic statistics cards
- [x] S3 browser component

## Event Schemas

### Base Event Structure

```json
{
  "event_type": "string",
  "event_id": "uuid",
  "timestamp": "RFC3339",
  "proxy_id": "string",
  "request_id": "string",
  "version": 1
}
```

### upload_started

```json
{
  "event_type": "upload_started",
  "topic": "video-uploads",
  "partition": 0,
  "s3_key": "default/video-uploads/lfs/2026/02/04/obj-uuid",
  "content_type": "video/mp4",
  "expected_size": 104857600,
  "client_ip": "10.0.0.5",
  "api_type": "http"
}
```

### upload_completed

```json
{
  "event_type": "upload_completed",
  "topic": "video-uploads",
  "partition": 0,
  "kafka_offset": 42,
  "s3_bucket": "kafscale-lfs",
  "s3_key": "default/video-uploads/lfs/2026/02/04/obj-uuid",
  "size": 104857600,
  "sha256": "abc123...",
  "checksum": "def456...",
  "checksum_alg": "sha256",
  "duration_ms": 5000,
  "content_type": "video/mp4"
}
```

### upload_failed

```json
{
  "event_type": "upload_failed",
  "topic": "video-uploads",
  "s3_key": "default/video-uploads/lfs/2026/02/04/obj-uuid",
  "error_code": "checksum_mismatch",
  "error_message": "expected abc, got def",
  "stage": "validation",
  "size_uploaded": 52428800,
  "duration_ms": 2500
}
```

### download_requested

```json
{
  "event_type": "download_requested",
  "s3_bucket": "kafscale-lfs",
  "s3_key": "default/video-uploads/lfs/2026/02/04/obj-uuid",
  "mode": "presign",
  "client_ip": "10.0.0.10",
  "ttl_seconds": 120
}
```

### download_completed

```json
{
  "event_type": "download_completed",
  "s3_key": "default/video-uploads/lfs/2026/02/04/obj-uuid",
  "mode": "presign",
  "duration_ms": 150,
  "size": 104857600
}
```

### orphan_detected

```json
{
  "event_type": "orphan_detected",
  "detection_source": "upload_failure",
  "topic": "video-uploads",
  "s3_bucket": "kafscale-lfs",
  "s3_key": "default/video-uploads/lfs/2026/02/04/obj-uuid",
  "size": 104857600,
  "original_request_id": "req-abc-123",
  "reason": "kafka_produce_failed"
}
```

## Configuration

### LFS Proxy Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_LFS_TRACKER_ENABLED` | `true` | Enable/disable event tracking |
| `KAFSCALE_LFS_TRACKER_TOPIC` | `__lfs_ops_state` | Tracker topic name |
| `KAFSCALE_LFS_TRACKER_BATCH_SIZE` | `100` | Events per batch |
| `KAFSCALE_LFS_TRACKER_FLUSH_MS` | `100` | Max flush interval (ms) |
| `KAFSCALE_LFS_TRACKER_ENSURE_TOPIC` | `true` | Create tracker topic on startup |
| `KAFSCALE_LFS_TRACKER_PARTITIONS` | `3` | Tracker topic partitions |
| `KAFSCALE_LFS_TRACKER_REPLICATION_FACTOR` | `1` | Tracker topic replication factor |

## Large Upload Profile (Beast Mode)

For 6+ GB uploads, use these settings:

| Env Var | Suggested Value | Reason |
| --- | --- | --- |
| `KAFSCALE_LFS_PROXY_MAX_BLOB_SIZE` | `7516192768` | Allows 7 GB uploads |
| `KAFSCALE_LFS_PROXY_CHUNK_SIZE` | `16777216` | 16 MB parts → fewer parts |
| `KAFSCALE_LFS_PROXY_HTTP_READ_TIMEOUT_SEC` | `1800` | Long upload streams |
| `KAFSCALE_LFS_PROXY_HTTP_WRITE_TIMEOUT_SEC` | `1800` | Long upload streams |
| `KAFSCALE_LFS_PROXY_HTTP_IDLE_TIMEOUT_SEC` | `120` | Keeps connections alive |

### Console Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_CONSOLE_LFS_ENABLED` | `true` | Enable LFS dashboard |
| `KAFSCALE_CONSOLE_LFS_S3_PRESIGN_TTL` | `300` | Admin presign URL TTL (seconds) |

## API Reference

### GET /ui/api/lfs/status

Returns overall LFS status and statistics.

**Response:**
```json
{
  "enabled": true,
  "proxy_count": 3,
  "s3_bucket": "kafscale-lfs",
  "topics_with_lfs": ["video-uploads", "medical-scans"],
  "stats": {
    "total_objects": 15420,
    "total_bytes": 1073741824000,
    "uploads_24h": 342,
    "downloads_24h": 1205,
    "errors_24h": 3,
    "orphans_pending": 2
  }
}
```

### GET /ui/api/lfs/objects

Returns paginated list of LFS objects.

**Query Parameters:**
- `topic` - Filter by topic name
- `limit` - Page size (default 50, max 200)
- `cursor` - Pagination cursor
- `date_from` - Start date (YYYY-MM-DD)
- `date_to` - End date (YYYY-MM-DD)

**Response:**
```json
{
  "objects": [
    {
      "s3_key": "default/video-uploads/lfs/2026/02/04/obj-uuid",
      "topic": "video-uploads",
      "partition": 0,
      "kafka_offset": 42,
      "size": 104857600,
      "sha256": "abc123...",
      "content_type": "video/mp4",
      "created_at": "2026-02-04T10:30:05Z",
      "proxy_id": "lfs-proxy-0"
    }
  ],
  "next_cursor": "yyy",
  "total_count": 1542
}
```

### GET /ui/api/lfs/topics

Returns per-topic LFS statistics.

**Response:**
```json
{
  "topics": [
    {
      "name": "video-uploads",
      "object_count": 5420,
      "total_bytes": 536870912000,
      "avg_object_size": 99012345,
      "uploads_24h": 120,
      "errors_24h": 1,
      "first_object": "2026-01-15T08:00:00Z",
      "last_object": "2026-02-04T16:30:00Z"
    }
  ]
}
```

### GET /ui/api/lfs/events (SSE)

Returns real-time event stream.

**Query Parameters:**
- `types` - Comma-separated event types to filter

**Response:** Server-Sent Events stream

### GET /ui/api/lfs/orphans

Returns list of detected orphan objects.

**Response:**
```json
{
  "orphans": [
    {
      "s3_key": "default/video-uploads/lfs/2026/02/04/obj-orphan",
      "s3_bucket": "kafscale-lfs",
      "topic": "video-uploads",
      "size": 52428800,
      "detected_at": "2026-02-04T12:00:00Z",
      "reason": "kafka_produce_failed",
      "age_hours": 5
    }
  ],
  "total_size": 52428800,
  "count": 1
}
```

### GET /ui/api/lfs/s3/browse

Browse S3 objects.

**Query Parameters:**
- `prefix` - S3 key prefix
- `delimiter` - Delimiter for directory-style listing (default `/`)
- `max_keys` - Max objects to return (default 100)

**Response:**
```json
{
  "objects": [
    {
      "key": "default/video-uploads/lfs/2026/02/04/obj-uuid",
      "size": 104857600,
      "last_modified": "2026-02-04T10:30:05Z",
      "etag": "abc123"
    }
  ],
  "common_prefixes": ["default/video-uploads/lfs/2026/02/05/"],
  "is_truncated": false
}
```

### POST /ui/api/lfs/s3/presign

Generate presigned URL for admin access.

**Request:**
```json
{
  "s3_key": "default/video-uploads/lfs/2026/02/04/obj-uuid",
  "ttl_seconds": 300
}
```

**Response:**
```json
{
  "url": "https://s3.amazonaws.com/kafscale-lfs/...",
  "expires_at": "2026-02-04T17:10:00Z"
}
```

## Testing

### Verify Event Emission

```bash
# Upload a file
curl -X POST http://localhost:8080/lfs/produce \
  -H "X-Kafka-Topic: test-uploads" \
  --data-binary @testfile.bin

# Consume tracker events
kcat -b localhost:9092 -C -t __lfs_ops_state -o beginning
```

### Verify Console APIs

```bash
# Get LFS status
curl http://localhost:3080/ui/api/lfs/status

# List objects
curl "http://localhost:3080/ui/api/lfs/objects?topic=test-uploads&limit=10"

# Get topic stats
curl http://localhost:3080/ui/api/lfs/topics
```

### Verify S3 Browser

```bash
# Browse S3
curl "http://localhost:3080/ui/api/lfs/s3/browse?prefix=default/test-uploads/"

# Generate presigned URL
curl -X POST http://localhost:3080/ui/api/lfs/s3/presign \
  -H "Content-Type: application/json" \
  -d '{"s3_key": "default/test-uploads/lfs/2026/02/04/obj-xxx", "ttl_seconds": 300}'
```
