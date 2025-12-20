---
layout: doc
title: Configuration Reference
description: Broker, S3, etcd, consumer group, and operator configuration for KafScale.
---

# Configuration Reference

## Broker configuration

Key broker settings (environment variables):

- `KAFSCALE_SEGMENT_BYTES` – Segment flush threshold in bytes (default `4194304`).
- `KAFSCALE_FLUSH_INTERVAL_MS` – Flush interval (default `500`).
- `KAFSCALE_CACHE_BYTES` – Broker cache size in bytes.
- `KAFSCALE_READAHEAD_SEGMENTS` – Segment read-ahead count.
- `KAFSCALE_AUTO_CREATE_TOPICS` – Auto-create topics (`true/false`).
- `KAFSCALE_AUTO_CREATE_PARTITIONS` – Partition count for auto-created topics.
- `KAFSCALE_THROUGHPUT_WINDOW_SEC` – Throughput window seconds.

## S3 configuration

- `KAFSCALE_S3_BUCKET` – S3 bucket for segments/snapshots.
- `KAFSCALE_S3_REGION` – S3 region.
- `KAFSCALE_S3_ENDPOINT` – S3 endpoint override.
- `KAFSCALE_S3_PATH_STYLE` – Path-style addressing (`true/false`).
- `KAFSCALE_S3_KMS_ARN` – KMS key ARN for SSE-KMS.
- `KAFSCALE_S3_ACCESS_KEY`, `KAFSCALE_S3_SECRET_KEY`, `KAFSCALE_S3_SESSION_TOKEN` – Credentials.
- `KAFSCALE_S3_LATENCY_WARN_MS`, `KAFSCALE_S3_LATENCY_CRIT_MS` – Latency thresholds.
- `KAFSCALE_S3_ERROR_RATE_WARN`, `KAFSCALE_S3_ERROR_RATE_CRIT` – Error-rate thresholds.
- `KAFSCALE_S3_HEALTH_WINDOW_SEC` – S3 health sampling window.

## etcd configuration

- `KAFSCALE_ETCD_ENDPOINTS` – etcd endpoints for metadata/offsets.
- `KAFSCALE_ETCD_USERNAME`, `KAFSCALE_ETCD_PASSWORD` – etcd basic auth.

## Consumer group settings

Session timeout and heartbeat intervals are negotiated by Kafka clients, following the protocol defaults. Broker identity and startup behavior are controlled via:

- `KAFSCALE_BROKER_ID` – Node ID for group membership.
- `KAFSCALE_STARTUP_TIMEOUT_SEC` – Broker startup timeout.

## Operator configuration

- `KAFSCALE_OPERATOR_ETCD_ENDPOINTS` – Use external etcd instead of managed etcd.
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET` – Snapshot bucket override.
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PREFIX` – Snapshot prefix.
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SCHEDULE` – Cron schedule.
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT` – S3 endpoint override.
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_STALE_AFTER_SEC` – Staleness threshold.

## Security posture

Security details live in [Security](/security/). TLS and auth are operator-configured; brokers default to plaintext until TLS env vars are set.
