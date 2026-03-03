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

# Broker Deep Dive: LFS Proxy + IDoc Exploder Integration

This document details broker-level integration concerns for LFS proxy and the IDoc exploder:
producer/consumer behavior, topic configuration, retention, and operational guidance.

## 1) Producer Behavior (LFS Proxy Front)

### Kafka Producers
- Producers send records with `LFS_BLOB` header to indicate large payloads.
- LFS proxy uploads the payload to S3 and replaces the record value with an LFS envelope JSON.
- The envelope is written to Kafka with the same topic/partition and key (if provided).

Recommended producer settings:
- **acks=all** for durability.
- **idempotence enabled** if supported.
- **batching** OK; LFS proxy rewrites per record.

### HTTP Producers
- `POST /lfs/produce` accepts raw blob and writes a single pointer record.
- Use `X-Kafka-Topic` and optional key/partition headers.

## 2) Consumer Groups (Exploder + Downstream)

### Exploder
- Consumes pointer topics (LFS envelopes), resolves blobs, emits structured topics.
- Design choice: **one consumer group per exploder deployment** to avoid duplicate explosion.
- If multiple exploders are needed, partition by topic or use distinct group IDs.

### Downstream Analytics
- Consumers read exploded topics (headers/items/partners/etc.).
- These are normal small JSON records, safe for standard consumer groups.

## 3) Topic Configuration

### Pointer Topics (raw IDoc LFS envelopes)
- **Retention**: should be long enough to allow re-processing (days to weeks).
- **Compaction**: avoid unless key semantics require it. Pointer topics are often append-only.
- **Replication factor**: align with broker durability target.
- **Cleanup policy**: `delete` (default) with time-based retention.

### Exploded Topics (semantic streams)
- **Retention**: depends on analytics window; can be shorter if materialized downstream.
- **Compaction**: optional if keys represent stable entities (e.g., doc number + segment).
- **Partitioning**: recommended by source topic + document ID to preserve ordering.

## 4) Retention Strategy

Recommended baseline:
- Pointer topics: 7–30 days (longer for audit/replay).
- Exploded topics: 7–14 days unless persisted downstream.
- S3 blobs: lifecycle policy aligned with pointer retention (or longer for audit).

## 5) Ordering & Exactly-Once Considerations

- LFS proxy does not change offsets or ordering; it preserves partition order.
- Exploder re-emits records based on the consumed order but may generate multiple output records per input.
- Exactly-once is not guaranteed end-to-end; use idempotent sinks or compaction if needed.

## 6) Topic Naming Patterns

Recommended pattern:
- Pointer topics: `idoc-raw.<type>`
- Exploded topics: `idoc.<stream>`

Example:
- `idoc-raw.orders05`
- `idoc.items`, `idoc.partners`, `idoc.dates`

## 7) Operational Policies

- Use TLS/SASL between LFS proxy and brokers where possible.
- Avoid public exposure of LFS proxy Kafka listener unless required.
- Enforce topic creation policies or pre-create topics with desired configs.
- Monitor:
  - `kafscale_lfs_proxy_requests_total`
  - `kafscale_lfs_proxy_orphan_objects_total`
  - Exploder throughput/error metrics (once added)

## 8) Failure Modes

- **Broker unavailable**: LFS proxy logs orphans; exploder retries via consumer offsets.
- **Exploder crash**: reprocessing occurs when consumer group resumes.
- **Checksum mismatch**: LFS proxy deletes blob; record is rejected.

## 9) Recommended Topic Defaults

| Topic Type | Retention | Cleanup | Compaction | Notes |
|---|---|---|---|---|
| Pointer | 7–30 days | delete | no | Replay window for raw XML |
| Exploded | 7–14 days | delete | optional | Materialize in lake/warehouse |

## 10) Next Integrations

- Add per-topic config support in Helm/Operator to set retention explicitly.
- Add exploder metrics for throughput/error rate and offsets.
- Provide a broker-side policy template for IDoc pipelines.
