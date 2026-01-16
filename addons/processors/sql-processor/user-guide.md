<!--
Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

# KAFSQL User Guide

This guide targets operators and platform engineers deploying KAFSQL. It
explains configuration, runtime behavior, and operational guidance. For
implementation details, see `developer.md`.

## What It Does

- Reads completed KFS segments directly from S3.
- Exposes the Postgres wire protocol for JDBC/BI compatibility.
- Supports ad-hoc SQL with Kafka-native extensions (LAST/TAIL/SCAN FULL).
- Provides bounded two-topic joins for S3-native enrichment queries.

## Feature Highlights

- Stateless executors and S3-native reads (no Kafka broker dependency).
- Kafka semantics exposed as SQL columns.
- Query governance and cost visibility.
- Optional proxy for external access and ACL enforcement.

## What It Does Not Do

- Continuous streaming queries or materialized views.
- Full SQL feature parity or multi-join chains.
- Writes or updates.

## Prerequisites

- Access to the S3 bucket containing KFS segments.
- Optional: etcd endpoints for topic metadata (or rely on S3 discovery).
- Optional: schema definitions in YAML for typed columns.
- Optional: proxy deployment for external access.

## Segment Layout

```
s3://{bucket}/{namespace}/{topic}/{partition}/segment-{base_offset}.kfs
s3://{bucket}/{namespace}/{topic}/{partition}/segment-{base_offset}.index
```

KFS segment formats are defined in `kafscale-spec.md`.

## Configuration Overview

KAFSQL reads a YAML config file mounted into the container.

Required fields:
- `s3.bucket`

Common fields:
```yaml
s3:
  bucket: kafscale-data
  namespace: production
  endpoint: ""
  region: us-east-1
  path_style: false

metadata:
  discovery: etcd
  etcd:
    endpoints:
      - http://etcd.kafscale.svc.cluster.local:2379
  snapshot:
    key: /kafscale/metadata/snapshot
    ttl_seconds: 60

server:
  listen: ":5432"
  max_connections: 100
  metrics_listen: ":9090"

discovery_cache:
  ttl_seconds: 60
  max_entries: 10000

query:
  default_limit: 1000
  require_time_bound: true
  max_unbounded_scan: 1000
  max_scan_bytes: 10737418240
  max_scan_segments: 10000
  max_rows: 100000
  timeout_seconds: 30
  max_concurrent: 20
  queue_size: 50
  queue_timeout_seconds: 10

result_cache:
  ttl_seconds: 30
  max_entries: 100
  max_rows: 10000

discovery_manifest:
  enabled: false
  key: manifest.json
  ttl_seconds: 60
  build_interval_seconds: 0
  build_max_segments: 0
  build_max_bytes: 0
  build_lease_ttl_seconds: 120

time_index:
  enabled: false
  key_suffix: .kfst
  build_max_segments: 0
  build_max_bytes: 0
  build_lease_ttl_seconds: 120

proxy:
  listen: ":5432"
  upstreams:
    - kafsql.kafka.svc.cluster.local:5432
  max_connections: 200
  cache_ttl_seconds: 10
  cache_max_entries: 1000
  acl:
    allow: []
    deny: []
```

For MinIO or any non-AWS S3 endpoint:
```yaml
s3:
  bucket: kafscale-snapshots
  namespace: kafscale-demo
  endpoint: http://minio.kafscale-demo.svc.cluster.local:9000
  region: us-east-1
  path_style: true
```

## Query Model

Implicit columns:
- `_topic`, `_partition`, `_offset`, `_ts`
- `_key`, `_value`, `_headers`, `_segment`

Extensions:
- `TAIL <n>`: read last N records.
- `LAST <duration>`: time-bounded scan.
- `SCAN FULL`: explicit unbounded scan.

System introspection:
- `SHOW TOPICS;`
- `SHOW PARTITIONS FROM <topic>;`
- `DESCRIBE <topic>;`
- `EXPLAIN SELECT ...;`

Supported SQL subset:
- `SELECT` with projections and aliases.
- `WHERE` on `_partition`, `_offset`, `_ts`, `_key`.
- `ORDER BY _ts` (ASC/DESC).
- Aggregates: `COUNT`, `MIN`, `MAX`, `SUM`, `AVG`.
- `GROUP BY` on explicit columns.
- JSON helpers in SELECT: `json_value`, `json_query`, `json_exists`.

Join constraints (v0.6):
- Two topics only.
- Key equality or JSON field equality.
- Requires `WITHIN <duration>` and `LAST <duration>`.
- Left or inner joins only.

Join output naming:
- Right-side implicit columns are prefixed with `_right_` unless aliased.

## Query Governance

Queries without time bounds are rejected by default. Use:
- `LAST <duration>`
- `TAIL <n>`
- `SCAN FULL` with an explicit LIMIT

Guardrails:
- `query.max_scan_bytes` and `query.max_scan_segments` cap scan size.
- `query.max_rows` caps returned rows.
- `query.timeout_seconds` cancels slow queries.
- `query.max_concurrent` + queue settings cap concurrent work.

EXPLAIN provides best-effort estimates using segment sizes and any available
manifest/time index metadata.

## Schema-on-Read Columns

If you define schemas per topic, KAFSQL exposes typed columns alongside the
implicit Kafka columns. Example:

```yaml
metadata:
  topics:
    - name: orders
      partitions: [0, 1]
      schema:
        columns:
          - name: order_id
            type: string
            path: $.order_id
          - name: amount
            type: double
            path: $.amount
```

## Result Caching

Result caching is enabled when `result_cache.*` is set and applies to SELECT
queries with explicit time bounds. Cache is skipped for:
- `TAIL`
- `SCAN FULL`
- unbounded queries without time windows

Each cache entry stores rows and schema metadata up to `result_cache.max_rows`.

## Example Queries

```
SELECT * FROM orders TAIL 100;

SELECT * FROM orders LAST 15m;

SELECT _partition, count(*), max(_ts) AS latest
FROM orders LAST 5m
GROUP BY _partition;

SELECT o._key, o._value, p._value
FROM orders o
JOIN payments p ON o._key = p._key
WITHIN 10m
LAST 1h;

EXPLAIN SELECT * FROM orders LAST 24h;
```

## Client Compatibility

- Simple query protocol is supported by all drivers.
- Extended protocol is supported by the executor, but the proxy only accepts
  simple query messages.
- `pg_catalog` and `information_schema` are populated for BI tools.

## Proxy Access (Optional)

The proxy is used for external access:
- TLS termination (via ingress or mesh).
- Auth via external gateway or mesh plus topic ACL enforcement.
- Query auditing.

Deploy proxy only when you need external access. In-cluster use can skip it.

Proxy ACLs are applied before any S3 access. If the proxy cannot authorize a
query, it returns an error without forwarding.

## External Metadata (Phase 4)

To reduce S3 list storms and improve time pruning, KAFSQL can use:
- Partition manifests (`manifest.json`).
- Time index sidecars (`.kfst`) for min/max ts and offsets.

Use the backfill tool to generate or refresh these:
```
./kafsql-backfill -config /config/config.yaml -mode all
./kafsql-backfill -config /config/config.yaml -mode manifest
./kafsql-backfill -config /config/config.yaml -mode time-index -interval 5m
```

## Metrics

Prometheus metrics are exposed on `server.metrics_listen` (default `:9090`):
- Query totals/durations/rows/bytes.
- S3 request counts and durations.
- Queue depth and rejection counters.

## Logs and Audit

Executor logs include query duration, rows, segments, bytes, and a trimmed query.
Proxy audit logs include allow/deny decisions, topics, and client IP.

## Helm Deployment

Use the Helm chart values as a base:
- `addons/processors/sql-processor/deploy/helm/sql-processor/values.yaml`
- `addons/processors/sql-processor/deploy/helm/sql-processor/config/config.yaml`

The chart mounts the config file and deploys optional proxy pods if enabled.

## Local Run

```
make build
./bin/sql-processor -config config/config.yaml
```

Connect with:
```
psql -h localhost -p 5432 -d kfs
```
