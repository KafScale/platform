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

# Iceberg Processor User Guide

This guide is for operators and platform engineers deploying the Iceberg
processor in Kubernetes. It focuses on configuration, behavior, and operations.
For implementation details and code changes, see `developer.md`.

## What It Does

- Reads completed KafScale segments from S3.
- Decodes records and writes them to Iceberg tables (append-only).
- Tracks offsets with a lease-per-partition model (at-least-once).
- Optional JSON schema validation and schema-driven columns.

## Feature Highlights

- Storage-native processing (no Kafka protocol or brokers required).
- Iceberg REST catalog support with auto-create tables.
- Mapping-driven or registry-driven columns with schema evolution.
- Lease-based offsets with replay-safe at-least-once semantics.
- Metrics and health endpoints for ops visibility.

## Prerequisites

- Access to the S3 bucket where KafScale snapshots are stored.
- An Iceberg REST catalog endpoint reachable from the cluster.
- An offsets backend (etcd by default).
- Optional: JSON schema registry endpoint for validation or column discovery.

## Configuration Overview

The processor reads a YAML config file (mounted into the container).

Required fields:
- `s3.bucket`
- `iceberg.catalog.type`
- `iceberg.catalog.uri`
- `etcd.endpoints` (when `offsets.backend=etcd`)
- `mappings[]` with `topic` and `table`

Common fields:
```yaml
s3:
  bucket: kafscale-data
  namespace: production
  endpoint: ""
  region: us-east-1
  path_style: false

iceberg:
  catalog:
    type: rest
    uri: https://iceberg-catalog.example.com
    token: ""
    username: ""
    password: ""
  warehouse: s3://iceberg-warehouse/production

offsets:
  backend: etcd
  lease_ttl_seconds: 30
  key_prefix: processors

discovery:
  mode: auto

etcd:
  endpoints:
    - http://etcd.kafscale.svc.cluster.local:2379
  username: ""
  password: ""
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

Warehouse semantics:
`iceberg.warehouse` is the S3 prefix where Iceberg metadata and parquet files
  are written directly by the processor and where the Iceberg REST service has 
  to point to.

## Security and Data Governance

The operator is responsible for storage security and governance:
- IAM roles/permissions for S3 access.
- Encryption at rest (SSE-S3 or SSE-KMS) and in transit.
- Bucket policies, logging, and data retention.

If you use Databricks (Unity Catalog) on AWS S3, point the catalog at the same
warehouse path. The processor writes Iceberg tables there; Databricks reads the
metadata and parquet files directly from S3.

## Topic-to-Table Mapping

```yaml
mappings:
  - topic: orders
    table: prod.orders
    mode: append
    create_table_if_missing: true
```

Notes:
- Only `append` is supported.
- `create_table_if_missing` auto-creates tables when topics are new.

## Schema Columns and Evolution

You can define columns directly in the mapping or resolve them from a registry.

Mapping-defined columns:
```yaml
mappings:
  - topic: orders
    table: prod.orders
    mode: append
    create_table_if_missing: true
    schema:
      columns:
        - name: order_id
          type: long
          required: true
        - name: status
          type: string
      allow_type_widening: true
```

Registry-driven columns:
```yaml
schema:
  mode: "off"
  registry:
    base_url: https://schemas.example.com
    timeout_seconds: 5
    cache_seconds: 300

mappings:
  - topic: orders
    table: prod.orders
    schema:
      source: registry
```

Supported column types:
`boolean`, `int`, `long`, `float`, `double`, `string`, `binary`, `timestamp`,
`date`.

## Schema Validation (Optional)

`schema.mode` controls JSON validation against the registry:
- `off`: no validation.
- `lenient`: drops invalid records and continues.
- `strict`: stops on validation errors.

Validation fetches schemas from `schema.registry.base_url/<topic>.json`.

## Discovery Modes

- `auto`: uses etcd for topic/partition lists when provided, always uses S3 for
  segment discovery.
- `etcd`: requires `etcd.endpoints` and skips S3 topic listing.
- `s3`: ignores etcd and relies on S3 listing only.

## Offsets and Leases

Offsets are tracked per topic partition with a TTL lease:
- Only one worker advances a partition at a time.
- If a pod dies, the lease expires and another pod resumes.
- At-least-once semantics by design.

Tune `offsets.lease_ttl_seconds` based on segment size and processing time.

## Record IDs and Idempotency

Each record includes a deterministic `record_id` column of the form
`<topic>:<partition>:<offset>`. Downstream consumers can use this to dedupe
replays (at-least-once semantics) without relying on Kafka metadata.

## Write Serialization (Iceberg)

Table create/load/evolve/commit operations are serialized per topic to avoid
Iceberg commit races. This keeps correctness but reduces per-topic concurrency.

## Deployment (Helm)

Use `deploy/helm/iceberg-processor/values.yaml` as the base.
Key Helm values:
- `config.s3.*`
- `config.iceberg.*`
- `config.etcd.*`
- `config.mappings`
- `s3.credentialsSecretRef` with `AWS_ACCESS_KEY_ID` and
  `AWS_SECRET_ACCESS_KEY`

## Metrics and Health

Default metrics bind: `KAFSCALE_METRICS_ADDR` (default `:9093`).
- `/metrics` exposes Prometheus metrics.
- `/healthz` returns `ok`.

Key metrics:
- `kafscale_processor_records_total{topic,result}`
- `kafscale_processor_batches_total{topic}`
- `kafscale_processor_write_latency_ms{topic}`
- `kafscale_processor_errors_total{stage}`
- `kafscale_processor_last_offset{topic,partition}`
- `kafscale_processor_watermark_offset{topic,partition}`
- `kafscale_processor_watermark_timestamp_ms{topic,partition}`

## Scaling (Operational Behavior)

Work is partition-scoped. One worker holds the lease for a partition at a time.
Throughput scales by increasing partitions. The Helm chart does not install an
HPA; set `replicaCount` (or add your own HPA) and size pod resources based on
your workload.

Minimal HPA example:
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: iceberg-processor
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: iceberg-processor
  minReplicas: 1
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 80
```

## Troubleshooting

- Missing data: verify S3 path `s3://{bucket}/{namespace}/{topic}/{partition}`.
- Schema errors: check registry URL and payloads for valid JSON.
- Catalog errors: verify REST endpoint and credentials.
- Offsets not advancing: check etcd connectivity and lease TTL.

## Local Demo

From repo root:
```
make iceberg-demo
```

This boots a local kind cluster, deploys the processor, produces data, and
validates the S3 and Iceberg outputs. See the root `Makefile` for image and
warehouse overrides.
