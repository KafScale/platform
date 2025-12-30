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

# Processor Skeleton (Template)

This folder is a minimal starting point for building custom storage-native
processors that read Kafscale segments from S3 and write to a downstream sink.
It is intentionally small and does not implement Kafka protocols or consumer
group coordination.

## What This Template Covers

- Segment discovery in S3 (completed `.kfs` + `.index` pairs).
- Segment decoding based on `kafscale-spec.md`.
- Topic-to-sink mapping via YAML.
- Lease-per-partition offset tracking for at-least-once processing.
- Dockerfile, Helm chart, and basic build/test scripts.

## Suggested Layout

```
processors/skeleton/
├── cmd/processor/        # main entry point
├── config/               # sample config
├── internal/config/      # YAML parsing + validation
├── internal/discovery/   # segment listing and completion checks
├── internal/decoder/     # Kafscale segment decoding
├── internal/checkpoint/  # lease + offset persistence
├── internal/sink/        # sink interface + adapters
├── deploy/helm/          # Helm chart
├── Dockerfile
└── Makefile
```

## Extension Points

- Swap `internal/sink` to target Iceberg, Delta, OLAP, or custom services.
- Replace `internal/checkpoint` if you prefer a state table over etcd.
- Add schema registry or validation logic in `internal/decoder`.

## Config Example

```yaml
s3:
  bucket: kafscale-data
  namespace: production

mappings:
  - topic: orders
    sink: iceberg.prod.orders
    mode: append
```

## Semantics

- At-least-once processing by design.
- Scaling is per partition; do not split a partition across workers.
- Offsets advance only after the sink confirms a successful write.

## Operational Notes (Tradeoffs)

- The skeleton pins a worker to a single partition lease and renews it. This is
  simple and correct for sinks that require strict ordering, but it limits
  throughput per worker. If you need higher throughput, allow multiple leases
  per worker and keep per-partition ordering intact.
- The processor includes a per-topic lock helper that wraps sink writes. Use
  this to serialize table create/evolve/commit for sinks like Iceberg that
  cannot tolerate concurrent updates.
- For a full example, see the Iceberg processor implementation in
  `addons/processors/iceberg-processor/internal/processor/processor.go` and
  `addons/processors/iceberg-processor/internal/sink/iceberg.go`.
- The per-topic lock map grows with topic count; for long-lived clusters with
  high topic churn, consider adding eviction or a bounded lock pool.

## Quick Start (Local)

```bash
make build
./bin/skeleton-processor -config config/config.yaml
```

Or run the dev script:

```bash
./scripts/dev-run.sh
```
