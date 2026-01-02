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

# Iceberg Processor Developer Guide

This guide is for contributors working on the processor codebase. It covers
architecture, local development, and test conventions. For operational usage,
see `user-guide.md`.

## Architecture Overview

Core modules live under `internal/`:
- `internal/discovery`: lists topics/partitions and detects completed segments.
- `internal/decoder`: decodes KafScale segment batches into records.
- `internal/checkpoint`: lease + offset storage (etcd backend).
- `internal/schema`: JSON schema validation (optional).
- `internal/sink`: Iceberg writer and schema evolution.
- `internal/processor`: orchestrates discovery, decode, validation, and sink.
- `internal/server`: metrics and health endpoints.

## Schema Evolution (Implementation)

Schema evolution is driven by mapping definitions or a JSON schema registry.

- `internal/sink/iceberg.go` resolves columns:
  - `schema.source: mapping` uses `mappings[].schema.columns`.
  - `schema.source: registry` pulls `<topic>.json` from `schema.registry.base_url`.
  - `schema.source: none` keeps base fields only.
- Field IDs are preserved when evolving an existing table schema.
- New columns are additive; incompatible type changes are rejected.
- Optional type widening supports:
  - `int` -> `long`
  - `float` -> `double`

Base fields are always present:
`record_id`, `topic`, `partition`, `offset`, `timestamp_ms`, `key`, `value`,
`headers`.

## Local Build and Tests

From `addons/processors/iceberg-processor`:
```
make build
make test
```

For strict runs:
```
GOCACHE=$(pwd)/.gocache go vet ./...
GOCACHE=$(pwd)/.gocache go test -race ./...
```

`make test-strict` runs the same checks but may need a writable `GOCACHE`
depending on your environment.

## Integration Tests

Some tests are gated by environment variables:
- MinIO decode tests require S3-compatible credentials.
- Iceberg REST tests require an accessible REST catalog.
- etcd checkpoint tests require `ICEBERG_PROCESSOR_ETCD_ENDPOINTS`.

Prefer local, deterministic unit tests where possible.

## Config Changes

When changing configuration:
- Update `internal/config/config.go` validation rules.
- Update `config/config.yaml`.
- Update Helm config templates:
  - `deploy/helm/iceberg-processor/config/config.yaml`
  - `deploy/helm/iceberg-processor/values.yaml`

## Adding Column Types

To add a new column type:
- Extend `isSupportedColumnType` in `internal/config/config.go`.
- Extend `icebergTypeForColumn` in `internal/sink/iceberg.go`.
- Add Arrow builder wiring in `recordsToArrow` helper functions.
- Add tests under `internal/sink`.

## Local Demo Workflow

Use the root `Makefile` target:
```
make iceberg-demo
```

It spins up a kind cluster, deploys the REST catalog and processor, produces
records, and validates the output. Adjust image/tag overrides in the root
`Makefile` if you need local builds.

## Dependency Notes

- Iceberg Go is pinned to `github.com/apache/iceberg-go v0.4.0` due to an
  upstream grpc module path conflict.

## Debug Switches

- `ICEBERG_PROCESSOR_REST_DEBUG=1` enables REST catalog request/response logging for the Iceberg sink.

## Integration Test Env Vars

- `ICEBERG_PROCESSOR_ETCD_ENDPOINTS` for etcd checkpoint tests.
- `ICEBERG_PROCESSOR_CATALOG_URI`, `ICEBERG_PROCESSOR_CATALOG_TYPE`, `ICEBERG_PROCESSOR_CATALOG_TOKEN`, `ICEBERG_PROCESSOR_WAREHOUSE` for Iceberg REST integration tests.
- `ICEBERG_PROCESSOR_MINIO_ENDPOINT`, `ICEBERG_PROCESSOR_S3_BUCKET`, `ICEBERG_PROCESSOR_SEGMENT_KEY`, `ICEBERG_PROCESSOR_INDEX_KEY`, `ICEBERG_PROCESSOR_TOPIC`, `ICEBERG_PROCESSOR_PARTITION` for MinIO decoder tests.
