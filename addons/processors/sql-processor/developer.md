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

# KAFSQL Developer Guide

This guide covers code layout, key flows, and how to extend KAFSQL.

## Code Layout

```
addons/processors/sql-processor/
├── cmd/processor/            # KAFSQL server entry point
├── cmd/proxy/                # optional external proxy
├── cmd/backfill/             # manifest/time-index backfill tool
├── config/                   # sample config
├── internal/config/          # config parsing + validation
├── internal/discovery/       # segment listing + manifest/time index
├── internal/decoder/         # KFS segment decode
├── internal/server/          # Postgres wire server + query execution
├── internal/proxy/           # auth/ACL proxy
└── deploy/helm/              # Helm chart
```

## Query Execution Flow

1. Parse SQL and Kafka extensions.
2. Resolve topics/partitions (etcd or discovery).
3. List segments (S3, manifest, or cache).
4. Apply filters and decode records.
5. Stream rows via Postgres wire protocol.

## Extended Protocol Support

KAFSQL supports a minimal extended protocol:
- `Parse`, `Bind`, `Execute`, `Sync`, `Close`
- No bind parameters and single-statement only.

Client drivers should use simple query mode where possible.

## External Metadata

Optional helpers:
- Manifest reader: `internal/discovery/manifest.go`
- Manifest builder: `internal/discovery/manifest_builder.go`
- Time index reader: `internal/discovery/time_index.go`
- Time index builder: `internal/discovery/time_index_builder.go`

These are additive and do not change KFS data files.

Build safety:
- Builders use an in-process lease (`build_lease.go`) to avoid concurrent writes.
- `build_max_segments` and `build_max_bytes` cap build cost.

## Result Cache and Backpressure

- Result cache is built from streamed rows in `internal/server/row_collector.go`.
- Cache keys include normalized SQL + time window, skipping tail/scan-full cases.
- Backpressure uses a queue limiter to cap concurrent queries and waiting time.

## Proxy Behavior

- The proxy only supports the simple query protocol (no extended protocol).
- ACL decisions are cached per connection for short TTLs.

## Testing

From `addons/processors/sql-processor`:
```
make test
make test-minio
make coverage
```

For MinIO integration tests, ensure the helper container is running (Makefile
target `ensure-minio` does this).

## Adding New SQL Features

- Extend the parser in `internal/sql`.
- Add planning or validation in `internal/server`.
- Update tests in `internal/server/server_test.go`.
- Keep query governance intact (time-bounded by default).

## Docker Images

The Dockerfile builds:
- `/app/sql-processor`
- `/app/kafsql-proxy`
- `/app/kafsql-backfill`

Local build:
```
make docker-build-sql-processor
```
