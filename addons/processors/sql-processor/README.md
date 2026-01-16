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

# KAFSQL Processor

KAFSQL is a storage-native SQL processor that reads Kafscale KFS segments from
S3 and exposes a Postgres wire protocol endpoint for ad-hoc queries. It is
stateless, uses S3 as source of truth, and targets the 80% of Kafka data
questions that are simple and time-bounded.

## Quick Links

- User guide: `addons/processors/sql-processor/user-guide.md`
- Developer guide: `addons/processors/sql-processor/developer.md`
- Reference config: `addons/processors/sql-processor/config/config.yaml`
- Helm values: `addons/processors/sql-processor/deploy/helm/sql-processor/values.yaml`

## What It Does

- Read KFS segments directly from S3 (no broker dependency).
- Expose Postgres wire protocol for JDBC/BI compatibility.
- Support single-topic queries plus bounded two-topic joins.
- Provide Kafka-native SQL extensions (`LAST`, `TAIL`, `SCAN FULL`).

## What It Does Not Do

- Continuous streaming queries or materialized views.
- Full SQL feature parity or multi-join chains.
- Writes or updates.

## Layout

```
sql-processor/
├── cmd/processor/        # main entry point
├── config/               # sample config
├── internal/config/      # YAML parsing + validation
├── internal/discovery/   # segment listing and completion checks
├── internal/decoder/     # Kafscale segment decoding
├── internal/processor/   # query orchestration
├── internal/server/      # Postgres wire server (planned)
├── deploy/helm/          # Helm chart
├── Dockerfile
└── Makefile
```

## Quick Start (Local)

```
make build
./bin/sql-processor -config config/config.yaml
```

## Config Example

```yaml
s3:
  bucket: kafscale-data
  namespace: production

metadata:
  discovery: etcd

server:
  listen: ":5432"
```

For full configuration and operational guidance, use the user guide.
