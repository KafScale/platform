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

# Dependency security review — 2026-11-15

The security changes are based on PR #332. Alert closure must be checked on
`main` after the security PR merges and GitHub scans that commit.

| Component | Patched version | Scope |
| --- | --- | --- |
| OpenTelemetry SDK and trace | 1.47.0 | Root, SQL and Iceberg Go modules |
| klauspost/compress | 1.20.2 | Root, SQL and Iceberg Go modules |
| golang.org/x/net | 0.60.0 | Root, SQL and Iceberg Go modules |
| go-jose/v4 | 4.1.6 | Iceberg processor |
| fast-xml-parser | 5.12.0 | Node LFS SDK; compatible XML builder 1.3.1 |
| Go build toolchain | 1.26.11 | Go modules and container builders |
| etcd | 3.6.14 | Root/Iceberg modules, managed server and snapshot tools |
| etcd | 3.5.33 | SQL module and Docker Compose |

CI Python dependencies have complete version and SHA-256 locks under
`.github/requirements/`. The generated file headers contain regeneration
commands; retain the Apache license header when regenerating them. SDK build tools are part of the lock; installation of the local
project disables build isolation and dependency resolution. The schema-registry
lock includes Avro, JSON Schema and Protobuf serializer dependencies. Both locks
are generated for Python 3.12, matching CI.

The proxy logs a fixed category for group-routing errors. Raw errors may contain
consumer-group or decoder input. The security coverage gate includes the reason
mapping, and a listener regression checks both log privacy and the Kafka error
response. The Node SDK also tests actual S3 XML request serialization and response
parsing with the dependency overrides.

## Remaining Avro decoder advisories

`hamba/avro` is archived; its final release has no fix for
[GO-2026-5046](https://pkg.go.dev/vuln/GO-2026-5046),
[GO-2026-5047](https://pkg.go.dev/vuln/GO-2026-5047) or
[GO-2026-5048](https://pkg.go.dev/vuln/GO-2026-5048). These concern binary decoder
CPU use, integer overflow and unbounded map allocation. A version bump within
`github.com/hamba/avro/v2` cannot resolve them.

The built-in registry uses this library for schema parsing and compatibility;
its production code does not decode Avro record payloads. Its E2E tests decode
controlled test records. The Iceberg processor depends on Apache Iceberg Go,
which uses this library's OCF decoder for manifests. Treat catalog metadata and
manifest files as trusted inputs pending a reviewed decoder migration. These
findings remain in the Go vulnerability scan and are not dismissed or claimed
fixed by this PR. A migration must preserve registry schema identity and Iceberg
manifest compatibility and enforce decoder allocation limits; the maintained
fork's map-allocation fix requires explicit configuration.
