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

# LFS SDK Roadmap

This document tracks SDK coverage for LFS (Large File Support) across languages.
The goal is to wrap plain Kafka clients with LFS-aware helpers for producing and
consuming large payloads.

## Scope
- **Consumer helpers**: detect LFS envelopes and resolve blobs from S3.
- **Producer helpers**: upload via LFS proxy HTTP API and emit pointer envelopes.
- **Utilities**: checksum validation, envelope parsing, and metadata helpers.

## Status Summary
- **Go**: ✅ Consumer + producer SDKs are implemented in `pkg/lfs/`.
- **Java**: ❌ Planned (consumer + producer wrappers).
- **JavaScript/TypeScript**: ⏳ Planned for March 2026 (low priority).
- **Python**: ❌ Planned.

## Priority
SDKs are the highest priority milestone after LFS core integration. The intent is
full feature parity across Go, Java, JavaScript/TypeScript, and Python (JS planned March 2026).

## Planned Deliverables

### Go SDK (hardening)
- Examples + doc updates in `pkg/lfs/doc.go`.
- Integration tests: LFS proxy + MinIO.

### Java SDK
- Consumer wrapper (KafkaConsumer) with envelope detection and S3 fetch.
- Producer wrapper for `/lfs/produce` streaming.
- JUnit + TestContainers integration tests.

### JavaScript/TypeScript SDK (Target: March 2026)
- Consumer helper using AWS SDK v3.
- Producer helper using LFS proxy HTTP API.
- Types, examples, and integration tests.

### Python SDK
- Consumer helper using boto3.
- Producer helper using LFS proxy HTTP API.
- Examples and integration tests.

## Notes
- All SDKs must preserve Kafka semantics and remain optional to adopt.
- Use the LFS envelope schema as the single source of truth.
- Ensure parity in checksum validation and metadata exposure.
