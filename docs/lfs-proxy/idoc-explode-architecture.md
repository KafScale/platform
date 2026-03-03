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

# IDoc Explode Processor: Solution Design & Architecture

This document describes the solution design for the IDoc explode processor, its architecture, and how it integrates with KafScale brokers and LFS.

## Goal

Convert large SAP IDoc XML documents (stored via LFS) into structured, topic-oriented event streams (headers, items, partners, statuses, dates, segments) for downstream analytics and correlation.

## Scope

The LFS exploder is part of the LFS module; it reuses `pkg/lfs` for envelope detection and S3 resolution.

- Input: IDoc XML (ORDERS05, DELVRY03, INVOIC02, etc.)
- Transport: LFS pointer records in Kafka (LFS proxy) or raw XML files
- Output: JSON records partitioned by semantic topic (header/items/partners/status/dates/segments)
- Initial implementation: storage-native processor with LFS resolution and topic exposure

## Architecture Overview

```
+-----------------------------+
| SAP / EDI / IDoc Producers  |
+--------------+--------------+
               |
               | (Kafka produce + LFS_BLOB)
               v
+-----------------------------+       +--------------------+
| LFS Proxy (Kafka + HTTP)    |------>| S3 / MinIO         |
| - uploads XML to S3         |       | (blob storage)     |
| - writes pointer envelopes  |       +--------------------+
+--------------+--------------+
               |
               | (pointer records in Kafka)
               v
+-----------------------------+
| Explode Processor           |
| - resolves LFS blobs        |
| - parses IDoc XML           |
| - routes segments to topics |
+--------------+--------------+
               |
               v
+-----------------------------+
| Downstream Topics           |
| - idoc-headers              |
| - idoc-items                |
| - idoc-partners             |
| - idoc-status               |
| - idoc-dates                |
| - idoc-segments             |
+-----------------------------+
```

## Integration with Broker

### Current (Phase 4 Core)

- The explode logic is implemented as a library in `pkg/idoc`.
- A reference CLI `cmd/idoc-explode` consumes:
  - raw XML files, or
  - LFS envelope JSONL records (e.g., exported from Kafka),
  then resolves via S3 and writes topic-specific JSONL files.
- This provides a deterministic, testable baseline for segment routing.

### Target Broker Integration

The processor will be wired into a broker-connected execution path in one of two ways:

1) **Processor Framework (preferred)**
   - Use the existing processor skeleton pattern (segment discovery → decode → sink).
   - Decode Kafka records from S3 segments, detect LFS envelopes, resolve blobs via `pkg/lfs/Resolver`.
   - Explode the XML with `pkg/idoc.ExplodeXML`.
   - Emit JSON payloads to downstream topics via a Kafka producer or S3-native topic sink.

2) **Explode Service (deferred)**
   - Dedicated service consuming Kafka directly.
   - Writes new Kafka topics for each segment type.
   - This is now lowest priority and only needed if the processor pipeline is insufficient.

## Data Flow

### 1) Ingestion
- Producer sets `LFS_BLOB` header on Kafka records with large XML.
- LFS proxy uploads XML to S3 and replaces record value with envelope JSON.

### 2) Resolution
- Explode processor detects envelopes with `pkg/lfs.IsLfsEnvelope`.
- Fetches blob via `pkg/lfs.Resolver` using `pkg/lfs.S3Reader`.
- Validates checksum (configurable; defaults to SHA-256).

### 3) XML Parsing
- XML is parsed in streaming mode (no DOM load).
- Each XML element becomes a `Segment` with:
  - `name`, `path`, `attributes`, `value`.

### 4) Topic Exposure
- Segment types are routed based on configured segment name lists:
  - items, partners, statuses, dates.
- Full segment stream is optionally emitted for traceability.

## Output Topics

Default topics (configurable via env):

- `idoc-headers` (root metadata)
- `idoc-items`
- `idoc-partners`
- `idoc-status`
- `idoc-dates`
- `idoc-segments` (full raw segments)

## Config & Env

Key environment controls (defaults are in `.env.example`):

- `KAFSCALE_IDOC_ITEM_SEGMENTS`
- `KAFSCALE_IDOC_PARTNER_SEGMENTS`
- `KAFSCALE_IDOC_STATUS_SEGMENTS`
- `KAFSCALE_IDOC_DATE_SEGMENTS`
- `KAFSCALE_IDOC_TOPIC_*`
- `KAFSCALE_IDOC_MAX_BLOB_SIZE`
- `KAFSCALE_IDOC_VALIDATE_CHECKSUM`

LFS resolver uses the standard proxy S3 config:

- `KAFSCALE_LFS_PROXY_S3_BUCKET`
- `KAFSCALE_LFS_PROXY_S3_REGION`
- `KAFSCALE_LFS_PROXY_S3_ENDPOINT`
- `KAFSCALE_LFS_PROXY_S3_ACCESS_KEY`
- `KAFSCALE_LFS_PROXY_S3_SECRET_KEY`

## Failure Modes

- **Checksum mismatch** → reject record; log error; no downstream emission.
- **S3 fetch error** → skip record; retry in subsequent run.
- **XML parse error** → skip record; emit error metric.
- **Oversized blob** → reject to protect memory bounds.

## Security Notes

- No credentials in logs or envelopes.
- LFS resolution uses existing S3 auth/secret handling.
- Broker integration will use existing proxy TLS/SASL when enabled.

## Future Enhancements

- Schema registry for IDoc segment schemas.
- Correlation rule engine for cross-IDoc events.
- Reintegration adapters (BAPI, ACK IDocs).
