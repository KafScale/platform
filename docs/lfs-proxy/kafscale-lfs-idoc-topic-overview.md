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

# KafScale + LFS Proxy + IDoc Exploder: Topic Overview

This document summarizes the topic landscape when KafScale, the LFS proxy, and the IDoc exploder are used together.
It distinguishes between **pointer topics** (LFS envelopes) and **exploded topics** (semantic IDoc streams).

## 1) Core Topics (Pointer Streams)

These are the topics that carry LFS pointer envelopes instead of raw XML payloads.

| Topic | Payload | Description |
|---|---|---|
| `idoc-raw` (example) | LFS envelope JSON | Raw IDoc documents, stored in S3 and referenced via pointer records. |
| `orders-idoc` | LFS envelope JSON | ORDERS05 IDocs (pointer only). |
| `delivery-idoc` | LFS envelope JSON | DELVRY03 IDocs (pointer only). |
| `invoice-idoc` | LFS envelope JSON | INVOIC02 IDocs (pointer only). |

**Note:** The exact topic names are configurable. The important point is that these topics hold **LFS envelopes**, not XML.

## 2) Exploded Topics (Semantic Streams)

The exploder resolves LFS envelopes, parses XML, and emits JSON records per semantic stream.

| Topic | Payload | Description |
|---|---|---|
| `idoc-headers` | JSON | Root-level metadata (doc number, type, sender/receiver). |
| `idoc-items` | JSON | Line items (e.g., E1EDP01 segments). |
| `idoc-partners` | JSON | Partner segments (e.g., sold-to, ship-to). |
| `idoc-status` | JSON | Status segments (e.g., E1STATS). |
| `idoc-dates` | JSON | Date segments (e.g., E1EDK03). |
| `idoc-segments` | JSON | Full segment stream for traceability. |

**Config keys:**
- `KAFSCALE_IDOC_TOPIC_HEADER`
- `KAFSCALE_IDOC_TOPIC_ITEMS`
- `KAFSCALE_IDOC_TOPIC_PARTNERS`
- `KAFSCALE_IDOC_TOPIC_STATUS`
- `KAFSCALE_IDOC_TOPIC_DATES`
- `KAFSCALE_IDOC_TOPIC_SEGMENTS`

## End-to-End Flow

1) Producer sends IDoc XML with `LFS_BLOB` header → LFS proxy stores blob in S3 and writes pointer to a Kafka topic.
2) Exploder resolves the LFS envelope, downloads XML, and emits structured JSON events into topic streams.
3) Downstream systems consume semantic topics for correlation and analytics.

## Example: ORDERS05

- Input pointer topic: `orders-idoc`
- Output topics:
  - `idoc-headers` (order metadata)
  - `idoc-items` (line items)
  - `idoc-partners` (sold-to/ship-to)
  - `idoc-dates` (requested/confirmed)
  - `idoc-status` (status transitions)
  - `idoc-segments` (full trace)

## Recommended Naming Pattern

- Pointer topics: `idoc-raw.<type>` (e.g., `idoc-raw.orders05`)
- Exploded topics: `idoc.<stream>` (e.g., `idoc.items`, `idoc.partners`)

This keeps raw and semantic streams distinct and avoids accidental consumption of large XML payloads.
