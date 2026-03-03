---
layout: doc
title: LFS Demos
description: Runnable demos for the KafScale Large File Support system — from local IDoc processing to full Kubernetes pipelines.
permalink: /lfs-demos/
nav_title: LFS Demos
nav_order: 4
nav_group: LFS
---

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

# LFS Demos

KafScale ships with a set of runnable demos that exercise the LFS pipeline end-to-end. They range from a lightweight local demo (MinIO only, no cluster) to full Kubernetes deployments with industry-specific content-explosion patterns.

## Quick reference

| Make target | What it runs | Needs cluster? |
|---|---|---|
| `make lfs-demo` | Baseline LFS proxy flow | Yes (kind) |
| `make lfs-demo-idoc` | SAP IDoc explode via LFS | No (MinIO only) |
| `make lfs-demo-medical` | Healthcare imaging (E60) | Yes (kind) |
| `make lfs-demo-video` | Video streaming (E61) | Yes (kind) |
| `make lfs-demo-industrial` | Industrial IoT mixed payloads (E62) | Yes (kind) |
| `make e72-browser-demo` | Browser SPA uploads (E72) | Yes (kind) |

All demos are environment-driven — override any setting via env vars without touching code.

---

## Baseline LFS demo

```bash
make lfs-demo
```

The baseline demo bootstraps a kind cluster, deploys MinIO and the LFS proxy, then runs the full claim-check flow:

1. Build and load the `kafscale-lfs-proxy` image into kind
2. Deploy broker, etcd, MinIO, and LFS proxy into the `kafscale-demo` namespace
3. Create a demo topic and upload binary blobs via the LFS proxy HTTP API
4. Verify pointer envelopes arrive in Kafka and blobs exist in S3

| Variable | Default | Description |
|---|---|---|
| `LFS_DEMO_TOPIC` | `lfs-demo-topic` | Kafka topic for pointer records |
| `LFS_DEMO_BLOB_SIZE` | `524288` (512 KB) | Size of each test blob |
| `LFS_DEMO_BLOB_COUNT` | `5` | Number of blobs to upload |
| `LFS_DEMO_TIMEOUT_SEC` | `120` | Test timeout |

---

## IDoc explode demo (local, no cluster)

```bash
make lfs-demo-idoc
```

This is the fastest way to see the LFS pipeline in action. It only needs a local MinIO container — no Kubernetes cluster required.

The demo walks through the complete data flow that the [LFS Proxy](/lfs-proxy/) performs in production:

**Step 1 — Blob upload.** The demo uploads a realistic SAP ORDERS05 IDoc XML (a purchase order with 3 line items, 3 business partners, 3 dates, and 2 status records) to MinIO, simulating what the LFS proxy does when it receives a large payload.

**Step 2 — Envelope creation.** A KafScale LFS envelope is generated — the compact JSON pointer record that Kafka consumers receive instead of the raw blob:

```json
{
  "kfs_lfs": 1,
  "bucket": "kafscale",
  "key": "lfs/idoc-demo/idoc-inbound/0/0-idoc-sample.xml",
  "content_type": "application/xml",
  "size": 2706,
  "sha256": "96985f1043a285..."
}
```

**Step 3 — Resolve and explode.** The `idoc-explode` processor reads the envelope, resolves the blob from S3 via `pkg/lfs.Resolver`, validates the SHA-256 checksum, then parses the XML and routes segments to topic-specific streams:

```
Segment routing:
  E1EDP01, E1EDP19  ->  idoc-items      (order line items)
  E1EDKA1           ->  idoc-partners   (business partners)
  E1STATS           ->  idoc-status     (processing status)
  E1EDK03           ->  idoc-dates      (dates/deadlines)
  EDI_DC40, E1EDK01 ->  idoc-segments   (all raw segments)
  (root)            ->  idoc-headers    (IDoc metadata)
```

**Step 4 — Topic streams.** Each output file maps to a Kafka topic. Routed records carry their child fields as a self-contained JSON object:

```json
{"name":"E1EDP01","path":"IDOC/E1EDP01","fields":{
  "POSEX":"000010","MATNR":"MAT-HYD-4200",
  "ARKTX":"Hydraulic Pump HP-4200","MENGE":"5",
  "NETWR":"12500.00","WAERS":"EUR"}}
```

```json
{"name":"E1EDKA1","path":"IDOC/E1EDKA1","fields":{
  "PARVW":"AG","NAME1":"GlobalParts AG",
  "STRAS":"Industriestr. 42","ORT01":"Stuttgart",
  "LAND1":"DE"}}
```

The sample IDoc produces 94 records across 6 topics.

---

## Industry demos

The three industry demos build on the baseline flow and demonstrate the **content explosion pattern** — a single large upload that produces multiple derived topic streams for downstream analytics.

### Medical imaging (E60)

```bash
make lfs-demo-medical
```

Simulates a radiology department uploading DICOM imaging files (CT/MRI scans, whole-slide pathology images). A single scan upload explodes into:

| Topic | Content | LFS blob? |
|---|---|---|
| `medical-images` | Original DICOM blob pointer | Yes |
| `medical-metadata` | Patient ID, modality, study date | No |
| `medical-audit` | Access timestamps, user actions | No |

Demonstrates checksum integrity validation and audit trail logging relevant to healthcare compliance scenarios.

### Video streaming (E61)

```bash
make lfs-demo-video
```

Simulates a media platform ingesting large video files. A single video upload explodes into:

| Topic | Content | LFS blob? |
|---|---|---|
| `video-raw` | Original video blob pointer | Yes |
| `video-metadata` | Codec, duration, resolution, bitrate | No |
| `video-frames` | Keyframe timestamps and S3 references | No |
| `video-ai-tags` | Scene detection, object labels | No |

Uses HTTP streaming upload for memory-efficient transfer of multi-gigabyte files.

### Industrial IoT (E62)

```bash
make lfs-demo-industrial
```

Simulates a factory floor with **mixed payload sizes** flowing through the same Kafka interface — small telemetry readings alongside large thermal/visual inspection images:

| Topic | Content | LFS blob? | Typical size |
|---|---|---|---|
| `sensor-telemetry` | Real-time sensor readings | No | ~1 KB |
| `inspection-images` | Thermal/visual inspection photos | Yes | ~200 MB |
| `defect-events` | Anomaly detection alerts | No | ~2 KB |
| `quality-reports` | Aggregated quality metrics | No | ~10 KB |

Demonstrates automatic routing based on the `LFS_BLOB` header — small payloads flow through Kafka directly, large payloads are offloaded to S3.

---

## SDK demo applications

### E70 — Java SDK

Located in `examples/E70_java-lfs-sdk-demo/`. Demonstrates the Java LFS producer SDK uploading files via the HTTP API, consuming pointer records from Kafka, and resolving blobs from S3.

```bash
cd examples/E70_java-lfs-sdk-demo
make run-all   # builds SDK, starts port-forwards, runs demo
```

Key env vars: `LFS_HTTP_ENDPOINT`, `LFS_TOPIC`, `KAFKA_BOOTSTRAP`, `LFS_PAYLOAD_SIZE`.

### E71 — Python SDK

Located in `examples/E71_python-lfs-sdk-demo/`. Demonstrates the Python LFS SDK with three payload size presets for video upload testing:

```bash
cd examples/E71_python-lfs-sdk-demo
make run-small    # 1 MB
make run-midsize  # 50 MB
make run-large    # 200 MB
```

Requires `pip install -e lfs-client-sdk/python` for the SDK.

### E72 — Browser SDK

Located in `examples/E72_browser-lfs-sdk-demo/`. A single-page application that uploads files directly from the browser to the LFS proxy — no backend server required.

```bash
make e72-browser-demo       # local with port-forward
make e72-browser-demo-k8s   # in-cluster deployment
```

Features drag-and-drop upload, real-time progress, SHA-256 verification, presigned URL download, and an inline video player for MP4 content.

---

## Common environment

All demos share these defaults:

| Variable | Default | Description |
|---|---|---|
| `KAFSCALE_DEMO_NAMESPACE` | `kafscale-demo` | Kubernetes namespace |
| `MINIO_BUCKET` | `kafscale` | S3 bucket |
| `MINIO_REGION` | `us-east-1` | S3 region |
| `MINIO_ROOT_USER` | `minioadmin` | MinIO credentials |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | MinIO credentials |
| `LFS_PROXY_IMAGE` | `ghcr.io/kafscale/kafscale-lfs-proxy:dev` | Proxy container image |

## Related docs

- [LFS Proxy](/lfs-proxy/) — Architecture and configuration
- [LFS Helm Deployment](/lfs-helm/) — Kubernetes deployment guide
- [LFS Client SDKs](/lfs-sdks/) — SDK API reference for Go, Java, Python, JS
