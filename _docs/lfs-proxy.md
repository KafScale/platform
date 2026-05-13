---
layout: doc
title: LFS Proxy
description: Large File Support proxy for streaming binary payloads via S3 and Kafka.
permalink: /lfs-proxy/
nav_title: LFS Proxy
nav_order: 1
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

# LFS Proxy

The **LFS (Large File Support) Proxy** enables KafScale to handle large binary payloads — medical images, video files, industrial sensor dumps, SAP IDocs — that exceed typical Kafka message size limits.

Instead of pushing multi-megabyte blobs through Kafka, the proxy implements the **claim-check pattern**: large payloads are uploaded to S3-compatible storage, and a compact JSON envelope (pointer record) is published to Kafka. Consumers use the envelope to fetch the original object on demand.

## How it works

```
Producer ──▶ LFS Proxy ──▶ S3 (blob)
                │
                ▼
            Kafka (pointer envelope)
                │
                ▼
Consumer ◀── LFS SDK ──▶ S3 (fetch blob)
```

1. **Write path (Kafka protocol)**: The proxy intercepts Produce requests. Records tagged with an `LFS_BLOB` header are rewritten: the payload is uploaded to S3 and the Kafka record is replaced with a JSON envelope containing the S3 key, checksum, and content type.

2. **Write path (HTTP API)**: Clients can also upload files via the REST API (`POST /v1/topics/{topic}/records`). The proxy uploads the file to S3 and publishes the envelope to Kafka in one operation.

3. **Read path**: Consumer SDKs (Go, Java, Python, JS) detect LFS envelopes and transparently fetch the original object from S3.

## Key features

- **Transparent Kafka proxy** — existing producers work without code changes by adding an `LFS_BLOB` header
- **HTTP upload API** — RESTful endpoint for browser and SDK uploads with OpenAPI spec
- **Checksum verification** — SHA-256, CRC-32, or MD5 integrity checks on upload and download
- **TLS and SASL** — full TLS support for HTTP endpoints and SASL/SCRAM for Kafka backend
- **Prometheus metrics** — upload/download counters, latencies, S3 operation histograms
- **CORS support** — configurable cross-origin headers for browser-based uploads
- **Helm chart** — production-ready Kubernetes deployment via the KafScale Helm chart

## Data flow

### Object key format

S3 objects are stored under a deterministic key:

```
lfs/{namespace}/{topic}/{partition}/{offset}-{uuid}.bin
```

### Envelope format

```json
{
  "lfs_version": 1,
  "s3_bucket": "kafscale",
  "s3_key": "lfs/default/demo-topic/0/42-abc123.bin",
  "content_type": "application/octet-stream",
  "content_length": 10485760,
  "checksum_algo": "sha256",
  "checksum": "e3b0c44298fc1c14..."
}
```

## Configuration

The LFS proxy is configured via environment variables or CLI flags:

| Variable | Default | Description |
|---|---|---|
| `LFS_S3_BUCKET` | `kafscale` | S3 bucket for blob storage |
| `LFS_S3_REGION` | `us-east-1` | S3 region |
| `LFS_S3_ENDPOINT` | — | Custom S3 endpoint (for MinIO) |
| `LFS_S3_PATH_STYLE` | `false` | Use path-style S3 addressing |
| `LFS_KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `LFS_HTTP_ADDR` | `:8080` | HTTP API listen address |
| `LFS_METRICS_ADDR` | `:9095` | Prometheus metrics listen address |
| `LFS_CHECKSUM_ALGO` | `sha256` | Checksum algorithm (`sha256`, `crc32`, `md5`) |
| `LFS_MAX_UPLOAD_SIZE` | `0` (unlimited) | Maximum upload size in bytes |
| `LFS_CORS_ORIGINS` | `*` | Allowed CORS origins |

## Quick start

```bash
# Start MinIO + broker + LFS proxy locally
make lfs-demo

# Upload a file via HTTP
curl -X POST http://localhost:8080/v1/topics/demo-topic/records \
  -F "file=@large-file.bin"

# Consume the envelope
kafka-console-consumer --topic demo-topic --from-beginning
```

## Related docs

- [LFS Demos](/lfs-demos/) — Runnable demos from local IDoc to full Kubernetes pipelines
- [LFS Helm deployment](/lfs-helm/) — Full Helm chart configuration reference
- [LFS Client SDKs](/lfs-sdks/) — Java, Python, JS, and browser SDKs
- [Iceberg Processor](/processors/iceberg/) — LFS-aware Iceberg sink
- [Architecture](/architecture/) — Overall KafScale architecture
