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

2. **Write path (HTTP API)**: Clients can also upload files via the REST API (`POST /lfs/produce` or the multipart upload session endpoints under `/lfs/uploads/...`). The proxy uploads the file to S3 and publishes the envelope to Kafka in one operation. See the OpenAPI spec at `cmd/proxy/openapi.yaml` for full schema.

3. **Read path**: Consumer SDKs (Go, Java, Python, JS) detect LFS envelopes and fetch the object via the proxy's `POST /lfs/download` endpoint. The proxy verifies the envelope-recorded SHA-256 against the bytes returned from S3 **before** delivering them to the client (see [Trust model and integrity verification](#trust-model-and-integrity-verification) below).

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
  "s3_bucket": "my-bucket",
  "s3_key": "lfs/default/demo-topic/0/42-abc123.bin",
  "content_type": "application/octet-stream",
  "content_length": 10485760,
  "checksum_algo": "sha256",
  "checksum": "e3b0c44298fc1c14..."
}
```

## Trust model and integrity verification

> **Kafka is the authority. S3 is untrusted storage.**

The envelope lives in Kafka and carries the authoritative SHA-256 checksum recorded at upload time. The proxy treats the S3 object as untrusted on the download path: it reads the bytes into temporary storage, verifies their SHA-256 against the envelope-supplied checksum, and **only then** streams the verified bytes to the client (`200 OK` with `Content-Length` set to the verified size). On mismatch — or if S3 returns more bytes than the envelope declares — the proxy returns `502` with `code: integrity_failure` and **no payload bytes ever reach the client**.

This design holds across HTTP/1.1, HTTP/2, every HTTP client library (Go, Java, Python `requests`, JavaScript `fetch`, `curl --output`), and every HTTP intermediary (nginx-ingress, ALB, CDN). No framing tricks, no trailers, no connection-abort signalling.

### Stream-mode download request

```bash
curl -X POST http://localhost:8080/lfs/download \
  -H "X-API-Key: $KAFSCALE_LFS_PROXY_HTTP_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "bucket": "my-bucket",
    "key": "lfs/default/demo-topic/0/42-abc123.bin",
    "mode": "stream",
    "integrity": {
      "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      "checksum_alg": "sha256",
      "size": 10485760
    }
  }' \
  -o downloaded-blob.bin
```

Both `integrity.sha256` AND `integrity.size` are **required** on stream-mode requests — the size enables a hard cap on the S3 read so a compromised bucket cannot exhaust proxy temporary storage. SDKs populate them from the Kafka envelope automatically.

### Presign-mode download (off by default)

`mode: presign` returns a time-limited URL the client uses to fetch the object directly from S3. The proxy does **no** integrity verification on this path — the client is responsible for hashing the downloaded bytes against the `integrity` block echoed in the response. Disabled by default; enable per-deployment by setting `KAFSCALE_LFS_PROXY_PRESIGN_ENABLED=true`.

### Error codes on `/lfs/download`

| HTTP | `code` | Meaning |
|---|---|---|
| 400 | `missing_integrity` | `integrity.sha256` was not supplied |
| 400 | `missing_integrity_size` | stream mode requires `integrity.size` |
| 400 | `payload_too_large` | `integrity.size` exceeds `KAFSCALE_LFS_PROXY_MAX_BLOB_SIZE` |
| 400 | `presign_disabled` | presign mode requested but operator did not opt in |
| 400 | `unsupported_checksum_alg` | only `sha256` is accepted |
| 502 | `integrity_failure` | SHA-256 mismatch or S3 returned more bytes than declared |
| 502 | `s3_get_failed` | S3 read failed |

## Configuration

The LFS proxy is configured via environment variables. All variables are prefixed `KAFSCALE_LFS_PROXY_`.

| Variable | Default | Description |
|---|---|---|
| `KAFSCALE_LFS_PROXY_S3_BUCKET` | **required** | S3 bucket for blob storage. The bucket name `kafscale-lfs` is permanently blocklisted at startup (CVE — registered by a third party). Use your own name. |
| `KAFSCALE_LFS_PROXY_S3_REGION` | **required** | S3 region |
| `KAFSCALE_LFS_PROXY_S3_ENDPOINT` | — | Custom S3 endpoint (for MinIO or non-AWS S3) |
| `KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE` | auto | Use path-style S3 addressing (defaults true when endpoint is set) |
| `KAFSCALE_LFS_PROXY_S3_ACCESS_KEY` | — | S3 access key (or use IAM role / instance profile) |
| `KAFSCALE_LFS_PROXY_S3_SECRET_KEY` | — | S3 secret key |
| `KAFSCALE_LFS_PROXY_S3_SESSION_TOKEN` | — | S3 session token (for STS) |
| `KAFSCALE_LFS_PROXY_S3_PUBLIC_ENDPOINT` | — | Endpoint advertised in presigned URLs (for split-network deployments) |
| `KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET` | `false` | Create the bucket on startup if it doesn't exist |
| `KAFSCALE_LFS_PROXY_MAX_BLOB_SIZE` | `5368709120` (5 GiB) | Upper bound on per-object size for both uploads and downloads. Download requests with `integrity.size` larger than this are rejected with `payload_too_large`. |
| `KAFSCALE_LFS_PROXY_CHUNK_SIZE` | `5242880` (5 MiB) | Multipart upload chunk size |
| `KAFSCALE_LFS_PROXY_CHECKSUM_ALGO` | `sha256` | Checksum algorithm (only `sha256` is currently honored by the integrity-verification download path) |
| `KAFSCALE_LFS_PROXY_HTTP_API_KEY` | — | If set, required as `X-API-Key:` or `Authorization: Bearer ...` on HTTP requests |
| `KAFSCALE_LFS_PROXY_PRESIGN_ENABLED` | `false` | Opt-in to presigned-URL download mode |
| `KAFSCALE_LFS_PROXY_ID` | hostname | Proxy instance identifier (in ops-tracker events) |

## Quick start

```bash
# Start MinIO + broker + LFS proxy locally
make lfs-demo

# Upload a file via the HTTP API
curl -X POST http://localhost:8080/lfs/produce \
  -H "X-API-Key: $KAFSCALE_LFS_PROXY_HTTP_API_KEY" \
  -F "topic=demo-topic" \
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
