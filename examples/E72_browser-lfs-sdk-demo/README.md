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

# E72 - Browser LFS SDK Demo

A Single Page Application (SPA) demonstrating the browser-native LFS SDK for uploading large files directly from the browser.

## Features

- **Drag & Drop Upload**: Select files via drag-and-drop or file browser
- **Progress Tracking**: Real-time upload progress with percentage
- **E2E Tests**: Automated tests for small (1KB), medium (100KB), and large (1MB) payloads
- **Download Test**: Fetch a blob using the pointer record and preview MP4 files
- **No Node.js Required**: Pure browser JavaScript using fetch/XHR APIs
- **No librdkafka**: Direct HTTP upload to LFS proxy

## Quick Start

### 1. Start the LFS Demo Stack

```bash
# From repository root
LFS_DEMO_CLEANUP=0 make lfs-demo
```

### 2. Port-Forward Services

```bash
# LFS Proxy HTTP endpoint (required)
kubectl -n kafscale-demo port-forward svc/lfs-proxy 8080:8080 &

# Optional: MinIO for blob verification
kubectl -n kafscale-demo port-forward svc/minio 9000:9000 &
```

### 3. Run the Demo

```bash
# From repository root
make e72-browser-demo

# Or rebuild proxy + refresh demo + open SPA:
make e72-browser-demo-test

# Or manually:
cd examples/E72_browser-lfs-sdk-demo
python3 -m http.server 3000
# Open http://localhost:3000
```

## Using the Demo

### Manual Upload

1. Open http://localhost:3000 in your browser
2. Configure the LFS Proxy endpoint (default: `http://localhost:8080/lfs/produce`)
3. Set the Kafka topic (default: `browser-uploads`)
4. Drag & drop a file or click to browse
5. Click "Upload to LFS"
6. View the returned envelope with S3 key and SHA-256 checksum

### Download Test

1. Paste the envelope JSON in the "Download Test" section (auto-filled after upload)
2. Set the LFS proxy base URL (default: `http://localhost:8080`)
3. Choose **Presign** (default) or **Stream via Proxy**
4. Click **Fetch Object**

If the content type is `video/mp4`, the file is shown in a video viewer. Otherwise, the UI shows pointer metadata and download details.

Notes:
- Presign mode returns a short-lived URL (TTL defaults to 120 seconds) and refreshes when expired.
- Stream mode downloads through the LFS proxy without exposing S3 URLs.
- If you have a presigned URL, paste it into "Direct Download URL" to bypass the proxy.
- If presigned URLs point to an internal S3 host, set `KAFSCALE_LFS_PROXY_S3_PUBLIC_ENDPOINT` so the proxy rewrites the host for browsers.

### E2E Tests

1. Click "Run E2E Tests"
2. Watch as synthetic payloads (1KB, 100KB, 1MB) are uploaded
3. Each test shows ✓ (pass) or ✗ (fail) with checksum verification

## Large Uploads (SPA Improvement Plan)

To make 6+ GB uploads resilient in the browser, we should move from single-request
uploads to **chunked, resumable uploads** with retries:

1) **Chunked upload protocol**
   - Split files into fixed chunks (e.g., 16 MB).
   - Send `Content-Range` and a stable upload ID per chunk.
   - Proxy streams/assembles parts into S3 multipart uploads.

2) **Resumable retries**
   - Retry failed chunks with exponential backoff.
   - Track completed parts locally and resume after interruption.

3) **Progress & recovery**
   - Update UI progress by bytes accepted per chunk.
   - On failure, show which part failed and allow “resume”.

4) **Backend alignment**
   - Ensure part size ≥ 5 MB and total parts ≤ 10,000.
   - Proxy already supports multipart uploads; we can extend the API to accept chunked uploads.

## Browser SDK API

The demo includes an inline implementation of the browser LFS SDK:

```javascript
// Create producer
const producer = new LfsProducer({
  endpoint: 'http://localhost:8080/lfs/produce',
  timeout: 300000,  // 5 minutes
  retries: 3,
});

// Upload with progress
const envelope = await producer.produce('my-topic', file, {
  key: file.name,
  headers: { 'Content-Type': file.type },
  onProgress: (p) => console.log(`${p.percent}% uploaded`)
});

console.log('Blob stored:', envelope.key);
console.log('SHA-256:', envelope.sha256);
```

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Browser (localhost:3000)                                │
│  ┌────────────────────────────────────────────────────┐  │
│  │  index.html                                        │  │
│  │  ├── File Input (drag & drop)                      │  │
│  │  ├── Progress Bar (XHR upload.onprogress)          │  │
│  │  └── LfsProducer (inline SDK)                      │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
                           │
                    POST /lfs/produce
                    X-Kafka-Topic: browser-uploads
                    X-Kafka-Key: filename.ext
                           │
                           ▼
              ┌────────────────────────┐
              │ LFS Proxy :8080        │
              │ ├── Upload to S3       │
              │ └── Send pointer to    │
              │     Kafka              │
              └────────────────────────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
         ┌─────────┐             ┌─────────────┐
         │ MinIO   │             │ Kafka       │
         │ (blob)  │             │ (envelope)  │
         └─────────┘             └─────────────┘
```

## CORS Configuration

For production deployments, enable CORS on the LFS proxy:

```yaml
# Helm values
lfsProxy:
  http:
    cors:
      enabled: true
      allowOrigins: ["https://app.example.com"]
      allowMethods: ["POST", "OPTIONS"]
      allowHeaders: ["X-Kafka-Topic", "X-Kafka-Key", "Content-Type", "X-Request-ID"]
```

## Files

| File | Description |
|------|-------------|
| `index.html` | SPA with inline SDK and demo UI |
| `PLAN.md` | Architecture and implementation plan |
| `README.md` | This documentation |
| `Makefile` | Build and run targets |

## Environment Variables

The demo uses browser-based configuration via the UI. For automated testing:

| Variable | Default | Description |
|----------|---------|-------------|
| Browser URL | `http://localhost:3000` | Demo server address |
| LFS Endpoint | `http://localhost:8080/lfs/produce` | LFS proxy HTTP endpoint |
| Topic | `browser-uploads` | Kafka topic for uploads |

## Troubleshooting

### CORS Error

If you see `Access-Control-Allow-Origin` errors:
- Ensure the LFS proxy is running with CORS enabled
- Check that the origin matches the allowed origins

### Network Error

If uploads fail with "Network error":
- Verify the LFS proxy is reachable: `curl http://localhost:8080/readyz`
- Check port-forwarding is active

### Upload Timeout

For very large files:
- Increase the `timeout` option in LfsProducer
- Check network bandwidth to LFS proxy
