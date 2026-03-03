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

# E72 Browser-Native LFS SDK Plan

## Overview

Create a pure browser-native JavaScript/TypeScript SDK for LFS that:
- Uses only browser APIs (no Node.js dependencies)
- No librdkafka or native Kafka client
- Works with `fetch()` API for HTTP and S3

## Why Browser-Native?

| Current JS SDK | Browser SDK (E72) |
|----------------|-------------------|
| `undici` (Node.js HTTP) | Native `fetch()` |
| `@aws-sdk/client-s3` (Node.js) | Pre-signed URLs or fetch-based S3 |
| `@confluentinc/kafka-javascript` (librdkafka) | ❌ Not needed - HTTP only |
| Runs in Node.js only | Runs in browsers + Node.js 18+ |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Browser Application                     │
├─────────────────────────────────────────────────────────────┤
│  @kafscale/lfs-browser-sdk                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ LfsProducer │  │ LfsResolver │  │ LfsEnvelope (types) │  │
│  │ (fetch API) │  │ (fetch/S3)  │  │                     │  │
│  └──────┬──────┘  └──────┬──────┘  └─────────────────────┘  │
└─────────┼────────────────┼──────────────────────────────────┘
          │                │
          ▼                ▼
    ┌─────────────┐  ┌─────────────┐
    │ LFS Proxy   │  │ S3/MinIO    │
    │ /lfs/produce│  │ (CORS or    │
    │ HTTP POST   │  │ pre-signed) │
    └─────────────┘  └─────────────┘
```

## SDK Components

### 1. LfsProducer (Browser-Compatible)

```typescript
// lfs-client-sdk/js-browser/src/producer.ts

export interface LfsProducerOptions {
  endpoint: string;           // e.g., "https://lfs-proxy.example.com/lfs/produce"
  timeout?: number;           // Request timeout (ms)
  retries?: number;           // Retry count for transient errors
  onProgress?: (progress: UploadProgress) => void;
}

export interface UploadProgress {
  loaded: number;
  total: number;
  percent: number;
}

export class LfsProducer {
  constructor(options: LfsProducerOptions);

  async produce(
    topic: string,
    payload: Blob | ArrayBuffer | File,
    options?: ProduceOptions
  ): Promise<LfsEnvelope>;
}
```

**Key Features:**
- Uses `fetch()` with `ReadableStream` for progress tracking
- Retry with exponential backoff
- Supports `File` objects for drag-and-drop uploads
- No Node.js dependencies

### 2. LfsResolver (Pre-Signed URL Pattern)

```typescript
// lfs-client-sdk/js-browser/src/resolver.ts

export interface ResolverOptions {
  // Function to get pre-signed URL for an S3 key
  getPresignedUrl: (key: string, bucket: string) => Promise<string>;
  validateChecksum?: boolean;
  maxSize?: number;
}

export class LfsResolver {
  constructor(options: ResolverOptions);

  async resolve(envelopeJson: string | Uint8Array): Promise<ResolvedRecord>;
}
```

**Why Pre-Signed URLs?**
- S3 CORS is complex to configure
- Pre-signed URLs are the standard browser pattern
- Backend generates URLs, browser downloads directly
- Works with any S3-compatible storage

### 3. LfsEnvelope (Types Only)

```typescript
// lfs-client-sdk/js-browser/src/envelope.ts

export interface LfsEnvelope {
  kfs_lfs: number;
  bucket: string;
  key: string;
  size: number;
  sha256: string;
  checksum?: string;
  checksum_alg?: string;
  content_type?: string;
  original_headers?: Record<string, string>;
  created_at?: string;
  proxy_id?: string;
}

export function isLfsEnvelope(data: unknown): data is LfsEnvelope;
export function decodeLfsEnvelope(data: string | Uint8Array): LfsEnvelope;
```

## Browser-Specific Considerations

### 1. CORS Configuration

LFS Proxy needs CORS headers for browser access:

```yaml
# Helm values
lfsProxy:
  http:
    cors:
      enabled: true
      allowOrigins: ["https://app.example.com"]
      allowMethods: ["POST", "OPTIONS"]
      allowHeaders: ["X-Kafka-Topic", "X-Kafka-Key", "Content-Type"]
```

### 2. Large File Uploads

For files > 100MB, use streaming upload with progress:

```typescript
const producer = new LfsProducer({
  endpoint: 'https://lfs.example.com/lfs/produce',
  onProgress: (p) => console.log(`${p.percent}% uploaded`)
});

// Drag-and-drop file upload
const envelope = await producer.produce('uploads', file, {
  headers: { 'Content-Type': file.type }
});
```

### 3. Consuming in Browsers

Browsers can't connect to Kafka directly. Options:

| Approach | Description | Use Case |
|----------|-------------|----------|
| REST API | Backend exposes `/api/messages` | Simple polling |
| WebSocket | Real-time message push | Live dashboards |
| SSE | Server-sent events | One-way streaming |
| Pre-fetched | Backend sends envelope in response | Request-response apps |

The SDK provides **envelope resolution**, not Kafka consumption:

```typescript
// Backend returns envelope JSON in API response
const response = await fetch('/api/latest-video');
const envelope = await response.json();

// SDK resolves the blob
const record = await resolver.resolve(envelope);
console.log('Video bytes:', record.payload.length);
```

## Package Structure

```
lfs-client-sdk/js-browser/
├── package.json          # No Node.js deps, "type": "module"
├── tsconfig.json         # ES2020 target, DOM lib
├── src/
│   ├── index.ts          # Main exports
│   ├── producer.ts       # fetch-based producer
│   ├── resolver.ts       # Pre-signed URL resolver
│   ├── envelope.ts       # Types + detection
│   └── sha256.ts         # SubtleCrypto SHA-256
├── dist/                 # ESM + UMD bundles
│   ├── index.esm.js      # ES modules (tree-shakeable)
│   ├── index.umd.js      # UMD for script tags
│   └── index.d.ts        # TypeScript types
└── examples/
    └── browser-upload.html
```

## Demo (E72)

### Files to Create

| File | Purpose |
|------|---------|
| `examples/E72_browser-lfs-sdk-demo/index.html` | Simple upload form |
| `examples/E72_browser-lfs-sdk-demo/demo.ts` | Demo TypeScript |
| `examples/E72_browser-lfs-sdk-demo/serve.sh` | Local dev server |
| `examples/E72_browser-lfs-sdk-demo/README.md` | Documentation |

### Demo Features

1. **Drag-and-drop file upload** with progress bar
2. **Show returned envelope** with S3 key
3. **Resolve and preview** (for images/text)
4. **Error handling** with retry indicator

### Demo Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Browser (localhost:3000)                                │
│  ┌────────────────────────────────────────────────────┐  │
│  │  E72 Demo App                                      │  │
│  │  ┌──────────────┐  ┌──────────────┐                │  │
│  │  │ File Input   │  │ Progress Bar │                │  │
│  │  └──────┬───────┘  └──────────────┘                │  │
│  │         │                                          │  │
│  │         ▼                                          │  │
│  │  ┌──────────────────────────────────────────────┐  │  │
│  │  │ @kafscale/lfs-browser-sdk                    │  │  │
│  │  │ LfsProducer.produce(topic, file)             │  │  │
│  │  └──────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │ LFS Proxy :8080        │
              │ POST /lfs/produce      │
              │ (CORS enabled)         │
              └────────────────────────┘
```

## Implementation Tasks

### Phase 1: SDK Core (P0)

| ID | Task | Notes |
|----|------|-------|
| JS-BROWSER-001 | Create `lfs-client-sdk/js-browser/` scaffold | `package.json` with no Node deps |
| JS-BROWSER-002 | Implement `LfsProducer` with fetch | Streaming upload, progress callback |
| JS-BROWSER-003 | Implement `LfsEnvelope` types | Same as existing, browser-safe |
| JS-BROWSER-004 | Implement browser SHA-256 | `crypto.subtle.digest()` |
| JS-BROWSER-005 | Implement `LfsResolver` | Pre-signed URL pattern |
| JS-BROWSER-006 | Add retry/backoff | Same logic as Python/Java |

### Phase 2: Build & Bundle (P1)

| ID | Task | Notes |
|----|------|-------|
| JS-BROWSER-010 | Configure esbuild/rollup | ESM + UMD outputs |
| JS-BROWSER-011 | Generate TypeScript declarations | `index.d.ts` |
| JS-BROWSER-012 | Add sourcemaps | For debugging |
| JS-BROWSER-013 | Minified production build | `index.min.js` |

### Phase 3: Demo (P1)

| ID | Task | Notes |
|----|------|-------|
| JS-BROWSER-020 | Create E72 demo HTML | Drag-drop upload form |
| JS-BROWSER-021 | Add progress visualization | CSS progress bar |
| JS-BROWSER-022 | Add LFS proxy CORS support | Helm + handler update |
| JS-BROWSER-023 | Document prerequisites | CORS, proxy setup |

### Phase 4: Testing (P2)

| ID | Task | Notes |
|----|------|-------|
| JS-BROWSER-030 | Unit tests (Vitest) | Browser-compatible test runner |
| JS-BROWSER-031 | E2E with Playwright | Automated browser tests |
| JS-BROWSER-032 | Bundle size check | < 10KB gzipped goal |

## Dependencies

```json
{
  "name": "@kafscale/lfs-browser-sdk",
  "version": "0.1.0",
  "type": "module",
  "exports": {
    ".": {
      "import": "./dist/index.esm.js",
      "require": "./dist/index.umd.js",
      "types": "./dist/index.d.ts"
    }
  },
  "devDependencies": {
    "typescript": "^5.6.0",
    "esbuild": "^0.24.0",
    "vitest": "^2.0.0"
  }
}
```

**Zero runtime dependencies** - uses only browser APIs.

## Open Questions

1. **CORS on LFS Proxy**: Add optional CORS middleware?
2. **Pre-signed URL backend**: Provide example Express/FastAPI handler?
3. **WebSocket consumer**: Out of scope for SDK, but document pattern?
4. **React/Vue hooks**: Future enhancement or separate package?

## Success Criteria

- [ ] SDK works in Chrome, Firefox, Safari, Edge
- [ ] Upload 100MB file with progress callback
- [ ] Resolve envelope and fetch blob via pre-signed URL
- [ ] Bundle size < 10KB gzipped
- [ ] E72 demo runs against `make lfs-demo` stack
