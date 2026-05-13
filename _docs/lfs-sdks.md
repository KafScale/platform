---
layout: doc
title: LFS Client SDKs
description: Multi-language SDKs for producing and consuming large files via KafScale LFS.
permalink: /lfs-sdks/
nav_title: LFS SDKs
nav_order: 3
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

# LFS Client SDKs

KafScale provides LFS client SDKs in four languages. Each SDK handles envelope encoding/decoding, HTTP upload to the LFS proxy, and transparent S3 object resolution.

## Go (built-in)

The Go SDK lives in `pkg/lfs/` and is used internally by the LFS proxy, console, and processors.

```go
import "github.com/KafScale/platform/pkg/lfs"

// Produce a large file
producer := lfs.NewProducer(lfs.ProducerConfig{
    ProxyAddr: "http://localhost:8080",
    Topic:     "demo-topic",
})
err := producer.Upload(ctx, "large-file.bin", fileReader)

// Consume and resolve
consumer := lfs.NewConsumer(lfs.ConsumerConfig{
    S3Bucket:   "kafscale",
    S3Endpoint: "http://localhost:9000",
})
reader, err := consumer.Resolve(ctx, envelope)
```

## Java

Maven-based SDK with retry/backoff and configurable HTTP timeouts.

```xml
<dependency>
    <groupId>org.kafscale</groupId>
    <artifactId>lfs-sdk</artifactId>
    <version>0.1.0</version>
</dependency>
```

```java
import org.kafscale.lfs.LfsProducer;

LfsProducer producer = new LfsProducer.Builder()
    .proxyUrl("http://localhost:8080")
    .topic("demo-topic")
    .retryAttempts(3)
    .connectTimeoutMs(5000)
    .build();

producer.upload("video.mp4", inputStream, "video/mp4");
```

Build from source:

```bash
cd lfs-client-sdk/java
mvn clean package
```

## Python

Pip-installable SDK with retry/backoff and envelope codec.

```bash
pip install lfs-client-sdk/python/
```

```python
from lfs_sdk import LfsProducer

producer = LfsProducer(
    proxy_url="http://localhost:8080",
    topic="demo-topic",
    max_retries=3,
)
producer.upload("scan.dcm", open("scan.dcm", "rb"), content_type="application/dicom")
```

## JavaScript (Node.js)

TypeScript SDK with streaming upload support.

```bash
cd lfs-client-sdk/js
npm install && npm run build
```

```typescript
import { LfsProducer } from '@kafscale/lfs-sdk';

const producer = new LfsProducer({
  proxyUrl: 'http://localhost:8080',
  topic: 'demo-topic',
});
await producer.upload('payload.bin', readStream);
```

## Browser SDK

Lightweight SDK for single-page applications. Uploads files directly from the browser to the LFS proxy HTTP API.

```typescript
import { LfsProducer } from '@kafscale/lfs-browser-sdk';

const producer = new LfsProducer({
  proxyUrl: 'http://lfs.example.com',
  topic: 'uploads',
});

// Upload from a file input
const file = document.getElementById('fileInput').files[0];
await producer.upload(file.name, file);
```

See the E72 browser demo (`examples/E72_browser-lfs-sdk-demo/`) for a complete single-page application.

## Demo applications

| Demo | Language | Description |
|---|---|---|
| E60 | — | Medical imaging LFS design |
| E61 | — | Video streaming LFS design |
| E62 | — | Industrial IoT LFS design |
| E70 | Java | Java SDK producer with video upload |
| E71 | Python | Python SDK video upload |
| E72 | Browser | Browser-native SPA file manager |

Run any demo:

```bash
make lfs-demo          # Core LFS stack
make lfs-demo-medical  # E60 medical demo
make lfs-demo-video    # E61 video demo
```

## Related docs

- [LFS Demos](/lfs-demos/) — Runnable demos for each SDK and industry scenario
- [LFS Proxy](/lfs-proxy/) — Proxy architecture and configuration
- [LFS Helm deployment](/lfs-helm/) — Kubernetes deployment guide
