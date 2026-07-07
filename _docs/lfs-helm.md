---
layout: doc
title: LFS Helm Deployment
description: Deploy and configure LFS via the KafscaleCluster CRD and Helm chart.
permalink: /lfs-helm/
nav_title: LFS Helm
nav_order: 2
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

# LFS on Kubernetes

Large File Support (LFS) uses the claim-check pattern: blobs land in S3 and Kafka
carries a pointer. The **operator** deploys an LFS proxy when you enable
`spec.lfsProxy` on your `KafscaleCluster`.

Standalone Helm `lfsProxy.*` values were removed in v1.6. Configure LFS on the
cluster CRD, not via chart values.

## Prerequisites

- Kubernetes 1.24+
- Helm 3.x
- S3-compatible storage (AWS S3 or MinIO)
- KafScale operator installed

## Installation

1. Install the Helm chart (operator + console):

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  -n kafscale --create-namespace
```

2. Apply a `KafscaleCluster` with LFS enabled:

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafscaleCluster
metadata:
  name: kafscale
  namespace: kafscale
spec:
  lfsProxy:
    enabled: true
    http:
      enabled: true
      port: 8080
    s3:
      bucket: my-bucket
      region: us-east-1
      credentialsSecretRef: s3-credentials
  s3:
    bucket: kafscale
    region: us-east-1
    credentialsSecretRef: kafscale-s3-credentials
```

`spec.lfsProxy.s3.bucket` and `region` are required. The bucket name
`kafscale-lfs` is permanently blocklisted at startup — use your own name.

## LFS demo UI (optional)

The chart can deploy a browser demo UI via `lfsDemos`:

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  -f deploy/helm/kafscale/values-lfs-demo.yaml
```

This enables the demo UI only. LFS itself still requires `spec.lfsProxy` on your
`KafscaleCluster`.

## Core CRD settings

```yaml
spec:
  lfsProxy:
    enabled: true
    replicas: 1
    http:
      enabled: true
      port: 8080
      apiKeySecretRef: lfs-api-key
    s3:
      bucket: my-bucket
      region: us-east-1
      forcePathStyle: true   # Required for MinIO
      credentialsSecretRef: s3-credentials
    service:
      type: ClusterIP
      port: 9092
    metrics:
      enabled: true
      port: 9095
```

## HTTP API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/lfs/produce` | POST | Upload blob to S3, produce pointer to Kafka |
| `/lfs/download` | POST | Get presigned URL or stream blob from S3 |
| `/readyz` | GET | Readiness probe |
| `/livez` | GET | Liveness probe |

OpenAPI spec: [`api/lfs-proxy/openapi.yaml`](https://github.com/KafScale/platform/blob/main/api/lfs-proxy/openapi.yaml)

## Monitoring

When `spec.lfsProxy.metrics.enabled` is true, the operator exposes a metrics
Service on the configured port.

| Metric | Type | Description |
|--------|------|-------------|
| `lfs_uploads_total` | Counter | Total upload operations |
| `lfs_downloads_total` | Counter | Total download operations |
| `lfs_upload_bytes_total` | Counter | Total bytes uploaded |
| `lfs_upload_duration_seconds` | Histogram | Upload latency |
| `lfs_s3_operations_total` | Counter | S3 operations by type |
| `lfs_s3_errors_total` | Counter | S3 operation failures |

## Local development with Docker Compose

For local development without Kubernetes:

```bash
cd deploy/docker-compose
docker compose up -d
```

This starts MinIO, a KafScale broker, and the LFS proxy with pre-configured defaults.