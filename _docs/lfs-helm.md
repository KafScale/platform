---
layout: doc
title: LFS Helm Deployment
description: Deploy and configure the LFS proxy via the KafScale Helm chart.
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

# LFS Proxy Helm Deployment

The LFS Proxy is deployed as part of the KafScale Helm chart and provides:

- **Kafka Protocol Support**: Transparent claim-check pattern for large messages via Kafka protocol (port 9092)
- **HTTP API**: RESTful endpoint for browser and SDK uploads (port 8080)
- **S3 Storage**: Configurable S3-compatible object storage backend
- **CORS Support**: Configurable cross-origin resource sharing for browser access
- **Metrics**: Prometheus-compatible metrics endpoint (port 9095)

## Prerequisites

- Kubernetes 1.24+
- Helm 3.x
- S3-compatible storage (AWS S3 or MinIO)
- KafScale operator installed

## Installation

```bash
# Install with LFS proxy enabled
helm install kafscale deploy/helm/kafscale \
  -f deploy/helm/kafscale/values-lfs-demo.yaml \
  --set lfsProxy.enabled=true
```

## Helm values

### Core settings

```yaml
lfsProxy:
  enabled: true
  replicas: 1
  image:
    repository: ghcr.io/kafscale/kafscale-lfs-proxy
    tag: latest

  # S3 backend
  s3:
    bucket: kafscale
    region: us-east-1
    endpoint: ""          # Custom endpoint for MinIO
    pathStyle: false       # Set true for MinIO
    credentialsSecretRef: s3-credentials

  # HTTP API
  http:
    port: 8080
    corsOrigins: "*"
    maxUploadSize: 0       # 0 = unlimited

  # Kafka backend
  kafka:
    brokers: kafscale-broker:9092

  # Metrics
  metrics:
    enabled: true
    port: 9095
    serviceMonitor:
      enabled: false
    prometheusRule:
      enabled: false

  # Resources
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: "1"
      memory: 512Mi
```

### TLS configuration

```yaml
lfsProxy:
  tls:
    enabled: false
    certSecretRef: lfs-proxy-tls
  kafka:
    sasl:
      enabled: false
      mechanism: SCRAM-SHA-256
      credentialsSecretRef: kafka-sasl
```

### Ingress

```yaml
lfsProxy:
  ingress:
    enabled: false
    className: nginx
    host: lfs.example.com
    tls:
      enabled: false
      secretName: lfs-tls
```

## Monitoring

When `metrics.serviceMonitor.enabled` is set to `true`, the chart creates a Prometheus `ServiceMonitor` that scrapes the LFS proxy metrics endpoint.

Key metrics:

| Metric | Type | Description |
|---|---|---|
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

This starts MinIO, a KafScale broker, and the LFS proxy with pre-configured defaults. See `deploy/docker-compose/README.md` for details.
