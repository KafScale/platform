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

This document describes how to deploy the LFS Proxy using the KafScale Helm chart.

## Overview

The LFS Proxy is deployed as part of the KafScale Helm chart and provides:

- **Kafka Protocol Support**: Transparent claim-check pattern for large messages via Kafka protocol (port 9092)
- **HTTP API**: RESTful endpoint for browser and SDK uploads (port 8080)
- **S3 Storage**: Configurable S3-compatible object storage backend
- **CORS Support**: Configurable cross-origin resource sharing for browser access
- **Metrics**: Prometheus-compatible metrics endpoint (port 9095)
- **Health Checks**: Kubernetes readiness/liveness probes (port 9094)

## Quick Start

### Basic Deployment

```bash
helm upgrade --install kafscale ./deploy/helm/kafscale \
  --set lfsProxy.enabled=true \
  --set lfsProxy.http.enabled=true \
  --set lfsProxy.s3.bucket=my-bucket \
  --set lfsProxy.s3.endpoint=http://minio:9000 \
  --set lfsProxy.s3.accessKey=minioadmin \
  --set lfsProxy.s3.secretKey=minioadmin
```

### Demo Stack with Browser UI

```bash
helm upgrade --install kafscale ./deploy/helm/kafscale \
  -n kafscale-demo --create-namespace \
  -f ./deploy/helm/kafscale/values-lfs-demo.yaml \
  --set lfsProxy.s3.endpoint=http://minio:9000 \
  --set lfsProxy.s3.accessKey=minioadmin \
  --set lfsProxy.s3.secretKey=minioadmin
```

## Configuration Reference

### LFS Proxy Settings (`lfsProxy.*`)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `lfsProxy.enabled` | Enable LFS Proxy deployment | `false` |
| `lfsProxy.replicaCount` | Number of replicas | `2` |
| `lfsProxy.image.repository` | Image repository | `ghcr.io/kafscale/kafscale-lfs-proxy` |
| `lfsProxy.image.tag` | Image tag (defaults to chart appVersion) | `""` |
| `lfsProxy.image.pullPolicy` | Image pull policy | `IfNotPresent` |

### HTTP API Settings (`lfsProxy.http.*`)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `lfsProxy.http.enabled` | Enable HTTP API endpoint | `false` |
| `lfsProxy.http.port` | HTTP API port | `8080` |
| `lfsProxy.http.apiKey` | API key for authentication (optional) | `""` |

### CORS Settings (`lfsProxy.http.cors.*`)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `lfsProxy.http.cors.enabled` | Enable CORS headers | `false` |
| `lfsProxy.http.cors.allowOrigins` | Allowed origins | `["*"]` |
| `lfsProxy.http.cors.allowMethods` | Allowed HTTP methods | `["POST", "OPTIONS"]` |
| `lfsProxy.http.cors.allowHeaders` | Allowed request headers | See values.yaml |
| `lfsProxy.http.cors.exposeHeaders` | Exposed response headers | `["X-Request-ID"]` |

### S3 Storage Settings (`lfsProxy.s3.*`)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `lfsProxy.s3.bucket` | S3 bucket name | `""` |
| `lfsProxy.s3.region` | S3 region | `""` |
| `lfsProxy.s3.endpoint` | S3 endpoint URL (for MinIO/custom) | `""` |
| `lfsProxy.s3.existingSecret` | Existing secret with credentials | `""` |
| `lfsProxy.s3.accessKey` | S3 access key (use existingSecret in prod) | `""` |
| `lfsProxy.s3.secretKey` | S3 secret key (use existingSecret in prod) | `""` |
| `lfsProxy.s3.forcePathStyle` | Force path-style URLs (required for MinIO) | `false` |
| `lfsProxy.s3.ensureBucket` | Create bucket if not exists | `false` |
| `lfsProxy.s3.maxBlobSize` | Maximum blob size in bytes | `5368709120` (5GB) |
| `lfsProxy.s3.chunkSize` | Multipart upload chunk size | `5242880` (5MB) |

### Ingress Settings (`lfsProxy.ingress.*`)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `lfsProxy.ingress.enabled` | Enable HTTP ingress | `false` |
| `lfsProxy.ingress.className` | Ingress class name | `""` |
| `lfsProxy.ingress.annotations` | Ingress annotations | `{}` |
| `lfsProxy.ingress.hosts` | Ingress host configuration | See values.yaml |
| `lfsProxy.ingress.tls` | TLS configuration | `[]` |

### Service Settings (`lfsProxy.service.*`)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `lfsProxy.service.type` | Service type | `ClusterIP` |
| `lfsProxy.service.port` | Kafka protocol port | `9092` |
| `lfsProxy.service.annotations` | Service annotations | `{}` |

### Health & Metrics (`lfsProxy.health.*`, `lfsProxy.metrics.*`)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `lfsProxy.health.enabled` | Enable health endpoints | `true` |
| `lfsProxy.health.port` | Health check port | `9094` |
| `lfsProxy.metrics.enabled` | Enable Prometheus metrics | `true` |
| `lfsProxy.metrics.port` | Metrics port | `9095` |
| `lfsProxy.metrics.serviceMonitor.enabled` | Create ServiceMonitor | `false` |

## LFS Demo Settings (`lfsDemos.*`)

The Helm chart includes optional browser-based demos for testing and demonstration.

### E72 Browser Demo (`lfsDemos.e72Browser.*`)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `lfsDemos.enabled` | Enable LFS demos | `false` |
| `lfsDemos.e72Browser.enabled` | Enable E72 browser demo | `true` |
| `lfsDemos.e72Browser.lfsProxyEndpoint` | LFS proxy endpoint (auto-detected) | `""` |
| `lfsDemos.e72Browser.defaultTopic` | Default Kafka topic | `browser-uploads` |
| `lfsDemos.e72Browser.service.type` | Service type | `NodePort` |
| `lfsDemos.e72Browser.service.port` | Service port | `80` |
| `lfsDemos.e72Browser.service.nodePort` | NodePort (when type=NodePort) | `30072` |

## Deployment Scenarios

### Production with External S3

```yaml
# values-production.yaml
lfsProxy:
  enabled: true
  replicaCount: 3

  http:
    enabled: true
    apiKey: "your-secure-api-key"
    cors:
      enabled: true
      allowOrigins: ["https://app.example.com"]

  s3:
    bucket: production-lfs-bucket
    region: us-west-2
    existingSecret: lfs-s3-credentials  # Secret with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

  ingress:
    enabled: true
    className: nginx
    annotations:
      cert-manager.io/cluster-issuer: letsencrypt
    hosts:
      - host: lfs.example.com
        paths:
          - path: /lfs
            pathType: Prefix
    tls:
      - secretName: lfs-tls
        hosts:
          - lfs.example.com

  resources:
    requests:
      cpu: 500m
      memory: 512Mi
    limits:
      cpu: 2000m
      memory: 2Gi
```

### Development with MinIO

```yaml
# values-dev.yaml
lfsProxy:
  enabled: true
  replicaCount: 1

  http:
    enabled: true
    cors:
      enabled: true
      allowOrigins: ["*"]

  s3:
    bucket: dev-lfs
    endpoint: http://minio:9000
    accessKey: minioadmin
    secretKey: minioadmin
    forcePathStyle: true
    ensureBucket: true

lfsDemos:
  enabled: true
  e72Browser:
    enabled: true
```

### Air-Gapped / On-Premises

```yaml
# values-airgap.yaml
imagePullSecrets:
  - private-registry-secret

lfsProxy:
  enabled: true
  image:
    repository: registry.internal/kafscale/kafscale-lfs-proxy
    tag: v1.5.0

  http:
    enabled: true

  s3:
    bucket: lfs-storage
    endpoint: https://s3.internal.example.com
    existingSecret: s3-credentials
    forcePathStyle: true
```

## Accessing the LFS Proxy

### Port Forwarding (Development)

```bash
# LFS Proxy HTTP API
kubectl port-forward svc/kafscale-lfs-proxy 8080:8080 &

# E72 Browser Demo
kubectl port-forward svc/kafscale-lfs-demo-e72 3000:80 &

# Open browser demo
open http://localhost:3000
```

### NodePort Access

When using `service.type: NodePort`:

```bash
# Get node IP
NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')

# Access E72 demo (default NodePort 30072)
open http://${NODE_IP}:30072
```

### Ingress Access

With ingress enabled:

```bash
# Assuming DNS is configured
curl https://lfs.example.com/readyz
```

## Security Considerations

### Credentials Management

**Never commit credentials to version control.** Use one of these approaches:

1. **Existing Secret** (Recommended):
   ```bash
   kubectl create secret generic lfs-s3-credentials \
     --from-literal=AWS_ACCESS_KEY_ID=your-key \
     --from-literal=AWS_SECRET_ACCESS_KEY=your-secret
   ```
   ```yaml
   lfsProxy:
     s3:
       existingSecret: lfs-s3-credentials
   ```

2. **Helm --set flags**:
   ```bash
   helm upgrade --install kafscale ./deploy/helm/kafscale \
     --set lfsProxy.s3.accessKey=$AWS_ACCESS_KEY_ID \
     --set lfsProxy.s3.secretKey=$AWS_SECRET_ACCESS_KEY
   ```

### API Key Authentication

For production HTTP endpoints:

```yaml
lfsProxy:
  http:
    enabled: true
    apiKey: "generate-a-strong-random-key"
```

Clients must include the header: `X-API-Key: your-key`

### CORS Configuration

Restrict origins in production:

```yaml
lfsProxy:
  http:
    cors:
      enabled: true
      allowOrigins:
        - "https://app.example.com"
        - "https://admin.example.com"
```

## Monitoring

### Prometheus Metrics

Enable ServiceMonitor for Prometheus Operator:

```yaml
lfsProxy:
  metrics:
    enabled: true
    serviceMonitor:
      enabled: true
      interval: 30s
      labels:
        release: prometheus
```

### Key Metrics

| Metric | Description |
|--------|-------------|
| `lfs_proxy_uploads_total` | Total upload count |
| `lfs_proxy_upload_bytes_total` | Total bytes uploaded |
| `lfs_proxy_upload_duration_seconds` | Upload duration histogram |
| `lfs_proxy_downloads_total` | Total download count |
| `lfs_proxy_s3_operations_total` | S3 operation count by type |

### Health Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/readyz` | Kubernetes readiness probe |
| `/livez` | Kubernetes liveness probe |
| `/metrics` | Prometheus metrics (port 9095) |

## Troubleshooting

### Common Issues

**Upload fails with CORS error:**
- Ensure `lfsProxy.http.cors.enabled: true`
- Check `allowOrigins` includes your domain
- Verify `allowHeaders` includes all required headers

**S3 connection fails:**
- Check `s3.endpoint` is reachable from the cluster
- Verify credentials are correct
- For MinIO, ensure `forcePathStyle: true`

**Large uploads fail:**
- Check `s3.maxBlobSize` is sufficient
- Verify S3 bucket policy allows multipart uploads
- Check network timeouts

### Debug Commands

```bash
# Check pod logs
kubectl logs -l app.kubernetes.io/component=lfs-proxy -f

# Test health endpoint
kubectl exec -it deploy/kafscale-lfs-proxy -- wget -qO- http://localhost:9094/readyz

# Check S3 connectivity
kubectl exec -it deploy/kafscale-lfs-proxy -- wget -qO- http://minio:9000/minio/health/live
```

## API Documentation (OpenAPI/Swagger)

The LFS Proxy HTTP API is fully documented using **OpenAPI 3.0**.

### Specification Location

| Resource | Path |
|----------|------|
| **OpenAPI Spec** | [`api/lfs-proxy/openapi.yaml`](../../api/lfs-proxy/openapi.yaml) |

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/lfs/produce` | `POST` | Upload blob to S3, produce pointer record to Kafka |
| `/lfs/download` | `POST` | Get presigned URL or stream blob from S3 |
| `/readyz` | `GET` | Kubernetes readiness probe (port 9094) |
| `/livez` | `GET` | Kubernetes liveness probe (port 9094) |
| `/metrics` | `GET` | Prometheus metrics (port 9095) |

### Viewing the Spec

**Option 1: Swagger Editor (online)**
1. Open [editor.swagger.io](https://editor.swagger.io)
2. File → Import URL or paste contents of `api/lfs-proxy/openapi.yaml`

**Option 2: Swagger UI (Docker)**
```bash
docker run -p 8081:8080 \
  -e SWAGGER_JSON=/spec/openapi.yaml \
  -v $(pwd)/api/lfs-proxy:/spec \
  swaggerapi/swagger-ui

# Open http://localhost:8081
```

**Option 3: Redoc (Docker)**
```bash
docker run -p 8081:80 \
  -v $(pwd)/api/lfs-proxy/openapi.yaml:/usr/share/nginx/html/openapi.yaml \
  -e SPEC_URL=openapi.yaml \
  redocly/redoc

# Open http://localhost:8081
```

### Request Headers

| Header | Required | Description |
|--------|----------|-------------|
| `X-Kafka-Topic` | Yes | Target Kafka topic |
| `X-Kafka-Key` | No | Base64-encoded message key |
| `X-Kafka-Partition` | No | Target partition (int32) |
| `X-LFS-Checksum` | No | Expected checksum value |
| `X-LFS-Checksum-Alg` | No | Checksum algorithm: `sha256`, `md5`, `crc32`, `none` |
| `X-Request-ID` | No | Request correlation ID (auto-generated if missing) |
| `X-API-Key` | Conditional | API key (if `http.apiKey` configured) |
| `Content-Type` | No | MIME type stored with blob |

### Response: LFS Envelope

```json
{
  "kfs_lfs": 1,
  "bucket": "kafscale",
  "key": "default/my-topic/lfs/2026/02/04/abc123.bin",
  "size": 104857600,
  "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "checksum": "e3b0c44298fc1c14...",
  "checksum_alg": "sha256",
  "content_type": "video/mp4",
  "created_at": "2026-02-04T12:00:00Z",
  "proxy_id": "lfs-proxy-abc123"
}
```

## Related Documentation

- [Data Flow](data-flow.md) - End-to-end write/read flows
- [SDK Solution](sdk-solution.md) - Client SDK design
- [Security Tasks](security-tasks.md) - Security hardening plan
- [OWASP Report](OWASP-Hardening-Report.md) - Security inspection notes
- [OpenAPI Spec](../../api/lfs-proxy/openapi.yaml) - HTTP API specification
