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

# KafScale Helm Chart

Helm chart for deploying KafScale components including the operator, console, proxy, LFS proxy, and MCP server.

## Prerequisites

- Kubernetes 1.24+
- Helm 3.x
- (Optional) Prometheus Operator for ServiceMonitor resources

## Installation

### Add the repository (if published)

```bash
helm repo add kafscale https://charts.kafscale.io
helm repo update
```

### Install from local chart

```bash
helm upgrade --install kafscale ./deploy/helm/kafscale \
  -n kafscale-system --create-namespace
```

## Components

| Component | Description | Default |
|-----------|-------------|---------|
| **Operator** | KafScale cluster operator | Enabled |
| **Console** | Web-based management UI | Enabled |
| **Proxy** | Kafka protocol proxy | Disabled |
| **LFS Proxy** | Large File Support proxy | Disabled |
| **MCP** | Model Context Protocol server | Disabled |

## Quick Start Examples

### Minimal Installation

```bash
helm upgrade --install kafscale ./deploy/helm/kafscale
```

### With LFS Proxy and MinIO

```bash
helm upgrade --install kafscale ./deploy/helm/kafscale \
  --set lfsProxy.enabled=true \
  --set lfsProxy.http.enabled=true \
  --set lfsProxy.s3.bucket=kafscale \
  --set lfsProxy.s3.endpoint=http://minio:9000 \
  --set lfsProxy.s3.accessKey=minioadmin \
  --set lfsProxy.s3.secretKey=minioadmin \
  --set lfsProxy.s3.forcePathStyle=true
```

### LFS Demo Stack

Deploy the full LFS demo stack with browser UI:

```bash
helm upgrade --install kafscale ./deploy/helm/kafscale \
  -n kafscale-demo --create-namespace \
  -f ./deploy/helm/kafscale/values-lfs-demo.yaml \
  --set lfsProxy.s3.endpoint=http://minio:9000 \
  --set lfsProxy.s3.accessKey=minioadmin \
  --set lfsProxy.s3.secretKey=minioadmin
```

## Values Files

| File | Description |
|------|-------------|
| `values.yaml` | Default values (production-ready defaults) |
| `values-lfs-demo.yaml` | LFS demo stack with browser UI enabled |

## Configuration

See [values.yaml](values.yaml) for the full list of configurable parameters.

### Key Sections

| Section | Description |
|---------|-------------|
| `operator.*` | KafScale operator settings |
| `console.*` | Console UI settings |
| `proxy.*` | Kafka proxy settings |
| `lfsProxy.*` | LFS proxy settings |
| `lfsProxy.http.*` | HTTP API settings |
| `lfsProxy.http.cors.*` | CORS configuration |
| `lfsProxy.s3.*` | S3 storage backend |
| `lfsProxy.ingress.*` | HTTP ingress |
| `lfsDemos.*` | Demo applications |
| `mcp.*` | MCP server settings |

## LFS Proxy

The LFS Proxy implements the claim-check pattern for large Kafka messages:

```
┌─────────┐     ┌───────────┐     ┌─────────┐
│ Client  │────▶│ LFS Proxy │────▶│   S3    │
│ (SDK)   │     │           │     │ (blob)  │
└─────────┘     └─────┬─────┘     └─────────┘
                      │
                      ▼
                ┌───────────┐
                │   Kafka   │
                │ (pointer) │
                └───────────┘
```

### Enable HTTP API

```yaml
lfsProxy:
  enabled: true
  http:
    enabled: true
    port: 8080
    cors:
      enabled: true
      allowOrigins: ["*"]
```

### S3 Configuration

```yaml
lfsProxy:
  s3:
    bucket: my-lfs-bucket
    region: us-east-1
    endpoint: ""  # Leave empty for AWS S3
    existingSecret: s3-credentials  # Recommended for production
```

For detailed LFS proxy documentation, see [docs/lfs-proxy/helm-deployment.md](../../../docs/lfs-proxy/helm-deployment.md).

### HTTP API Specification (OpenAPI/Swagger)

The LFS Proxy HTTP API is documented using OpenAPI 3.0:

| Resource | Location |
|----------|----------|
| **OpenAPI Spec** | [`api/lfs-proxy/openapi.yaml`](../../../api/lfs-proxy/openapi.yaml) |
| **Swagger UI** | Import the spec into [Swagger Editor](https://editor.swagger.io) or [Stoplight](https://stoplight.io) |

**API Endpoints:**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/lfs/produce` | POST | Upload blob to S3, produce pointer to Kafka |
| `/lfs/download` | POST | Get presigned URL or stream blob from S3 |
| `/readyz` | GET | Kubernetes readiness probe |
| `/livez` | GET | Kubernetes liveness probe |
| `/metrics` | GET | Prometheus metrics (port 9095) |

**Example: View API spec locally:**
```bash
# Using Swagger UI Docker
docker run -p 8081:8080 -e SWAGGER_JSON=/spec/openapi.yaml \
  -v $(pwd)/api/lfs-proxy:/spec swaggerapi/swagger-ui

# Open http://localhost:8081
```

## Browser Demo (E72)

The E72 browser demo provides a web UI for testing LFS uploads:

```yaml
lfsDemos:
  enabled: true
  e72Browser:
    enabled: true
    service:
      type: NodePort
      nodePort: 30072
```

Access via: `http://<node-ip>:30072`

## Local Registry (Stage Release)

For air-gapped or LAN installs, you can publish images to a local registry (for example `192.168.0.131:5100`) and point the chart at it.

### 1) Configure Docker to allow the registry (insecure HTTP)

Docker Desktop on macOS:

1. Open Docker Desktop → Settings → Docker Engine.
2. Add the registry under `insecure-registries`:
   ```json
   {
     "insecure-registries": ["192.168.0.131:5100"]
   }
   ```
3. Apply & Restart Docker.

Verify:
```bash
docker info | grep -n "Insecure Registries"
docker info | grep -n "192.168.0.131"
```

### 2) Push images to the registry

Use the stage release target (local buildx):
```bash
make stage-release STAGE_REGISTRY=192.168.0.131:5100 STAGE_TAG=dev
```

If you want to run the GitHub Actions workflow locally instead, use:
```bash
make stage-release-act STAGE_REGISTRY=192.168.0.131:5100 STAGE_TAG=dev
```
This target builds a local `act` runner image first (`make act-image`) and executes the workflow inside that container.

### 3) Install the chart using the staged registry

```bash
helm upgrade --install kafscale ./deploy/helm/kafscale \
  -n kafscale-demo --create-namespace \
  --set global.imageRegistry=192.168.0.131:5100
```

Note: if you set `global.imageRegistry`, individual component image repositories inherit it.

## Monitoring

### Enable ServiceMonitor

```yaml
lfsProxy:
  metrics:
    enabled: true
    serviceMonitor:
      enabled: true
      interval: 30s
```

### Enable PrometheusRule

```yaml
lfsProxy:
  metrics:
    prometheusRule:
      enabled: true
```

## Security

### Credentials Best Practices

1. **Use existing secrets** instead of inline values:
   ```bash
   kubectl create secret generic s3-creds \
     --from-literal=AWS_ACCESS_KEY_ID=xxx \
     --from-literal=AWS_SECRET_ACCESS_KEY=xxx
   ```
   ```yaml
   lfsProxy:
     s3:
       existingSecret: s3-creds
   ```

2. **Enable API key** for HTTP endpoints:
   ```yaml
   lfsProxy:
     http:
       apiKey: "your-secure-key"
   ```

3. **Restrict CORS origins** in production:
   ```yaml
   lfsProxy:
     http:
       cors:
         allowOrigins: ["https://app.example.com"]
   ```

## Uninstall

```bash
helm uninstall kafscale -n kafscale-system
```

## Documentation

- [LFS Proxy Helm Deployment](../../../docs/lfs-proxy/helm-deployment.md)
- [LFS Proxy Data Flow](../../../docs/lfs-proxy/data-flow.md)
- [LFS SDK Documentation](../../../docs/lfs-proxy/sdk-solution.md)
- [Operations Guide](../../../docs/operations.md)
