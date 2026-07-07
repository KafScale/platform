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

Helm chart for deploying KafScale components including the operator, console,
Kafka proxy, and MCP server. LFS is configured on the `KafscaleCluster` CRD
(`spec.lfsProxy`) and deployed by the operator.

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
| **Proxy** | Kafka protocol proxy (recommended external entrypoint) | Disabled |
| **MCP** | Model Context Protocol server | Disabled |

## Quick Start Examples

### Minimal Installation

```bash
helm upgrade --install kafscale ./deploy/helm/kafscale
```

### With Kafka Proxy (external access)

```bash
helm upgrade --install kafscale ./deploy/helm/kafscale \
  --set proxy.enabled=true \
  --set proxy.replicaCount=2 \
  --set proxy.service.type=LoadBalancer \
  --set proxy.advertisedHost=kafka.example.com \
  --set proxy.etcdEndpoints[0]=http://kafscale-etcd-client.kafscale.svc.cluster.local:2379
```

### LFS Demo Stack

Deploy the browser demo UI (`lfsDemos`) and enable LFS on your cluster CRD:

```bash
helm upgrade --install kafscale ./deploy/helm/kafscale \
  -n kafscale-demo --create-namespace \
  -f ./deploy/helm/kafscale/values-lfs-demo.yaml
```

Then apply a `KafscaleCluster` with `spec.lfsProxy.enabled=true` and S3 settings.
See the [LFS on KafscaleCluster](#lfs-on-kafscalecluster) section below.

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
| `proxy.*` | Kafka proxy settings (external entrypoint) |
| `lfsDemos.*` | Optional LFS browser demo UI |
| `mcp.*` | MCP server settings |

### Proxy Service

The proxy is the single Kafka entrypoint, so its Service is the one clients connect to.

| Key | Description | Default |
|-----|-------------|---------|
| `proxy.service.type` | Service type (`ClusterIP`, `NodePort`, `LoadBalancer`) | `LoadBalancer` |
| `proxy.service.port` | Service port for the Kafka listener | `9092` |
| `proxy.service.nodePort` | Pin the node port for the Kafka listener. Only rendered when `type == NodePort`; leave empty to let Kubernetes auto-assign | `""` |

When `type` is `NodePort` and `nodePort` is empty, Kubernetes assigns a random
node port, so a fixed host-to-node-port mapping (for example a kind
`hostPort: 9092 -> containerPort: 30092` mapping) never reaches the proxy. Set
`proxy.service.nodePort` to a value in the valid NodePort range `30000-32767`
to pin it. A value outside that range renders fine but is rejected by the API
server at apply time.

```yaml
proxy:
  enabled: true
  service:
    type: NodePort
    nodePort: 30092
```

## LFS on KafscaleCluster

Large File Support (LFS) uses the claim-check pattern: blobs land in S3 and Kafka
carries a pointer. The operator deploys an LFS proxy Deployment when you enable
`spec.lfsProxy` on your `KafscaleCluster` (not via Helm `lfsProxy.*` values —
that chart surface was removed in v1.6).

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

Example (`KafscaleCluster` fragment):

```yaml
spec:
  lfsProxy:
    enabled: true
    http:
      enabled: true
      port: 8080
    s3:
      bucket: my-lfs-bucket
      region: us-east-1
      credentialsSecretRef: s3-credentials
```

`bucket` and `region` are required. Do not use the blocklisted default bucket
name `kafscale-lfs`.

### HTTP API Specification (OpenAPI/Swagger)

| Resource | Location |
|----------|----------|
| **OpenAPI Spec** | [`api/lfs-proxy/openapi.yaml`](../../../api/lfs-proxy/openapi.yaml) |
| **Proxy spec (embedded)** | [`cmd/proxy/openapi.yaml`](../../../cmd/proxy/openapi.yaml) |

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/lfs/produce` | POST | Upload blob to S3, produce pointer to Kafka |
| `/lfs/download` | POST | Get presigned URL or stream blob from S3 |
| `/readyz` | GET | Readiness probe |
| `/livez` | GET | Liveness probe |

For local development without Kubernetes, see
[`deploy/docker-compose/README.md`](../../docker-compose/README.md).

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

## Security

### Credentials Best Practices

1. **Use Kubernetes secrets** for S3 credentials on the `KafscaleCluster`:
   ```bash
   kubectl create secret generic s3-creds \
     --from-literal=AWS_ACCESS_KEY_ID=xxx \
     --from-literal=AWS_SECRET_ACCESS_KEY=xxx
   ```
   ```yaml
   spec:
     lfsProxy:
       s3:
         credentialsSecretRef: s3-creds
   ```

2. **Enable HTTP API key** via `spec.lfsProxy.http.apiKeySecretRef`.

3. **Restrict external Kafka access** with `proxy.service.loadBalancerSourceRanges`
   or cloud LB annotations.

## Uninstall

```bash
helm uninstall kafscale -n kafscale-system
```

## Documentation

- [Operations Guide](../../../docs/operations.md) — external access, proxy, TLS
- [Docker Compose (local LFS)](../../docker-compose/README.md)
- [OpenAPI spec](../../../api/lfs-proxy/openapi.yaml)
