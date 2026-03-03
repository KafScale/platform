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

# LFS Demo Plan

## Overview

This document plans the LFS Demo following the patterns established by `iceberg-demo` and `kafsql-demo` in the KafScale project.

## Demo Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            LFS Demo Architecture                            │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐
  │  Producer  │────▶│ LFS Proxy  │────▶│   MinIO    │     │   Kafka    │
  │  (e2e-cli) │     │ (9092)     │     │   (S3)     │     │  (Broker)  │
  └────────────┘     └─────┬──────┘     └────────────┘     └─────┬──────┘
                           │                                      │
                           │        Pointer Record               │
                           └─────────────────────────────────────▶│
                                                                  │
                                                                  ▼
                                                         ┌────────────────┐
                                                         │    Consumer    │
                                                         │ (LFS SDK +     │
                                                         │  S3 resolve)   │
                                                         └────────────────┘
```

## Prerequisites (from existing demos)

- kind cluster (`kafscale-demo`)
- MinIO deployment (for S3-compatible storage)
- KafScale broker(s) running
- etcd for metadata

## Demo Components to Build

### 1. Docker Image: `lfs-proxy`

**File:** `deploy/docker/lfs-proxy.Dockerfile`

```dockerfile
# syntax=docker/dockerfile:1.7
ARG GO_VERSION=1.25.2
FROM golang:${GO_VERSION}-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG GO_BUILD_FLAGS=""

WORKDIR /src
RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download
COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build ${GO_BUILD_FLAGS} -ldflags="-s -w" -o /out/lfs-proxy ./cmd/lfs-proxy

FROM alpine:3.19
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 kafscale
USER 10001
WORKDIR /app

COPY --from=builder /out/lfs-proxy /usr/local/bin/kafscale-lfs-proxy

EXPOSE 9092 8080 9094 9095
ENTRYPOINT ["/usr/local/bin/kafscale-lfs-proxy"]
```

### 2. Demo Script: `scripts/lfs-demo.sh`

**Core Logic:**
1. Validate environment variables
2. Deploy LFS proxy to kind cluster
3. Create demo topic
4. Produce large blob messages via e2e-client with `LFS_BLOB` header
5. Verify blobs in MinIO
6. Consume via LFS-aware consumer (or verify pointer records)
7. Display metrics

### 3. Makefile Targets

Add to root `Makefile`:

```makefile
# LFS Demo Variables
LFS_PROXY_IMAGE ?= $(REGISTRY)/kafscale-lfs-proxy:dev
LFS_DEMO_NAMESPACE ?= $(KAFSCALE_DEMO_NAMESPACE)
LFS_DEMO_TOPIC ?= lfs-demo-topic
LFS_DEMO_BLOB_SIZE ?= 10485760  # 10MB
LFS_DEMO_BLOB_COUNT ?= 5
LFS_DEMO_TIMEOUT_SEC ?= 120

# Build target
LFS_PROXY_SRCS := $(shell find cmd/lfs-proxy pkg/lfs go.mod go.sum)
docker-build-lfs-proxy: $(STAMP_DIR)/lfs-proxy.image
$(STAMP_DIR)/lfs-proxy.image: $(LFS_PROXY_SRCS)
	@mkdir -p $(STAMP_DIR)
	$(DOCKER_BUILD_CMD) $(DOCKER_BUILD_ARGS) -t $(LFS_PROXY_IMAGE) -f deploy/docker/lfs-proxy.Dockerfile .
	@touch $(STAMP_DIR)/lfs-proxy.image

# Demo target
lfs-demo: KAFSCALE_DEMO_PROXY=0
lfs-demo: KAFSCALE_DEMO_CONSOLE=0
lfs-demo: KAFSCALE_DEMO_BROKER_REPLICAS=1
lfs-demo: demo-platform-bootstrap ## Run the LFS proxy demo on kind.
	$(MAKE) docker-build-lfs-proxy
	KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) \
	KAFSCALE_DEMO_NAMESPACE=$(KAFSCALE_DEMO_NAMESPACE) \
	KAFSCALE_KIND_CLUSTER=$(KAFSCALE_KIND_CLUSTER) \
	LFS_DEMO_NAMESPACE=$(LFS_DEMO_NAMESPACE) \
	LFS_DEMO_TOPIC=$(LFS_DEMO_TOPIC) \
	LFS_DEMO_BLOB_SIZE=$(LFS_DEMO_BLOB_SIZE) \
	LFS_DEMO_BLOB_COUNT=$(LFS_DEMO_BLOB_COUNT) \
	LFS_DEMO_TIMEOUT_SEC=$(LFS_DEMO_TIMEOUT_SEC) \
	LFS_PROXY_IMAGE=$(LFS_PROXY_IMAGE) \
	E2E_CLIENT_IMAGE=$(E2E_CLIENT_IMAGE) \
	MINIO_BUCKET=$(MINIO_BUCKET) \
	MINIO_REGION=$(MINIO_REGION) \
	MINIO_ROOT_USER=$(MINIO_ROOT_USER) \
	MINIO_ROOT_PASSWORD=$(MINIO_ROOT_PASSWORD) \
	bash scripts/lfs-demo.sh
```

### 4. Helm Values Update

Add to `deploy/helm/kafscale/values.yaml`:

```yaml
lfsProxy:
  enabled: false
  replicaCount: 1
  image:
    repository: ghcr.io/kafscale/kafscale-lfs-proxy
    tag: ""
    pullPolicy: IfNotPresent
  service:
    type: ClusterIP
    port: 9092
  health:
    enabled: true
    port: 9094
  metrics:
    enabled: true
    port: 9095
  http:
    enabled: true
    port: 8080
  s3:
    bucket: "kafscale-lfs"
    region: "us-east-1"
    endpoint: ""
    namespace: "default"
    forcePathStyle: false
    ensureBucket: false
    accessKeySecretRef: "kafscale-s3-credentials"
    secretKeySecretRef: "kafscale-s3-credentials"
  config:
    maxBlobSize: 5368709120  # 5GB
    chunkSize: 5242880       # 5MB
  etcdEndpoints: []
  backends: []
  resources: {}
  nodeSelector: {}
  tolerations: []
  affinity: {}
```

---

## Demo Script Outline

**File:** `scripts/lfs-demo.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# Variables
LFS_DEMO_NAMESPACE="${LFS_DEMO_NAMESPACE:-kafscale-demo}"
LFS_DEMO_TOPIC="${LFS_DEMO_TOPIC:-lfs-demo-topic}"
LFS_DEMO_BLOB_SIZE="${LFS_DEMO_BLOB_SIZE:-10485760}"
LFS_DEMO_BLOB_COUNT="${LFS_DEMO_BLOB_COUNT:-5}"

# Required env vars check
required_vars=(
  KUBECONFIG
  KAFSCALE_DEMO_NAMESPACE
  KAFSCALE_KIND_CLUSTER
  LFS_PROXY_IMAGE
  E2E_CLIENT_IMAGE
  MINIO_BUCKET
  MINIO_REGION
  MINIO_ROOT_USER
  MINIO_ROOT_PASSWORD
)

for var in "${required_vars[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    echo "missing required env var: ${var}" >&2
    exit 1
  fi
done

echo "=========================================="
echo " LFS Proxy Demo"
echo "=========================================="

# 1. Load LFS proxy image to kind
echo "[1/7] Loading LFS proxy image..."
kind load docker-image "${LFS_PROXY_IMAGE}" --name "${KAFSCALE_KIND_CLUSTER}"

# 2. Deploy LFS proxy
echo "[2/7] Deploying LFS proxy..."
kubectl -n "${LFS_DEMO_NAMESPACE}" apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: lfs-proxy
spec:
  replicas: 1
  selector:
    matchLabels:
      app: lfs-proxy
  template:
    metadata:
      labels:
        app: lfs-proxy
    spec:
      containers:
      - name: lfs-proxy
        image: ${LFS_PROXY_IMAGE}
        ports:
        - containerPort: 9092
        - containerPort: 8080
        - containerPort: 9094
        - containerPort: 9095
        env:
        - name: KAFSCALE_LFS_PROXY_ADDR
          value: ":9092"
        - name: KAFSCALE_LFS_PROXY_HTTP_ADDR
          value: ":8080"
        - name: KAFSCALE_LFS_PROXY_HEALTH_ADDR
          value: ":9094"
        - name: KAFSCALE_LFS_PROXY_METRICS_ADDR
          value: ":9095"
        - name: KAFSCALE_LFS_PROXY_BACKENDS
          value: "kafscale-broker.${LFS_DEMO_NAMESPACE}.svc.cluster.local:9092"
        - name: KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS
          value: "http://kafscale-etcd-client.${LFS_DEMO_NAMESPACE}.svc.cluster.local:2379"
        - name: KAFSCALE_LFS_PROXY_S3_BUCKET
          value: "${MINIO_BUCKET}"
        - name: KAFSCALE_LFS_PROXY_S3_REGION
          value: "${MINIO_REGION}"
        - name: KAFSCALE_LFS_PROXY_S3_ENDPOINT
          value: "http://minio.${LFS_DEMO_NAMESPACE}.svc.cluster.local:9000"
        - name: KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE
          value: "true"
        - name: KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET
          value: "true"
        - name: KAFSCALE_LFS_PROXY_S3_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: kafscale-s3-credentials
              key: KAFSCALE_S3_ACCESS_KEY
        - name: KAFSCALE_LFS_PROXY_S3_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: kafscale-s3-credentials
              key: KAFSCALE_S3_SECRET_KEY
---
apiVersion: v1
kind: Service
metadata:
  name: lfs-proxy
spec:
  selector:
    app: lfs-proxy
  ports:
  - name: kafka
    port: 9092
    targetPort: 9092
  - name: http
    port: 8080
    targetPort: 8080
  - name: health
    port: 9094
    targetPort: 9094
  - name: metrics
    port: 9095
    targetPort: 9095
EOF

# 3. Wait for LFS proxy
echo "[3/7] Waiting for LFS proxy..."
kubectl -n "${LFS_DEMO_NAMESPACE}" rollout status deployment/lfs-proxy --timeout=120s

# 4. Create demo topic
echo "[4/7] Creating demo topic..."
# Use e2e-client or kafka CLI to create topic

# 5. Produce large blob messages
echo "[5/7] Producing ${LFS_DEMO_BLOB_COUNT} blobs of ${LFS_DEMO_BLOB_SIZE} bytes..."
# Use e2e-client with LFS_BLOB header

# 6. Verify blobs in MinIO
echo "[6/7] Verifying blobs in MinIO..."
# kubectl exec into minio and list objects

# 7. Show metrics
echo "[7/7] LFS Proxy Metrics:"
kubectl -n "${LFS_DEMO_NAMESPACE}" port-forward svc/lfs-proxy 9095:9095 &
PF_PID=$!
sleep 2
curl -s http://localhost:9095/metrics | grep kafscale_lfs
kill $PF_PID 2>/dev/null || true

echo ""
echo "=========================================="
echo " LFS Demo Complete!"
echo "=========================================="
echo ""
echo "LFS Proxy: lfs-proxy.${LFS_DEMO_NAMESPACE}.svc.cluster.local:9092"
echo "Blobs stored in: s3://${MINIO_BUCKET}/default/${LFS_DEMO_TOPIC}/lfs/"
echo ""
```

---

## Implementation Tasks

Add to `tasks.md`:

| ID | Task | Priority | Status | Notes |
|----|------|----------|--------|-------|
| DEMO-001 | Create `deploy/docker/lfs-proxy.Dockerfile` | P0 | [ ] | Multi-stage Alpine build |
| DEMO-002 | Add `docker-build-lfs-proxy` to Makefile | P0 | [ ] | Follow broker pattern |
| DEMO-003 | Create `scripts/lfs-demo.sh` | P0 | [ ] | Follow kafsql-demo pattern |
| DEMO-004 | Add `lfs-demo` target to Makefile | P0 | [ ] | With all env vars |
| DEMO-005 | Add `lfsProxy` section to Helm values | P1 | [ ] | For production deploy |
| DEMO-006 | Create Helm template `lfs-proxy-deployment.yaml` | P1 | [ ] | K8s deployment |
| DEMO-007 | Create Helm template `lfs-proxy-service.yaml` | P1 | [ ] | Service exposure |
| DEMO-008 | Add e2e-client LFS producer mode | P1 | [ ] | `--lfs-blob` flag |
| DEMO-009 | Document demo in `examples/` | P2 | [ ] | Tutorial guide |

---

## Demo User Experience

```bash
# Run the LFS demo
make lfs-demo

# Expected output:
# ==========================================
#  LFS Proxy Demo
# ==========================================
# [1/7] Loading LFS proxy image...
# [2/7] Deploying LFS proxy...
# [3/7] Waiting for LFS proxy...
# [4/7] Creating demo topic...
# [5/7] Producing 5 blobs of 10485760 bytes...
# [6/7] Verifying blobs in MinIO...
# [7/7] LFS Proxy Metrics:
# kafscale_lfs_proxy_upload_bytes_total 52428800
# kafscale_lfs_proxy_requests_total{topic="lfs-demo-topic",status="ok",type="lfs"} 5
#
# ==========================================
#  LFS Demo Complete!
# ==========================================
#
# LFS Proxy: lfs-proxy.kafscale-demo.svc.cluster.local:9092
# Blobs stored in: s3://kafscale/default/lfs-demo-topic/lfs/
```

---

## Files to Create

| File | Description |
|------|-------------|
| `deploy/docker/lfs-proxy.Dockerfile` | Docker image build |
| `scripts/lfs-demo.sh` | Demo orchestration script |
| `deploy/helm/kafscale/templates/lfs-proxy-deployment.yaml` | K8s Deployment |
| `deploy/helm/kafscale/templates/lfs-proxy-service.yaml` | K8s Service |
| `examples/E60_lfs-demo/README.md` | Demo documentation |

---

## Dependencies

- `cmd/lfs-proxy/` must be complete (Phase 1)
- `pkg/lfs/` envelope package
- MinIO for S3-compatible storage
- e2e-client with LFS support (or manual curl/kafka-cli)
