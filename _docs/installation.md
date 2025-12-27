---
layout: doc
title: Installation
description: Install KafScale with Helm, review CRD examples, and prepare local dev tooling.
permalink: /installation/
nav_title: Installation
nav_order: 4
---

# Installation

## Helm chart

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set operator.etcdEndpoints[0]=http://etcd.kafscale.svc:2379 \
  --set operator.image.tag=v1.1.0 \
  --set console.image.tag=v1.1.0
```

Key values:

| Value | Purpose |
|-------|---------|
| `operator.replicaCount` | Operator replicas (default 2) |
| `operator.etcdEndpoints` | External etcd endpoints; leave empty for managed etcd |
| `console.auth.username` | Console login username |
| `console.auth.password` | Console login password |
| `console.service.type` | Service type for UI (ClusterIP, LoadBalancer, NodePort) |
| `console.service.port` | Service port for UI (default 8080) |
| `console.ingress.enabled` | Enable ingress for UI |
| `console.ingress.host` | Ingress hostname |

For managed etcd (simplest setup):

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set operator.etcdEndpoints={} \
  --set operator.image.tag=v1.1.0 \
  --set console.image.tag=v1.1.0
```

---

## Docker Compose (local dev)

For local development without Kubernetes:

```bash
git clone https://github.com/novatechflow/kafscale.git
cd kafscale
docker-compose up -d
```

This starts a broker on port 9092, etcd on port 2379, and MinIO on port 9000.

Alternatively, use the Makefile:

```bash
make demo-platform
```

---

## Kubernetes CRDs

### KafScaleCluster

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: demo
  namespace: kafscale
spec:
  brokers:
    replicas: 3
  s3:
    bucket: kafscale-demo
    region: us-east-1
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints: []
```

With external etcd:

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: demo
  namespace: kafscale
spec:
  brokers:
    replicas: 3
  s3:
    bucket: kafscale-demo
    region: us-east-1
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints:
      - http://etcd-0.etcd.kafscale.svc:2379
      - http://etcd-1.etcd.kafscale.svc:2379
      - http://etcd-2.etcd.kafscale.svc:2379
```

With S3-compatible storage (MinIO):

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: demo
  namespace: kafscale
spec:
  brokers:
    replicas: 3
  s3:
    bucket: kafscale-demo
    endpoint: http://minio.kafscale.svc:9000
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints: []
```

### KafScaleTopic

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleTopic
metadata:
  name: orders
  namespace: kafscale
spec:
  clusterRef: demo
  partitions: 3
```

With retention and compression:

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleTopic
metadata:
  name: logs
  namespace: kafscale
spec:
  clusterRef: demo
  partitions: 6
  config:
    retention.ms: "604800000"
    compression.type: "zstd"
```

### KafScaleSnapshot (etcd backup)

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleSnapshot
metadata:
  name: daily-backup
  namespace: kafscale
spec:
  clusterRef: demo
  schedule: "0 2 * * *"
  s3:
    bucket: kafscale-backups
    prefix: etcd-snapshots/
```

---

## S3 credentials secret

Create the secret before deploying a cluster:

```bash
kubectl -n kafscale create secret generic kafscale-s3 \
  --from-literal=AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY \
  --from-literal=AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
```

For temporary credentials (STS):

```bash
kubectl -n kafscale create secret generic kafscale-s3 \
  --from-literal=AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY \
  --from-literal=AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY \
  --from-literal=AWS_SESSION_TOKEN=YOUR_SESSION_TOKEN
```

For IAM roles (EKS with IRSA), omit the secret and annotate the service account:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kafscale-broker
  namespace: kafscale
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/kafscale-s3-role
```

---

## Resource requirements

No hard-coded limits. Sizing depends on throughput, segment size, and cache targets.

| Component | Development | Production |
|-----------|-------------|------------|
| Broker | 1 vCPU, 2Gi memory | 2-4 vCPU, 4-8Gi memory |
| Operator | 0.5 vCPU, 512Mi memory | 1 vCPU, 1Gi memory |
| etcd (per node) | 1 vCPU, 1Gi memory | 2 vCPU, 4Gi memory |

Start small and scale horizontally based on metrics.

---

## Next steps

- [Quickstart](/quickstart/) for a complete walkthrough
- [Runtime Settings](/configuration/) for environment variables
- [Operations](/operations/) for production hardening
