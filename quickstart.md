---
layout: doc
title: Quickstart
description: Install the KafScale operator, create your first topic, and stream messages in minutes.
---

# Quickstart

This guide covers a minimal installation on a cloud Kubernetes cluster using the Helm chart in the repository.

## Prerequisites

- Kubernetes 1.26+
- Helm 3.12+
- `kubectl` access to your cluster
- An S3-compatible bucket and credentials
- Optional: external etcd endpoints (or let the operator manage etcd)

<div class="step-grid">
  <div class="step-card" markdown="1">
### 1) Create a namespace

```bash
kubectl create namespace kafscale
```
  </div>
  <div class="step-card" markdown="1">
### 2) Create an S3 credentials secret

```bash
kubectl -n kafscale create secret generic kafscale-s3 \
  --from-literal=AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY \
  --from-literal=AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
```

Optional session token:

```bash
kubectl -n kafscale patch secret kafscale-s3 -p \
  '{"data":{"AWS_SESSION_TOKEN":"'"'"'$(printf %s "YOUR_SESSION_TOKEN" | base64)'"'"'"}}'
```
  </div>
  <div class="step-card" markdown="1">
### 3) Install the operator (managed etcd)

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set operator.etcdEndpoints={} \
  --set operator.image.tag=latest \
  --set console.image.tag=latest
```

External etcd option:

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set operator.etcdEndpoints[0]=http://etcd.kafscale.svc:2379 \
  --set operator.image.tag=latest \
  --set console.image.tag=latest
```
  </div>
  <div class="step-card" markdown="1">
### 4) Create a KafScaleCluster

```bash
cat <<'EOF' | kubectl apply -n kafscale -f -
apiVersion: kafscale.novatechflow.io/v1alpha1
kind: KafscaleCluster
metadata:
  name: demo
spec:
  brokers:
    replicas: 3
  s3:
    bucket: kafscale-demo
    region: us-east-1
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints: []
EOF
```

If you are using external etcd, set `spec.etcd.endpoints` instead:

```bash
cat <<'EOF' | kubectl apply -n kafscale -f -
apiVersion: kafscale.novatechflow.io/v1alpha1
kind: KafscaleCluster
metadata:
  name: demo
spec:
  brokers:
    replicas: 3
  s3:
    bucket: kafscale-demo
    region: us-east-1
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints:
      - http://etcd.kafscale.svc:2379
EOF
```

For S3-compatible endpoints (MinIO, etc), add:

```yaml
s3:
  endpoint: http://minio.kafscale.svc:9000
```
  </div>
  <div class="step-card" markdown="1">
### 5) Create a topic

```bash
cat <<'EOF' | kubectl apply -n kafscale -f -
apiVersion: kafscale.novatechflow.io/v1alpha1
kind: KafscaleTopic
metadata:
  name: orders
spec:
  clusterRef: demo
  partitions: 3
EOF
```
  </div>
  <div class="step-card" markdown="1">
### 6) Produce and consume

```bash
kubectl -n kafscale port-forward svc/demo-broker 9092:9092
```

```bash
kafka-console-producer --bootstrap-server 127.0.0.1:9092 --topic orders
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic orders --from-beginning
```
  </div>
  <div class="step-card" markdown="1">
### 7) Verify messages in S3

```bash
aws s3 ls s3://kafscale-demo/
```
  </div>
</div>

## Next steps

- [Operations](/operations/) for production hardening
- [Configuration](/configuration/) for full env var reference
- [API](/api/) for protocol support
