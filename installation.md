---
layout: doc
title: Installation
description: Install KafScale with Helm, review CRD examples, and prepare local dev tooling.
---

# Installation

## Helm chart reference

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set operator.etcdEndpoints[0]=http://etcd.kafscale.svc:2379 \
  --set operator.image.tag=v0.1.0 \
  --set console.image.tag=v0.1.0
```

Key values to review:

| Value | Purpose |
| --- | --- |
| `operator.replicaCount` | Operator replicas (default `2`). |
| `operator.etcdEndpoints` | External etcd endpoints. Leave empty to use managed etcd. |
| `console.auth.username` / `console.auth.password` | Enable console login. |
| `console.service.*` | Service type/port for UI exposure. |
| `console.ingress.*` | Publish the UI via ingress (optional). |

## Docker compose (local dev)

A docker-compose stack is planned for local dev. For now, the quickest path is the Makefile demo:

```bash
make demo-platform
```

## Kubernetes CRD examples

### KafscaleCluster

```yaml
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
```

### KafscaleTopic

```yaml
apiVersion: kafscale.novatechflow.io/v1alpha1
kind: KafscaleTopic
metadata:
  name: orders
spec:
  clusterRef: demo
  partitions: 3
```

## Environment variables reference

See [Configuration](/configuration/) for broker, S3, etcd, and operator settings.

## Minimum resource requirements

There are no hard-coded limits; sizing depends on throughput, segment size, and cache targets. Start with 1-2 vCPU and 2-4Gi memory per broker for development, then profile and scale horizontally.
