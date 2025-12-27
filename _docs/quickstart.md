---
layout: doc
title: Quickstart
description: Install the KafScale operator, create your first topic, and stream messages in minutes.
permalink: /quickstart/
nav_title: Quickstart
nav_order: 2
---

# Quickstart

This guide covers a minimal installation on a cloud Kubernetes cluster using the Helm chart in the repository.

## Prerequisites

- Kubernetes 1.26+
- Helm 3.12+
- `kubectl` access to your cluster
- An S3-compatible bucket and credentials
- Optional: external etcd endpoints (or let the operator manage etcd)

---

## 1. Create a namespace

```bash
kubectl create namespace kafscale
```

---

## 2. Create an S3 credentials secret

```bash
kubectl -n kafscale create secret generic kafscale-s3 \
  --from-literal=AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY \
  --from-literal=AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
```

Optional session token:

```bash
kubectl -n kafscale patch secret kafscale-s3 -p \
  '{"data":{"AWS_SESSION_TOKEN":"'$(printf %s "TOKEN" | base64)'"}}'
```

---

## 3. Install the operator

With managed etcd (simplest):

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale \
  --create-namespace \
  --set operator.etcdEndpoints={} \
  --set operator.image.tag=latest \
  --set console.image.tag=latest
```

With external etcd:

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale \
  --create-namespace \
  --set operator.etcdEndpoints[0]=http://etcd.kafscale.svc:2379 \
  --set operator.image.tag=latest \
  --set console.image.tag=latest
```

---

## 3.1 Optional: expose the console UI

The console service is a ClusterIP by default. For external access:

- Enable console ingress in Helm values (`console.ingress.*`), or
- Set `console.service.type=LoadBalancer`.

---

## 4. Create a KafScaleCluster

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

```bash
kubectl apply -f cluster.yaml
```

For external etcd, set `spec.etcd.endpoints` to your etcd service.

For S3-compatible storage (MinIO), add `s3.endpoint`.

---

## 5. Create a topic

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

```bash
kubectl apply -f topic.yaml
```

---

## 6. Produce and consume

Port-forward the broker service:

```bash
kubectl -n kafscale port-forward svc/demo-broker 9092:9092
```

Produce messages:

```bash
kafka-console-producer \
  --bootstrap-server 127.0.0.1:9092 \
  --topic orders
```

Consume messages:

```bash
kafka-console-consumer \
  --bootstrap-server 127.0.0.1:9092 \
  --topic orders \
  --from-beginning
```

---

## 7. Verify messages in S3

```bash
aws s3 ls s3://kafscale-demo/default/orders/
```

You should see segment files:

```
segment-00000000000000000000.kfs
segment-00000000000000000000.index
```

## External broker access

External clients: configure `spec.brokers.advertisedHost` / `advertisedPort` and
`spec.brokers.service` in your `KafscaleCluster` so Kafka clients learn a
reachable endpoint. See `docs/operations.md` and `deploy/helm/README.md` for
examples.

---

## Next steps

- [Architecture](/architecture/) for how KafScale works
- [Operations](/operations/) for production hardening
- [Runtime Settings](/configuration/) for full environment variable reference
- [Protocol](/protocol/) for Kafka API compatibility details
