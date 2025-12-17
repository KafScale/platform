# Kafscale Production Operations Guide

This guide explains how to install the Kafscale control plane via Helm, configure S3 and etcd dependencies, and expose the operations console.  It assumes you already have a Kubernetes cluster and the prerequisites listed below.

## Prerequisites

- Kubernetes 1.26+
- Helm 3.12+
- A reachable etcd cluster (the operator uses it for metadata + leader election)
- An S3 bucket per environment plus IAM credentials with permission to list/create objects
- Optional: an ingress controller for the console UI

## Installing with Helm

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set operator.etcdEndpoints[0]=http://etcd.kafscale.svc:2379 \
  --set operator.image.tag=v0.1.0 \
  --set console.image.tag=v0.1.0
```

The chart ships the `KafscaleCluster` and `KafscaleTopic` CRDs so the operator can immediately reconcile resources.  Create a Kubernetes secret that contains your S3 access/secret keys, reference it inside a `KafscaleCluster` resource (see `config/samples/`), and the operator will launch broker pods with the right IAM credentials.

### Values to pay attention to

| Value | Purpose |
|-------|---------|
| `operator.replicaCount` | Number of operator replicas (default `2`).  Operators use etcd to elect a leader and stay HA. |
| `operator.leaderKey` | etcd prefix used for the HA lock.  Multiple clusters can coexist by using different prefixes. |
| `console.service.*` | Type/port used to expose the UI.  Combine with `.console.ingress` to publish via an ingress controller. |
| `imagePullSecrets` | Provide if your container registry (e.g., GHCR) is private. |

## Post-install Steps

1. Apply a `KafscaleCluster` custom resource describing the S3 bucket, etcd endpoints, cache sizes, and credentials secret.
2. Apply any required `KafscaleTopic` resources.  The operator writes the desired metadata to etcd and the brokers begin serving Kafka clients right away.
3. Expose the console UI (optional) by enabling ingress in `values.yaml` or by creating a LoadBalancer service.

## Security & Hardening

- **RBAC** – The Helm chart creates a scoped service account and RBAC role so the operator only touches its CRDs, Secrets, and Deployments inside the release namespace.
- **S3 credentials** – Credentials live in user-managed Kubernetes secrets.  The operator never writes them to etcd.
- **TLS** – Brokers and the console ship HTTPS/TLS flags (`KAFSCALE_BROKER_TLS_*`, `KAFSCALE_CONSOLE_TLS_*`).  Mount certs as secrets via the Helm values and set the env vars to force TLS for client connections.
- **Network policies** – If your cluster enforces policies, allow the operator + brokers to reach etcd and S3 endpoints and lock everything else down.
- **Health / metrics** – Prometheus can scrape `/metrics` on the brokers and operator for early detection of S3 pressure or degraded nodes.  The console also renders the health state for on-call staff.

## Upgrades & Rollbacks

- Use `helm upgrade --install` with the desired image tags.  The operator drains brokers through the gRPC control plane before restarting pods.
- CRD schema changes follow Kubernetes best practices; run `helm upgrade` to pick them up.
- Rollbacks can be performed with `helm rollback kafscale <REVISION>` which restores the previous deployment and service versions.  Brokers are stateless so the recovery window is short.
