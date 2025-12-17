## Kafscale Helm Chart

This chart deploys the Kafscale operator (which in turn manages broker pods) and the lightweight operations console.  CRDs are packaged with the release so `helm install` takes care of registering the `KafscaleCluster` and `KafscaleTopic` resources before the operator starts.

### Prerequisites

- Kubernetes 1.26+
- Helm 3.12+
- Access to an etcd cluster that the operator can reach
- IAM credentials/secret containing the S3 access keys that the operator will mount into broker pods

### Quickstart

```bash
helm repo add novatechflow https://charts.novatechflow.dev
helm repo update

helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set operator.etcdEndpoints[0]=http://etcd.kafscale.svc:2379 \
  --set operator.image.tag=v0.1.0 \
  --set console.image.tag=v0.1.0
```

After the chart is running you can create a cluster by applying a `KafscaleCluster` resource (see `config/samples/` for an example).  The console service is exposed as a ClusterIP by default; enable ingress by toggling `.Values.console.ingress`.

### Values Overview

| Key | Description | Default |
|-----|-------------|---------|
| `operator.replicaCount` | Number of operator replicas (each performs leader election via etcd). | `2` |
| `operator.leaderKey` | etcd prefix used for operator HA leader election. | `/kafscale/operator/leader` |
| `operator.etcdEndpoints` | List of etcd endpoints the operator will connect to. | `["http://etcd:2379"]` |
| `console.service.type` | Kubernetes service type for the console. | `ClusterIP` |
| `console.ingress.*` | Optional ingress configuration for exposing the console. | disabled |

Consult `values.yaml` for all tunables, including resource requests, node selectors, and pull secrets.
