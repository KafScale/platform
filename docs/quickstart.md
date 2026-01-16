<!--
Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

# Kafscale Quickstart

This guide covers a minimal installation on a cloud Kubernetes cluster using the Helm chart in this repo.

## Prerequisites

- Kubernetes 1.26+
- Helm 3.12+
- `kubectl` access to your cluster
- An S3-compatible bucket and credentials
- Optional: external etcd endpoints (or let the operator manage etcd)

## 1) Create a Namespace

```bash
kubectl create namespace kafscale
```

## 2) Create an S3 Credentials Secret

The operator passes the secret through to broker pods as environment variables. Use AWS-style keys.

```bash
kubectl -n kafscale create secret generic kafscale-s3 \
  --from-literal=AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY \
  --from-literal=AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
```

If you need a session token:

```bash
kubectl -n kafscale patch secret kafscale-s3 -p \
  '{"data":{"AWS_SESSION_TOKEN":"'$(printf %s "YOUR_SESSION_TOKEN" | base64)'"}}'
```

## 3) Install the Operator (Cloud Kubernetes)

### Option A: Operator-managed etcd

Leave etcd endpoints empty in both the Helm values and the cluster spec. The operator will create a 3-node etcd StatefulSet and use it.

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set operator.etcdEndpoints={} \
  --set operator.image.useLatest=true \
  --set operator.brokerImage.useLatest=true \
  --set console.image.useLatest=true
```

### Option B: External etcd

Provide reachable etcd endpoints.

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set operator.etcdEndpoints[0]=http://etcd.kafscale.svc:2379 \
  --set operator.image.useLatest=true \
  --set operator.brokerImage.useLatest=true \
  --set console.image.useLatest=true
```

## 4) Create a KafscaleCluster

This creates the broker deployment and wires S3 + etcd into the broker env.

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

## 5) Create a Topic

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

## 6) Produce and Consume

Port-forward the broker and use Kafka CLI tools:

```bash
kubectl -n kafscale port-forward svc/demo-broker 9092:9092
```

```bash
kafka-console-producer --bootstrap-server 127.0.0.1:9092 --topic orders --producer-property enable.idempotence=false
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic orders --from-beginning
```
Note: Kafka clients that default to idempotent producers or transactions must disable them explicitly.

External clients: configure `spec.brokers.advertisedHost` / `advertisedPort` and
`spec.brokers.service` in your `KafscaleCluster` so Kafka clients learn a
reachable endpoint. See `docs/operations.md` and `deploy/helm/README.md` for
examples.

## Next Steps

- `docs/operations.md` for etcd/S3 HA and backup guidance
- `docs/development.md` for build/test workflows
- `docs/user-guide.md` for runtime behavior and limits
