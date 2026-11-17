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
  --set operator.etcdEndpoints={}
```

Image tags follow the chart `appVersion` unless you override them. The current
chart `appVersion` may be older than this release. For a full list of image
keys, including `useLatest` and `tag` for the operator, broker, etcd-tools,
console, proxy, schema registry, MCP and the LFS browser demo, see
[Installing with Helm](operations.md#installing-with-helm).

### Option B: External etcd

Provide reachable etcd endpoints.

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set operator.etcdEndpoints[0]=http://etcd.kafscale.svc:2379
```

`http://etcd.kafscale.svc:2379` is an example external etcd Service, not the
operator-managed etcd client. Managed etcd is
`<cluster>-etcd-client.<namespace>.svc.cluster.local:2379`. For the image
tags, see the note under Option A and
[Installing with Helm](operations.md#installing-with-helm).

## 4) Create a KafscaleCluster

This creates the broker deployment and wires S3 + etcd into the broker env.

```bash
cat <<'EOF' | kubectl apply -n kafscale -f -
apiVersion: kafscale.io/v1alpha1
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
apiVersion: kafscale.io/v1alpha1
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
apiVersion: kafscale.io/v1alpha1
kind: KafscaleTopic
metadata:
  name: orders
spec:
  clusterRef: demo
  partitions: 3
EOF
```

## 6) Produce and Consume

A laptop cannot use `kubectl port-forward` to `svc/demo-broker` once there is
more than one broker, or when `spec.brokers.advertisedHost` is unset. Metadata
then advertises each broker as
`<cluster>-broker-<n>.<cluster>-broker-headless.<namespace>.svc.cluster.local:9092`,
which is only reachable from inside the cluster. For the quickstart cluster
`demo` in namespace `kafscale`, that is
`demo-broker-0.demo-broker-headless.kafscale.svc.cluster.local:9092`.

Run the client in the cluster, or enable the Kafka proxy (`proxy.enabled=true`)
and set `proxy.advertisedHost` / `proxy.advertisedPort` to an address the
client can reach. For local clusters, pin `proxy.service.nodePort` (see
`docs/operations.md`).

Port-forward to `svc/<cluster>-broker` only works with a single broker and
with `spec.brokers.advertisedHost` set to an address the client can reach, for
example `127.0.0.1`:

```bash
kubectl -n kafscale port-forward svc/demo-broker 9092:9092
```

```bash
kafka-console-producer --bootstrap-server 127.0.0.1:9092 --topic orders --producer-property enable.idempotence=false
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic orders --from-beginning
```

Note: Kafka clients that default to idempotent producers or transactions must disable them explicitly.

Direct broker exposure via `spec.brokers.service` is optional when you
intentionally bypass the proxy.

Service names for a `KafscaleCluster` named `<cluster>` in namespace
`<namespace>` are documented in [Service names](operations.md#service-names).
The operator-managed etcd client is
`<cluster>-etcd-client.<namespace>.svc.cluster.local:2379`, and the broker
Service is `<cluster>-broker.<namespace>.svc.cluster.local:9092` (metrics on
port 9093).

## 7) Delete a topic

Deleting a `KafscaleTopic` custom resource does not delete the Kafka topic.
The operator does not call `DeleteTopics` and does not write a
`/kafscale/deleted-topics` record, so the topic stays in Kafka metadata and
its S3 data is not recorded for `kafscale-cli prune`.

To delete a topic, use the Kafka `DeleteTopics` API from a client that can
reach the brokers, or the console `DELETE /ui/api/status/topics/<topic>`
endpoint after signing in (see [Console](operations.md#console)). If you
created the topic with a `KafscaleTopic`, delete that resource as well, so
the operator does not publish the topic back into the cluster snapshot.

## Next Steps

- `docs/operations.md` for etcd/S3 HA and backup guidance
- `docs/development.md` for build/test workflows
- `docs/user-guide.md` for runtime behavior and limits
