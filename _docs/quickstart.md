---
layout: doc
title: "KafScale Quickstart: First Topic on Kubernetes"
description: Install the KafScale operator, create your first topic, and stream messages in minutes.
permalink: /quickstart/
nav_title: Quickstart
nav_order: 2
redirect_from:
  - /installation/
---

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
- `jq` (used to build the login request and the optional session token patch)
- An S3-compatible bucket and credentials
- Optional: external etcd endpoints (or let the operator manage etcd)

## 1) Create a Namespace

```bash
kubectl create namespace kafscale
```

## 2) Create an S3 Credentials Secret

The operator passes the secret through to broker pods as environment variables. Use AWS-style keys.

```bash
printf 'AWS secret access key: '
read -rs AWS_SECRET_ACCESS_KEY
echo
printf '%s' "$AWS_SECRET_ACCESS_KEY" | kubectl -n kafscale create secret generic kafscale-s3 \
  --from-literal=AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY \
  --from-file=AWS_SECRET_ACCESS_KEY=/dev/stdin
unset AWS_SECRET_ACCESS_KEY
```

If you need a session token:

```bash
printf 'AWS session token: '
read -rs AWS_SESSION_TOKEN
echo
printf '%s' "$AWS_SESSION_TOKEN" | jq -n --rawfile t /dev/stdin \
  '{data:{AWS_SESSION_TOKEN:($t|@base64)}}' | \
  kubectl -n kafscale patch secret kafscale-s3 --type=merge --patch-file=/dev/stdin
unset AWS_SESSION_TOKEN
```

## 3) Install the Operator (Cloud Kubernetes)

### Option A: Operator-managed etcd

Leave etcd endpoints empty in both the Helm values and the cluster spec. The operator will create a 3-node etcd StatefulSet and use it.

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set 'operator.etcdEndpoints={}'
```

Image tags follow the chart `appVersion` unless you override them. If you
install from a checkout of `main`, that tag may be older than the branch; set
`useLatest=true` on each image the install runs. For a full list of image
keys, including `useLatest` and `tag` for the operator, broker, etcd-tools,
console, proxy, schema registry, MCP and the operator-managed LFS proxy, see
[Installing with Helm](/operations/#installing-with-helm).

### Option B: External etcd

Provide reachable etcd endpoints.

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set 'operator.etcdEndpoints[0]=http://etcd.kafscale.svc:2379'
```

`http://etcd.kafscale.svc:2379` is an example external etcd Service, not the
operator-managed etcd client. Managed etcd is
`<cluster>-etcd-client.<namespace>.svc.cluster.local:2379`. For the image
tags, see the note under Option A and
[Installing with Helm](/operations/#installing-with-helm).

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

The S3 bucket must already exist. Create it if needed. For MinIO and other
in-cluster S3-compatible endpoints, apply this Job so the Service name
resolves and the Job reads credentials from the `kafscale-s3` Secret
created in step 2:

```bash
cat <<'EOF' | kubectl apply -n kafscale -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: s3-mb
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: s3-mb
          image: amazon/aws-cli:2.15.0
          args:
            - s3
            - mb
            - s3://kafscale-demo
            - --endpoint-url
            - http://minio.kafscale.svc:9000
          envFrom:
            - secretRef:
                name: kafscale-s3
          env:
            - name: AWS_DEFAULT_REGION
              value: us-east-1
EOF
kubectl -n kafscale wait --for=condition=complete job/s3-mb --timeout=120s
kubectl -n kafscale logs job/s3-mb
kubectl -n kafscale delete job s3-mb
```

`BucketAlreadyOwnedByYou` means the bucket already exists.

For AWS S3, drop `--endpoint-url`, or run `aws s3 mb s3://kafscale-demo --region us-east-1`.

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

The cluster in step 4 has `spec.brokers.replicas: 3`. Metadata advertises each
broker as
`<cluster>-broker-<n>.<cluster>-broker-headless.<namespace>.svc.cluster.local:9092`,
which is only reachable from inside the cluster. For the quickstart cluster
`demo` in namespace `kafscale`, that is
`demo-broker-0.demo-broker-headless.kafscale.svc.cluster.local:9092`.
`kubectl port-forward` to `svc/demo-broker` cannot work with this cluster.

Run a client in the cluster:

```bash
kubectl -n kafscale run kcat --rm -it --restart=Never --image=edenhill/kcat:1.7.1 -- -b demo-broker:9092 -L
echo hello | kubectl -n kafscale run kcat-produce --rm -i --restart=Never --image=edenhill/kcat:1.7.1 -- -b demo-broker:9092 -t orders -P
kubectl -n kafscale run kcat-consume --rm -it --restart=Never --image=edenhill/kcat:1.7.1 -- -b demo-broker:9092 -t orders -C -o beginning -e
```

Or enable the Kafka proxy (`proxy.enabled=true`) and set
`proxy.advertisedHost` / `proxy.advertisedPort` to an address the client can
reach. For local clusters, pin `proxy.service.nodePort` (see
[`docs/operations.md`](/operations/)).

Port-forward to `svc/<cluster>-broker` only works when `spec.brokers.replicas`
is `1` and `spec.brokers.advertisedHost` is set to an address the client can
reach, for example `127.0.0.1`.

Note: Kafka clients that default to idempotent producers or transactions must disable them explicitly.

Service names for a `KafscaleCluster` named `<cluster>` in namespace
`<namespace>` are documented in [Service names](/operations/#service-names).
The operator-managed etcd client is
`<cluster>-etcd-client.<namespace>.svc.cluster.local:2379`, and the broker
Service is `<cluster>-broker.<namespace>.svc.cluster.local:9092` (metrics on
port 9093).

## 7) Delete a topic

Deleting a `KafscaleTopic` custom resource does not delete the Kafka topic.
The operator does not call `DeleteTopics` and does not write a
`/kafscale/deleted-topics` record, so the topic stays in Kafka metadata and
its S3 data is not recorded for `kafscale-cli prune`.

If you created the topic with a `KafscaleTopic`, delete that resource first.

```bash
kubectl -n kafscale delete kafscaletopic orders
```

Then use the Kafka `DeleteTopics` API from a client that can reach the
brokers, or the console `DELETE /ui/api/status/topics/<topic>` endpoint after
signing in (see [Console](/operations/#console)). The console needs
`console.etcdEndpoints` set to the cluster's etcd and `console.s3Namespace`
set to the cluster namespace. Without `console.etcdEndpoints`, that delete
returns `503` with `metadata store not configured`. Without
`console.s3Namespace`, it returns `503` with `console S3 namespace not configured; set KAFSCALE_S3_NAMESPACE to the KafscaleCluster namespace`.
Without `console.auth.existingSecret`, and without both `console.auth.username` and `console.auth.password`, login returns `503` and every other `/ui/api` call returns `503` with `ui auth disabled`. The chart sets those from the Secret named by `console.auth.existingSecret` (keys `username` and `password`), or from `--set console.auth.username` and `--set console.auth.password`.

For the quickstart cluster `demo` in namespace `kafscale`, when etcd is
operator-managed, first create the console login Secret. Enter the console
password when prompted, and keep this shell open: the login below runs here
and reads `$CONSOLE_PASSWORD`. If `kafscale-console-auth` already exists,
delete it first with `kubectl -n kafscale delete secret kafscale-console-auth`.

```bash
printf 'Console password: '
read -rs CONSOLE_PASSWORD
echo
printf '%s' "$CONSOLE_PASSWORD" | kubectl -n kafscale create secret generic kafscale-console-auth \
  --from-literal=username=kafscaleadmin --from-file=password=/dev/stdin
```

Then install or upgrade with:

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set 'operator.etcdEndpoints={}' \
  --set 'console.etcdEndpoints[0]=http://demo-etcd-client.kafscale.svc.cluster.local:2379' \
  --set 'console.s3Namespace=kafscale' \
  --set 'console.auth.existingSecret=kafscale-console-auth'
```

If you used the external etcd option in step 4, set `console.etcdEndpoints` to
that etcd endpoint instead, for example
`--set 'console.etcdEndpoints[0]=http://etcd.kafscale.svc:2379'`.

The Helm release named `kafscale` exposes the console as Service
`kafscale-console` on port 80. Port-forward it in a second terminal and
leave it running:

```bash
kubectl -n kafscale port-forward svc/kafscale-console 8080:80
```

Back in the first shell, where you entered the password, sign in with
`POST /ui/api/auth/login` and delete topic `orders`:

```bash
printf '%s' "$CONSOLE_PASSWORD" | jq -cRs '{username:"kafscaleadmin",password:.}' | \
  curl -sS -c console.cookies -X POST -H 'Content-Type: application/json' \
  -d @- http://127.0.0.1:8080/ui/api/auth/login

curl -sS -b console.cookies -X DELETE \
  http://127.0.0.1:8080/ui/api/status/topics/orders

unset CONSOLE_PASSWORD
```

`curl -c` stores the `kafscale_ui_session` cookie from login; `curl -b` sends it with the delete. If the CR still exists, the next operator reconcile publishes the topic again.

## Next Steps

- [`docs/operations.md`](/operations/) for etcd/S3 HA and backup guidance
- [`docs/development.md`](/development/) for build/test workflows
- [`docs/user-guide.md`](/user-guide/) for runtime behavior and limits
