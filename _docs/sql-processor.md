---
layout: doc
title: "KafScale SQL Processor: KAFSQL Addon"
description: Deploy and operate the KAFSQL processor addon.
permalink: /processors/sql/
nav_title: SQL
nav_order: 2
nav_group: Processors
---

<!--
Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

# KAFSQL Processor

KAFSQL is a storage-native SQL processor that reads Kafscale KFS segments from
S3 and exposes a Postgres wire protocol endpoint for ad-hoc queries. It is
stateless, uses S3 as source of truth, and targets the 80% of Kafka data
questions that are simple and time-bounded.

## Quick Links

- [User guide](https://github.com/KafScale/platform/blob/main/addons/processors/sql-processor/user-guide.md)
- [Developer guide](https://github.com/KafScale/platform/blob/main/addons/processors/sql-processor/developer.md)
- [Reference config](https://github.com/KafScale/platform/blob/main/addons/processors/sql-processor/config/config.yaml)
- [Helm values](https://github.com/KafScale/platform/blob/main/addons/processors/sql-processor/deploy/helm/sql-processor/values.yaml)
- [Demo video](/demos/#sql)

## What It Does

- Read KFS segments directly from S3 (no broker dependency).
- Expose Postgres wire protocol for JDBC/BI compatibility.
- Support single-topic queries plus bounded two-topic joins.
- Provide Kafka-native SQL extensions (`LAST`, `TAIL`, `SCAN FULL`).

## What It Does Not Do

- Continuous streaming queries or materialized views.
- Full SQL feature parity or multi-join chains.
- Writes or updates.

## Isolation and Access

KAFSQL never connects to the Kafka brokers and imports no Kafka client. It
reads completed segments straight from S3, so ad-hoc queries add no load to
produce and consume traffic. From etcd it only reads topic metadata and topic
generations.

The executor is read only. Its S3 credentials (`envFromSecret` or an IAM role
on the ServiceAccount) need exactly these permissions on `s3.bucket`, keys
under `<s3.namespace>/`:

- `s3:ListBucket` on the bucket, for the prefix `<s3.namespace>/`, to find
  topics and segments.
- `s3:GetObject` on `<s3.namespace>/*`, to read `.kfs` segments and their
  footers, plus the manifest when `manifest.enabled` is on and the time index
  files when `time_index.enabled` is on.

The executor never writes to S3 and needs no `s3:PutObject` or
`s3:DeleteObject`. The proxy does not touch S3 at all.

Only the separate `kafsql-backfill` tool (`cmd/backfill`) writes. It builds the
optional files the executor reads, and needs the read permissions above plus
`s3:PutObject` on:

- `<s3.namespace>/<manifest.key>` (default `manifest.json`) for
  `-mode manifest`.
- each segment key with `.kfs` replaced by `time_index.key_suffix` (default
  `.kfst`) for `-mode time-index`.

The executor accepts any user name, has no TLS and applies no topic ACLs. Do
not expose it directly outside a trusted network. To give other teams or
tools access, turn on the proxy: it enforces topic ACLs before any S3 read,
writes the audit log, and with the proxy on the chart limits the executor to
proxy pods with a NetworkPolicy (enforced when your CNI supports it). See
Exposing the Executor below.

## Layout

```
sql-processor/
├── cmd/processor/        # executor: Postgres wire server
├── cmd/proxy/            # optional proxy: ACLs and audit log
├── cmd/backfill/         # builds manifest and time index files
├── config/               # sample config
├── internal/config/      # YAML parsing + validation
├── internal/discovery/   # segment listing, manifest and time index
├── internal/decoder/     # Kafscale segment decoding
├── internal/metadata/    # topic metadata from etcd or S3
├── internal/sql/         # SQL parsing
├── internal/server/      # Postgres wire server and query execution
├── internal/proxy/       # proxy ACLs, audit and routing
├── deploy/helm/          # Helm chart
├── Dockerfile
└── Makefile
```

## Quick Start (Local)

Set `s3.bucket`, `s3.namespace`, `s3.endpoint` and the etcd endpoints in
`config/config.yaml` and export `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` first.

```bash
cd addons/processors/sql-processor
make build
./bin/sql-processor -config config/config.yaml
```

## Config Example

```yaml
s3:
  bucket: kafscale-data
  namespace: production

metadata:
  discovery: etcd

server:
  listen: ":5432"
```

For full configuration and operational guidance, use the user guide.

When etcd authentication is enabled, configure `metadata.etcd.username` and
`metadata.etcd.password`, or set `KAFSQL_METADATA_ETCD_USERNAME` and
`KAFSQL_METADATA_ETCD_PASSWORD`. Both metadata and generation reads use these
credentials. Generation client initialization is retried after an error.

## Proxy Access (Optional)

The proxy is used for external access:
- TLS termination (via ingress or mesh).
- Auth via external gateway or mesh plus topic ACL enforcement.
- Query auditing.

Deploy proxy only when you need external access. In-cluster use can skip it.

With `proxy.enabled: true` the chart points the proxy at the executors through
the ClusterIP Service `<fullname>-executor` (it sets `KAFSQL_PROXY_UPSTREAMS`;
an entry with the same name in `proxy.env` overrides it). It also adds the
NetworkPolicy `<fullname>-executor`, which lets only the proxy pods reach the
executor Postgres port. Every other ingress to the executor pods is denied,
metrics included, unless you add rules in `proxy.networkPolicy.extraIngress`.
The policy only takes effect when your CNI enforces NetworkPolicy.

Proxy ACLs are applied before any S3 access. If the proxy cannot authorize a
query, it returns an error without forwarding.

## Helm Deployment

The chart lives in `addons/processors/sql-processor/deploy/helm/sql-processor`.
By default it runs `ghcr.io/kafscale/kafscale-sql-processor` with the chart's
`appVersion` as the tag. It does not expose the executor by default, see
Exposing the Executor below.
The chart mounts `config/config.yaml` from the chart as the config file. Override
single settings with `KAFSQL_*` environment variables in `env` (for example
`KAFSQL_S3_BUCKET`, `KAFSQL_S3_ENDPOINT`, `KAFSQL_METADATA_ETCD_ENDPOINTS`), or
supply a whole file with `config.existingConfigMap`.

Helm values:

| Key | Default | What it does |
| --- | --- | --- |
| `image.repository` | `ghcr.io/kafscale/kafscale-sql-processor` | Executor and proxy image. |
| `image.tag` | `""` | Image tag. Empty uses the chart `appVersion`. |
| `image.pullPolicy` | `IfNotPresent` | Image pull policy. |
| `nameOverride` | `""` | Replaces the chart name in resource names and labels. |
| `fullnameOverride` | `""` | Replaces the full resource name (default `<release>-sql-processor`). |
| `replicaCount` | `1` | Executor replicas. |
| `serviceAccount.create` | `true` | Create a ServiceAccount. |
| `serviceAccount.name` | `""` | ServiceAccount name. Empty uses the full name. |
| `serviceAccount.annotations` | `{}` | ServiceAccount annotations, for example an IAM role. |
| `rbac.create` | `false` | Create a Role and RoleBinding. |
| `rbac.rules` | `[]` | Rules for that Role. |
| `env` | `[]` | Executor env vars, for example `KAFSQL_*` overrides. |
| `envFromSecret` | `""` | Secret loaded into the executor with `envFrom`, for example `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. |
| `resources` | `{}` | Executor resources. |
| `service.enabled` | `false` | Create the direct executor Service `<fullname>` for Postgres clients. Rendering fails if `proxy.enabled` is also `true`. |
| `service.type` | `ClusterIP` | Executor Service type. |
| `service.port` | `5432` | Port of the direct executor Service, and of `<fullname>-executor` when the proxy is on. |
| `service.targetPort` | `5432` | Executor container port. Keep it equal to the port in `server.listen`. |
| `service.annotations` | `{}` | Executor Service annotations. |
| `proxy.enabled` | `false` | Deploy the KAFSQL proxy and its Service. |
| `proxy.replicaCount` | `2` | Proxy replicas. |
| `proxy.service.type` | `LoadBalancer` | Proxy Service type. |
| `proxy.service.port` | `5432` | Proxy Service and target port. Keep it equal to the port in `proxy.listen`. |
| `proxy.networkPolicy.enabled` | `true` | With the proxy on, add a NetworkPolicy that lets only proxy pods reach the executor Postgres port. |
| `proxy.networkPolicy.extraIngress` | `[]` | Extra NetworkPolicy ingress rules for the executor pods, for example metrics scraping. |
| `proxy.env` | `[]` | Proxy env vars. |
| `proxy.envFromSecret` | `""` | Secret loaded into the proxy with `envFrom`. |
| `proxy.resources` | `{}` | Proxy resources. |
| `config.existingConfigMap` | `""` | Use your own ConfigMap with a `config.yaml` key instead of the chart file. |
| `config.path` | `/config/config.yaml` | Config path passed to `-config`. |

Keep S3 keys in a Kubernetes Secret and point `envFromSecret` at it.
The Secret needs `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Do not pass
keys with `--set`. Read them without echo and create the Secret from a
mode-600 file:

```bash
read -rs -p "S3 access key ID: " S3_KEY_ID; echo
read -rs -p "S3 secret access key: " S3_SECRET; echo
( umask 077; printf 'AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n' "$S3_KEY_ID" "$S3_SECRET" > sql-s3.env )
unset S3_KEY_ID S3_SECRET
kubectl -n kafscale create secret generic sql-s3-credentials --from-env-file=sql-s3.env
rm -f sql-s3.env
```

Then install with your values file, named `my-kafsql-values.yaml` here, that sets
`envFromSecret: sql-s3-credentials`, `service.enabled: true` for direct access
without the proxy, and your settings:

```bash
helm upgrade --install kafsql addons/processors/sql-processor/deploy/helm/sql-processor \
  --namespace kafscale \
  -f my-kafsql-values.yaml
kubectl -n kafscale rollout status deployment/kafsql-sql-processor
```

Clients in the cluster connect to `kafsql-sql-processor.kafscale.svc:5432`.

### Exposing the Executor

The executor accepts any user name, has no TLS and applies no topic ACLs. The
proxy enforces the ACLs and writes the audit log. So the chart creates no
Service for the executor unless you ask:

- Direct access, no proxy: set `service.enabled: true`. Any workload that can
  reach the Service can query every topic. Use it for trusted in-cluster
  clients and local demos.
- Through the proxy: set `proxy.enabled: true` and leave `service.enabled`
  off. Clients use the proxy Service, and the executor is reachable only
  through `<fullname>-executor`, limited to proxy pods by a NetworkPolicy.

Setting both stops rendering with this error, because the direct Service would
let clients skip the proxy ACLs and audit log:

```text
service.enabled and proxy.enabled cannot both be true: the direct executor Service bypasses the proxy ACLs and audit log. Use the proxy Service, or turn the proxy off.
```

Executor pods carry `app.kubernetes.io/component: executor` and proxy pods
`app.kubernetes.io/component: proxy`. Each Service selects only its own
component.
`addons/processors/sql-processor/deploy/kind/values-kind.yaml` is a full
working example for the kind walkthrough below.

## Try It on kind

Watch the SQL processor run first in the [SQL processor demo](/demos/#sql). Clone https://github.com/KafScale/platform and run the commands below from the repo root.

This walkthrough runs KAFSQL on a local kind cluster, produces records, and
queries them with `psql`. Run every command from the repo root.

You need Docker, kind, kubectl, Helm and `psql`.

`make demo-platform-bootstrap` deletes every kind cluster on the host before
it creates `kafscale-demo`. The first run builds the platform images locally.
Kind then pulls `ghcr.io/kafscale/kafscale-sql-processor` (tagged with the
chart `appVersion`) and `edenhill/kcat:1.7.1` from public registries. On a
host where the kind nodes cannot reach those registries, load the images once
the cluster from step 1 exists:

```bash
for image in \
  "ghcr.io/kafscale/kafscale-sql-processor:$(sed -n 's/^appVersion: "\(.*\)"/\1/p' addons/processors/sql-processor/deploy/helm/sql-processor/Chart.yaml)" \
  edenhill/kcat:1.7.1; do
  docker pull "$image" && scripts/kind-load-image.sh "$image" kafscale-demo
done
```

If you already ran the Iceberg processor walkthrough, the platform is up and
you can skip step 1.

### 1. Start the platform

```bash
make demo-platform-bootstrap
kind export kubeconfig --name kafscale-demo
kubectl -n kafscale-demo get kafscalecluster,kafscaletopic
```

On hosts where kube-proxy cannot use iptables, start it with the nftables
kube-proxy instead:

```bash
KAFSCALE_KIND_KUBE_PROXY_MODE=nftables make demo-platform-bootstrap
kind export kubeconfig --name kafscale-demo
```

This gives you namespace `kafscale-demo` with MinIO, operator-managed etcd,
the KafscaleCluster `kafscale` with two brokers, and topic `demo-topic-2` with
two partitions. Brokers write segments to bucket `kafscale-snapshots`.

### 2. Create the S3 credentials Secret

KAFSQL reads the MinIO keys from a Secret. This copies them from the platform
Secret through a mode-600 file, so no key appears on the command line:

```bash
(
  umask 077
  printf 'AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n' \
    "$(kubectl -n kafscale-demo get secret kafscale-s3-credentials -o jsonpath='{.data.KAFSCALE_S3_ACCESS_KEY}' | base64 -d)" \
    "$(kubectl -n kafscale-demo get secret kafscale-s3-credentials -o jsonpath='{.data.KAFSCALE_S3_SECRET_KEY}' | base64 -d)" \
    > sql-s3.env
)
kubectl -n kafscale-demo create secret generic sql-s3-credentials \
  --from-env-file=sql-s3.env --dry-run=client -o yaml | kubectl apply -f -
rm -f sql-s3.env
```

### 3. Install KAFSQL

```bash
helm upgrade --install kafsql addons/processors/sql-processor/deploy/helm/sql-processor \
  --namespace kafscale-demo \
  -f addons/processors/sql-processor/deploy/kind/values-kind.yaml
kubectl -n kafscale-demo rollout status deployment/kafsql --timeout=300s
kubectl -n kafscale-demo get deployment,service kafsql
```

`values-kind.yaml` points KAFSQL at MinIO and the cluster etcd with
`KAFSQL_*` environment variables and loads the Secret with `envFromSecret`.
`values-kind.yaml` also sets `service.enabled: true`, so the chart Service
`kafsql` exposes the executor Postgres port 5432 directly. That is fine for a
local demo; see Exposing the Executor before you do the same elsewhere.

### 4. Produce 200 records

```bash
seq 1 200 | awk '{printf "{\"order_id\":%d,\"status\":\"new\"}\n", $1}' | \
  kubectl -n kafscale-demo run kcat-produce --rm -i --restart=Never --image=edenhill/kcat:1.7.1 -- \
  -b kafscale-broker:9092 -t demo-topic-2 -P
```

KAFSQL reads completed segments from S3. Wait until the brokers have flushed
them:

```bash
for i in $(seq 1 60); do
  kubectl -n kafscale-demo exec deploy/minio -- sh -c 'ls /data/kafscale-snapshots/kafscale-demo/demo-topic-2/*/ 2>/dev/null' | grep -q '\.kfs' && break
  sleep 5
done
kubectl -n kafscale-demo exec deploy/minio -- sh -c 'ls /data/kafscale-snapshots/kafscale-demo/demo-topic-2/*/'
```

### 5. Query with psql

Forward the Service to local port 15432, so a local Postgres on 5432 does not
get in the way:

```bash
kubectl -n kafscale-demo port-forward svc/kafsql 15432:5432 >/tmp/kafsql-port-forward.log 2>&1 &
sleep 3
```

KAFSQL accepts any user name and does not use TLS on this port:

```bash
psql "host=localhost port=15432 user=kafsql dbname=kfs sslmode=disable" -c "SHOW TOPICS;"
psql "host=localhost port=15432 user=kafsql dbname=kfs sslmode=disable" -c "SELECT _partition, _offset, _ts, json_value(_value, '$.order_id') AS order_id, json_value(_value, '$.status') AS status FROM demo-topic-2 TAIL 5;"
psql "host=localhost port=15432 user=kafsql dbname=kfs sslmode=disable" -c "SELECT count(*) FROM demo-topic-2 LAST 1h;"
```

The count matches the 200 records you produced. For an interactive session,
run `psql "host=localhost port=15432 user=kafsql dbname=kfs sslmode=disable"`
and try the queries in Example Queries.

### 6. Clean up

Stop the port-forward and remove KAFSQL. The last command deletes the whole
kind cluster.

```bash
pkill -f 'kubectl -n kafscale-demo port-forward svc/kafsql' || true
helm -n kafscale-demo uninstall kafsql
kubectl -n kafscale-demo delete secret sql-s3-credentials
kind delete cluster --name kafscale-demo
```

## One-Command Demo

`make kafsql-demo` runs a scripted end-to-end check. From the repo root:

```bash
make kafsql-demo
```

It runs `make demo-platform-bootstrap` with one broker, then creates its own
namespace `kafsql-demo` with a KafscaleCluster `kafsql` and topic
`kafsql-demo-topic`. It produces 200 records, installs the chart, and runs
`psql` checks from a pod: `SHOW TOPICS`, `TAIL 5`, `count(*)` and the max
offset. On success it uninstalls the release and deletes the demo topic and
KafscaleCluster. The kind cluster stays up. To keep the demo objects too:

```bash
KAFSQL_DEMO_CLEANUP=0 make kafsql-demo
kind export kubeconfig --name kafscale-demo
```

On hosts where kube-proxy cannot use iptables, add
`KAFSCALE_KIND_KUBE_PROXY_MODE=nftables` in front of the same command.

The demo runs two managed etcd clusters on one kind node: three members for
the platform and one for `kafsql`. Each in-memory member requests 4 GiB, so
the node needs more than 16 GiB of allocatable memory. On smaller hosts, run the
platform with a single etcd member:

```bash
KAFSCALE_DEMO_ETCD_REPLICAS=1 KAFSQL_DEMO_CLEANUP=0 make kafsql-demo
```

Variables (set them in front of `make`):

| Variable | Default | What it does |
| --- | --- | --- |
| `KAFSQL_DEMO_CLEANUP` | `1` | `1` removes the release, producer job, topic and KafscaleCluster after a successful run. `0` keeps them. |
| `KAFSQL_DEMO_NAMESPACE` | `kafsql-demo` | Namespace for the demo cluster and KAFSQL. |
| `KAFSQL_DEMO_CLUSTER` | `kafsql` | KafscaleCluster the script creates. |
| `KAFSQL_DEMO_TOPIC` | `kafsql-demo-topic` | Topic to create and produce to. |
| `KAFSQL_DEMO_RECORDS` | `200` | Records to produce. The `count(*)` check expects this number. |
| `KAFSQL_DEMO_TIMEOUT_SEC` | `120` | Producer timeout in seconds. |
| `KAFSQL_DEMO_CREATE_BUCKET` | `1` | `1` creates the bucket with the AWS CLI. The pod reads the keys from the Secret. |
| `KAFSQL_DEMO_JOB_WAIT_TIMEOUT` | `180s` | How long to wait for the producer job. |
| `KAFSQL_DEMO_ETCD_WAIT_ATTEMPTS`, `KAFSQL_DEMO_ETCD_WAIT_SLEEP_SEC` | `80`, `3` | Retries and delay while waiting for an etcd leader. |
| `KAFSQL_PROCESSOR_RELEASE` | `kafsql-processor-dev` | Helm release name, also used as the Service name. |
| `SQL_PROCESSOR_IMAGE` | `ghcr.io/kafscale/kafscale-sql-processor:dev` | KAFSQL image, built by `make docker-build`. |
| `E2E_CLIENT_IMAGE` | `ghcr.io/kafscale/kafscale-e2e-client:dev` | Producer image. |
| `MINIO_BUCKET` | `kafscale` | Bucket for the demo cluster segments. |
| `MINIO_REGION` | `us-east-1` | S3 region. |
| `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` | Makefile demo defaults | MinIO keys. The script writes them to a mode-600 file and creates Secret `kafsql-s3-credentials` from it. |
| `KAFSCALE_DEMO_ETCD_REPLICAS` | `3` | Managed etcd members for the platform cluster. |
| `KUBECONFIG` | generated by Makefile | Initial kubeconfig. The script replaces it with a fresh temporary one from kind. |
| `TMPDIR` | `/tmp` | Directory scanned for temporary `kafscale-kind-kubeconfig.*` files to remove. |
| `KAFSCALE_DEMO_NAMESPACE` | `kafscale-demo` | Namespace of the platform operator and MinIO. |
| `KAFSCALE_KIND_CLUSTER` | `kafscale-demo` | kind cluster name. |
| `KAFSCALE_KIND_KUBE_PROXY_MODE` | empty | `nftables` creates the kind cluster with the nftables kube-proxy. |
