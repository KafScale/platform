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

# KAFSQL User Guide

This guide targets operators and platform engineers deploying KAFSQL. It
explains configuration, runtime behavior, and operational guidance. For
implementation details, see `developer.md`.

## What It Does

- Reads completed KFS segments directly from S3.
- Exposes the Postgres wire protocol for JDBC/BI compatibility.
- Supports ad-hoc SQL with Kafka-native extensions (LAST/TAIL/SCAN FULL).
- Provides bounded two-topic joins for S3-native enrichment queries.

## Feature Highlights

- Stateless executors and S3-native reads (no Kafka broker dependency).
- Kafka semantics exposed as SQL columns.
- Query governance and cost visibility.
- Optional proxy for external access and ACL enforcement.

## What It Does Not Do

- Continuous streaming queries or materialized views.
- Full SQL feature parity or multi-join chains.
- Writes or updates.

## Prerequisites

- Access to the S3 bucket containing KFS segments.
- Optional: etcd endpoints for topic metadata (or rely on S3 discovery).
- Optional: schema definitions in YAML for typed columns.
- Optional: proxy deployment for external access.

## Segment Layout

```
s3://{bucket}/{namespace}/{topic}/{partition}/segment-{base_offset}.kfs
s3://{bucket}/{namespace}/{topic}/{partition}/segment-{base_offset}.index
```

KFS segment formats are defined in `kafscale-spec.md`.

## Configuration Overview

KAFSQL reads a YAML config file mounted into the container.

Required fields:
- `s3.bucket`

Common fields:
```yaml
s3:
  bucket: kafscale-data
  namespace: production
  endpoint: ""
  region: us-east-1
  path_style: false

metadata:
  discovery: etcd
  etcd:
    endpoints:
      - http://etcd.kafscale.svc.cluster.local:2379
  snapshot:
    key: /kafscale/metadata/snapshot
    ttl_seconds: 60

server:
  listen: ":5432"
  max_connections: 100
  metrics_listen: ":9090"

discovery_cache:
  ttl_seconds: 60
  max_entries: 10000

query:
  default_limit: 1000
  require_time_bound: true
  max_unbounded_scan: 1000
  max_scan_bytes: 10737418240
  max_scan_segments: 10000
  max_rows: 100000
  timeout_seconds: 30
  max_concurrent: 20
  queue_size: 50
  queue_timeout_seconds: 10

result_cache:
  ttl_seconds: 30
  max_entries: 100
  max_rows: 10000

discovery_manifest:
  enabled: false
  key: manifest.json
  ttl_seconds: 60
  build_interval_seconds: 0
  build_max_segments: 0
  build_max_bytes: 0
  build_lease_ttl_seconds: 120

time_index:
  enabled: false
  key_suffix: .kfst
  build_max_segments: 0
  build_max_bytes: 0
  build_lease_ttl_seconds: 120

proxy:
  listen: ":5432"
  upstreams:
    - kafsql.kafka.svc.cluster.local:5432
  max_connections: 200
  cache_ttl_seconds: 10
  cache_max_entries: 1000
  acl:
    allow: []
    deny: []
```

For MinIO or any non-AWS S3 endpoint:
```yaml
s3:
  bucket: kafscale-snapshots
  namespace: kafscale-demo
  endpoint: http://minio.kafscale-demo.svc.cluster.local:9000
  region: us-east-1
  path_style: true
```

## Query Model

Implicit columns:
- `_topic`, `_partition`, `_offset`, `_ts`
- `_key`, `_value`, `_headers`, `_segment`

Extensions:
- `TAIL <n>`: read last N records.
- `LAST <duration>`: time-bounded scan.
- `SCAN FULL`: explicit unbounded scan.

System introspection:
- `SHOW TOPICS;`
- `SHOW PARTITIONS FROM <topic>;`
- `DESCRIBE <topic>;`
- `EXPLAIN SELECT ...;`

Supported SQL subset:
- `SELECT` with projections and aliases.
- `WHERE` on `_partition`, `_offset`, `_ts`, `_key`.
- `ORDER BY _ts` (ASC/DESC).
- Aggregates: `COUNT`, `MIN`, `MAX`, `SUM`, `AVG`.
- `GROUP BY` on explicit columns.
- JSON helpers in SELECT: `json_value`, `json_query`, `json_exists`.

Join constraints (v0.6):
- Two topics only.
- Key equality or JSON field equality.
- Requires `WITHIN <duration>` and `LAST <duration>`.
- Left or inner joins only.

Join output naming:
- Right-side implicit columns are prefixed with `_right_` unless aliased.

## Query Governance

Queries without time bounds are rejected by default. Use:
- `LAST <duration>`
- `TAIL <n>`
- `SCAN FULL` with an explicit LIMIT

Guardrails:
- `query.max_scan_bytes` and `query.max_scan_segments` cap scan size.
- `query.max_rows` caps returned rows.
- `query.timeout_seconds` cancels slow queries.
- `query.max_concurrent` + queue settings cap concurrent work.

EXPLAIN provides best-effort estimates using segment sizes and any available
manifest/time index metadata.

## Schema-on-Read Columns

If you define schemas per topic, KAFSQL exposes typed columns alongside the
implicit Kafka columns. Example:

```yaml
metadata:
  topics:
    - name: orders
      partitions: [0, 1]
      schema:
        columns:
          - name: order_id
            type: string
            path: $.order_id
          - name: amount
            type: double
            path: $.amount
```

## Result Caching

Result caching is enabled when `result_cache.*` is set and applies to SELECT
queries with explicit time bounds. Cache is skipped for:
- `TAIL`
- `SCAN FULL`
- unbounded queries without time windows

Each cache entry stores rows and schema metadata up to `result_cache.max_rows`.

## Example Queries

```
SELECT * FROM orders TAIL 100;

SELECT * FROM orders LAST 15m;

SELECT _partition, count(*), max(_ts) AS latest
FROM orders LAST 5m
GROUP BY _partition;

SELECT o._key, o._value, p._value
FROM orders o
JOIN payments p ON o._key = p._key
WITHIN 10m
LAST 1h;

EXPLAIN SELECT * FROM orders LAST 24h;
```

## Client Compatibility

- Simple query protocol is supported by all drivers.
- Extended protocol is supported by the executor, but the proxy only accepts
  simple query messages.
- `pg_catalog` and `information_schema` are populated for BI tools.

## Proxy Access (Optional)

The proxy is used for external access:
- TLS termination (via ingress or mesh).
- Auth via external gateway or mesh plus topic ACL enforcement.
- Query auditing.

Deploy proxy only when you need external access. In-cluster use can skip it.

Proxy ACLs are applied before any S3 access. If the proxy cannot authorize a
query, it returns an error without forwarding.

## External Metadata (Phase 4)

To reduce S3 list storms and improve time pruning, KAFSQL can use:
- Partition manifests (`manifest.json`).
- Time index sidecars (`.kfst`) for min/max ts and offsets.

Use the backfill tool to generate or refresh these:
```
./kafsql-backfill -config /config/config.yaml -mode all
./kafsql-backfill -config /config/config.yaml -mode manifest
./kafsql-backfill -config /config/config.yaml -mode time-index -interval 5m
```

## Metrics

Prometheus metrics are exposed on `server.metrics_listen` (default `:9090`):
- Query totals/durations/rows/bytes.
- S3 request counts and durations.
- Queue depth and rejection counters.

## Logs and Audit

Executor logs include query duration, rows, segments, bytes, and a trimmed query.
Proxy audit logs include allow/deny decisions, topics, and client IP.

## Helm Deployment

The chart lives in `addons/processors/sql-processor/deploy/helm/sql-processor`.
By default it runs `ghcr.io/kafscale/kafscale-sql-processor` with the chart's
`appVersion` as the tag, and creates a ClusterIP Service for the Postgres port.
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
| `service.enabled` | `true` | Create the executor Service. |
| `service.type` | `ClusterIP` | Executor Service type. |
| `service.port` | `5432` | Service port for Postgres clients. |
| `service.targetPort` | `5432` | Executor container port. Keep it equal to the port in `server.listen`. |
| `service.annotations` | `{}` | Executor Service annotations. |
| `proxy.enabled` | `false` | Deploy the KAFSQL proxy and its Service. |
| `proxy.replicaCount` | `2` | Proxy replicas. |
| `proxy.service.type` | `LoadBalancer` | Proxy Service type. |
| `proxy.service.port` | `5432` | Proxy Service and target port. Keep it equal to the port in `proxy.listen`. |
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
`envFromSecret: sql-s3-credentials` and your settings:

```bash
helm upgrade --install kafsql addons/processors/sql-processor/deploy/helm/sql-processor \
  --namespace kafscale \
  -f my-kafsql-values.yaml
kubectl -n kafscale rollout status deployment/kafsql-sql-processor
```

Clients in the cluster connect to `kafsql-sql-processor.kafscale.svc:5432`.
`addons/processors/sql-processor/deploy/kind/values-kind.yaml` is a full
working example for the kind walkthrough below.

## Local Run

Build and run the binary from the repo root against your own S3 bucket. `config/config.yaml` is a
template: set `s3.bucket`, `s3.namespace`, `s3.endpoint` and the etcd
endpoints for your environment first, and export `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` (for example with `read -rs`).

```bash
cd addons/processors/sql-processor
make build
./bin/sql-processor -config config/config.yaml
```

Connect with:

```bash
psql "host=localhost port=5432 user=kafsql dbname=kfs sslmode=disable"
```

## Try It on kind

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
The chart Service `kafsql` exposes the Postgres port 5432.

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
