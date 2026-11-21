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

# Iceberg Processor User Guide

This guide is for operators and platform engineers deploying the Iceberg
processor in Kubernetes. It focuses on configuration, behavior, and operations.
For implementation details and code changes, see `developer.md`.

## What It Does

- Reads completed KafScale segments from S3.
- Decodes records and writes them to Iceberg tables (append-only).
- Tracks offsets with a lease-per-partition model (at-least-once).
- Optional JSON schema validation and schema-driven columns.

## Feature Highlights

- Storage-native processing (no Kafka protocol or brokers required).
- Iceberg REST catalog support with auto-create tables.
- Mapping-driven or registry-driven columns with schema evolution.
- Lease-based offsets with replay-safe at-least-once semantics.
- Metrics and health endpoints for ops visibility.

## Prerequisites

- Access to the S3 bucket where KafScale snapshots are stored.
- An Iceberg REST catalog endpoint reachable from the cluster.
- An offsets backend (etcd by default).
- Optional: JSON schema registry endpoint for validation or column discovery.

## Configuration Overview

The processor reads a YAML config file (mounted into the container).

Required fields:
- `s3.bucket`
- `iceberg.catalog.type`
- `iceberg.catalog.uri`
- `etcd.endpoints` (when `offsets.backend=etcd`)
- `mappings[]` with `topic` and `table`

Common fields:
```yaml
s3:
  bucket: kafscale-data
  namespace: production
  endpoint: ""
  region: us-east-1
  path_style: false

iceberg:
  catalog:
    type: rest
    uri: https://iceberg-catalog.example.com
    token: ""
    username: ""
    password: ""
  warehouse: s3://iceberg-warehouse/production

offsets:
  backend: etcd
  lease_ttl_seconds: 30
  key_prefix: processors

discovery:
  mode: auto

etcd:
  endpoints:
    - http://etcd.kafscale.svc.cluster.local:2379
  username: ""
  password: ""
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

Warehouse semantics:
`iceberg.warehouse` is the S3 prefix where Iceberg metadata and parquet files
  are written directly by the processor and where the Iceberg REST service has 
  to point to.

## Security and Data Governance

The operator is responsible for storage security and governance:
- IAM roles/permissions for S3 access.
- Encryption at rest (SSE-S3 or SSE-KMS) and in transit.
- Bucket policies, logging, and data retention.

If you use Databricks (Unity Catalog) on AWS S3, point the catalog at the same
warehouse path. The processor writes Iceberg tables there; Databricks reads the
metadata and parquet files directly from S3.

## Topic-to-Table Mapping

```yaml
mappings:
  - topic: orders
    table: prod.orders
    mode: append
    create_table_if_missing: true
```

Notes:
- Only `append` is supported.
- `create_table_if_missing` auto-creates tables when topics are new.

## Schema Columns and Evolution

You can define columns directly in the mapping or resolve them from a registry.

Mapping-defined columns:
```yaml
mappings:
  - topic: orders
    table: prod.orders
    mode: append
    create_table_if_missing: true
    schema:
      columns:
        - name: order_id
          type: long
          required: true
        - name: status
          type: string
      allow_type_widening: true
```

Registry-driven columns:
```yaml
schema:
  mode: "off"
  registry:
    base_url: https://schemas.example.com
    timeout_seconds: 5
    cache_seconds: 300

mappings:
  - topic: orders
    table: prod.orders
    schema:
      source: registry
```

Supported column types:
`boolean`, `int`, `long`, `float`, `double`, `string`, `binary`, `timestamp`,
`date`.

## Schema Validation (Optional)

`schema.mode` controls JSON validation against the registry:
- `off`: no validation.
- `lenient`: drops invalid records and continues.
- `strict`: stops on validation errors.

Validation fetches schemas from `schema.registry.base_url/<topic>.json`.

## Discovery Modes

- `auto`: uses etcd for topic/partition lists when provided, always uses S3 for
  segment discovery.
- `etcd`: requires `etcd.endpoints` and skips S3 topic listing.
- `s3`: ignores etcd and relies on S3 listing only.

## Offsets and Leases

Offsets are tracked per topic partition with a TTL lease:
- Only one worker advances a partition at a time.
- If a pod dies, the lease expires and another pod resumes.
- At-least-once semantics by design.

Tune `offsets.lease_ttl_seconds` based on segment size and processing time.

## Record IDs and Idempotency

Each record includes a deterministic `record_id` column of the form
`<topic>:<partition>:<offset>`. Downstream consumers can use this to dedupe
replays (at-least-once semantics) without relying on Kafka metadata.

## Write Serialization (Iceberg)

Table create/load/evolve/commit operations are serialized per topic to avoid
Iceberg commit races. This keeps correctness but reduces per-topic concurrency.

## Deployment (Helm)

The chart lives in `addons/processors/iceberg-processor/deploy/helm/iceberg-processor`.
By default it runs `ghcr.io/kafscale/kafscale-iceberg-processor` with the
chart's `appVersion` as the tag. Use `deploy/helm/iceberg-processor/values.yaml`
as the base and override what you need in your own values file.

Helm values:

| Key | Default | What it does |
| --- | --- | --- |
| `image.repository` | `ghcr.io/kafscale/kafscale-iceberg-processor` | Processor image. |
| `image.tag` | `""` | Image tag. Empty uses the chart `appVersion`. |
| `image.pullPolicy` | `IfNotPresent` | Image pull policy. |
| `nameOverride` | `""` | Replaces the chart name in resource names and labels. |
| `fullnameOverride` | `""` | Replaces the full resource name (default `<release>-iceberg-processor`). |
| `replicaCount` | `1` | Processor replicas. Work is split per partition lease. |
| `serviceAccount.create` | `true` | Create a ServiceAccount. |
| `serviceAccount.name` | `""` | ServiceAccount name. Empty uses the full name. |
| `serviceAccount.annotations` | `{}` | ServiceAccount annotations, for example an IAM role. |
| `rbac.create` | `false` | Create a Role and RoleBinding. |
| `rbac.rules` | `[]` | Rules for that Role. |
| `env` | `[]` | Extra container env vars. |
| `envFromSecret` | `""` | Secret loaded with `envFrom`. Ignored when `s3.credentialsSecretRef` is set. |
| `s3.credentialsSecretRef` | `""` | Secret with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, loaded with `envFrom`. |
| `resources` | `{}` | Container resources. |
| `service.type` | `ClusterIP` | Metrics Service type. |
| `service.port` | `9093` | Metrics Service and declared container port. To change the listener too, set `KAFSCALE_METRICS_ADDR` in `env`. |
| `service.annotations` | `{}` | Metrics Service annotations. |
| `config.existingConfigMap` | `""` | Use your own ConfigMap with a `config.yaml` key instead of rendering `config`. |
| `config.path` | `/config/config.yaml` | Config path passed to `-config`. |
| `config.s3`, `config.iceberg`, `config.offsets`, `config.processor`, `config.discovery`, `config.etcd`, `config.schema`, `config.mappings` | see `values.yaml` | Rendered into `config.yaml`. See Configuration Overview. |

Keep S3 keys in a Kubernetes Secret and point `s3.credentialsSecretRef` at it.
The Secret needs `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Do not pass
keys with `--set`. Read them without echo and create the Secret from a
mode-600 file:

```bash
read -rs -p "S3 access key ID: " S3_KEY_ID; echo
read -rs -p "S3 secret access key: " S3_SECRET; echo
( umask 077; printf 'AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n' "$S3_KEY_ID" "$S3_SECRET" > iceberg-s3.env )
unset S3_KEY_ID S3_SECRET
kubectl -n kafscale create secret generic iceberg-s3-credentials --from-env-file=iceberg-s3.env
rm -f iceberg-s3.env
```

Then install with your values file, named `my-iceberg-values.yaml` here, that sets
`s3.credentialsSecretRef: iceberg-s3-credentials` and your settings:

```bash
helm upgrade --install iceberg-processor addons/processors/iceberg-processor/deploy/helm/iceberg-processor \
  --namespace kafscale \
  -f my-iceberg-values.yaml
kubectl -n kafscale rollout status deployment/iceberg-processor-iceberg-processor
```

`addons/processors/iceberg-processor/deploy/kind/values-kind.yaml` is a full
working example for the kind walkthrough below.

## Metrics and Health

Default metrics bind: `KAFSCALE_METRICS_ADDR` (default `:9093`).
- `/metrics` exposes Prometheus metrics.
- `/healthz` returns `ok`.

Key metrics:
- `kafscale_processor_records_total{topic,result}`
- `kafscale_processor_batches_total{topic}`
- `kafscale_processor_write_latency_ms{topic}`
- `kafscale_processor_errors_total{stage}`
- `kafscale_processor_last_offset{topic,partition}`
- `kafscale_processor_watermark_offset{topic,partition}`
- `kafscale_processor_watermark_timestamp_ms{topic,partition}`

## Scaling (Operational Behavior)

Work is partition-scoped. One worker holds the lease for a partition at a time.
Throughput scales by increasing partitions. The Helm chart does not install an
HPA; set `replicaCount` (or add your own HPA) and size pod resources based on
your workload.

Minimal HPA example:
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: iceberg-processor
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: iceberg-processor
  minReplicas: 1
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 80
```

## Troubleshooting

- Missing data: verify S3 path `s3://{bucket}/{namespace}/{topic}/{partition}`.
- Schema errors: check registry URL and payloads for valid JSON.
- Catalog errors: verify REST endpoint and credentials.
- Offsets not advancing: check etcd connectivity and lease TTL.

## Try It on kind

This walkthrough runs the processor on a local kind cluster and proves the
result three ways: parquet files in MinIO, the table in the REST catalog, and
the rows read back by DuckDB, an independent engine. Run every command from
the repo root.

You need Docker, kind, kubectl, Helm, curl, jq and the DuckDB CLI 2.0 or
newer (see https://duckdb.org/install). DuckDB downloads its `httpfs`, `aws`
and `iceberg` extensions on first use.

`make demo-platform-bootstrap` deletes every kind cluster on the host before
it creates `kafscale-demo`. The first run builds the platform images locally.
Kind then pulls `tabulario/iceberg-rest:1.6.0`,
`ghcr.io/kafscale/kafscale-iceberg-processor` (tagged with the chart
`appVersion`) and `edenhill/kcat:1.7.1` from public registries. On a host
where the kind nodes cannot reach those registries, load the images once the
cluster from step 1 exists:

```bash
for image in tabulario/iceberg-rest:1.6.0 \
  "ghcr.io/kafscale/kafscale-iceberg-processor:$(sed -n 's/^appVersion: "\(.*\)"/\1/p' addons/processors/iceberg-processor/deploy/helm/iceberg-processor/Chart.yaml)" \
  edenhill/kcat:1.7.1; do
  docker pull "$image" && scripts/kind-load-image.sh "$image" kafscale-demo
done
```

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
the KafscaleCluster `kafscale` with two brokers, and topic `demo-topic-1` with
three partitions. Brokers write segments to bucket `kafscale-snapshots`.

### 2. Create the S3 credentials Secret

The processor and the catalog read the MinIO keys from a Secret. This copies
them from the platform Secret through a mode-600 file, so no key appears on
the command line:

```bash
(
  umask 077
  printf 'AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n' \
    "$(kubectl -n kafscale-demo get secret kafscale-s3-credentials -o jsonpath='{.data.KAFSCALE_S3_ACCESS_KEY}' | base64 -d)" \
    "$(kubectl -n kafscale-demo get secret kafscale-s3-credentials -o jsonpath='{.data.KAFSCALE_S3_SECRET_KEY}' | base64 -d)" \
    > iceberg-s3.env
)
kubectl -n kafscale-demo create secret generic iceberg-s3-credentials \
  --from-env-file=iceberg-s3.env --dry-run=client -o yaml | kubectl apply -f -
rm -f iceberg-s3.env
```

### 3. Start the Iceberg REST catalog

```bash
kubectl apply -f addons/processors/iceberg-processor/deploy/kind/iceberg-rest.yaml
kubectl -n kafscale-demo rollout status deployment/iceberg-rest --timeout=300s
```

The catalog stores tables under `s3://kafscale-snapshots/iceberg`.

### 4. Install the processor

```bash
helm upgrade --install iceberg-processor addons/processors/iceberg-processor/deploy/helm/iceberg-processor \
  --namespace kafscale-demo \
  -f addons/processors/iceberg-processor/deploy/kind/values-kind.yaml
kubectl -n kafscale-demo rollout status deployment/iceberg-processor --timeout=300s
kubectl -n kafscale-demo get deployment iceberg-rest iceberg-processor
```

`values-kind.yaml` maps topic `demo-topic-1` to table `demo.demo_topic_1`.
The processor creates the namespace and the table on first write.

### 5. Produce 50 records

```bash
seq 1 50 | awk '{printf "{\"order_id\":%d,\"status\":\"new\"}\n", $1}' | \
  kubectl -n kafscale-demo run kcat-produce --rm -i --restart=Never --image=edenhill/kcat:1.7.1 -- \
  -b kafscale-broker:9092 -t demo-topic-1 -P
```

### 6. Prove it

Segments written by the brokers, then the parquet files written by the
processor. The loop waits until the first parquet file shows up:

```bash
kubectl -n kafscale-demo exec deploy/minio -- sh -c 'ls /data/kafscale-snapshots/kafscale-demo/demo-topic-1/*/'
for i in $(seq 1 60); do
  kubectl -n kafscale-demo exec deploy/minio -- sh -c 'ls /data/kafscale-snapshots/iceberg/demo/demo_topic_1/data/ 2>/dev/null' | grep -q parquet && break
  sleep 5
done
kubectl -n kafscale-demo exec deploy/minio -- ls /data/kafscale-snapshots/iceberg/demo/demo_topic_1/data/
```

Open port-forwards to the catalog and to MinIO:

```bash
kubectl -n kafscale-demo port-forward svc/iceberg-rest 8181:8181 >/tmp/iceberg-rest-port-forward.log 2>&1 &
kubectl -n kafscale-demo port-forward svc/minio 9000:9000 >/tmp/minio-port-forward.log 2>&1 &
sleep 3
```

Ask the catalog for the table. `total-records` should be 50:

```bash
curl -fsS http://localhost:8181/v1/namespaces/demo/tables | jq -c .
curl -fsS http://localhost:8181/v1/namespaces/demo/tables/demo_topic_1 | jq '{
  location: .metadata.location,
  columns: [.metadata.schemas[-1].fields[] | "\(.name) \(.type)"],
  "total-records": .metadata.snapshots[-1].summary["total-records"]
}'
```

Read the rows with DuckDB through the same REST catalog. DuckDB takes the
MinIO keys from environment variables that are filled from the Secret:

```bash
export AWS_ACCESS_KEY_ID="$(kubectl -n kafscale-demo get secret iceberg-s3-credentials -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)"
export AWS_SECRET_ACCESS_KEY="$(kubectl -n kafscale-demo get secret iceberg-s3-credentials -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)"
duckdb <<'SQL'
INSTALL httpfs;
LOAD httpfs;
INSTALL aws;
LOAD aws;
INSTALL iceberg;
LOAD iceberg;
CREATE SECRET minio (TYPE s3, PROVIDER credential_chain, CHAIN 'env', ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');
ATTACH 'warehouse' AS lake (TYPE iceberg, URI 'http://localhost:8181', AUTHORIZATION_TYPE 'none');
SELECT count(*) AS records FROM lake.demo.demo_topic_1;
SELECT record_id, "partition", "offset", decode(value) AS value FROM lake.demo.demo_topic_1 ORDER BY "partition", "offset" LIMIT 5;
SQL
```

The count matches the 50 records you produced, and `value` shows the JSON
payloads.

DuckDB downloads `httpfs` over plain HTTP, because it needs `httpfs` for
HTTPS. If your network only allows HTTPS, install `httpfs` from a file first.
After that, DuckDB uses HTTPS for the other extensions:

```bash
duckdb_version="$(duckdb -noheader -list -c 'SELECT version();')"
duckdb_platform="$(duckdb -noheader -list -c 'PRAGMA platform;')"
curl -fsSL "https://extensions.duckdb.org/${duckdb_version}/${duckdb_platform}/httpfs.duckdb_extension.gz" | gunzip > /tmp/httpfs.duckdb_extension
duckdb -c "INSTALL '/tmp/httpfs.duckdb_extension'; LOAD httpfs; INSTALL aws; INSTALL avro; INSTALL iceberg;"
rm -f /tmp/httpfs.duckdb_extension
```

### 7. Clean up

Stop the port-forwards and remove the processor and catalog. The last command
deletes the whole kind cluster.

```bash
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
pkill -f 'kubectl -n kafscale-demo port-forward svc/iceberg-rest' || true
pkill -f 'kubectl -n kafscale-demo port-forward svc/minio' || true
helm -n kafscale-demo uninstall iceberg-processor
kubectl delete -f addons/processors/iceberg-processor/deploy/kind/iceberg-rest.yaml
kubectl -n kafscale-demo delete secret iceberg-s3-credentials
kind delete cluster --name kafscale-demo
```

## One-Command Demo

`make iceberg-demo` runs the same flow as a scripted end-to-end check. From
the repo root:

```bash
make iceberg-demo
```

It runs `make demo-platform-bootstrap` with one broker, builds the processor
image locally as `iceberg-processor:dev` (this needs `rsync`), deploys the REST catalog and the
chart, produces 50 records to `demo-topic-1`, and checks the segments, the
parquet files and the catalog `total-records`. On success it deletes the
cluster objects it created and then the whole kind cluster. To keep everything
running afterwards:

```bash
ICEBERG_DEMO_CLEANUP=0 ICEBERG_DEMO_CLEANUP_KIND=0 make iceberg-demo
kind export kubeconfig --name kafscale-demo
```

On hosts where kube-proxy cannot use iptables, add
`KAFSCALE_KIND_KUBE_PROXY_MODE=nftables` in front of the same command.

Variables (set them in front of `make`):

| Variable | Default | What it does |
| --- | --- | --- |
| `ICEBERG_DEMO_CLEANUP` | `1` | `1` deletes the demo topic and KafscaleCluster after a successful run. `0` keeps them. |
| `ICEBERG_DEMO_CLEANUP_KIND` | `1` | `1` deletes the kind cluster after a successful run. `0` keeps it. Failed runs always keep it. |
| `ICEBERG_DEMO_KEEP_KUBECONFIG` | `0` | `1` keeps the temporary kubeconfig the script writes and prints its path. |
| `ICEBERG_DEMO_NAMESPACE` | `kafscale-demo` | Namespace of the KafscaleCluster, catalog and processor. |
| `ICEBERG_DEMO_CLUSTER` | `kafscale` | KafscaleCluster the demo uses. It must already exist. |
| `ICEBERG_DEMO_TOPIC` | `demo-topic-1` | Topic to create and produce to. |
| `ICEBERG_DEMO_TABLE` | `demo.demo_topic_1` | Target table as `namespace.table`. |
| `ICEBERG_DEMO_TABLE_NAMESPACE`, `ICEBERG_DEMO_TABLE_NAME` | parts of `ICEBERG_DEMO_TABLE` | Table namespace and name, if you want to set them apart. |
| `ICEBERG_DEMO_RECORDS` | `50` | Records to produce. |
| `ICEBERG_DEMO_TIMEOUT_SEC` | `120` | Producer timeout in seconds. |
| `ICEBERG_DEMO_CREATE_BUCKET` | `0` | `1` creates the warehouse bucket with the AWS CLI before the run. |
| `ICEBERG_DEMO_JOB_WAIT_TIMEOUT` | `180s` | How long to wait for the producer job. |
| `ICEBERG_DEMO_TOPIC_WAIT_TIMEOUT` | `180s` | How long to wait for the topic to be Ready. |
| `ICEBERG_DEMO_KUBE_SYSTEM_TIMEOUT` | `120s` | How long to wait for the kind control plane and CoreDNS. |
| `ICEBERG_DEMO_ETCD_WAIT_ATTEMPTS`, `ICEBERG_DEMO_ETCD_WAIT_SLEEP_SEC` | `80`, `3` | Retries and delay while waiting for an etcd leader. |
| `ICEBERG_DEMO_ETCD_STABLE_ATTEMPTS`, `ICEBERG_DEMO_ETCD_STABLE_SLEEP_SEC`, `ICEBERG_DEMO_ETCD_STABLE_CONSECUTIVE` | `30`, `2`, `3` | Retries, delay and consecutive healthy checks for etcd stability. |
| `ICEBERG_REST_IMAGE` | `tabulario/iceberg-rest:1.6.0` | REST catalog image. |
| `ICEBERG_REST_PORT` | `8181` | REST catalog port. |
| `ICEBERG_WAREHOUSE_BUCKET` | `kafscale-snapshots` | Bucket for segments and the warehouse. |
| `ICEBERG_WAREHOUSE_PREFIX` | `iceberg` | Warehouse prefix in that bucket. |
| `ICEBERG_PROCESSOR_IMAGE` | `iceberg-processor:dev` | Processor image to build and load. |
| `ICEBERG_PROCESSOR_BUILD_FLAGS` | empty | Extra Go build flags for the processor image. |
| `ICEBERG_PROCESSOR_RELEASE` | `iceberg-processor-dev` | Helm release name. |
| `ICEBERG_PROCESSOR_REST_DEBUG` | empty | Non-empty turns on REST catalog request logging in the processor. |
| `E2E_CLIENT_IMAGE` | `ghcr.io/kafscale/kafscale-e2e-client:dev` | Producer image. |
| `MINIO_REGION` | `us-east-1` | S3 region. |
| `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` | Makefile demo defaults | MinIO keys. The script writes them to a mode-600 file and creates Secret `iceberg-s3-credentials` from it. |
| `KUBECONFIG` | generated by Makefile | Initial kubeconfig. The script replaces it with a fresh temporary one from kind. |
| `TMPDIR` | `/tmp` | Directory scanned for temporary `kafscale-kind-kubeconfig.*` files to remove. |
| `KAFSCALE_DEMO_NAMESPACE` | `kafscale-demo` | Namespace of the platform operator and MinIO. |
| `KAFSCALE_KIND_CLUSTER` | `kafscale-demo` | kind cluster name. |
| `KAFSCALE_KIND_KUBE_PROXY_MODE` | empty | `nftables` creates the kind cluster with the nftables kube-proxy. |
