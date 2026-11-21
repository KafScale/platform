---
layout: doc
title: "KafScale Iceberg Processor: Table Sink Addon"
description: Deploy and operate the Iceberg processor addon.
permalink: /processors/iceberg/
nav_title: Iceberg
nav_order: 1
nav_group: Processors
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

# Iceberg Processor (Addon)

Storage-native processor that reads completed KafScale segments from S3 and
writes to Apache Iceberg tables. It is not a Kafka broker. It trades latency
(seconds to minutes) for simpler operations and lower cost.

## Quick Links

- [User guide](https://github.com/KafScale/platform/blob/main/addons/processors/iceberg-processor/user-guide.md)
- [Developer guide](https://github.com/KafScale/platform/blob/main/addons/processors/iceberg-processor/developer.md)
- [Reference config](https://github.com/KafScale/platform/blob/main/addons/processors/iceberg-processor/config/config.yaml)
- [Helm values](https://github.com/KafScale/platform/blob/main/addons/processors/iceberg-processor/deploy/helm/iceberg-processor/values.yaml)
- [Demo video](/demos/#iceberg)

## What It Does (High Level)

- Discovers completed KafScale segments in S3.
- Decodes segments and batches records by topic.
- Maps topics to Iceberg tables via YAML.
- Writes in append mode with at-least-once semantics.
- Persists offsets via a lease-per-partition model.
- Evolves Iceberg schemas from mapping-defined columns or a schema registry.

## Isolation and Access

The processor never connects to the Kafka brokers and imports no Kafka client.
It reads completed segments straight from S3, so it adds no load to produce
and consume traffic. Its only other dependency is etcd, where it keeps
partition leases, offsets and watermarks under `offsets.key_prefix` (default
`processors`) and, in `auto` or `etcd` discovery, reads the topic list.

One set of S3 credentials (`s3.credentialsSecretRef` or an IAM role on the
ServiceAccount) is used for both the segment bucket and the warehouse. It
needs exactly these permissions:

Segment bucket `s3.bucket`, keys under `<s3.namespace>/`, read only:

- `s3:ListBucket` on the bucket, for the prefix `<s3.namespace>/`, to find
  segments.
- `s3:GetObject` on `<s3.namespace>/*`, to read `.kfs` segments, including a
  ranged read of the segment footer.
- With an `lfs.mode` other than `off`, also `s3:GetObject` on the LFS blob keys
  the records point to, in the same bucket.

Warehouse `iceberg.warehouse`, table location `<location>/data` and
`<location>/metadata`:

- `s3:PutObject`, to write parquet data files, manifests and manifest lists.
  Large files use multipart upload, which `s3:PutObject` covers.
- `s3:GetObject`, to read the current table metadata and manifests before an
  append.

The processor needs no `s3:DeleteObject` anywhere: it never deletes objects
and never expires snapshots. With the REST catalog, the catalog server writes
the table metadata files with its own credentials, so give it read and write
access to the same warehouse prefix.

## Segment Layout

```
s3://{bucket}/{namespace}/{topic}/{partition}/segment-{base_offset}.kfs
s3://{bucket}/{namespace}/{topic}/{partition}/segment-{base_offset}.index
```

Segment formats are defined in `kafscale-spec.md`.

## Minimal Config (Example)

```yaml
s3:
  bucket: kafscale-data
  namespace: production

iceberg:
  catalog:
    type: rest
    uri: https://iceberg-catalog.example.com
  warehouse: s3://iceberg-warehouse/production

etcd:
  endpoints:
    - http://etcd.kafscale.svc.cluster.local:2379

mappings:
  - topic: orders
    table: prod.orders
    mode: append
    create_table_if_missing: true
```

For full configuration and operational guidance, use the user guide.

## LFS Environment Overrides

These override the `lfs` block of every mapping in the Iceberg processor config:

- `KAFSCALE_ICEBERG_LFS_MODE` – LFS handling mode (config default `off`).
- `KAFSCALE_ICEBERG_LFS_MAX_INLINE_SIZE` – Maximum size to inline, in bytes.
- `KAFSCALE_ICEBERG_LFS_STORE_METADATA` – Store LFS metadata columns.
- `KAFSCALE_ICEBERG_LFS_VALIDATE_CHECKSUM` – Validate checksums (config default `true`).
- `KAFSCALE_ICEBERG_LFS_RESOLVE_CONCURRENCY` – Parallel blob resolves (config default `4`).

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

## Try It on kind

Watch the Iceberg processor run first in the [Iceberg processor demo](/demos/#iceberg). Clone https://github.com/KafScale/platform and run the commands below from the repo root.

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
