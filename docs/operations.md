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

By default, image tags follow the chart `appVersion`. Override `operator.image.tag` and `console.image.tag` to pin a different release (for example, `v1.1.0`). For a dev/latest install, set `operator.image.useLatest=true`, `console.image.useLatest=true`, and `operator.brokerImage.useLatest=true` (this also forces `imagePullPolicy=Always` for those images).

### Values to pay attention to

| Value | Purpose |
|-------|---------|
| `operator.replicaCount` | Number of operator replicas (default `2`).  Operators use etcd to elect a leader and stay HA. |
| `operator.leaderKey` | Kubernetes leader election lock name. Use a DNS-1123 compatible name. |
| `console.service.*` | Type/port used to expose the UI.  Combine with `.console.ingress` to publish via an ingress controller. |
| `console.auth.*` | Console login credentials. Set both `console.auth.username` and `console.auth.password` to enable the UI. |
| `imagePullSecrets` | Provide if your container registry (e.g., GHCR) is private. |

## Post-install Steps

1. Apply a `KafscaleCluster` custom resource describing the S3 bucket, etcd endpoints, cache sizes, and credentials secret.
2. Apply any required `KafscaleTopic` resources.  The operator writes the desired metadata to etcd and the brokers begin serving Kafka clients right away.
3. Expose the console UI (optional) by enabling ingress in `values.yaml` or by creating a LoadBalancer service.

## Security & Hardening

- **RBAC** – The Helm chart creates a scoped service account and RBAC role so the operator only touches its CRDs, Secrets, and Deployments inside the release namespace.
- **S3 credentials** – Credentials live in user-managed Kubernetes secrets. The operator never writes them to etcd. Snapshot jobs map `KAFSCALE_S3_ACCESS_KEY`/`KAFSCALE_S3_SECRET_KEY` into the AWS CLI env vars (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`) automatically.
- **Console auth** – The UI requires `KAFSCALE_UI_USERNAME` and `KAFSCALE_UI_PASSWORD`. There are no defaults; if unset, the login screen shows a warning and the API blocks access. In Helm, set `console.auth.username` and `console.auth.password`, for example:

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --set console.auth.username=kafscaleadmin \
  --set console.auth.password='use-a-secret'
```

- **TLS** – Terminate TLS at your ingress or service mesh; broker/console TLS env flags are not wired in v1.
- **Admin APIs** – Create/Delete Topics are enabled by default. Set `KAFSCALE_ALLOW_ADMIN_APIS=false` on broker pods to disable them, and gate external access via mTLS, ingress auth, or network policies.
- **Network policies** – If your cluster enforces policies, allow the operator + brokers to reach etcd and S3 endpoints and lock everything else down.
- **Health / metrics** – Prometheus can scrape `/metrics` on the brokers and operator for early detection of S3 pressure or degraded nodes. The operator exposes metrics on port `8080` and the Helm chart can create a metrics Service, ServiceMonitor, and PrometheusRule.
- **Startup gating** – Broker pods exit immediately if they cannot read metadata or write a probe object to S3 during startup, so Kubernetes restarts them rather than leaving a stuck listener in place.
- **Leader IDs** – Each broker advertises a numeric `NodeID` in etcd. In the single-node demo you’ll always see `Leader=0` in the Console’s topic detail because the only broker has ID `0`. In real clusters those IDs align with the broker addresses the operator published; if you see `Leader=3`, look for the broker with `NodeID 3` in the metadata payload.

## Ops API Examples

Kafscale exposes Kafka admin APIs for operator workflows (consumer group visibility,
config inspection, cleanup). The canonical plan and scope live in `docs/ops-api.md`.

```bash
# List consumer groups
kafka-consumer-groups.sh --bootstrap-server <broker> --list

# Describe a consumer group
kafka-consumer-groups.sh --bootstrap-server <broker> --describe --group <group-id>

# Delete a consumer group
kafka-consumer-groups.sh --bootstrap-server <broker> --delete --group <group-id>

# Read topic configs
kafka-configs.sh --bootstrap-server <broker> --describe --entity-type topics --entity-name <topic>

# Increase partition count for a topic (additive only)
kafka-topics.sh --bootstrap-server <broker> --alter --topic <topic> --partitions <count>

# Update topic retention (whitelist only)
kafka-configs.sh --bootstrap-server <broker> --alter --entity-type topics --entity-name <topic> \
  --add-config retention.ms=604800000
```

## etcd Availability & Storage

Kafscale depends on etcd for metadata + offsets. Treat etcd as a production datastore:

- Run a dedicated etcd cluster (do not share the Kubernetes control-plane etcd).
- Use SSD-backed disks for data and WAL volumes; avoid networked storage when possible.
- Deploy an odd number of members (3 for most clusters, 5 for higher fault tolerance).
- Spread members across zones/racks to survive single-AZ failures.
- Enable compaction/defragmentation and monitor fsync/proposal latency.

### Operator-managed etcd (default path)

If no etcd endpoints are supplied, the operator will provision a 3-node etcd StatefulSet for you. Recommended settings:

- Use an SSD-capable StorageClass for the etcd PVCs (`storageClassName`), with enough IOPS headroom.
- Set a PodDisruptionBudget so only one etcd pod can be evicted at a time.
- Pin etcd pods across zones with topology spread or anti-affinity.
- Enable snapshot backups to a dedicated S3 bucket and retain at least 7 days of snapshots.
- Monitor leader changes, fsync latency, and disk usage; alert on slow or flapping members.

### Etcd Endpoint Resolution

The operator resolves etcd endpoints in this order:

1. `KafscaleCluster.spec.etcd.endpoints`
2. `KAFSCALE_OPERATOR_ETCD_ENDPOINTS`
3. Managed etcd (operator creates a 3-node StatefulSet)

### Etcd Schema Direction

Kafscale uses a snapshot-based metadata schema today: the operator publishes a full metadata snapshot to etcd and brokers consume it. We avoid per-key writes for broker registrations and assignments until the ops surface requires it.

Snapshot job defaults and operator env overrides:

- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET` (default: cluster S3 bucket)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PREFIX` (default: `etcd-snapshots`)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SCHEDULE` (default: `0 * * * *`)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_ETCDCTL_IMAGE` (default: `quay.io/coreos/etcd:v3.5.12`)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_IMAGE` (default: `amazon/aws-cli:2.15.0`)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT` (optional, for MinIO or custom S3)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_STALE_AFTER_SEC` (default: 7200)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET` (optional, set to `1` to auto-create the backup bucket)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PROTECT_BUCKET` (optional, set to `1` to enable versioning + block public access)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SKIP_PREFLIGHT` (optional, set to `1` to skip the operator S3 write check)

The operator performs an S3 write preflight before enabling snapshots. If the check fails, the `EtcdSnapshotAccess` condition is set to `False` and reconciliation returns an error until access is restored. Snapshots are uploaded as timestamped files plus a `.sha256` checksum for recovery validation.

Minimal env + spec checklist for a smooth run:
- Operator env: `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET`, `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT` (if non-AWS), optionally `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET=1`.
- Cluster spec: `spec.s3.bucket`, `spec.s3.region`, `spec.s3.credentialsSecretRef`, `spec.s3.endpoint` (if non-AWS). Optional read replica: `spec.s3.readBucket`, `spec.s3.readRegion`, `spec.s3.readEndpoint`.
- Secret keys: `KAFSCALE_S3_ACCESS_KEY`, `KAFSCALE_S3_SECRET_KEY`.
- Console auth env: `KAFSCALE_UI_USERNAME`, `KAFSCALE_UI_PASSWORD`.

Recommended operator alerting (when using Prometheus Operator):
- `KafscaleSnapshotAccessFailed` – S3 snapshot writes failing.
- `KafscaleSnapshotStale` – last successful snapshot older than the staleness threshold.
- `KafscaleSnapshotNeverSucceeded` – no successful snapshots recorded.

## Environment Variable Index

### Operator

- `KAFSCALE_OPERATOR_ETCD_ENDPOINTS` – Comma-separated etcd endpoints to use instead of managed etcd.
- `KAFSCALE_OPERATOR_ETCD_IMAGE` – Managed etcd image (default `quay.io/coreos/etcd:v3.5.12`).
- `KAFSCALE_OPERATOR_ETCD_STORAGE_SIZE` – PVC size for managed etcd (default `10Gi`).
- `KAFSCALE_OPERATOR_ETCD_STORAGE_CLASS` – StorageClass for managed etcd PVCs.
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET` – Override snapshot bucket (defaults to cluster S3 bucket).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PREFIX` – Snapshot prefix (default `etcd-snapshots`).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SCHEDULE` – Cron schedule for snapshots (default `0 * * * *`).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_ETCDCTL_IMAGE` – Etcdctl image for snapshots.
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_IMAGE` – AWS CLI image for uploads.
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT` – S3 endpoint override (MinIO/custom).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_STALE_AFTER_SEC` – Staleness threshold seconds (default `7200`).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET` – Auto-create the snapshot bucket (`1` to enable).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PROTECT_BUCKET` – Enable versioning + public access block (`1` to enable).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SKIP_PREFLIGHT` – Skip the S3 write preflight (`1` to enable).
- `KAFSCALE_OPERATOR_LEADER_KEY` – Override the operator leader election ID (default `kafscale-operator`).
- `KAFSCALE_S3_NAMESPACE` – Prefix used for broker S3 object keys (defaults to the cluster namespace).
- `KAFSCALE_SEGMENT_BYTES` – Broker segment flush threshold in bytes (default `4194304`).
- `KAFSCALE_FLUSH_INTERVAL_MS` – Broker flush interval in milliseconds (default `500`).

### Broker

- `KAFSCALE_BROKER_ADDR` – Kafka listener address (host:port).
- `KAFSCALE_BROKER_HOST` – Advertised host (used with `KAFSCALE_BROKER_PORT`).
- `KAFSCALE_BROKER_PORT` – Advertised port (default `9092`).
- `KAFSCALE_BROKER_ID` – Broker node ID.
- `KAFSCALE_METRICS_ADDR` – Metrics listen address.
- `KAFSCALE_CONTROL_ADDR` – Control-plane listen address.
- `KAFSCALE_ETCD_ENDPOINTS` – Etcd endpoints for metadata/offsets.
- `KAFSCALE_ETCD_USERNAME`, `KAFSCALE_ETCD_PASSWORD` – Etcd basic auth.
- `KAFSCALE_S3_BUCKET` – S3 bucket for segments/snapshots.
- `KAFSCALE_S3_REGION` – S3 region.
- `KAFSCALE_S3_ENDPOINT` – S3 endpoint override.
- `KAFSCALE_S3_READ_BUCKET` – Optional read replica bucket (CRR/MRAP).
- `KAFSCALE_S3_READ_REGION` – Optional read replica region.
- `KAFSCALE_S3_READ_ENDPOINT` – Optional read replica endpoint override.
- `KAFSCALE_S3_PATH_STYLE` – Force path-style addressing (`true/false`).
- `KAFSCALE_S3_KMS_ARN` – KMS key ARN for SSE-KMS.
- `KAFSCALE_S3_ACCESS_KEY`, `KAFSCALE_S3_SECRET_KEY`, `KAFSCALE_S3_SESSION_TOKEN` – S3 credentials.

Read replica example (multi-region reads):

```bash
export KAFSCALE_S3_BUCKET=prod-segments
export KAFSCALE_S3_REGION=us-east-1
export KAFSCALE_S3_READ_BUCKET=prod-segments-replica
export KAFSCALE_S3_READ_REGION=eu-west-1
```
- `KAFSCALE_CACHE_BYTES` – Broker cache size in bytes.
- `KAFSCALE_READAHEAD_SEGMENTS` – Segment readahead count.
- `KAFSCALE_AUTO_CREATE_TOPICS` – Auto-create topics (`true/false`).
- `KAFSCALE_AUTO_CREATE_PARTITIONS` – Partition count for auto-created topics.
- `KAFSCALE_USE_MEMORY_S3` – Use in-memory S3 client (dev only).
- `KAFSCALE_LOG_LEVEL` – Log level (`debug`, `info`, `warn`, `error`).
- `KAFSCALE_TRACE_KAFKA` – Enable protocol tracing (`true/false`).
- `KAFSCALE_THROUGHPUT_WINDOW_SEC` – Throughput window size seconds.
- `KAFSCALE_S3_HEALTH_WINDOW_SEC` – S3 health sampling window seconds.
- `KAFSCALE_S3_LATENCY_WARN_MS`, `KAFSCALE_S3_LATENCY_CRIT_MS` – Latency thresholds.
- `KAFSCALE_S3_ERROR_RATE_WARN`, `KAFSCALE_S3_ERROR_RATE_CRIT` – Error-rate thresholds.
- `KAFSCALE_STARTUP_TIMEOUT_SEC` – Broker startup timeout.

### Console

- `KAFSCALE_CONSOLE_HTTP_ADDR` – Console listen address.
- `KAFSCALE_CONSOLE_ETCD_ENDPOINTS` – Etcd endpoints for metadata read-only access.
- `KAFSCALE_CONSOLE_ETCD_USERNAME`, `KAFSCALE_CONSOLE_ETCD_PASSWORD` – Etcd auth for console.
- `KAFSCALE_CONSOLE_BROKER_METRICS_URL` – Broker Prometheus endpoint.
- `KAFSCALE_UI_USERNAME`, `KAFSCALE_UI_PASSWORD` – Console login credentials.

## External Broker Access

By default, brokers advertise the in-cluster service DNS name. That works for
clients running inside Kubernetes, but external clients must connect to a
reachable address. Configure both the broker Service exposure and the advertised
address so clients learn the external endpoint from metadata responses.

Broker exposure settings (KafscaleCluster `spec.brokers`):
- `advertisedHost` / `advertisedPort` – Address Kafka clients should connect to.
- `service.type` – `ClusterIP`, `LoadBalancer`, or `NodePort`.
- `service.annotations` – Cloud provider LB annotations.
- `service.loadBalancerIP` / `service.loadBalancerSourceRanges` – Static IP + CIDR allowlist.
- `service.externalTrafficPolicy` – `Cluster` or `Local`.
- `service.kafkaNodePort` / `service.metricsNodePort` – Optional NodePort overrides.

Helm chart docs: `deploy/helm/README.md`.

Example (GKE/AWS/Azure load balancer):

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafscaleCluster
metadata:
  name: kafscale
  namespace: kafscale
spec:
  brokers:
    advertisedHost: kafka.example.com
    advertisedPort: 9092
    service:
      type: LoadBalancer
      annotations:
        networking.gke.io/load-balancer-type: "External"
      loadBalancerSourceRanges:
        - 203.0.113.0/24
  s3:
    bucket: kafscale
    region: us-east-1
    credentialsSecretRef: kafscale-s3-credentials
  etcd:
    endpoints: []
```

TLS note: brokers speak plaintext today. If you need TLS for Kafka traffic,
terminate TLS at your load balancer, ingress TCP proxy, or service mesh and
advertise that endpoint in `advertisedHost`/`advertisedPort`. See `docs/security.md`
for the current transport security posture.

Example certificate (cert-manager) for a TCP proxy or load balancer that uses a
Kubernetes TLS secret:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: kafscale-kafka-cert
  namespace: kafscale
spec:
  secretName: kafscale-kafka-tls
  dnsNames:
    - kafka.example.com
  issuerRef:
    name: letsencrypt-prod
    kind: ClusterIssuer
```

## Upgrades & Rollbacks

- Use `helm upgrade --install` with the desired image tags.  The operator drains brokers through the gRPC control plane before restarting pods.
- CRD schema changes follow Kubernetes best practices; run `helm upgrade` to pick them up.
- Rollbacks can be performed with `helm rollback kafscale <REVISION>` which restores the previous deployment and service versions.  Brokers are stateless so the recovery window is short.

## S3 Cost Estimation (Example)

Assumptions:

- 100 GB/day ingestion
- 7-day retention
- 4 MB average segment size
- 3 broker pods

Monthly costs (US-East-1):

| Item | Calculation | Cost |
|------|-------------|------|
| Storage | 700 GB x $0.023/GB | $16.10 |
| PUT requests | 25,000/day x 30 x $0.005/1000 | $3.75 |
| GET requests | 100,000/day x 30 x $0.0004/1000 | $1.20 |
| Data transfer (in-region) | Free | $0 |
| **Total S3** | | **~$21/month** |

## Multi-Region S3 (CRR) for Global Reads

Kafscale writes to a primary bucket and can optionally read from a replica bucket in the broker region. With S3 Cross-Region Replication (CRR), objects written to the primary are asynchronously copied to replica buckets in other regions. Brokers attempt reads from their local replica and fall back to the primary if the replica is missing an object (for example, due to CRR lag).

### CRR Setup (AWS)

1. **Create buckets in each region**:
   - Primary: `kafscale-prod-us-east-1`
   - Replica: `kafscale-prod-eu-west-1`
   - Replica: `kafscale-prod-ap-southeast-1`

2. **Enable versioning on all buckets** (required for CRR).

3. **Create a replication rule** on the primary bucket:
   - Replicate all objects (or the Kafscale prefix) to both replica buckets.
   - Use an IAM role that can write to the replica buckets.

4. **Optional**: Enable encryption (SSE-KMS) on all buckets with compatible keys.

### Kafscale Configuration (Per Region)

Primary region (writers and primary readers):
```yaml
spec:
  s3:
    bucket: kafscale-prod-us-east-1
    region: us-east-1
    credentialsSecretRef: kafscale-s3-creds
```

EU read replica:
```yaml
spec:
  s3:
    bucket: kafscale-prod-us-east-1
    region: us-east-1
    readBucket: kafscale-prod-eu-west-1
    readRegion: eu-west-1
    credentialsSecretRef: kafscale-s3-creds
```

Asia read replica:
```yaml
spec:
  s3:
    bucket: kafscale-prod-us-east-1
    region: us-east-1
    readBucket: kafscale-prod-ap-southeast-1
    readRegion: ap-southeast-1
    credentialsSecretRef: kafscale-s3-creds
```

### How Reads and Writes Work

- **Writes** always go to the primary bucket.
- **Reads** go to the local `readBucket` first; on miss or error, the broker retries against the primary bucket.
- **List/restore** operations use the primary bucket to avoid missing newly written segments.

### Latency and Consistency Impact

- **Lower read latency** for consumers in EU/Asia when objects are already replicated.
- **CRR lag** means the newest segments may not appear immediately in the replica; the broker will fall back to the primary for those reads.
- **Extra cross-region traffic** occurs only on replica misses; steady-state reads stay in-region once replication catches up.
