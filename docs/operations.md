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

### Per-cluster broker image

`spec.brokers.image` overrides the operator's `BROKER_IMAGE` default for that cluster:

```yaml
spec:
  brokers:
    image:
      repository: ghcr.io/kafscale/kafscale-broker
      tag: v1.6.3
```

Omitting the image keeps the operator default. A repository without a tag is used
as supplied, including a complete `repository@sha256:...` digest reference. A tag
without a repository replaces the tag or digest in the operator default. When both
are set, the tag replaces any tag or digest in the supplied repository reference.
The operator's image pull policy still applies. Updating the image changes the
broker StatefulSet pod template, triggering its normal rolling update; removing
the override returns that cluster to the operator default.

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

## Broker partition restore

On first access after a broker restart, a partition rebuilds its in-memory segment
catalog from S3 before serving requests for that partition. Restore uses up to 16
workers per partition to read segment footers and indexes. These requests also
share the broker-wide `KAFSCALE_S3_CONCURRENCY` limit with other S3 operations.

Broker logs report restore start, the listed segment count, progress every five
seconds during downloads, and completion with restored/skipped counts, last offset,
and duration. Failures include processed counts and the error. A failed restore
cancels the remaining workers and does not install a partial catalog.

Restore still lists all objects and reads a footer and index per eligible segment;
large partitions can still take time. This change does not merge small segments or
add a persistent manifest. Requests for the restoring partition wait for completion.

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
- **Topic deletion** – DeleteTopics keeps the topic's data in S3. Each broker drops its cached partition logs for the topic, and a topic re-created with the same name starts empty at offset 0 under a new prefix. A topic that was never deleted stores segments under `<namespace>/<topic>/<partition>/`. After the Nth delete, the next topic with that name stores them under `<namespace>/<topic>@<N>/<partition>/`. `<namespace>` is `KAFSCALE_S3_NAMESPACE`. LFS objects stay under `<namespace>/<topic>/lfs/`.
- **Find retained topic data** – Each delete writes a record to etcd at `/kafscale/deleted-topics/<topic>/<generation>` with `name`, `topic_id`, `generation`, `prefix` and `deleted_at`. The current generation of a topic name is at `/kafscale/topic-generations/<topic>`. List the records with `etcdctl get --prefix /kafscale/deleted-topics/`. The data under each `prefix` stays in S3 until you remove it.
- **Network policies** – If your cluster enforces policies, allow the operator + brokers to reach etcd and S3 endpoints and lock everything else down.
- **Pod Security (restricted)** – Chart-templated workloads (operator, proxy, console, MCP) ship PSA `restricted`-compatible `securityContext` defaults (`runAsNonRoot`, dropped capabilities, `readOnlyRootFilesystem`, `seccompProfile: RuntimeDefault`, UID/GID `10001`). Override per component via `<component>.podSecurityContext` and `<component>.containerSecurityContext`. The proxy mounts a writable `/tmp` `emptyDir` for LFS verify temp files.
- **Pod placement** – Operator-managed broker and etcd StatefulSets, and the chart proxy Deployment (when `proxy.affinity` is empty), use soft (`preferredDuringSchedulingIgnoredDuringExecution`) pod anti-affinity on `kubernetes.io/hostname` so replicas spread across nodes on multi-node clusters while still scheduling on single-node dev clusters. Set `proxy.affinity` explicitly to replace the chart default. For operator-managed brokers and etcd, use `spec.brokers.affinity` and `spec.etcd.affinity` as described below.
- **ACLs (v1.5)** – Optional, basic ACL enforcement is available at the broker. With proxy mTLS enabled, identity comes from the client certificate mapping. Without it, identity comes from Kafka `client.id` or network identity, which is not authentication. Configure via `KAFSCALE_ACL_ENABLED` plus either `KAFSCALE_ACL_JSON` or `KAFSCALE_ACL_FILE`. Use `KAFSCALE_ACL_FAIL_OPEN=true` to allow traffic when the ACL config is missing/invalid (default is fail-closed).
  - **Authenticated proxy** – With `KAFSCALE_PROXY_IDENTITY_KEY_FILE` configured,
    the broker requires the proxy identity handshake and ignores legacy principal
    sources. See [Kafka proxy authentication](kafka-authentication.md).
  - **Principal source** – Set `KAFSCALE_PRINCIPAL_SOURCE` to `client_id` (default), `remote_addr`, or `proxy_addr`. Use `proxy_addr` with `KAFSCALE_PROXY_PROTOCOL=true` to derive principals from a trusted TCP proxy (PROXY protocol v1/v2).
  - **Auth denials** – Broker logs emit a rate-limited `authorization denied` entry with principal/action/resource context.
  - **Trust boundary** – Only enable `proxy_addr`/PROXY protocol when brokers are reachable *only* through a trusted LB or sidecar that injects the header. Do not expose brokers directly, or clients can spoof identity.
  - **Fail-closed** – When `KAFSCALE_PROXY_PROTOCOL=true`, brokers reject connections that do not include a valid PROXY header.
  - **Header limits** – PROXY v1 headers are capped at 256 bytes; oversized headers are rejected.
  - **Health checks** – PROXY v2 `LOCAL` connections are accepted (no identity); ensure LB health checks don’t rely on ACL-protected operations.

Example ACL values (Helm operator):
```yaml
operator:
  acl:
    enabled: true
    configJson: |
      {"default_policy":"deny","principals":[
        {"name":"analytics","allow":[{"action":"fetch","resource":"topic","name":"orders-*"}]}
      ]}
  auth:
    principalSource: "proxy_addr"
    proxyProtocol: true
```
- **Health / metrics** – Prometheus can scrape `/metrics` on the brokers and operator for early detection of S3 pressure or degraded nodes. The operator exposes metrics on port `8080` and the Helm chart can create a metrics Service, ServiceMonitor, and PrometheusRule.
- **Startup gating** – Broker pods exit immediately if they cannot read metadata or write a probe object to S3 during startup, so Kubernetes restarts them rather than leaving a stuck listener in place.
- **Leader IDs** – Each broker advertises a numeric `NodeID` in etcd. In the single-node demo you’ll always see `Leader=0` in the Console’s topic detail because the only broker has ID `0`. In real clusters those IDs align with the broker addresses the operator published; if you see `Leader=3`, look for the broker with `NodeID 3` in the metadata payload.

### Proxy TLS via LoadBalancer

For per-client authentication and broker ACLs, use the native proxy mTLS listener
with TLS passthrough. See [Kafka proxy authentication](kafka-authentication.md).
The external termination configuration below provides transport security but does
not forward certificate identities.

The proxy is the external Kafka entrypoint. For TLS in v1.5, terminate TLS at
the cloud LoadBalancer by supplying Service annotations in the Helm values.
This keeps broker traffic plaintext inside the cluster while enabling HTTPS/TLS
for clients.

Example (provider-specific annotations omitted here):

```yaml
proxy:
  enabled: true
  service:
    type: LoadBalancer
    port: 9092
    annotations:
      # Add your cloud provider TLS annotations here (ACM / GCP / Azure, etc.)
    loadBalancerSourceRanges:
      - 203.0.113.0/24
```

If you need in-cluster ACME/Let’s Encrypt support, document it as an optional
stack (Traefik or another TCP-capable gateway + cert-manager). Keep it off by
default to avoid extra operational dependencies.

#### Provider Examples (Verify with your cloud)

These snippets show common patterns; annotation keys vary by provider and
feature. Always validate against your cloud provider docs.

AWS NLB TLS (example):
```yaml
proxy:
  service:
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
      service.beta.kubernetes.io/aws-load-balancer-ssl-cert: "arn:aws:acm:REGION:ACCOUNT:certificate/ID"
      service.beta.kubernetes.io/aws-load-balancer-ssl-ports: "9092"
      service.beta.kubernetes.io/aws-load-balancer-backend-protocol: "tcp"
```

GCP / Azure:
- L4 LoadBalancers typically do TCP pass-through; TLS termination often requires
  a provider gateway/ingress (or a TCP-capable ingress controller).
- If you terminate TLS outside the Service, keep the proxy Service as plain TCP.

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
- HA requires a stable client access layer: use a Kafka-aware proxy or per-broker endpoints so bootstrap addresses and leader routing survive broker churn.

### Operator-managed etcd (default path)

If no etcd endpoints are supplied, the operator will provision a 3-node etcd StatefulSet for you. Recommended settings:

- Use an SSD-capable StorageClass for the etcd PVCs (`storageClassName`), with enough IOPS headroom.
- Set a PodDisruptionBudget so only one etcd pod can be evicted at a time.
- Pin etcd pods across zones with topology spread or anti-affinity.
- Enable snapshot backups to a dedicated S3 bucket and retain at least 7 days of snapshots.
- Monitor leader changes, fsync latency, and disk usage; alert on slow or flapping members.

The operator tunes managed etcd automatically:

- In-process periodic compaction (5m retention) and a 4 GiB backend quota on the etcd StatefulSet.
- `{cluster}-etcd-maintenance` CronJob (every 6h): per-member defrag and NOSPACE alarm disarm.
- `{cluster}-etcd-maintenance-check` CronJob (every 15m): runs maintenance only when backend size crosses 75% of quota or a NOSPACE alarm is active.

Override schedules and thresholds via the operator etcd env vars listed below.

### Etcd Endpoint Resolution

The operator resolves etcd endpoints in this order:

1. `KafscaleCluster.spec.etcd.endpoints`
2. `KAFSCALE_OPERATOR_ETCD_ENDPOINTS`
3. Managed etcd (operator creates a 3-node StatefulSet)

### Etcd Schema Direction

Kafscale uses a snapshot-based metadata schema today: the operator publishes a full metadata snapshot to etcd and brokers consume it. We avoid per-key writes for broker registrations and assignments until the ops surface requires it.

### Etcd Availability Signals (clients)

When etcd is unavailable, the broker rejects producer/admin/consumer-group operations with `REQUEST_TIMED_OUT`. Producers see per-partition errors in the Produce response; admin and group APIs return the same code in their response payloads.

Snapshot job defaults and operator env overrides:

- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET` (default: `kafscale-etcd-<namespace>-<cluster>`, separate from broker segment storage)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PREFIX` (default: `etcd-snapshots`)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SCHEDULE` (default: `0 * * * *`)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_ETCDCTL_IMAGE` (default: `ghcr.io/kafscale/kafscale-etcd-tools:dev`; this `:dev` tag is the code default, so pin a release tag in production)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_IMAGE` (default: `amazon/aws-cli:2.15.0`)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT` (optional, for MinIO or custom S3)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_STALE_AFTER_SEC` (default: 7200)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET` (optional, set to `1` to auto-create the backup bucket)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PROTECT_BUCKET` (optional, set to `1` to enable versioning + block public access)
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SKIP_PREFLIGHT` (optional, set to `1` to skip the operator S3 write check)

The operator performs an S3 write preflight before enabling snapshots. If the check fails, the `EtcdSnapshotAccess` condition is set to `False` and reconciliation returns an error until access is restored. Snapshots are uploaded as timestamped files plus a `.sha256` checksum for recovery validation.

### Snapshot Restore (KafScale managed etcd)

Snapshot restore refers to **etcd operational data** (cluster metadata/offsets). It is not the broker topic snapshot flow.

When the KafScale operator manages etcd, each cluster pod runs ```restore init containers``` before etcd starts:

- The snapshot download container pulls the latest `.db` snapshot from the configured bucket/prefix.
- The restore container runs `etcdctl snapshot restore` if the data directory is empty and a snapshot file is present.
- If no snapshot is available, etcd starts with a clean data directory.

The restore image must include `/bin/sh` and `etcdctl`. Override with `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_ETCDCTL_IMAGE` if you use a custom image.

### Topic Recovery On The DR Spine

For broker topic data, KafScale now supports **recovery into a new topic** on the DR side. This is intentionally not an in-place rollback on the primary cluster.

Use `kafscale-cli restore` to create a fresh target topic, copy `.kfs` segment/index pairs up to a cutoff timestamp, and set the recovered topic's next offsets:

```bash
kafscale-cli restore \
  --topic orders \
  --target-topic orders-restore-20260513 \
  --to 2026-05-13T14:23:00Z
```

Operational semantics:

- Recovery runs against the existing KafScale S3 + etcd control plane, including `KAFSCALE_S3_BUCKET`, `KAFSCALE_S3_REGION`, `KAFSCALE_S3_ENDPOINT`, `KAFSCALE_S3_PATH_STYLE`, and `KAFSCALE_ETCD_ENDPOINTS`.
- The target topic must be new. KafScale refuses to restore over an existing persisted topic.
- Recovery copies every fully eligible segment/index pair, then inspects the first intersecting segment and truncates it at the first record whose timestamp exceeds the cutoff.
- If the intersecting batch is compressed, exact truncation is not safe, so the restore fails and the operator should choose an earlier cutoff or a segment boundary instead.
- Offsets are preserved inside the recovered topic so replay, validation, and downstream cutover can happen without rewriting the source topic.
- The safer pattern is restore, validate, then cut consumers or downstream jobs over deliberately.

### Consumer Offsets After Restore

Etcd restores recover committed consumer offsets. If a consumer has **no committed offsets**, it may start at the end and see zero records even though data exists in S3. In production:

- Ensure consumers commit offsets (default for most Kafka clients).
- Set `auto.offset.reset=earliest` as a safety net for new or uncommitted consumers.

Minimal env + spec checklist for a smooth run:
- Operator env: `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET` (optional override), `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT` (if non-AWS), optionally `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET=1`.
- Cluster spec: `spec.s3.bucket`, `spec.s3.region`, `spec.s3.credentialsSecretRef`, `spec.s3.endpoint` (if non-AWS). Optional read replica: `spec.s3.readBucket`, `spec.s3.readRegion`, `spec.s3.readEndpoint`.
- Secret keys: `KAFSCALE_S3_ACCESS_KEY`, `KAFSCALE_S3_SECRET_KEY`.
- Console auth env: `KAFSCALE_UI_USERNAME`, `KAFSCALE_UI_PASSWORD`.

Recommended operator alerting (when using Prometheus Operator):
- `KafscaleSnapshotAccessFailed` – S3 snapshot writes failing.
- `KafscaleSnapshotStale` – last successful snapshot older than the staleness threshold.
- `KafscaleSnapshotNeverSucceeded` – no successful snapshots recorded.

## Proxy readiness

The proxy exposes `/readyz` and `/livez` on `KAFSCALE_PROXY_HEALTH_ADDR`
(default `:9094`).

`/readyz` answers two questions, not one:

1. **Is a backend known?** Cached backend state when fresh, otherwise a live
   metadata fetch from etcd.
2. **Does that backend actually serve Fetch?** A Fetch round-trip on a fresh
   connection that must decode cleanly.

The second question exists because the proxy is a full Fetch codec: it decodes,
merges and re-encodes broker Fetch responses. A proxy-to-broker Fetch
serialization or connection-state mismatch therefore breaks consume while
Metadata and ListOffsets keep answering normally. Readiness based on the first
question alone reports `ready` while every consumer silently receives zero
records, and Kubernetes keeps the pod in the Service.

The probe asks for one partition of a deliberately unknown topic ID, so it needs
no real topic and reads no data; a correct broker answers `UNKNOWN_TOPIC_ID`.
The fresh connection is deliberate too: it detects a stale or half-open pooled
connection, which a cached-state check cannot.

Set `KAFSCALE_PROXY_READYZ_FETCH_PROBE=false` to fall back to the shallow
"a backend exists" check. That is an escape hatch for a probe that reports a
false negative in the field, not a recommended steady state: it removes the only
readiness signal that distinguishes "serving" from "running".

**Startup ordering.** With the probe enabled the proxy stays NotReady until a
broker serves Fetch. An installer that waits for proxy readiness before creating
the cluster resource that produces the broker will therefore deadlock. Create
the broker first, or do not gate the install on proxy readiness.

## Environment Variable Index

### Operator

- `KAFSCALE_OPERATOR_ETCD_ENDPOINTS` – Comma-separated etcd endpoints to use instead of managed etcd.
- `KAFSCALE_OPERATOR_ETCD_IMAGE` – Managed etcd image (default `kubesphere/etcd:3.6.4-0`).
- `KAFSCALE_OPERATOR_ETCD_REPLICAS` – Managed etcd replica count (default `3`).
- `KAFSCALE_OPERATOR_ETCD_STORAGE_SIZE` – PVC size for managed etcd (default `10Gi`).
- `KAFSCALE_OPERATOR_ETCD_STORAGE_CLASS` – StorageClass for managed etcd PVCs.
- `KAFSCALE_OPERATOR_ETCD_STORAGE_MEMORY` – Use in-memory `emptyDir` for etcd data (`1` to enable, test/dev only).
- `KAFSCALE_OPERATOR_ETCD_AUTO_COMPACTION_MODE` – Managed etcd compaction mode (`periodic` or `revision`; default `periodic`).
- `KAFSCALE_OPERATOR_ETCD_AUTO_COMPACTION_RETENTION` – Compaction retention (`5m` for periodic mode; default `5m`).
- `KAFSCALE_OPERATOR_ETCD_QUOTA_BACKEND_BYTES` – Managed etcd backend quota in bytes (default `4294967296`, 4 GiB).
- `KAFSCALE_OPERATOR_ETCD_MAINTENANCE_SCHEDULE` – Baseline maintenance CronJob schedule (default `0 */6 * * *`).
- `KAFSCALE_OPERATOR_ETCD_MAINTENANCE_CHECK_SCHEDULE` – Reactive size-check CronJob schedule (default `*/15 * * * *`).
- `KAFSCALE_OPERATOR_ETCD_MAINTENANCE_ENABLED` – Enable maintenance CronJobs (`0`/`false` to disable; enabled by default).
- `KAFSCALE_OPERATOR_ETCD_MAINTENANCE_SIZE_THRESHOLD_PCT` – Backend size threshold percent of quota before reactive maintenance runs (default `75`).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET` – Override snapshot bucket (default: `kafscale-etcd-<namespace>-<cluster>`).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PREFIX` – Snapshot prefix (default `etcd-snapshots`).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SCHEDULE` – Cron schedule for snapshots (default `0 * * * *`).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_ETCDCTL_IMAGE` – Etcdctl image for snapshots, restore and maintenance jobs (default `ghcr.io/kafscale/kafscale-etcd-tools:dev`).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_IMAGE` – AWS CLI image for uploads.
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT` – S3 endpoint override (MinIO/custom).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_STALE_AFTER_SEC` – Staleness threshold seconds (default `7200`).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET` – Auto-create the snapshot bucket (`1` to enable).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PROTECT_BUCKET` – Enable versioning + public access block (`1` to enable).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SKIP_PREFLIGHT` – Skip the S3 write preflight (`1` to enable).
- `KAFSCALE_OPERATOR_ETCD_DEFRAG_SCHEDULE` – Deprecated alias for `KAFSCALE_OPERATOR_ETCD_MAINTENANCE_SCHEDULE`. Used only when the new variable is unset.
- `KAFSCALE_OPERATOR_ETCD_DEFRAG_ENABLED` – Deprecated alias for `KAFSCALE_OPERATOR_ETCD_MAINTENANCE_ENABLED`. Setting either one to `0`/`false`/`no`/`off` disables maintenance.
- `KAFSCALE_OPERATOR_ETCD_SILENCE_LOGS` – Suppress etcd client logs from the operator snapshot publisher (`true` to enable).
- `KAFSCALE_OPERATOR_LEADER_KEY` – Override the operator leader election ID (default `kafscale-operator`).
- `KAFSCALE_S3_NAMESPACE` – Prefix used for broker S3 object keys (defaults to the cluster namespace).
- `KAFSCALE_SEGMENT_BYTES` – Broker segment flush threshold in bytes (default `4194304`).
- `KAFSCALE_FLUSH_INTERVAL_MS` – Broker flush interval in milliseconds (default `500`).

### Broker

- `KAFSCALE_BROKER_ADDR` – Kafka listener address (host:port).
- `KAFSCALE_RESPONSE_WRITE_TIMEOUT_MS` – Timeout for one Kafka response write on the broker and proxy (default `30000` milliseconds; nonpositive uses the default). On broker pods, configure `KafscaleCluster.spec.config.responseWriteTimeoutMs` with a positive value. A write timeout closes the socket and drains complete requests already read, preserving acks=0 produces.
- `KAFSCALE_BROKER_HOST` – Advertised host (used with `KAFSCALE_BROKER_PORT`).
- `KAFSCALE_BROKER_PORT` – Advertised port. The broker binary defaults to `19092` when run directly. The operator always sets it from the cluster spec (`9092` by default), and a port in `KAFSCALE_BROKER_ADDR` takes precedence.
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
- `KAFSCALE_S3_CONCURRENCY` – Broker-wide cap on concurrent S3 operations (default `64`, `0` to disable). Lower for slower S3-compatible backends. The `kafscale-cli` tool reads the same variable with a default of `16`.

Read replica example (multi-region reads):

```bash
export KAFSCALE_S3_BUCKET=prod-segments
export KAFSCALE_S3_REGION=us-east-1
export KAFSCALE_S3_READ_BUCKET=prod-segments-replica
export KAFSCALE_S3_READ_REGION=eu-west-1
```
- `KAFSCALE_CACHE_BYTES` – Broker cache size in bytes. `KAFSCALE_CACHE_SIZE` is accepted as a legacy alias when `KAFSCALE_CACHE_BYTES` is unset or not positive.
- `KAFSCALE_READAHEAD_SEGMENTS` – Segment readahead count.
- `KAFSCALE_AUTO_CREATE_TOPICS` – Auto-create topics (`true/false`).
- `KAFSCALE_AUTO_CREATE_PARTITIONS` – Partition count for auto-created topics.
- `KAFSCALE_USE_MEMORY_S3` – Use in-memory S3 client (dev only).
- `KAFSCALE_OFFSETS_RETENTION_MINUTES` – Retention for Empty consumer groups and their committed offsets (default `10080`, seven days). Zero or negative disables automatic expiry. In Kubernetes, set `KafscaleCluster.spec.config.offsetsRetentionMinutes`; omission uses the broker default. Retention is checked once per minute under the group coordination lease.
- `KAFSCALE_PRODUCE_SYNC_FLUSH` – Flush to S3 on Produce when `acks != 0` (default `true`).
- `KAFSCALE_LOG_LEVEL` – Log level (`debug`, `info`, `warn`, `error`).
- `KAFSCALE_TRACE_KAFKA` – Enable protocol tracing (`true/false`).
- `KAFSCALE_THROUGHPUT_WINDOW_SEC` – Throughput window size seconds.
- `KAFSCALE_S3_HEALTH_WINDOW_SEC` – S3 health sampling window seconds.
- `KAFSCALE_S3_LATENCY_WARN_MS`, `KAFSCALE_S3_LATENCY_CRIT_MS` – Latency thresholds.
- `KAFSCALE_S3_ERROR_RATE_WARN`, `KAFSCALE_S3_ERROR_RATE_CRIT` – Error-rate thresholds.
- `KAFSCALE_STARTUP_TIMEOUT_SEC` – Broker startup timeout.
- `KAFSCALE_LAG_SAMPLE_INTERVAL_SEC` – Consumer lag sampling interval in seconds (default `5`).
- `KAFSCALE_ENV` – Set to `prod` to log a warning whenever a broker setting falls back to its default because the variable is unset or invalid.

### Produce Flush Policy (cost vs durability)

By default, the broker flushes to S3 for every Produce request with `acks != 0`
to minimize the loss window after a broker crash. You can relax this for lower
S3 write costs by disabling sync flush and relying on the background flush
interval/segment size.

Durability-optimized (default):
- `KAFSCALE_PRODUCE_SYNC_FLUSH=true`
- `KAFSCALE_FLUSH_INTERVAL_MS=500`
- `KAFSCALE_SEGMENT_BYTES=4194304`

Cost-optimized (accepts a larger loss window after crash):
- `KAFSCALE_PRODUCE_SYNC_FLUSH=false`
- `KAFSCALE_FLUSH_INTERVAL_MS=2000` (or higher)
- `KAFSCALE_SEGMENT_BYTES=33554432` (or higher)

### Proxy

- `KAFSCALE_PROXY_ADDR` – Proxy listen address (host:port).
- `KAFSCALE_PROXY_HEALTH_ADDR` – Health/readiness listen address for `/readyz` and `/livez`. The proxy binary has no default: if unset, the health server does not start. The Helm chart sets `:9094` when `proxy.health.enabled` is true.
- `KAFSCALE_PROXY_ADVERTISED_HOST` – Address Kafka clients should connect to.
- `KAFSCALE_PROXY_ADVERTISED_PORT` – Advertised port (default `9092`).
- `KAFSCALE_PROXY_ETCD_ENDPOINTS` – Etcd endpoints for metadata snapshots.
- `KAFSCALE_PROXY_ETCD_USERNAME`, `KAFSCALE_PROXY_ETCD_PASSWORD` – Etcd auth for proxy.
- `KAFSCALE_PROXY_BACKENDS` – Optional comma-separated broker list (`host:port`) for backend routing.
- `KAFSCALE_PROXY_BACKEND_CACHE_TTL_SEC` – Seconds to cache backend metadata before refreshing from etcd (default `60`).
- `KAFSCALE_PROXY_BACKEND_BACKOFF_MS` – Backoff between backend connection retries (default `500`).
- `KAFSCALE_PROXY_BACKEND_RETRIES` – Backend connection retry count (default `6`).
- `KAFSCALE_PROXY_READYZ_FETCH_PROBE` – Gate `/readyz` on a real Fetch round-trip (default `true`). See [Proxy readiness](#proxy-readiness).
- `KAFSCALE_PROXY_LFS_ENABLED` – Enable the LFS module on the unified proxy (default `false`). See [LFS proxy](#lfs-proxy) for its settings.

### Console

- `KAFSCALE_CONSOLE_HTTP_ADDR` – Console listen address.
- `KAFSCALE_CONSOLE_ETCD_ENDPOINTS` – Etcd endpoints for metadata read-only access.
- `KAFSCALE_CONSOLE_ETCD_USERNAME`, `KAFSCALE_CONSOLE_ETCD_PASSWORD` – Etcd auth for console.
- `KAFSCALE_CONSOLE_BROKER_METRICS_URL` – Broker Prometheus endpoint.
- `KAFSCALE_UI_USERNAME`, `KAFSCALE_UI_PASSWORD` – Console login credentials.
- `KAFSCALE_CONSOLE_LFS_ENABLED` – Enable the LFS views and API (`true` to enable; any other value disables them).
- `KAFSCALE_CONSOLE_KAFKA_BROKERS` – Comma-separated brokers used to consume the LFS tracker topic.
- `KAFSCALE_LFS_TRACKER_TOPIC` – Tracker topic to read (default `__lfs_ops_state`).
- `KAFSCALE_CONSOLE_LFS_S3_BUCKET`, `KAFSCALE_CONSOLE_LFS_S3_REGION`, `KAFSCALE_CONSOLE_LFS_S3_ENDPOINT` – S3 location for browsing LFS objects. Path-style addressing is used when an endpoint is set.
- `KAFSCALE_CONSOLE_LFS_S3_ACCESS_KEY`, `KAFSCALE_CONSOLE_LFS_S3_SECRET_KEY` – S3 credentials for the console.
- `KAFSCALE_CONSOLE_LFS_S3_PRESIGN_TTL` – Presigned URL lifetime in seconds (default `300`).

When LFS is enabled, the console serves these authenticated endpoints: `/ui/api/lfs/status`, `/ui/api/lfs/objects`, `/ui/api/lfs/topics`, `/ui/api/lfs/topics/{topic}`, `/ui/api/lfs/events`, `/ui/api/lfs/orphans`, `/ui/api/lfs/s3/browse` and `/ui/api/lfs/s3/presign`.

### LFS proxy

These settings apply when `KAFSCALE_PROXY_LFS_ENABLED=true`. Defaults come from `cmd/proxy/lfs.go` and `cmd/proxy/lfs_tracker.go`.

S3 storage:

- `KAFSCALE_LFS_PROXY_S3_BUCKET` – Required. The proxy refuses known example bucket names.
- `KAFSCALE_LFS_PROXY_S3_REGION` – Required.
- `KAFSCALE_LFS_PROXY_S3_ENDPOINT` – Endpoint override (MinIO or other S3-compatible stores).
- `KAFSCALE_LFS_PROXY_S3_PUBLIC_ENDPOINT` – Endpoint used in URLs handed to clients.
- `KAFSCALE_LFS_PROXY_S3_ACCESS_KEY`, `KAFSCALE_LFS_PROXY_S3_SECRET_KEY`, `KAFSCALE_LFS_PROXY_S3_SESSION_TOKEN` – S3 credentials.
- `KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE` – Path-style addressing (default `true` when an endpoint is set, otherwise `false`).
- `KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET` – Create the bucket at startup (default `false`).
- `KAFSCALE_LFS_PROXY_S3_HEALTH_INTERVAL_SEC` – S3 health check interval (default `30`).
- `KAFSCALE_S3_NAMESPACE` – Object key prefix (default `default` for the LFS module).

Blobs and checksums:

- `KAFSCALE_LFS_PROXY_MAX_BLOB_SIZE` – Maximum blob size in bytes (default `5368709120`, 5 GiB).
- `KAFSCALE_LFS_PROXY_CHUNK_SIZE` – Multipart chunk size in bytes (default `5242880`, 5 MiB).
- `KAFSCALE_LFS_PROXY_CHECKSUM_ALGO` – Checksum algorithm (default `sha256`).
- `KAFSCALE_LFS_PROXY_ID` – Proxy identifier recorded in tracker events (no default).
- `KAFSCALE_LFS_PROXY_TOPIC_MAX_LENGTH` – Maximum topic name length (default `249`).

HTTP API:

- `KAFSCALE_LFS_PROXY_HTTP_ADDR` – Listen address for the LFS HTTP API. No default: the API is off unless set.
- `KAFSCALE_LFS_PROXY_METRICS_ADDR` – Listen address for LFS metrics (`/metrics`). No default.
- `KAFSCALE_LFS_PROXY_HTTP_API_KEY` – API key required on HTTP requests (unset means no key check).
- `KAFSCALE_LFS_PROXY_HTTP_READ_TIMEOUT_SEC` (default `30`), `KAFSCALE_LFS_PROXY_HTTP_WRITE_TIMEOUT_SEC` (default `300`), `KAFSCALE_LFS_PROXY_HTTP_IDLE_TIMEOUT_SEC` (default `60`), `KAFSCALE_LFS_PROXY_HTTP_HEADER_TIMEOUT_SEC` (default `10`), `KAFSCALE_LFS_PROXY_HTTP_SHUTDOWN_TIMEOUT_SEC` (default `10`).
- `KAFSCALE_LFS_PROXY_HTTP_MAX_HEADER_BYTES` – Default `1048576` (1 MiB).
- `KAFSCALE_LFS_PROXY_UPLOAD_SESSION_TTL_SEC` – Multipart upload session lifetime (default `3600`).
- `KAFSCALE_LFS_PROXY_DOWNLOAD_TTL_SEC` – Maximum download URL lifetime (default `120`).
- `KAFSCALE_LFS_PROXY_PRESIGN_ENABLED` – Enable presigned URLs (default `false`).
- `KAFSCALE_LFS_PROXY_HTTP_TLS_ENABLED` – Serve the HTTP API over TLS (default `false`). Requires `KAFSCALE_LFS_PROXY_HTTP_TLS_CERT_FILE` and `KAFSCALE_LFS_PROXY_HTTP_TLS_KEY_FILE`. Minimum TLS 1.2.
- `KAFSCALE_LFS_PROXY_HTTP_TLS_CLIENT_CA_FILE` – CA for client certificates. Client certificates are verified if given.
- `KAFSCALE_LFS_PROXY_HTTP_TLS_REQUIRE_CLIENT_CERT` – Require client certificates when a client CA is set (default `false`).

HTTP endpoints (served on `KAFSCALE_LFS_PROXY_HTTP_ADDR`):

- `POST /lfs/produce` – Upload a blob and produce its envelope to Kafka.
- `POST /lfs/download` – Get a download for a stored blob.
- `POST /lfs/uploads` – Start a multipart upload session.
- `PUT /lfs/uploads/{id}/parts/{n}` – Upload a part.
- `POST /lfs/uploads/{id}/complete` – Complete the upload.
- `DELETE /lfs/uploads/{id}` – Abort the upload.
- `GET /swagger` and `GET /api/openapi.yaml` – API docs and OpenAPI spec.

Multipart upload cleanup:

Upload sessions live in proxy memory. Expiry removes a session without aborting
its S3 multipart upload, and a proxy restart also loses the session. Retrying a
failed abort is therefore possible only while the session remains available.
Configure an S3 bucket lifecycle rule with `AbortIncompleteMultipartUpload` for
the LFS object prefix, or the whole bucket if it is dedicated to LFS. For example,
set `DaysAfterInitiation` to `7`, choosing an interval longer than the longest
intended upload. S3 then makes incomplete uploads eligible for cleanup after that
interval; session expiry itself does not perform storage cleanup. This action
leaves completed objects intact. Merge the rule into the bucket's existing
lifecycle configuration. For S3-compatible storage, verify that the provider
supports and applies this action. See the
[Amazon S3 lifecycle configuration guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html).

Kafka backends:

- `KAFSCALE_LFS_PROXY_BACKENDS` – Comma-separated broker list used by the LFS module and tracker.
- `KAFSCALE_LFS_PROXY_DIAL_TIMEOUT_MS` (default `5000`), `KAFSCALE_LFS_PROXY_BACKEND_RETRIES` (default `6`, minimum `1`), `KAFSCALE_LFS_PROXY_BACKEND_BACKOFF_MS` (default `500`).
- `KAFSCALE_LFS_PROXY_BACKEND_SASL_MECHANISM`, `KAFSCALE_LFS_PROXY_BACKEND_SASL_USERNAME`, `KAFSCALE_LFS_PROXY_BACKEND_SASL_PASSWORD` – SASL for backend connections.
- `KAFSCALE_LFS_PROXY_BACKEND_TLS_ENABLED` (default `false`), `KAFSCALE_LFS_PROXY_BACKEND_TLS_CA_FILE`, `KAFSCALE_LFS_PROXY_BACKEND_TLS_CERT_FILE`, `KAFSCALE_LFS_PROXY_BACKEND_TLS_KEY_FILE` (cert and key must be set together), `KAFSCALE_LFS_PROXY_BACKEND_TLS_SERVER_NAME`, `KAFSCALE_LFS_PROXY_BACKEND_TLS_INSECURE_SKIP_VERIFY` (default `false`).

LFS tracker (writes LFS operation events to Kafka):

- `KAFSCALE_LFS_TRACKER_ENABLED` (default `true`), `KAFSCALE_LFS_TRACKER_TOPIC` (default `__lfs_ops_state`), `KAFSCALE_LFS_TRACKER_BATCH_SIZE` (default `100`), `KAFSCALE_LFS_TRACKER_FLUSH_MS` (default `100`), `KAFSCALE_LFS_TRACKER_ENSURE_TOPIC` (default `true`), `KAFSCALE_LFS_TRACKER_PARTITIONS` (default `3`), `KAFSCALE_LFS_TRACKER_REPLICATION_FACTOR` (default `1`).

Operator note: for an LFS proxy deployment the operator also sets `KAFSCALE_LFS_PROXY_ADDR`, `KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS`, `KAFSCALE_LFS_PROXY_ADVERTISED_HOST`, `KAFSCALE_LFS_PROXY_ADVERTISED_PORT`, `KAFSCALE_LFS_PROXY_BACKEND_CACHE_TTL_SEC` and `KAFSCALE_LFS_PROXY_HEALTH_ADDR` (pkg/operator/lfs_proxy_resources.go). The proxy binary does not read these; it reads `KAFSCALE_PROXY_ADDR`, `KAFSCALE_PROXY_ETCD_ENDPOINTS`, `KAFSCALE_PROXY_ADVERTISED_HOST`, `KAFSCALE_PROXY_ADVERTISED_PORT`, `KAFSCALE_PROXY_BACKEND_CACHE_TTL_SEC` and `KAFSCALE_PROXY_HEALTH_ADDR` instead.

### Iceberg processor LFS overrides

The Iceberg processor addon has its own LFS environment overrides. See `addons/processors/iceberg-processor/README.md`.

### idoc-explode

`idoc-explode` (cmd/idoc-explode) reads SAP IDoc XML or JSONL envelopes and writes one file per topic. Flags: `-input` (file path; empty reads stdin) and `-out` (output directory).

- `KAFSCALE_IDOC_OUTPUT_DIR` – Default for `-out` (default `idoc-output`).
- `KAFSCALE_IDOC_ITEM_SEGMENTS` (default `E1EDP01,E1EDP19`), `KAFSCALE_IDOC_PARTNER_SEGMENTS` (default `E1EDKA1`), `KAFSCALE_IDOC_STATUS_SEGMENTS` (default `E1STATS`), `KAFSCALE_IDOC_DATE_SEGMENTS` (default `E1EDK03`).
- `KAFSCALE_IDOC_TOPIC_HEADER` (default `idoc-headers`), `KAFSCALE_IDOC_TOPIC_SEGMENTS` (default `idoc-segments`), `KAFSCALE_IDOC_TOPIC_ITEMS` (default `idoc-items`), `KAFSCALE_IDOC_TOPIC_PARTNERS` (default `idoc-partners`), `KAFSCALE_IDOC_TOPIC_STATUS` (default `idoc-status`), `KAFSCALE_IDOC_TOPIC_DATES` (default `idoc-dates`).
- `KAFSCALE_IDOC_MAX_BLOB_SIZE` – Maximum LFS blob size to resolve (default `0`, no limit).
- `KAFSCALE_IDOC_VALIDATE_CHECKSUM` – Validate LFS checksums (default `true`).
- LFS blobs are resolved from S3 using `KAFSCALE_LFS_PROXY_S3_BUCKET`, `KAFSCALE_LFS_PROXY_S3_REGION`, `KAFSCALE_LFS_PROXY_S3_ENDPOINT`, `KAFSCALE_LFS_PROXY_S3_ACCESS_KEY`, `KAFSCALE_LFS_PROXY_S3_SECRET_KEY`, `KAFSCALE_LFS_PROXY_S3_SESSION_TOKEN` and `KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE`. Without bucket and region, blobs are not fetched from S3.

### Topic auto-creation through the proxy

In plain mode, a Metadata request for a missing named topic is forwarded to a
broker before the proxy rewrites the advertised addresses. The broker decides
whether to create the topic using `KAFSCALE_AUTO_CREATE_TOPICS` and its configured
partition count. This preserves direct-broker behavior for older request versions
and clients that leave `AllowAutoTopicCreation` unset. If broker auto-creation is
disabled, the response contains `UNKNOWN_TOPIC_OR_PARTITION`. Known-topic and
ID-based lookups continue to use the metadata store. Authenticated proxies always
forward Metadata to enforce broker ACLs.

## External Kafka Access

By default, brokers advertise the in-cluster service DNS name. That works for
clients running inside Kubernetes. External clients need a reachable address in
Metadata responses.

**Recommended:** deploy the Kafka-aware proxy (`proxy.enabled=true`). When the
proxy is enabled, clients connect to the **proxy Service**, not individual broker
pods. The proxy answers Metadata/FindCoordinator with a single stable endpoint,
then forwards all other Kafka requests to brokers. This keeps clients connected as
broker pods scale or rotate.

**Optional:** expose brokers directly via `KafscaleCluster` `spec.brokers` Service
settings (LoadBalancer or NodePort). Use this only when you intentionally bypass
the proxy (traffic isolation, pinned producers, or legacy integrations). Broker
NodePort does not replace the proxy entrypoint in scaled deployments.

The Helm chart does **not** ship a Kafka Ingress resource. External TLS uses
LoadBalancer annotations or an external TCP gateway (see
[Proxy TLS via LoadBalancer](#proxy-tls-via-loadbalancer-recommended)).

### Kafka Proxy (recommended)

Recommended settings:
- Run 2+ proxy replicas behind a LoadBalancer service.
- Point the proxy at etcd via `KAFSCALE_PROXY_ETCD_ENDPOINTS` so it can read the cluster snapshot.
- Set `KAFSCALE_PROXY_ADVERTISED_HOST`/`KAFSCALE_PROXY_ADVERTISED_PORT` to the public DNS + port clients should use.

Helm values to enable:
- `proxy.enabled=true`
- `proxy.etcdEndpoints[0]=http://kafscale-etcd-client.<namespace>.svc.cluster.local:2379`
- `proxy.advertisedHost=<public DNS>`

Example (HA proxy + external access):

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set proxy.enabled=true \
  --set proxy.replicaCount=2 \
  --set proxy.service.type=LoadBalancer \
  --set proxy.service.port=9092 \
  --set proxy.advertisedHost=kafka.example.com \
  --set proxy.advertisedPort=9092 \
  --set proxy.etcdEndpoints[0]=http://kafscale-etcd-client.kafscale.svc.cluster.local:2379
```

For local clusters (kind, minikube), pin the proxy NodePort so host port mappings
stay stable. Set `proxy.service.type=NodePort` and `proxy.service.nodePort` to a
value in `30000–32767`, then set `proxy.advertisedHost` to the node IP (or the
mapped host address). See `deploy/helm/kafscale/README.md` for details.

Example (kind / local dev):

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set proxy.enabled=true \
  --set proxy.service.type=NodePort \
  --set proxy.service.nodePort=30092 \
  --set proxy.advertisedHost=127.0.0.1 \
  --set proxy.advertisedPort=30092 \
  --set proxy.etcdEndpoints[0]=http://kafscale-etcd-client.kafscale.svc.cluster.local:2379
```

Helm chart docs: `deploy/helm/kafscale/README.md`.

### Direct broker exposure (optional)

Broker exposure settings (KafscaleCluster `spec.brokers`):
- `advertisedHost` / `advertisedPort` – Address Kafka clients should connect to.
- `service.type` – `ClusterIP`, `LoadBalancer`, or `NodePort`.
- `service.annotations` – Cloud provider LB annotations.
- `service.loadBalancerIP` / `service.loadBalancerSourceRanges` – Static IP + CIDR allowlist.
- `service.externalTrafficPolicy` – `Cluster` or `Local`.
- `service.kafkaNodePort` / `service.metricsNodePort` – Optional NodePort overrides.

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
- Helm installs CRDs on the first installation but does not upgrade CRDs from
  `crds/`. Before upgrading the chart or applying a cluster with `spec.security`,
  apply the new schema explicitly:

  ```sh
  kubectl apply --server-side --field-manager=kafscale-crd-upgrade -f deploy/helm/kafscale/crds/
  kubectl wait --for=condition=Established crd/kafscaleclusters.kafscale.io
  kubectl explain kafscalecluster.spec.security --api-version=kafscale.io/v1alpha1
  ```

  Then run `helm upgrade --install` and reapply the cluster manifest with
  `kubectl apply --validate=strict -f cluster.yaml`. Fields submitted against an
  old schema may already have been pruned; upgrading the CRD does not restore
  them. See [Kafka proxy authentication](kafka-authentication.md) for the full
  configuration and rotation procedure.
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

## Broker and managed etcd affinity overrides

`spec.brokers.affinity` and `spec.etcd.affinity` accept Kubernetes `Affinity`
objects independently. Omit a field (or set it to `null`) to retain the default
soft pod anti-affinity on `kubernetes.io/hostname`. An explicit `affinity: {}`
disables that default. A non-empty object replaces the entire affinity; the
operator does not merge the default into it. Removing an override restores the
default on the next reconciliation. Changes update the StatefulSet pod template
and can trigger a rolling update. Etcd placement applies only to managed etcd;
it does not affect an externally configured etcd cluster.

For example, require brokers on a labeled node pool and disable etcd affinity
in a single-node development cluster (merge these fields into your existing CR):

```yaml
spec:
  brokers:
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
            - matchExpressions:
                - key: kafscale.io/node-pool
                  operator: In
                  values: [brokers]
  etcd:
    affinity: {}
```

To extend the default, include its soft anti-affinity term alongside your custom
rules. For a cluster named `example`, the broker selector is
`{app: kafscale-broker, cluster: example}`; etcd uses
`{app: kafscale-etcd, cluster: example}`. The default term has weight `100` and
topology key `kubernetes.io/hostname`. Required affinity/anti-affinity rules can
leave pods Pending when matching nodes or topology domains are unavailable.

The affinity CRD fields use a generated structural schema. After changing the
API types or upgrading Kubernetes API dependencies, regenerate those fields
with `python3 hack/update_affinity_schema.py` (requires Go and PyYAML).
`python3 hack/update_affinity_schema.py --check` checks for drift while preserving
the other manually maintained CRD fields.
