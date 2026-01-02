---
layout: doc
title: Runtime Settings
description: Complete reference for broker, S3, etcd, proxy, console, and operator configuration in KafScale.
permalink: /configuration/
nav_title: Runtime Settings
nav_order: 1
nav_group: References
---

# Runtime Settings

All configuration is done via environment variables. Set these in your Helm values, KafScaleCluster CRD, or container spec.

---

## Broker Configuration

### Core Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_BROKER_ID` | auto | Node ID for group membership; auto-assigned if empty |
| `KAFSCALE_BROKER_ADDR` | `:9092` | Kafka listener address (host:port) |
| `KAFSCALE_BROKER_HOST` | | Advertised host for client connections |
| `KAFSCALE_BROKER_PORT` | `9092` | Advertised port for client connections |
| `KAFSCALE_METRICS_ADDR` | `:9093` | Prometheus metrics listen address |
| `KAFSCALE_CONTROL_ADDR` | `:9094` | Control-plane listen address |
| `KAFSCALE_STARTUP_TIMEOUT_SEC` | `30` | Broker startup timeout |
| `KAFSCALE_LOG_LEVEL` | `info` | Log level: debug, info, warn, error |

### Segment and Flush Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_SEGMENT_BYTES` | `4194304` | Segment flush threshold in bytes (4MB) |
| `KAFSCALE_FLUSH_INTERVAL_MS` | `500` | Maximum time before flushing buffer to S3 |

Tuning guidance:
- **Lower latency**: Reduce `FLUSH_INTERVAL_MS` (increases S3 requests)
- **Higher throughput**: Increase `SEGMENT_BYTES` (larger batches, fewer uploads)
- **Cost optimization**: Increase both (fewer S3 PUT requests)

### Cache Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_CACHE_BYTES` | `1073741824` | Hot segment cache size in bytes (1GB) |
| `KAFSCALE_INDEX_CACHE_BYTES` | `104857600` | Index cache size in bytes (100MB) |
| `KAFSCALE_READAHEAD_SEGMENTS` | `2` | Number of segments to prefetch |

Cache sizing guidance:
- Size `CACHE_BYTES` to hold your hot working set (recent segments consumers are reading)
- Monitor `kafscale_cache_hit_ratio`; target > 90% for typical workloads
- `READAHEAD_SEGMENTS` helps sequential consumers; increase for high-throughput topics

### Topic Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_AUTO_CREATE_TOPICS` | `true` | Auto-create topics on first produce |
| `KAFSCALE_AUTO_CREATE_PARTITIONS` | `1` | Partition count for auto-created topics |
| `KAFSCALE_ALLOW_ADMIN_APIS` | `true` | Enable Create/Delete Topic APIs |

### Durability Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_PRODUCE_SYNC_FLUSH` | `true` | Flush to S3 on Produce when `acks != 0` |

**Durability vs Cost tradeoff:**

| Profile | Settings | Behavior |
|---------|----------|----------|
| Durability-optimized (default) | `SYNC_FLUSH=true`, `FLUSH_INTERVAL_MS=500`, `SEGMENT_BYTES=4MB` | Minimal data loss window after crash |
| Cost-optimized | `SYNC_FLUSH=false`, `FLUSH_INTERVAL_MS=2000`, `SEGMENT_BYTES=32MB` | Fewer S3 PUTs, larger loss window |

### Metrics Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_THROUGHPUT_WINDOW_SEC` | `60` | Window for throughput metrics calculation |

### Debug and Testing

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_USE_MEMORY_S3` | `false` | Use in-memory S3 client (testing only, data not persisted) |
| `KAFSCALE_TRACE_KAFKA` | `false` | Enable Kafka protocol tracing (verbose, impacts performance) |

---

## S3 Configuration

### Bucket Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_S3_BUCKET` | required | S3 bucket for segments and snapshots |
| `KAFSCALE_S3_REGION` | `us-east-1` | S3 region |
| `KAFSCALE_S3_ENDPOINT` | | S3 endpoint override (for MinIO, R2, etc.) |
| `KAFSCALE_S3_PATH_STYLE` | `false` | Use path-style addressing instead of virtual-hosted |
| `KAFSCALE_S3_NAMESPACE` | cluster namespace | Prefix for S3 object keys |

### Read Replica Settings (Multi-Region)

For reduced read latency in multi-region deployments, configure a read replica bucket using S3 Cross-Region Replication (CRR) or Multi-Region Access Points (MRAP):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_S3_READ_BUCKET` | | Read replica bucket name |
| `KAFSCALE_S3_READ_REGION` | | Read replica region |
| `KAFSCALE_S3_READ_ENDPOINT` | | Read replica endpoint override |

Example:

```bash
# Primary (writes)
export KAFSCALE_S3_BUCKET=prod-segments
export KAFSCALE_S3_REGION=us-east-1

# Replica (reads in EU)
export KAFSCALE_S3_READ_BUCKET=prod-segments-replica
export KAFSCALE_S3_READ_REGION=eu-west-1
```

### Encryption Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_S3_KMS_ARN` | | KMS key ARN for server-side encryption (SSE-KMS) |

For SSE-S3 (AES-256), configure at the bucket level. For SSE-KMS, set the key ARN here.

### Credentials

If not using IAM roles (EKS IRSA, EC2 instance profiles):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_S3_ACCESS_KEY` | | AWS access key ID |
| `KAFSCALE_S3_SECRET_KEY` | | AWS secret access key |
| `KAFSCALE_S3_SESSION_TOKEN` | | AWS session token (for temporary credentials) |

### Health Thresholds

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_S3_HEALTH_WINDOW_SEC` | `60` | Health metric sampling window |
| `KAFSCALE_S3_LATENCY_WARN_MS` | `500` | Latency threshold for degraded state |
| `KAFSCALE_S3_LATENCY_CRIT_MS` | `2000` | Latency threshold for unavailable state |
| `KAFSCALE_S3_ERROR_RATE_WARN` | `0.01` | Error rate threshold for degraded (1%) |
| `KAFSCALE_S3_ERROR_RATE_CRIT` | `0.05` | Error rate threshold for unavailable (5%) |

Health states:

| State | Condition | Behavior |
|-------|-----------|----------|
| Healthy | Error rate < 1%, latency < 500ms | Normal operation |
| Degraded | Error rate 1-5% or latency 500-2000ms | Accepts requests with warnings |
| Unavailable | Error rate > 5% or latency > 2000ms | Rejects produces, serves cached fetches |

### S3-Compatible Storage Backends

KafScale works with any S3-compatible storage. Set `KAFSCALE_S3_ENDPOINT` and credentials for your provider.

| Provider | Endpoint Example | Notes |
|----------|-----------------|-------|
| AWS S3 | (leave empty) | Native support |
| MinIO | `http://minio:9000` | Set `PATH_STYLE=true` |
| DigitalOcean Spaces | `https://nyc3.digitaloceanspaces.com` | Region = space region |
| Cloudflare R2 | `https://<ACCOUNT_ID>.r2.cloudflarestorage.com` | Region = `auto` |
| Backblaze B2 | `https://s3.us-west-004.backblazeb2.com` | |
| Wasabi | `https://s3.us-east-1.wasabisys.com` | |
| Google Cloud Storage | `https://storage.googleapis.com` | Requires HMAC keys |

See [Storage Compatibility](/docs/deployment/compatibility/) for detailed configuration examples.

**MinIO example:**

```yaml
spec:
  s3:
    bucket: kafscale-data
    endpoint: http://minio.minio-system.svc:9000
    pathStyle: true
    credentialsSecretRef: kafscale-minio
```

**DigitalOcean Spaces example:**

```yaml
spec:
  s3:
    bucket: kafscale-production
    region: nyc3
    endpoint: https://nyc3.digitaloceanspaces.com
    credentialsSecretRef: kafscale-do
```

---

## etcd Configuration

etcd stores topic metadata, consumer offsets, and cluster coordination state. It's the only stateful component you need to back up.

### Endpoint Resolution Order

The operator resolves etcd endpoints in this order:

1. `KafScaleCluster.spec.etcd.endpoints` (CRD)
2. `KAFSCALE_OPERATOR_ETCD_ENDPOINTS` (env var)
3. Managed etcd (operator creates a 3-node StatefulSet)

### Availability Signals

When etcd is unavailable, brokers reject producer/admin/consumer-group operations with `REQUEST_TIMED_OUT`. Producers see per-partition errors in the Produce response; admin and group APIs return the same code in their response payloads.

Fetch requests for cached segments continue to work—brokers serve reads from their local cache even during etcd outages.

### Connection Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_ETCD_ENDPOINTS` | required | Comma-separated etcd endpoints |
| `KAFSCALE_ETCD_USERNAME` | | etcd basic auth username |
| `KAFSCALE_ETCD_PASSWORD` | | etcd basic auth password |
| `KAFSCALE_ETCD_CERT_FILE` | | Path to client certificate |
| `KAFSCALE_ETCD_KEY_FILE` | | Path to client key |
| `KAFSCALE_ETCD_CA_FILE` | | Path to CA certificate |

Examples:

```bash
# Single node (development)
KAFSCALE_ETCD_ENDPOINTS=http://etcd.kafscale.svc:2379

# Cluster (production)
KAFSCALE_ETCD_ENDPOINTS=http://etcd-0.etcd:2379,http://etcd-1.etcd:2379,http://etcd-2.etcd:2379

# With TLS
KAFSCALE_ETCD_ENDPOINTS=https://etcd-0.etcd:2379,https://etcd-1.etcd:2379,https://etcd-2.etcd:2379
KAFSCALE_ETCD_CERT_FILE=/etc/kafscale/etcd/tls.crt
KAFSCALE_ETCD_KEY_FILE=/etc/kafscale/etcd/tls.key
KAFSCALE_ETCD_CA_FILE=/etc/kafscale/etcd/ca.crt
```

---

## Proxy Configuration

The proxy provides a single entry point that routes Kafka requests to the appropriate broker.

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_PROXY_ADDR` | `:9092` | Proxy listen address (host:port) |
| `KAFSCALE_PROXY_ADVERTISED_HOST` | | Address clients should connect to |
| `KAFSCALE_PROXY_ADVERTISED_PORT` | `9092` | Advertised port |
| `KAFSCALE_PROXY_ETCD_ENDPOINTS` | | etcd endpoints for metadata |
| `KAFSCALE_PROXY_ETCD_USERNAME` | | etcd basic auth username |
| `KAFSCALE_PROXY_ETCD_PASSWORD` | | etcd basic auth password |
| `KAFSCALE_PROXY_BACKENDS` | | Optional comma-separated broker list for static routing |

The proxy discovers brokers via etcd by default. Use `KAFSCALE_PROXY_BACKENDS` for static configuration:

```bash
KAFSCALE_PROXY_BACKENDS=broker-0:9092,broker-1:9092,broker-2:9092
```

### Helm Values

| Value | Default | Description |
|-------|---------|-------------|
| `proxy.enabled` | `false` | Deploy the proxy |
| `proxy.replicaCount` | `2` | Number of proxy replicas |
| `proxy.advertisedHost` | | Public DNS clients should use |
| `proxy.advertisedPort` | `9092` | Advertised port |
| `proxy.etcdEndpoints` | | etcd endpoints array |
| `proxy.service.type` | `ClusterIP` | Service type |
| `proxy.service.port` | `9092` | Service port |

---

## Console Configuration

The console provides a web UI for cluster monitoring and topic management.

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_CONSOLE_HTTP_ADDR` | `:8080` | Console listen address |
| `KAFSCALE_CONSOLE_ETCD_ENDPOINTS` | | etcd endpoints for metadata (read-only) |
| `KAFSCALE_CONSOLE_ETCD_USERNAME` | | etcd basic auth username |
| `KAFSCALE_CONSOLE_ETCD_PASSWORD` | | etcd basic auth password |
| `KAFSCALE_CONSOLE_BROKER_METRICS_URL` | | Broker Prometheus endpoint for metrics display |
| `KAFSCALE_UI_USERNAME` | | Console login username |
| `KAFSCALE_UI_PASSWORD` | | Console login password |

Example:

```yaml
console:
  env:
    - name: KAFSCALE_CONSOLE_HTTP_ADDR
      value: ":8080"
    - name: KAFSCALE_UI_USERNAME
      valueFrom:
        secretKeyRef:
          name: kafscale-console
          key: username
    - name: KAFSCALE_UI_PASSWORD
      valueFrom:
        secretKeyRef:
          name: kafscale-console
          key: password
```

---

## Operator Configuration

### Core Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_OPERATOR_RECONCILE_INTERVAL_SEC` | `30` | Reconciliation loop interval |
| `KAFSCALE_OPERATOR_LEADER_KEY` | `kafscale-operator` | Leader election ID |
| `KAFSCALE_OPERATOR_LOG_LEVEL` | `info` | Operator log level |

### Managed etcd Settings

When `etcd.endpoints` is empty in the CRD, the operator deploys a managed etcd cluster:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_OPERATOR_ETCD_ENDPOINTS` | | External etcd endpoints; empty uses managed etcd |
| `KAFSCALE_OPERATOR_ETCD_IMAGE` | `kubesphere/etcd:3.6.4-0` | Managed etcd image |
| `KAFSCALE_OPERATOR_ETCD_STORAGE_SIZE` | `10Gi` | PVC size for managed etcd |
| `KAFSCALE_OPERATOR_ETCD_STORAGE_CLASS` | | StorageClass for managed etcd PVCs |
| `KAFSCALE_OPERATOR_ETCD_STORAGE_MEMORY` | `0` | Use in-memory `emptyDir` for etcd data (test/dev only). |

### etcd Snapshot Settings

Snapshots are stored in a separate bucket from broker segments by default.

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET` | `kafscale-etcd-<namespace>-<cluster>` | S3 bucket for etcd snapshots |
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PREFIX` | `etcd-snapshots` | S3 key prefix for snapshots |
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SCHEDULE` | `0 * * * *` | Cron schedule for snapshots (hourly) |
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT` | | S3 endpoint override for snapshots |
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_STALE_AFTER_SEC` | `7200` | Snapshot staleness alert threshold (2 hours) |
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_ETCDCTL_IMAGE` | `kubesphere/etcd:3.6.4-0` | etcdctl image for snapshots |
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_IMAGE` | `amazon/aws-cli:2.15.0` | AWS CLI image for uploads |
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET` | `0` | Auto-create snapshot bucket (`1` to enable) |
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PROTECT_BUCKET` | `0` | Enable versioning + public access block (`1` to enable) |
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SKIP_PREFLIGHT` | `0` | Skip S3 write preflight check (`1` to enable) |

### Broker Defaults

The operator passes these to brokers if not overridden in the CRD:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_S3_NAMESPACE` | cluster namespace | S3 object key prefix |
| `KAFSCALE_SEGMENT_BYTES` | `4194304` | Default segment size |
| `KAFSCALE_FLUSH_INTERVAL_MS` | `500` | Default flush interval |

---

## TLS Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_TLS_ENABLED` | `false` | Enable TLS for client connections |
| `KAFSCALE_TLS_CERT_FILE` | | Path to server certificate |
| `KAFSCALE_TLS_KEY_FILE` | | Path to server private key |
| `KAFSCALE_TLS_CA_FILE` | | Path to CA certificate (for mTLS) |
| `KAFSCALE_TLS_CLIENT_AUTH` | `false` | Require client certificates |

CRD example:

```yaml
spec:
  tls:
    enabled: true
    secretRef: kafscale-tls
```

The secret should contain `tls.crt` and `tls.key`. For mTLS, include `ca.crt`.

See [Security](/security/) for complete TLS setup instructions.

---

## External Broker Access

By default, brokers advertise the in-cluster service DNS name. External clients need a reachable address.

### CRD Settings (`spec.brokers`)

| Field | Description |
|-------|-------------|
| `advertisedHost` | Address clients should connect to |
| `advertisedPort` | Advertised port (default `9092`) |
| `service.type` | `ClusterIP`, `LoadBalancer`, or `NodePort` |
| `service.annotations` | Cloud provider LB annotations |
| `service.loadBalancerIP` | Static IP for LoadBalancer |
| `service.loadBalancerSourceRanges` | CIDR allowlist for LoadBalancer |
| `service.externalTrafficPolicy` | `Cluster` or `Local` |
| `service.kafkaNodePort` | NodePort for Kafka listener |
| `service.metricsNodePort` | NodePort for metrics |

### LoadBalancer Example (AWS NLB)

```yaml
spec:
  brokers:
    replicas: 3
    advertisedHost: kafka.example.com
    advertisedPort: 9092
    service:
      type: LoadBalancer
      annotations:
        service.beta.kubernetes.io/aws-load-balancer-type: nlb
        service.beta.kubernetes.io/aws-load-balancer-scheme: internet-facing
```

### NodePort Example

```yaml
spec:
  brokers:
    replicas: 3
    advertisedHost: node1.example.com
    advertisedPort: 30092
    service:
      type: NodePort
      kafkaNodePort: 30092
      metricsNodePort: 30093
```

---

## Consumer Group Settings

Session timeout and heartbeat intervals are negotiated by Kafka clients following protocol defaults. KafScale respects the values sent by clients:

| Client Setting | Typical Default | Description |
|----------------|-----------------|-------------|
| `session.timeout.ms` | `45000` | Time before consumer is considered dead |
| `heartbeat.interval.ms` | `3000` | Heartbeat frequency |
| `max.poll.interval.ms` | `300000` | Max time between polls |

These are client-side settings configured in your Kafka producer/consumer, not broker configuration.

---

## Example Configurations

### Minimal Production

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: prod
  namespace: kafscale
spec:
  brokers:
    replicas: 3
    env:
      - name: KAFSCALE_SEGMENT_BYTES
        value: "16777216"
      - name: KAFSCALE_FLUSH_INTERVAL_MS
        value: "1000"
      - name: KAFSCALE_CACHE_BYTES
        value: "4294967296"
      - name: KAFSCALE_LOG_LEVEL
        value: "warn"
  s3:
    bucket: kafscale-prod
    region: us-east-1
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints:
      - http://etcd-0.etcd.kafscale.svc:2379
      - http://etcd-1.etcd.kafscale.svc:2379
      - http://etcd-2.etcd.kafscale.svc:2379
```

### DigitalOcean Spaces

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: prod
  namespace: kafscale
spec:
  brokers:
    replicas: 3
  s3:
    bucket: kafscale-prod
    region: nyc3
    endpoint: https://nyc3.digitaloceanspaces.com
    credentialsSecretRef: kafscale-do
  etcd:
    endpoints: []
```

### MinIO (On-Prem / Dev)

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: dev
  namespace: kafscale
spec:
  brokers:
    replicas: 1
    env:
      - name: KAFSCALE_LOG_LEVEL
        value: "debug"
      - name: KAFSCALE_TRACE_KAFKA
        value: "true"
  s3:
    bucket: kafscale-dev
    endpoint: http://minio.minio-system.svc:9000
    pathStyle: true
    credentialsSecretRef: kafscale-minio
  etcd:
    endpoints: []
```

### Multi-Region with Read Replicas

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: global
  namespace: kafscale
spec:
  brokers:
    replicas: 3
    env:
      - name: KAFSCALE_S3_READ_BUCKET
        value: "kafscale-replica"
      - name: KAFSCALE_S3_READ_REGION
        value: "eu-west-1"
  s3:
    bucket: kafscale-primary
    region: us-east-1
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints:
      - http://etcd-0.etcd.kafscale.svc:2379
      - http://etcd-1.etcd.kafscale.svc:2379
      - http://etcd-2.etcd.kafscale.svc:2379
```

---

## Next Steps

- [Installation](/installation/) for Helm and CRD setup
- [Storage Compatibility](/docs/deployment/compatibility/) for non-AWS backends
- [Operations](/operations/) for monitoring and scaling
- [Security](/security/) for TLS and authentication
