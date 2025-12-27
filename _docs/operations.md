---
layout: doc
title: Operations Guide
description: "Production operations guidance: monitoring, scaling, backups, upgrades, and troubleshooting for KafScale."
permalink: /operations/
nav_title: Operations
nav_order: 6
---

# Operations Guide

This guide covers day-to-day operations for KafScale clusters in production. For metrics details, see [Metrics Reference](/metrics/). For the admin API, see [Ops API](/ops-api/).

## Prerequisites

Before operating a production cluster:

- etcd cluster (3+ nodes recommended, odd quorum)
- S3 bucket with appropriate IAM permissions
- Kubernetes cluster with the KafScale operator installed
- `kubectl` and `helm` CLI tools

## Security & hardening

- **RBAC**: The Helm chart creates a scoped service account and RBAC role so the operator only touches its CRDs, Secrets, and Deployments inside the release namespace.
- **S3 credentials**: Credentials live in user-managed Kubernetes secrets. The operator never writes them to etcd. Snapshot jobs map `KAFSCALE_S3_ACCESS_KEY`/`KAFSCALE_S3_SECRET_KEY` into `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`.
- **Console auth**: The UI requires `KAFSCALE_UI_USERNAME` and `KAFSCALE_UI_PASSWORD`. In Helm, set `console.auth.username` and `console.auth.password`.
- **TLS**: Terminate TLS at your ingress or service mesh; broker/console TLS env flags are not wired in v1.
- **Network policies**: Allow the operator + brokers to reach etcd and S3 endpoints and lock everything else down.
- **Health/metrics**: Prometheus can scrape `/metrics` on brokers and operator for early detection of S3 pressure or degraded nodes.
- **Startup gating**: Broker pods exit if they cannot read metadata or write a probe object to S3, so Kubernetes restarts them instead of leaving a stuck listener in place.
- **Leader IDs**: Each broker advertises a numeric NodeID in etcd. In a single-node demo you’ll always see `Leader=0` in the Console’s topic detail.

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

For full Helm values and deployment notes, see [Helm](/helm/).

## Monitoring

### Prometheus endpoints

KafScale exposes Prometheus metrics on `/metrics`:

| Component | Endpoint | Default Port |
|-----------|----------|--------------|
| Broker | `http://<broker-host>:9093/metrics` | 9093 |
| Operator | `http://<operator-host>:8080/metrics` | 8080 |

### Grafana dashboards

For the full metrics catalog and dashboard context, see [Metrics](/metrics/).

Import the pre-built dashboards from the repository:

```bash
# Broker dashboard
kubectl apply -f https://raw.githubusercontent.com/novatechflow/kafscale/main/docs/grafana/broker-dashboard.json

# Operator dashboard  
kubectl apply -f https://raw.githubusercontent.com/novatechflow/kafscale/main/docs/grafana/operator-dashboard.json
```

Or import directly into Grafana from `docs/grafana/` in the main branch.

## Ops API examples

KafScale exposes Kafka admin APIs for operator workflows (consumer group visibility, config inspection, cleanup). The canonical plan and scope live in [Ops API](/ops-api/).

```bash
# List consumer groups
kafka-consumer-groups.sh --bootstrap-server <broker> --list

# Describe a consumer group
kafka-consumer-groups.sh --bootstrap-server <broker> --describe --group <group-id>

# Delete a consumer group
kafka-consumer-groups.sh --bootstrap-server <broker> --delete --group <group-id>

# Read topic configs
kafka-configs.sh --bootstrap-server <broker> --describe --entity-type topics --entity-name <topic>
```

## Scaling

### Horizontal scaling with HPA

Brokers are stateless and scale horizontally with Kubernetes HPA. No partition rebalancing is required—S3 is the source of truth.

**CPU-based HPA (recommended starting point):**

```bash
kubectl autoscale deployment kafscale-broker \
  --cpu-percent=70 \
  --min=3 \
  --max=12
```

**Custom metrics HPA (advanced):**

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: kafscale-broker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: kafscale-broker
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: Pods
    pods:
      metric:
        name: kafscale_produce_rps
      target:
        type: AverageValue
        averageValue: "1000"
```

### Scaling etcd

etcd should maintain an odd number of nodes (3, 5, 7). For scaling:

1. Add new etcd member to the cluster
2. Update `KAFSCALE_ETCD_ENDPOINTS` in broker configuration
3. Perform rolling restart of brokers

**Do not scale etcd down during active traffic.** Always ensure quorum (n/2 + 1 nodes).

## etcd availability & storage

KafScale depends on etcd for metadata + offsets. Treat etcd as a production datastore:

- Run a dedicated etcd cluster (do not share the Kubernetes control-plane etcd).
- Use SSD-backed disks for data and WAL volumes; avoid networked storage when possible.
- Deploy an odd number of members (3 for most clusters, 5 for higher fault tolerance).
- Spread members across zones/racks to survive single-AZ failures.
- Enable compaction/defragmentation and monitor fsync/proposal latency.

### Operator-managed etcd (default path)

If no etcd endpoints are supplied, the operator provisions a 3-node etcd StatefulSet. Recommended settings:

- Use an SSD-capable StorageClass for the etcd PVCs (`storageClassName`) with enough IOPS headroom.
- Set a PodDisruptionBudget so only one etcd pod can be evicted at a time.
- Pin etcd pods across zones with topology spread or anti-affinity.
- Enable snapshot backups to a dedicated S3 bucket and retain at least 7 days of snapshots.
- Monitor leader changes, fsync latency, and disk usage; alert on slow or flapping members.

### Etcd endpoint resolution

The operator resolves etcd endpoints in this order:

1. `KafscaleCluster.spec.etcd.endpoints`
2. `KAFSCALE_OPERATOR_ETCD_ENDPOINTS`
3. Managed etcd (operator creates a 3-node StatefulSet)

### Etcd schema direction

KafScale uses a snapshot-based metadata schema today: the operator publishes a full metadata snapshot to etcd and brokers consume it. We avoid per-key writes for broker registrations and assignments until the ops surface requires it.

## S3 Health Gating

Brokers monitor S3 health and reject requests when S3 is degraded. This prevents data loss during S3 outages.

### Health states

| State | Value | Behavior |
|-------|-------|----------|
| `healthy` | 0 | Normal operation |
| `degraded` | 1 | Elevated latency, reduced throughput |
| `unavailable` | 2 | Rejects produce requests, serves cached fetches only |

### Monitoring health transitions

```bash
# Check current state via metrics
curl -s http://broker:9093/metrics | grep kafscale_s3_health_state

# Check via ops API
curl http://broker:9093/api/v1/health
```

When S3 is unavailable:
- Produce requests return `KAFKA_STORAGE_ERROR`
- Fetch requests serve from cache if available
- Brokers automatically recover when S3 returns

## Backup and Disaster Recovery

Use this section to configure snapshots and practice restore procedures.

### etcd snapshots to S3

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

The operator performs an S3 write preflight before enabling snapshots. If the check fails, the `EtcdSnapshotAccess` condition is set to `False` and reconciliation returns an error until access is restored.

Minimal env + spec checklist for a smooth run:

- Operator env: `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET`, `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT` (if non-AWS), optionally `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET=1`.
- Cluster spec: `spec.s3.bucket`, `spec.s3.region`, `spec.s3.credentialsSecretRef`, `spec.s3.endpoint` (if non-AWS). Optional read replica: `spec.s3.readBucket`, `spec.s3.readRegion`, `spec.s3.readEndpoint`.
- Secret keys: `KAFSCALE_S3_ACCESS_KEY`, `KAFSCALE_S3_SECRET_KEY`.
- Console auth env: `KAFSCALE_UI_USERNAME`, `KAFSCALE_UI_PASSWORD`.

### Snapshot alerts

Configure these alerts in your monitoring system:

| Alert | Condition | Severity |
|-------|-----------|----------|
| `KafScaleSnapshotAccessFailed` | Snapshot writes failing | Critical |
| `KafScaleSnapshotStale` | Last successful snapshot > threshold | Warning |
| `KafScaleSnapshotNeverSucceeded` | No successful snapshots recorded | Critical |

### Manual snapshot

```bash
# Trigger manual snapshot via ops API
curl -X POST http://operator:8080/api/v1/etcd/snapshot

# Or use etcdctl directly
ETCDCTL_API=3 etcdctl snapshot save /tmp/kafscale-snapshot.db \
  --endpoints=$ETCD_ENDPOINTS \
  --cacert=/path/to/ca.crt \
  --cert=/path/to/client.crt \
  --key=/path/to/client.key
```

### Restore from snapshot

1. Stop all broker pods
2. Restore etcd from snapshot
3. Restart operator
4. Operator will reconcile broker deployments

```bash
# Scale down brokers
kubectl scale deployment kafscale-broker --replicas=0

# Restore etcd (example for single-node)
ETCDCTL_API=3 etcdctl snapshot restore /tmp/kafscale-snapshot.db \
  --data-dir=/var/lib/etcd-restore

# Restart operator
kubectl rollout restart deployment kafscale-operator

# Scale up brokers
kubectl scale deployment kafscale-broker --replicas=3
```

### S3 data durability

S3 segment data does not need backup—S3 provides 11 9's durability. However, ensure:

- S3 bucket versioning is enabled (protects against accidental deletion)
- Cross-region replication if required for DR
- Lifecycle policies match your retention requirements

## Multi-region S3 (CRR) for global reads

KafScale writes to a primary bucket and can optionally read from a replica bucket in the broker's region. With S3 Cross-Region Replication (CRR), objects written to the primary are asynchronously copied to replica buckets. Brokers attempt reads from their local replica and fall back to the primary if the object is missing (for example, due to CRR lag).

### Kubernetes deployment options

You have two choices for multi-region deployments:

1. **Separate K8s cluster per region** (recommended)  
   Deploy an independent Kubernetes cluster and KafScale operator in each region. Each operator manages only the brokers in its own cluster. This is simpler to operate and avoids cross-region Kubernetes networking.

2. **Single multi-region K8s cluster**  
   Run one Kubernetes cluster spanning multiple regions with node affinity rules to place broker pods in specific regions. The operator still runs once, but brokers are scheduled to nodes near their S3 replica.

The operator does not orchestrate cross-region scaling—it only manages brokers within its own cluster.

### CRR setup (AWS)

1. Create buckets in each region and enable versioning on all of them (required for CRR).
2. Configure CRR rules from the primary bucket to each replica bucket.
3. Update the cluster spec to include the replica details:
```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafscaleCluster
metadata:
  name: prod
spec:
  s3:
    bucket: kafscale-prod-us-east-1
    region: us-east-1
    readBucket: kafscale-prod-eu-west-1
    readRegion: eu-west-1
```

> **Note:** Versioning is required for CRR but increases storage costs (retains delete markers and old versions). Configure lifecycle rules to expire non-current versions after a retention period.

### Configure read access for satellite clusters

Use IAM and bucket policies to make replica buckets read-only from regional clusters.

**Practical approach:**

- **Primary bucket:** allow `PutObject`, `DeleteObject`, and `ListBucket` from the writer cluster's IAM role.
- **Replica buckets:** allow only `GetObject` and `ListBucket` for regional cluster roles, and explicitly deny writes.

**Example replica bucket policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowReadOnlyFromRegionalCluster",
      "Effect": "Allow",
      "Principal": {"AWS": "arn:aws:iam::<acct>:role/kafscale-broker-eu"},
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::kafscale-prod-eu-west-1",
        "arn:aws:s3:::kafscale-prod-eu-west-1/*"
      ]
    },
    {
      "Sid": "DenyWritesFromAllClusters",
      "Effect": "Deny",
      "Principal": "*",
      "Action": ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
      "Resource": "arn:aws:s3:::kafscale-prod-eu-west-1/*"
    }
  ]
}
```

**KafScale configuration:**

- Configure all regions with `spec.s3.bucket` pointing to the primary (write) bucket.
- Configure `spec.s3.readBucket` to the local replica.
- Give regional broker pods read-only credentials for replica buckets; only the primary writer role can write to the primary bucket.
- This prevents accidental writes if a read bucket is misconfigured as a write target.

### Verify CRR before go-live
```bash
# Write a test object to primary
aws s3 cp test.txt s3://kafscale-prod-us-east-1/crr-test/

# Check replication status (should be COMPLETED within seconds)
aws s3api head-object \
  --bucket kafscale-prod-us-east-1 \
  --key crr-test/test.txt \
  --query 'ReplicationStatus'

# Confirm object exists in replica
aws s3 ls s3://kafscale-prod-eu-west-1/crr-test/
```

### Monitor CRR and fallback

**Key metrics:**

| Metric | Description | Alert threshold |
|--------|-------------|-----------------|
| `kafscale_s3_replica_fallback_total` | Reads that fell back to primary | Rate > 0.1/s for 10m |
| `kafscale_s3_read_latency_ms` | Read latency (high = cross-region) | p99 > 200ms |
| `kafscale_s3_replica_miss_ratio` | Ratio of replica misses to total reads | > 5% sustained |

**Prometheus alerts:**
{% raw %}
```yaml
groups:
  - name: kafscale-crr
    rules:
      - alert: S3ReplicaFallbackHigh
        expr: rate(kafscale_s3_replica_fallback_total[5m]) > 0.1
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "High replica fallback rate in {{ $labels.region }}"
          description: "Brokers falling back to primary bucket - check CRR lag"

      - alert: S3CrossRegionLatency
        expr: histogram_quantile(0.99, kafscale_s3_read_latency_ms_bucket) > 200
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High S3 read latency - possible CRR issues"
```
{% endraw %}

**AWS-side CRR monitoring:**
```bash
# Check replication status on recent segments
aws s3api head-object \
  --bucket kafscale-prod-us-east-1 \
  --key production/orders/0/segment-00000000000000000042.kfs \
  --query 'ReplicationStatus'
# Expected: "COMPLETED", "PENDING", or "FAILED"
```

Enable S3 Replication Metrics in AWS for CloudWatch visibility into replication lag and failure rates.

### Notes

- CRR lag means the newest segments may not appear immediately in the replica (typically seconds, up to minutes under heavy load). The broker falls back to the primary for those reads.
- Extra cross-region traffic occurs only on replica misses; steady-state reads stay in-region once replication catches up.
- For non-AWS S3-compatible endpoints, set `spec.s3.readEndpoint` and/or `KAFSCALE_S3_READ_ENDPOINT`.

## Upgrades

Use these steps for safe rollouts and rollbacks.

### Helm upgrade (recommended)

```bash
# Check current version
helm list -n kafscale

# Upgrade with pinned image tag
helm upgrade kafscale novatechflow/kafscale \
  --namespace kafscale \
  --set broker.image.tag=v1.1.0 \
  --set operator.image.tag=v1.1.0

# Watch rollout
kubectl rollout status deployment/kafscale-broker -n kafscale
```

### Rolling restart

The operator drains brokers through the gRPC control plane before restarting pods. Active connections are gracefully closed.

```bash
# Trigger rolling restart
kubectl rollout restart deployment kafscale-broker -n kafscale
```

### Rollback

```bash
# View history
helm history kafscale -n kafscale

# Rollback to previous
helm rollback kafscale -n kafscale

# Rollback to specific revision
helm rollback kafscale 3 -n kafscale
```

## Capacity & cost {#capacity-cost}

Once retention and traffic are known, estimate S3 footprint and request cost.

### S3 cost estimation (example)

Assumptions:

- 100 GB/day ingestion
- 7-day retention
- 4 MB average segment size
- 3 broker pods

Monthly costs (us-east-1):

| Item | Calculation | Cost |
|------|-------------|------|
| Storage | 700 GB x $0.023/GB | $16.10 |
| PUT requests | 25,000/day x 30 x $0.005/1000 | $3.75 |
| GET requests | 100,000/day x 30 x $0.0004/1000 | $1.20 |
| Data transfer (in-region) | Free | $0 |
| **Total S3** | | **~$21/month** |

## Troubleshooting

### Common issues

**Brokers not starting**

```bash
# Check pod status
kubectl get pods -l app=kafscale-broker

# Check logs
kubectl logs -l app=kafscale-broker --tail=100

# Common causes:
# - etcd endpoints unreachable
# - S3 credentials invalid
# - Insufficient memory/CPU
```

**High produce latency**

1. Check `kafscale_s3_latency_ms_avg` metric
2. Verify S3 bucket is in same region as brokers
3. Check broker CPU/memory utilization
4. Consider increasing `KAFSCALE_SEGMENT_BYTES` for larger batches

**Consumer group rebalancing frequently**

1. Check `kafscale_consumer_group_members` for instability
2. Verify consumer `session.timeout.ms` is appropriate
3. Check network connectivity between consumers and brokers

**etcd connection errors**

```bash
# Verify etcd endpoints
ETCDCTL_API=3 etcdctl endpoint health --endpoints=$ETCD_ENDPOINTS

# Check broker environment
kubectl exec -it kafscale-broker-0 -- env | grep ETCD

# Common causes:
# - Incorrect endpoints
# - Certificate issues
# - etcd quorum lost
```

### Debug mode

Enable debug logging for troubleshooting:

```yaml
spec:
  broker:
    env:
    - name: KAFSCALE_LOG_LEVEL
      value: debug
```

Or via Helm:

```bash
helm upgrade kafscale novatechflow/kafscale \
  --set broker.logLevel=debug
```

## Runtime Settings Reference

For the full environment variable index, see [Runtime Settings](/configuration/).

### Environment variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFSCALE_ETCD_ENDPOINTS` | etcd endpoint(s), comma-separated | `localhost:2379` |
| `KAFSCALE_S3_BUCKET` | S3 bucket for segments | — (required) |
| `KAFSCALE_S3_REGION` | S3 region | `us-east-1` |
| `KAFSCALE_S3_READ_BUCKET` | Optional read replica bucket | — |
| `KAFSCALE_S3_READ_REGION` | Optional read replica region | — |
| `KAFSCALE_S3_READ_ENDPOINT` | Optional read replica endpoint | — |
| `KAFSCALE_SEGMENT_BYTES` | Segment size threshold | `4194304` (4MB) |
| `KAFSCALE_FLUSH_INTERVAL_MS` | Flush interval | `500` |
| `KAFSCALE_CACHE_SIZE` | L1 cache size | `1073741824` (1GB) |
| `KAFSCALE_LOG_LEVEL` | Log level | `info` |

### etcd requirements

| Requirement | Recommendation |
|-------------|----------------|
| Nodes | 3, 5, or 7 (odd quorum) |
| Storage | SSD with low latency |
| Memory | 2GB+ per node |
| Network | Low latency to brokers |

### S3 requirements

| Requirement | Recommendation |
|-------------|----------------|
| Storage class | Standard (or S3 Express for low latency) |
| Region | Same region as brokers |
| IAM permissions | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` |
| Versioning | Enabled (recommended) |
