---
layout: doc
title: Operations Guide
description: "Production operations guidance: monitoring, scaling, backups, upgrades, and troubleshooting for KafScale."
permalink: /operations/
nav_title: Operations
nav_order: 6
---

# Operations Guide

This guide covers day-to-day operations for KafScale clusters in production. For environment variable reference, see [Runtime Settings](/configuration/). For metrics details, see [Metrics Reference](/metrics/). For the admin API, see [Ops API](/ops-api/).

## Prerequisites

Before operating a production cluster:

- etcd cluster (3+ nodes recommended, odd quorum)
- S3 bucket with appropriate IAM permissions
- Kubernetes cluster with the KafScale operator installed
- `kubectl` and `helm` CLI tools

## Security & Hardening

- **RBAC**: The Helm chart creates a scoped service account and RBAC role so the operator only touches its CRDs, Secrets, and Deployments inside the release namespace.
- **S3 credentials**: Credentials live in user-managed Kubernetes secrets. The operator never writes them to etcd. Snapshot jobs map `KAFSCALE_S3_ACCESS_KEY`/`KAFSCALE_S3_SECRET_KEY` into `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`.
- **Console auth**: The UI requires `KAFSCALE_UI_USERNAME` and `KAFSCALE_UI_PASSWORD`. In Helm, set `console.auth.username` and `console.auth.password`.
- **TLS**: Terminate TLS at your ingress or service mesh; broker/console TLS env flags are not wired in v1.
- **Admin APIs**: Create/Delete Topics are enabled by default. Set `KAFSCALE_ALLOW_ADMIN_APIS=false` on broker pods to disable them, and gate external access via mTLS, ingress auth, or network policies.
- **Network policies**: Allow the operator + brokers to reach etcd and S3 endpoints and lock everything else down.
- **Health/metrics**: Prometheus can scrape `/metrics` on brokers and operator for early detection of S3 pressure or degraded nodes.
- **Startup gating**: Broker pods exit if they cannot read metadata or write a probe object to S3, so Kubernetes restarts them instead of leaving a stuck listener in place.
- **Leader IDs**: Each broker advertises a numeric NodeID in etcd. In a single-node demo you'll always see `Leader=0` in the Console's topic detail.

## External Broker Access

By default, brokers advertise the in-cluster service DNS name. That works for clients running inside Kubernetes, but external clients must connect to a reachable address. Configure both the broker Service exposure and the advertised address so clients learn the external endpoint from metadata responses.

See [Runtime Settings — External Broker Access](/configuration/#external-broker-access) for all CRD fields.

### Kafka Proxy (recommended for external access)

For external clients plus broker churn, deploy the Kafka-aware proxy. It answers Metadata/FindCoordinator requests with a single stable endpoint (the proxy service), then forwards all other Kafka requests to the brokers. This keeps clients connected even as broker pods scale or rotate.

Recommended settings:
- Run 2+ proxy replicas behind a LoadBalancer service
- Point the proxy at etcd via `KAFSCALE_PROXY_ETCD_ENDPOINTS`
- Set `KAFSCALE_PROXY_ADVERTISED_HOST`/`KAFSCALE_PROXY_ADVERTISED_PORT` to the public DNS + port

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

### Direct broker exposure

Use direct broker Service settings when you intentionally expose dedicated brokers (for example, isolating traffic or pinning producers to specific nodes). This requires explicit endpoint management.

Example (GKE/AWS/Azure load balancer):

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
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

### TLS termination

Brokers speak plaintext today. Terminate TLS at your load balancer, ingress TCP proxy, or service mesh and advertise that endpoint in `advertisedHost`/`advertisedPort`.

Example certificate (cert-manager):

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

## Monitoring

### Prometheus endpoints

| Component | Endpoint | Default Port |
|-----------|----------|--------------|
| Broker | `http://<broker-host>:9093/metrics` | 9093 |
| Operator | `http://<operator-host>:8080/metrics` | 8080 |

### Grafana dashboards

Import the pre-built dashboards from the repository:

```bash
kubectl apply -f https://raw.githubusercontent.com/novatechflow/kafscale/main/docs/grafana/broker-dashboard.json
kubectl apply -f https://raw.githubusercontent.com/novatechflow/kafscale/main/docs/grafana/operator-dashboard.json
```

For the full metrics catalog, see [Metrics](/metrics/).

## Ops API Examples

KafScale exposes Kafka admin APIs for operator workflows. See [Ops API](/ops-api/) for the full reference.

```bash
# List consumer groups
kafka-consumer-groups.sh --bootstrap-server <broker> --list

# Describe a consumer group
kafka-consumer-groups.sh --bootstrap-server <broker> --describe --group <group-id>

# Delete a consumer group
kafka-consumer-groups.sh --bootstrap-server <broker> --delete --group <group-id>

# Read topic configs
kafka-configs.sh --bootstrap-server <broker> --describe --entity-type topics --entity-name <topic>

# Increase partition count (additive only)
kafka-topics.sh --bootstrap-server <broker> --alter --topic <topic> --partitions <count>

# Update topic retention
kafka-configs.sh --bootstrap-server <broker> --alter --entity-type topics --entity-name <topic> \
  --add-config retention.ms=604800000
```

## Scaling

### Horizontal scaling

Brokers are stateless and scale horizontally. No partition rebalancing required—S3 is the source of truth.

```bash
kubectl scale deployment kafscale-broker --replicas=5
```

### HPA (CPU-based)

```bash
kubectl autoscale deployment kafscale-broker \
  --cpu-percent=70 \
  --min=3 \
  --max=12
```

### HPA (custom metrics)

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

## etcd Operations

KafScale depends on etcd for metadata and offsets. Treat it as a production datastore.

### Best practices

- Run a dedicated etcd cluster (do not share the Kubernetes control-plane etcd)
- Use SSD-backed disks for data and WAL volumes
- Deploy an odd number of members (3 for most clusters, 5 for higher fault tolerance)
- Spread members across zones/racks to survive single-AZ failures
- Enable compaction/defragmentation and monitor fsync/proposal latency

### Operator-managed etcd

If no etcd endpoints are supplied, the operator provisions a 3-node etcd StatefulSet. Recommended settings:

- Use an SSD-capable StorageClass for the etcd PVCs
- Set a PodDisruptionBudget so only one etcd pod can be evicted at a time
- Pin etcd pods across zones with topology spread or anti-affinity
- Enable snapshot backups to a dedicated S3 bucket
- Monitor leader changes, fsync latency, and disk usage

### Endpoint resolution order

The operator resolves etcd endpoints in this order:

1. `KafScaleCluster.spec.etcd.endpoints`
2. `KAFSCALE_OPERATOR_ETCD_ENDPOINTS`
3. Managed etcd (operator creates a 3-node StatefulSet)

### Availability signals

When etcd is unavailable, brokers reject producer/admin/consumer-group operations with `REQUEST_TIMED_OUT`. Producers see per-partition errors in the Produce response; admin and group APIs return the same code.

Fetch requests for cached segments continue to work during etcd outages.

## S3 Health Gating

Brokers monitor S3 health and reject requests when S3 is degraded.

| State | Value | Behavior |
|-------|-------|----------|
| `healthy` | 0 | Normal operation |
| `degraded` | 1 | Elevated latency, reduced throughput |
| `unavailable` | 2 | Rejects produce requests, serves cached fetches only |

```bash
# Check current state
curl -s http://broker:9093/metrics | grep kafscale_s3_health_state
```

When S3 is unavailable:
- Produce requests return `KAFKA_STORAGE_ERROR`
- Fetch requests serve from cache if available
- Brokers automatically recover when S3 returns

## Backup and Disaster Recovery

### etcd snapshots to S3

The operator uploads etcd snapshots to a dedicated S3 bucket (separate from broker segment storage). See [Runtime Settings](/configuration/) for the full variable reference.

Key defaults:

| Variable | Default |
|----------|---------|
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET` | `kafscale-etcd-<namespace>-<cluster>` |
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PREFIX` | `etcd-snapshots` |
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SCHEDULE` | `0 * * * *` (hourly) |
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_ETCDCTL_IMAGE` | `kubesphere/etcd:3.6.4-0` |
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_IMAGE` | `amazon/aws-cli:2.15.0` |
| `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_STALE_AFTER_SEC` | `7200` |

The operator performs an S3 write preflight before enabling snapshots. If the check fails, the `EtcdSnapshotAccess` condition is set to `False`.

### Snapshot restore (managed etcd)

When the operator manages etcd, each pod runs restore init containers before etcd starts:

1. Snapshot download container pulls the latest `.db` snapshot
2. Restore container runs `etcdctl snapshot restore` if data directory is empty
3. If no snapshot is available, etcd starts fresh

### Consumer Offsets After Restore

Etcd restores recover committed consumer offsets. If a consumer has **no committed offsets**, it may start at the end and see zero records even though data exists in S3. In production:

- Ensure consumers commit offsets (default for most Kafka clients).
- Set `auto.offset.reset=earliest` as a safety net for new or uncommitted consumers.

### Snapshot alerts

| Alert | Condition | Severity |
|-------|-----------|----------|
| `KafScaleSnapshotAccessFailed` | Snapshot writes failing | Critical |
| `KafScaleSnapshotStale` | Last snapshot > threshold | Warning |
| `KafScaleSnapshotNeverSucceeded` | No successful snapshots | Critical |

### Manual snapshot

```bash
# Via ops API
curl -X POST http://operator:8080/api/v1/etcd/snapshot

# Via etcdctl
ETCDCTL_API=3 etcdctl snapshot save /tmp/kafscale-snapshot.db \
  --endpoints=$ETCD_ENDPOINTS
```

### Restore from snapshot

```bash
# Scale down brokers
kubectl scale deployment kafscale-broker --replicas=0

# Restore etcd
ETCDCTL_API=3 etcdctl snapshot restore /tmp/kafscale-snapshot.db \
  --data-dir=/var/lib/etcd-restore

# Restart operator
kubectl rollout restart deployment kafscale-operator

# Scale up brokers
kubectl scale deployment kafscale-broker --replicas=3
```

### S3 data durability

S3 segment data does not need backup—S3 provides 11 9's durability. Ensure:

- S3 bucket versioning is enabled
- Cross-region replication if required for DR
- Lifecycle policies match retention requirements

## Multi-Region S3 (CRR)

KafScale writes to a primary bucket and can read from a replica bucket in the broker's region. With S3 Cross-Region Replication (CRR), objects are asynchronously copied to replica buckets. Brokers read from the local replica and fall back to primary on miss.

### Setup

1. Create buckets in each region and enable versioning (required for CRR)
2. Configure CRR rules from primary to each replica
3. Update cluster spec:

```yaml
spec:
  s3:
    bucket: kafscale-prod-us-east-1
    region: us-east-1
    readBucket: kafscale-prod-eu-west-1
    readRegion: eu-west-1
```

### IAM for read replicas

- **Primary bucket**: Allow `PutObject`, `DeleteObject`, `ListBucket` from writer cluster
- **Replica buckets**: Allow only `GetObject` and `ListBucket`, deny writes

### Verify CRR

```bash
# Write test object
aws s3 cp test.txt s3://kafscale-prod-us-east-1/crr-test/

# Check replication status
aws s3api head-object \
  --bucket kafscale-prod-us-east-1 \
  --key crr-test/test.txt \
  --query 'ReplicationStatus'

# Confirm in replica
aws s3 ls s3://kafscale-prod-eu-west-1/crr-test/
```

### Monitor CRR

| Metric | Alert threshold |
|--------|-----------------|
| `kafscale_s3_replica_fallback_total` | Rate > 0.1/s for 10m |
| `kafscale_s3_read_latency_ms` | p99 > 200ms |
| `kafscale_s3_replica_miss_ratio` | > 5% sustained |

## Upgrades

### Helm upgrade

```bash
helm upgrade kafscale novatechflow/kafscale \
  --namespace kafscale \
  --set broker.image.tag=v1.1.0 \
  --set operator.image.tag=v1.1.0

kubectl rollout status deployment/kafscale-broker -n kafscale
```

The operator drains brokers through the gRPC control plane before restarting pods.

### Rolling restart

```bash
kubectl rollout restart deployment kafscale-broker -n kafscale
```

### Rollback

```bash
helm history kafscale -n kafscale
helm rollback kafscale -n kafscale
```

## Capacity & Cost

### S3 cost estimation

Assumptions: 100 GB/day ingestion, 7-day retention, 4 MB segments, 3 brokers

| Item | Calculation | Cost |
|------|-------------|------|
| Storage | 700 GB x $0.023/GB | $16.10 |
| PUT requests | 25,000/day x 30 x $0.005/1000 | $3.75 |
| GET requests | 100,000/day x 30 x $0.0004/1000 | $1.20 |
| Data transfer (in-region) | Free | $0 |
| **Total S3** | | **~$21/month** |

### Durability vs cost tradeoff

See [Runtime Settings — Durability Settings](/configuration/) for the `KAFSCALE_PRODUCE_SYNC_FLUSH` tradeoff between durability and S3 write costs.

## Troubleshooting

### Brokers not starting

```bash
kubectl get pods -l app=kafscale-broker
kubectl logs -l app=kafscale-broker --tail=100
```

Common causes:
- etcd endpoints unreachable
- S3 credentials invalid
- Insufficient memory/CPU

### High produce latency

1. Check `kafscale_s3_latency_ms_avg` metric
2. Verify S3 bucket is in same region as brokers
3. Check broker CPU/memory utilization
4. Consider increasing `KAFSCALE_SEGMENT_BYTES` for larger batches

### Consumer group rebalancing

1. Check `kafscale_consumer_group_members` for instability
2. Verify consumer `session.timeout.ms` is appropriate
3. Check network connectivity

### etcd connection errors

```bash
ETCDCTL_API=3 etcdctl endpoint health --endpoints=$ETCD_ENDPOINTS
kubectl exec -it kafscale-broker-0 -- env | grep ETCD
```

### Debug mode

```bash
helm upgrade kafscale novatechflow/kafscale \
  --set broker.env.KAFSCALE_LOG_LEVEL=debug \
  --set broker.env.KAFSCALE_TRACE_KAFKA=true
```

---

For the complete environment variable reference, see [Runtime Settings](/configuration/).
