---
layout: doc
title: "KafScale FAQ: Compatibility, Licensing, and S3"
description: Common questions about KafScale, Kafka compatibility, and S3 durability.
permalink: /faq/
nav_title: FAQ
nav_order: 10
---

# FAQ

## General

### How does KafScale compare to WarpStream, Redpanda, or AutoMQ?

See [Comparison](/comparison/) for a detailed side-by-side analysis covering architecture, latency, licensing, and cost.

The short version: KafScale keeps no local disk on brokers, stores data in the open .kfs format and keeps metadata in etcd in your own cluster, under Apache 2.0. WarpStream is proprietary and part of Confluent. AutoMQ is a Kafka fork under Apache 2.0 since April 2025 that pairs S3 with a write-ahead log. Redpanda uses local disks and is licensed under BSL 1.1.

### Why would I use KafScale instead of Apache Kafka?

KafScale trades latency for operational simplicity and storage-native processing. If your workload can tolerate S3 write latency (AWS documents roughly 100–200 ms for small objects, see [Latency](/latency/#s3-latency-as-aws-publishes-it)), KafScale eliminates stateful brokers, partition rebalancing, and disk capacity planning.

**For AI agent infrastructure**: KafScale's architecture aligns with what agentic systems actually need. AI agents reasoning over business context require completeness and replay capability, not sub-millisecond latency. The immutable log in S3 becomes the system of record that agents query, replay, and reason over. Processors convert that log to tables without competing with streaming workloads for broker resources.

Traditional stream processing optimizes for latency. Milliseconds matter for fraud detection or trading. But AI agents have different requirements: they need to understand what happened, in what order, and why the current state exists. Event sourcing research from the Apache Flink community (FLIP-531) and platforms like Akka confirms this pattern: agentic systems need reproducible state at any point in time.

<svg class="diagram" viewBox="0 0 700 220" role="img" aria-label="When to choose KafScale vs Kafka">
  <style>
    .diagram-text { font-family: system-ui, sans-serif; font-size: 12px; fill: var(--diagram-text, #1e293b); }
    .diagram-title { font-family: system-ui, sans-serif; font-size: 14px; font-weight: 600; fill: var(--diagram-text, #1e293b); }
    .diagram-box { fill: var(--diagram-fill, #f8fafc); stroke: var(--diagram-stroke, #cbd5e1); stroke-width: 1.5; rx: 8; }
    .diagram-accent { fill: var(--diagram-accent, #0ea5e9); }
  </style>
  <rect x="10" y="10" width="330" height="200" class="diagram-box"/>
  <text x="175" y="35" text-anchor="middle" class="diagram-title">Choose KafScale</text>
  <text x="30" y="60" class="diagram-text">✓ ETL and data pipelines</text>
  <text x="30" y="82" class="diagram-text">✓ Log aggregation</text>
  <text x="30" y="104" class="diagram-text">✓ Async event processing</text>
  <text x="30" y="126" class="diagram-text">✓ AI agent infrastructure</text>
  <text x="30" y="148" class="diagram-text">✓ Event replay and audit</text>
  <text x="30" y="170" class="diagram-text">✓ Teams without Kafka expertise</text>
  <text x="30" y="192" class="diagram-text">✓ Latency tolerance: sub-second</text>
  <rect x="360" y="10" width="330" height="200" class="diagram-box"/>
  <text x="525" y="35" text-anchor="middle" class="diagram-title">Choose Apache Kafka</text>
  <text x="380" y="60" class="diagram-text">✓ Real-time trading systems</text>
  <text x="380" y="82" class="diagram-text">✓ Interactive applications</text>
  <text x="380" y="104" class="diagram-text">✓ Exactly-once semantics (EOS)</text>
  <text x="380" y="126" class="diagram-text">✓ Compacted topics</text>
  <text x="380" y="148" class="diagram-text">✓ Complex stream processing</text>
  <text x="380" y="170" class="diagram-text">✓ Sub-10ms latency required</text>
  <text x="380" y="192" class="diagram-text">✓ Stateful stream joins</text>
</svg>

### Is KafScale production ready?

KafScale is designed for production use, but comes with no warranties or guarantees. Review [Operations](/operations/) and [Security](/security/) to align it with your requirements. Start with non-critical workloads and expand as you gain confidence.

### What license is KafScale released under?

Apache 2.0. You can use it commercially, modify it, distribute it, and offer it as a service without restrictions. No BSL conversion periods, no usage fees, no control plane dependencies.

---

## Architecture

### Why does KafScale use native Kafka record format?

KafScale stores data in `.kfs` segments containing native Kafka V2 record batches, the same binary format Kafka uses internally. This is a deliberate choice:

**Format stability**: Kafka's on-disk format is one of the most stable interfaces in data infrastructure. In 15+ years, there have been exactly three message format versions:

| Version | Introduced | Status |
|---------|-----------|--------|
| V0 | Original (2011) | Removed in Kafka 4.0 |
| V1 | Kafka 0.10.0 (2016) | Removed in Kafka 4.0 |
| V2 | Kafka 0.11.0 (June 2017) | Current standard |

V2 has been the only supported format for 8+ years. The entire Kafka ecosystem (Confluent, Redpanda, every client library, Flink, Spark, Debezium, MirrorMaker) depends on this stability. Changing it would break everything.

**If Kafka ever changes**: KafScale is fully open source under Apache 2.0. Any format updates can be implemented immediately by the community. Contrast this with proprietary alternatives where you'd wait for a vendor to prioritize the update.

**No abstraction tax**: Using native format means zero conversion overhead. Producers write Kafka records; we store Kafka records; consumers read Kafka records.

### What about coupling processors to the storage format?

[Processors](/docs/#processors) read directly from S3, bypassing brokers entirely. This means they understand the `.kfs` segment format and coordinate via etcd.

This is intentional coupling to a stable interface, not a liability:

1. **The format won't change**: Kafka V2 record batches are a de facto standard
2. **Read-replica brokers would have the same coupling**: they'd also need to parse segments and query etcd
3. **The coupling is explicit and documented**: not hidden inside a proprietary broker
4. **Open format means open tooling**: anyone can build processors, analyzers, or integrations

The tradeoff: if KafScale's internal segment layout evolves, processors need updates. In practice, we version the segment format and maintain backward compatibility.

### Does KafScale work with clouds other than AWS?

Yes, on S3-compatible backends that support conditional writes (`If-None-Match: *` on `PutObject`). See [Supported storage](/supported/) for the compatibility matrix and configuration examples.

Backblaze B2, Google Cloud Storage, and Azure Blob Storage are not supported in this release. B2 does not honor conditional writes. The GCS XML API supports `If-None-Match` only on reads. Azure Blob is not S3-compatible.

---

## Latency and Performance

### What latency should I expect?

KafScale latency is S3 latency. Every acknowledged produce waits for an S3 write, so KafScale prioritizes durability and operational simplicity over sub-10ms latency.

AWS documents that applications "can achieve consistent small object latencies (and first-byte-out latencies for larger objects) of roughly 100–200 milliseconds" ([AWS S3 performance guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)), and that for requests under 512 KB "median latencies are often in the tens of milliseconds range" ([AWS S3 design patterns](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html)). Here is what each KafScale operation waits on. The last column is derived from those AWS figures, not from a KafScale benchmark:

| Operation | Waits on | Derived from AWS's S3 figures |
|-----------|----------|-------------------------------|
| Produce (default, `acks=1` or `all`) | One S3 PUT plus one etcd write, plus up to `linger.ms` on the producer | Roughly S3 PUT latency plus the etcd write plus `linger.ms`. Segment and index upload in parallel |
| Fetch of recent data | Broker memory | No S3 request |
| Fetch of older data | One S3 GET | Roughly S3 GET latency. Read-ahead prefetches the next segments |
| Consumer group join | etcd coordination | Not an S3 request |

See [Latency and batching](/latency/) for the full produce path, the AWS sources, measured throughput and tuning.

### Can I reduce latency?

Several factors affect latency:

1. **S3 region proximity**: Deploy brokers in the same region as your S3 bucket
2. **Sync flush**: With `KAFSCALE_PRODUCE_SYNC_FLUSH=false` the broker acks after buffering and flushes on `KAFSCALE_FLUSH_INTERVAL_MS` or `KAFSCALE_SEGMENT_BYTES`. Lower latency and fewer S3 requests, but a broker crash loses the unflushed tail
3. **Cache size**: Larger `KAFSCALE_CACHE_BYTES` improves fetch hit rates
4. **Producer batching**: `linger.ms` adds up to that much wait per batch but cuts S3 requests. See [producer batching settings](/latency/#producer-batching-settings)

The fundamental tradeoff is S3 round-trip time. If you need sub-10ms latency, KafScale is not the right choice.

### How large can messages be?

There's no configured maximum in KafScale. The theoretical limit is the 32-bit Kafka frame length (~2 GB), but the practical limit is broker memory since messages are fully buffered in RAM before flushing to S3.

**Rule of thumb**: max in-flight messages ≈ RAM / (2 × message size)

| Broker RAM | 10 MB messages | 50 MB messages | 100 MB messages |
|------------|----------------|----------------|-----------------|
| 16 GB      | ~800 in-flight | ~160 in-flight | ~80 in-flight   |

Large payloads (XML, JSON, binary blobs) work fine. They're stored as standard Kafka record batches inside `.kfs` segments. Multiple messages buffer until flush thresholds, then upload as a single segment object.

**Scaling for large messages**: Since brokers are stateless, you can scale pods automatically based on memory pressure (e.g., HPA at 80% memory). One stable endpoint, automatic scaling behind the scenes.

### How does KafScale handle backpressure?

When S3 latency exceeds thresholds, brokers enter `DEGRADED` state. If S3 becomes unavailable, brokers enter `UNAVAILABLE` state and reject produce requests while continuing to serve cached fetch requests. Clients should implement retry logic with exponential backoff.

---

## Kafka Compatibility

### Can I use existing Kafka clients?

Yes. KafScale implements the Kafka wire protocol for core APIs. Standard Kafka clients work. Producers must set `enable.idempotence=false`, because KafScale does not implement `InitProducerId`.

Tested clients include kafka-python, franz-go, librdkafka, Sarama, and the official Java client.

### Which Kafka APIs are supported?

KafScale supports 21 Kafka APIs covering produce, fetch, metadata, and consumer group operations. See [Protocol](/protocol/) for the complete compatibility matrix.

Not supported: transactions (exactly-once semantics), compacted topics, and the admin API for ACLs.

### Can I migrate from Kafka to KafScale?

Yes, but it requires replaying data. KafScale uses a different storage layout (S3 segments) than Kafka (local log files), though the record format is identical. Migration options:

1. **Dual-write**: Produce to both systems during transition
2. **MirrorMaker**: Use Kafka MirrorMaker to replicate topics to KafScale
3. **Consumer replay**: Consume from Kafka and produce to KafScale

### Can I migrate from WarpStream?

Yes, for workloads that do not use transactions or compacted topics. Copy topics with kaf-mirror, run both systems side by side, then cut clients over. See [Migrate from WarpStream](/migrate-from-warpstream/).

### Does Bento work with KafScale?

Yes. Use the Bento `kafka_franz` input and output and set `idempotent_write: false` on the output. See [Bento with KafScale](/bento/).

### Do consumer groups work?

Yes. KafScale implements the full consumer group protocol including JoinGroup, SyncGroup, Heartbeat, LeaveGroup, and OffsetCommit/Fetch. Consumer offsets are stored in etcd.

---

## Processors

### What are processors?

Processors are components that read directly from S3, bypassing brokers entirely. They enable analytical workloads without adding load to your streaming infrastructure. See [Processors](/docs/#processors) for details.

Available processors:

- **[Iceberg Processor](/processors/iceberg/)**: Continuous export to Apache Iceberg tables
- **[SQL Processor (KAFSQL)](/processors/sql/)**: Query KafScale segments with Postgres-compatible SQL

### Why bypass brokers for analytics?

Traditional Kafka Connect runs through brokers, competing with real-time consumers for broker resources. KafScale processors read segments directly from S3:

- **No broker contention**: Analytical queries don't impact streaming latency
- **Horizontal scale**: Add processors without broker capacity planning
- **Cost efficiency**: S3 reads are cheap; broker CPU is expensive

This architecture is ideal for AI/ML pipelines where you need to replay large volumes of historical data without impacting production consumers.

### Can I build custom processors?

Yes. The `.kfs` segment format is documented, and processors coordinate via etcd for offset tracking. See [Building Processors](https://github.com/KafScale/platform/tree/main/addons/processors/skeleton) for the SDK and examples.

---

## Storage and Durability

### How durable is my data?

S3 provides 99.999999999% (11 nines) durability. With `KAFSCALE_PRODUCE_SYNC_FLUSH=true` (the default), a produce is acknowledged only after the segment is written to S3 and its metadata is committed to etcd, so acknowledged data has the same durability as the S3 object. If sync flush is off, a broker crash can lose the unflushed tail.

<svg class="diagram" viewBox="0 0 700 160" role="img" aria-label="Data durability flow">
  <style>
    .diagram-text { font-family: system-ui, sans-serif; font-size: 11px; fill: var(--diagram-text, #1e293b); }
    .diagram-label { font-family: system-ui, sans-serif; font-size: 10px; fill: var(--diagram-label, #64748b); }
    .diagram-box { fill: var(--diagram-fill, #f8fafc); stroke: var(--diagram-stroke, #cbd5e1); stroke-width: 1.5; rx: 6; }
    .diagram-accent { fill: var(--diagram-accent, #0ea5e9); }
    .diagram-arrow { stroke: var(--diagram-stroke, #cbd5e1); stroke-width: 1.5; fill: none; marker-end: url(#arrowhead); }
  </style>
  <defs>
    <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
      <polygon points="0 0, 10 3.5, 0 7" fill="var(--diagram-stroke, #cbd5e1)"/>
    </marker>
  </defs>
  <rect x="10" y="50" width="120" height="60" class="diagram-box"/>
  <text x="70" y="75" text-anchor="middle" class="diagram-text">Producer</text>
  <text x="70" y="92" text-anchor="middle" class="diagram-label">sends record</text>
  <path d="M 135 80 L 175 80" class="diagram-arrow"/>
  <rect x="180" y="50" width="120" height="60" class="diagram-box"/>
  <text x="240" y="75" text-anchor="middle" class="diagram-text">Broker Buffer</text>
  <text x="240" y="92" text-anchor="middle" class="diagram-label">in-memory</text>
  <path d="M 305 80 L 345 80" class="diagram-arrow"/>
  <rect x="350" y="50" width="120" height="60" class="diagram-box"/>
  <text x="410" y="75" text-anchor="middle" class="diagram-text">S3 Upload</text>
  <text x="410" y="92" text-anchor="middle" class="diagram-label">segment + index</text>
  <path d="M 475 80 L 515 80" class="diagram-arrow"/>
  <rect x="520" y="50" width="120" height="60" class="diagram-box" style="stroke: var(--diagram-accent, #0ea5e9); stroke-width: 2;"/>
  <text x="580" y="75" text-anchor="middle" class="diagram-text">ACK to Producer</text>
  <text x="580" y="92" text-anchor="middle" class="diagram-label">11 nines durable</text>
  <text x="350" y="140" text-anchor="middle" class="diagram-label">Data is NOT acknowledged until S3 upload completes</text>
</svg>

### What happens if S3 goes down?

Brokers monitor S3 health over a sliding window (default 60 seconds). Defaults come from `cmd/broker/main.go`:

| State | Condition (defaults) | Behavior |
|-------|----------------------|----------|
| Healthy | Average latency below 500 ms and error rate below 20% | Produce and fetch proceed as usual |
| Degraded | Average latency at least 500 ms, or error rate at least 20%, and neither has reached the critical threshold | Produce and fetch are rejected with a retriable `REQUEST_TIMED_OUT` |
| Unavailable | Average latency at least 3000 ms, or error rate at least 60% | Produce and fetch are rejected with a retriable `KAFKA_STORAGE_ERROR` |

Monitor `kafscale_s3_health_state{state="healthy|degraded|unavailable"}`. The active state has value `1`, the others `0`. See [S3 health](/s3-health/) and implement client-side retries.

### What happens if a broker crashes?

Acknowledged data is safe when `KAFSCALE_PRODUCE_SYNC_FLUSH=true` (the default): a produce is acked only after the segment is in S3 and its metadata is in etcd. If sync flush is off, a crash can lose the unflushed tail (up to `KAFSCALE_FLUSH_INTERVAL_MS`).

Brokers are otherwise stateless. After a restart, the new pod takes partition leases and continues serving. Preferred leaders are reassigned as the broker set changes; the log data itself stays in S3. See [Scaling brokers](/operations/#scaling-brokers).

### How do I set retention?

KafScale uses S3 lifecycle policies for retention. Configure via AWS console, CLI, or Terraform:

```json
{
  "Rules": [{
    "ID": "kafscale-retention",
    "Status": "Enabled",
    "Filter": { "Prefix": "kafscale/" },
    "Expiration": { "Days": 7 }
  }]
}
```

Per-topic retention is possible using prefix-based rules (e.g., `kafscale/default/orders/`).

---

## Operations

### How do I scale KafScale?

Scale the broker StatefulSet through the `KafscaleCluster`. Set `spec.brokers.replicas`, or let the operator-managed HorizontalPodAutoscaler named `<cluster>-broker` add and remove brokers. The HPA minimum is `spec.brokers.replicas` (or 3 if that field is unset or 0) and the maximum is four times that minimum. It targets 70% CPU and 80% memory, so set requests in `spec.brokers.resources`.

Log data stays in S3. Partition leadership does move: the operator publishes a metadata snapshot for the live broker count, and brokers hand leases to the preferred leader. See [Scaling brokers](/operations/#scaling-brokers).

Do not `kubectl scale` the broker StatefulSet or Deployment directly. The operator reconciles replicas from the cluster resource and the HPA.

### What do I need to back up?

Only etcd. Broker state is ephemeral. S3 data is durable by default. etcd stores topic metadata, consumer offsets, and cluster configuration.

When etcd is operator-managed, the operator creates an hourly CronJob (`<cluster>-etcd-snapshot`) that writes snapshots to a dedicated bucket (default `kafscale-etcd-<namespace>-<cluster>`). There is no `spec.etcd.backup` field. See [Backups](/operations/#operator-managed-etcd-default-path) for the schedule, manual snapshot Job, and restore steps.

### Can I restore a topic to an earlier point in time?

Yes, but the supported model is **DR-side restore into a new topic**, not in-place rollback on the primary cluster.

KafScale can recover a topic by copying immutable `.kfs` segment/index pairs up to a chosen timestamp into a fresh target topic:

```bash
kafscale-cli restore \
  --topic orders \
  --target-topic orders-restore-20260513 \
  --to 2026-05-13T14:23:00Z
```

This is segment-granular rather than record-exact PITR. The safer workflow is restore, validate, and then cut consumers over deliberately.

### How do I monitor KafScale?

Brokers expose Prometheus metrics on port 9093. Key metrics:

- `kafscale_s3_health_state`: S3 availability (0/1/2)
- `kafscale_s3_latency_ms_avg`: S3 operation latency
- `kafscale_produce_rps`: Produce throughput
- `kafscale_fetch_rps`: Fetch throughput
- `kafscale_consumer_group_lag`: Consumer lag by group

See [Metrics](/metrics/) for the complete reference.

### Can I run KafScale outside Kubernetes?

The operator and CRDs are Kubernetes-native, but the broker binary can run standalone. You'll need to manage etcd and configuration yourself. See [Development](/development/) for running locally with Docker Compose.

---

## Security

### Does KafScale support TLS?

Yes. The KafScale proxy terminates TLS 1.3 for Kafka clients. Supplying `KAFSCALE_PROXY_TLS_CERT_FILE` and `KAFSCALE_PROXY_TLS_KEY_FILE` enables server TLS. Traffic from the proxy to brokers stays plaintext inside the cluster, so restrict it with the NetworkPolicies the operator creates. You can still terminate TLS at a load balancer, ingress or service mesh instead. See [mTLS authentication](/kafka-authentication/).

### Does KafScale support mTLS?

Yes, at the proxy. Clients present a certificate with a URI SAN, and `principals.json` maps that URI to a principal. The proxy forwards the principal to brokers with a per-connection HMAC proof, and broker ACLs authorize it. There is no CN, subject DN or `client.id` fallback. On a KafscaleCluster, set `spec.security.tlsSecretRef`, `identitySecretRef` and `aclSecretRef`. Certificates, CAs and mappings are reloaded for each new connection. It was merged to main after v1.6.3. See [mTLS authentication](/kafka-authentication/).

### Does KafScale support authentication?

Yes, with mTLS at the proxy. SASL is not supported, so set Kafka clients to `security.protocol=SSL` with a keystore and truststore. Without proxy mTLS, ACLs use `client.id` or network identity, which is not authentication.

### Is data encrypted at rest?

Use S3 server-side encryption (SSE-S3 or SSE-KMS). KafScale writes standard S3 objects, so all S3 encryption options apply.

---

## Troubleshooting

### Brokers won't start

Check etcd connectivity and that the S3 credentials Secret exists. Brokers run as a StatefulSet named `<cluster>-broker` (for the `demo` cluster, `demo-broker`):

```bash
kubectl -n kafscale logs statefulset/demo-broker
kubectl -n kafscale get secret kafscale-s3
```

Common issues: wrong etcd endpoints, expired AWS credentials, or the S3 bucket does not exist.

### High produce latency

Check S3 latency and broker resources:

```bash
kubectl -n kafscale port-forward pod/demo-broker-0 9093:9093
```

In another terminal:

```bash
curl -s http://127.0.0.1:9093/metrics | grep s3_latency
kubectl -n kafscale top pods
```

If S3 latency is high, verify the bucket is in the same region as your cluster.

### Consumer group rebalancing constantly

Check session timeout and network stability:

```bash
kubectl -n kafscale logs statefulset/demo-broker | grep -i rebalance
```

Increase `session.timeout.ms` on clients if pods are slow to respond to heartbeats.

---

## Contributing

### How can I contribute?

See [Contributing](/contributing/). We welcome bug reports, feature requests, documentation improvements, and code contributions.

### Where do I report bugs?

Open an issue on [GitHub](https://github.com/KafScale/platform/issues). Include KafScale version, Kubernetes version, and relevant logs.

### Is there a community?

Join the discussion on [GitHub Discussions](https://github.com/KafScale/platform/discussions) or the `#kafscale` channel on the Kubernetes Slack.
