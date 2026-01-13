---
layout: doc
title: FAQ
description: Common questions about KafScale, Kafka compatibility, and S3 durability.
permalink: /faq/
nav_title: FAQ
nav_order: 10
---

# FAQ

## General

### How does KafScale compare to WarpStream, Redpanda, or AutoMQ?

See [Comparison](/comparison/) for a detailed side-by-side analysis covering architecture, latency, licensing, and cost.

The short version: KafScale is the only S3-native, stateless Kafka-compatible platform under the Apache 2.0 license. WarpStream is now Confluent-owned (proprietary), AutoMQ uses BSL licensing, and Redpanda requires local disks.

### Why would I use KafScale instead of Apache Kafka?

KafScale trades latency for operational simplicity and storage-native processing. If your workload can tolerate hundreds of milliseconds of latency, KafScale eliminates stateful brokers, partition rebalancing, and disk capacity planning.

**For AI agent infrastructure**: KafScale's architecture aligns with what agentic systems actually need. AI agents reasoning over business context require completeness and replay capability—not sub-millisecond latency. The immutable log in S3 becomes the system of record that agents query, replay, and reason over. Processors convert that log to tables without competing with streaming workloads for broker resources.

Traditional stream processing optimizes for latency. Milliseconds matter for fraud detection or trading. But AI agents have different requirements: they need to understand what happened, in what order, and why the current state exists. Event sourcing research from the Apache Flink community (FLIP-531) and platforms like Akka confirms this pattern—agentic systems need reproducible state at any point in time.

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
  <text x="30" y="192" class="diagram-text">✓ Latency tolerance: 100-500ms</text>
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

KafScale stores data in `.kfs` segments containing native Kafka V2 record batches—the same binary format Kafka uses internally. This is a deliberate choice:

**Format stability**: Kafka's on-disk format is one of the most stable interfaces in data infrastructure. In 15+ years, there have been exactly three message format versions:

| Version | Introduced | Status |
|---------|-----------|--------|
| V0 | Original (2011) | Removed in Kafka 4.0 |
| V1 | Kafka 0.10.0 (2016) | Removed in Kafka 4.0 |
| V2 | Kafka 0.11.0 (June 2017) | Current standard |

V2 has been the only supported format for 8+ years. The entire Kafka ecosystem—Confluent, Redpanda, every client library, Flink, Spark, Debezium, MirrorMaker—depends on this stability. Changing it would break everything.

**If Kafka ever changes**: KafScale is fully open source under Apache 2.0. Any format updates can be implemented immediately by the community. Contrast this with proprietary alternatives where you'd wait for a vendor to prioritize the update.

**No abstraction tax**: Using native format means zero conversion overhead. Producers write Kafka records; we store Kafka records; consumers read Kafka records.

### What about coupling processors to the storage format?

[Processors](/processors/) read directly from S3, bypassing brokers entirely. This means they understand the `.kfs` segment format and coordinate via etcd.

This is intentional coupling to a stable interface, not a liability:

1. **The format won't change** — Kafka V2 record batches are a de facto standard
2. **Read-replica brokers would have the same coupling** — they'd also need to parse segments and query etcd
3. **The coupling is explicit and documented** — not hidden inside a proprietary broker
4. **Open format means open tooling** — anyone can build processors, analyzers, or integrations

The tradeoff: if KafScale's internal segment layout evolves, processors need updates. In practice, we version the segment format and maintain backward compatibility.

### Does KafScale work with clouds other than AWS?

Yes. KafScale works with any S3-compatible storage backend. See [Storage Compatibility](/docs/deployment/compatibility/) for configuration examples.

| Provider | Compatibility | Notes |
|----------|--------------|-------|
| AWS S3 | ✅ Native | Full support including IRSA |
| DigitalOcean Spaces | ✅ Native | Drop-in replacement |
| Cloudflare R2 | ✅ Native | Zero egress fees |
| Backblaze B2 | ✅ Native | S3-compatible API |
| MinIO | ✅ Native | Self-hosted, any infrastructure |
| Google Cloud Storage | ⚠️ Interop | Requires HMAC keys |
| Azure Blob Storage | ❌ Proxy | Requires MinIO Gateway |

---

## Latency and Performance

### What latency should I expect?

KafScale prioritizes durability and operational simplicity over sub-10ms latency. Typical latencies:

| Operation | p50 | p99 | Notes |
|-----------|-----|-----|-------|
| Produce | 200-300ms | 400-500ms | Depends on flush interval and S3 region |
| Fetch (cache hit) | 1-5ms | 10ms | Hot segment cache |
| Fetch (cache miss) | 50-100ms | 150ms | S3 GetObject |
| Consumer group join | 100-200ms | 500ms | etcd coordination |

### Can I reduce latency?

Several factors affect latency:

1. **S3 region proximity**: Deploy brokers in the same region as your S3 bucket
2. **Flush interval**: Lower `KAFSCALE_FLUSH_INTERVAL_MS` reduces produce latency but increases S3 requests
3. **Cache size**: Larger `KAFSCALE_CACHE_SIZE` improves fetch hit rates
4. **Segment size**: Smaller `KAFSCALE_SEGMENT_BYTES` flushes more frequently

The fundamental tradeoff is S3 round-trip time. If you need sub-50ms latency, KafScale is not the right choice.

### How does KafScale handle backpressure?

When S3 latency exceeds thresholds, brokers enter `DEGRADED` state. If S3 becomes unavailable, brokers enter `UNAVAILABLE` state and reject produce requests while continuing to serve cached fetch requests. Clients should implement retry logic with exponential backoff.

---

## Kafka Compatibility

### Can I use existing Kafka clients?

Yes. KafScale implements the Kafka wire protocol for core APIs. Any client that speaks Kafka protocol works without modification.

Tested clients include kafka-python, franz-go, librdkafka, Sarama, and the official Java client.

### Which Kafka APIs are supported?

KafScale supports 21 Kafka APIs covering produce, fetch, metadata, and consumer group operations. See [Protocol](/protocol/) for the complete compatibility matrix.

Not supported: transactions (exactly-once semantics), compacted topics, and the admin API for ACLs.

### Can I migrate from Kafka to KafScale?

Yes, but it requires replaying data. KafScale uses a different storage layout (S3 segments) than Kafka (local log files), though the record format is identical. Migration options:

1. **Dual-write**: Produce to both systems during transition
2. **MirrorMaker**: Use Kafka MirrorMaker to replicate topics to KafScale
3. **Consumer replay**: Consume from Kafka and produce to KafScale

### Do consumer groups work?

Yes. KafScale implements the full consumer group protocol including JoinGroup, SyncGroup, Heartbeat, LeaveGroup, and OffsetCommit/Fetch. Consumer offsets are stored in etcd.

---

## Processors

### What are processors?

Processors are components that read directly from S3, bypassing brokers entirely. They enable analytical workloads without adding load to your streaming infrastructure. See [Processors](/processors/) for details.

Available processors:

- **[Iceberg Processor](/processors/iceberg/)** — Continuous export to Apache Iceberg tables
- **[Parquet Processor](/processors/parquet/)** — Direct Parquet file generation
- **[S3 Sink Processor](/processors/s3-sink/)** — Raw segment archival

### Why bypass brokers for analytics?

Traditional Kafka Connect runs through brokers, competing with real-time consumers for broker resources. KafScale processors read segments directly from S3:

- **No broker contention**: Analytical queries don't impact streaming latency
- **Horizontal scale**: Add processors without broker capacity planning
- **Cost efficiency**: S3 reads are cheap; broker CPU is expensive

This architecture is ideal for AI/ML pipelines where you need to replay large volumes of historical data without impacting production consumers.

### Can I build custom processors?

Yes. The `.kfs` segment format is documented, and processors coordinate via etcd for offset tracking. See [Building Processors](/processors/building/) for the SDK and examples.

---

## Storage and Durability

### How durable is my data?

S3 provides 99.999999999% (11 nines) durability. Once data is acknowledged to the producer, it exists in S3 with the same durability guarantees as any S3 object.

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

Brokers monitor S3 health continuously. Based on error rates and latency:

| State | Condition | Behavior |
|-------|-----------|----------|
| Healthy | Error rate < 1%, latency < 500ms | Normal operation |
| Degraded | Error rate 1-5% or latency 500-2000ms | Accepts requests with warnings |
| Unavailable | Error rate > 5% or latency > 2000ms | Rejects produces, serves cached fetches |

Monitor `kafscale_s3_health_state` (0=healthy, 1=degraded, 2=unavailable) and implement client-side retries.

### What happens if a broker crashes?

Nothing is lost. Brokers are stateless. All data lives in S3, all metadata lives in etcd. When a broker restarts (or a new pod schedules), it reads state from etcd and resumes serving requests. No partition rebalancing required.

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

Horizontally. Add more broker replicas. Since brokers are stateless and S3 is the source of truth, there's no partition rebalancing or data migration. New brokers immediately start serving requests.

```bash
kubectl scale deployment demo-broker --replicas=5
```

Or use HPA for automatic scaling based on CPU or custom metrics.

### What do I need to back up?

Only etcd. Broker state is ephemeral. S3 data is durable by default. etcd stores topic metadata, consumer offsets, and cluster configuration.

The operator can automate etcd snapshots to S3:

```yaml
spec:
  etcd:
    backup:
      enabled: true
      bucket: kafscale-backups
      interval: 1h
```

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

Yes. Configure TLS for client connections and inter-broker communication via the CRD:

```yaml
spec:
  tls:
    enabled: true
    secretRef: kafscale-tls
```

The secret should contain `tls.crt` and `tls.key`.

### Does KafScale support authentication?

SASL/PLAIN and SASL/SCRAM are on the roadmap. Currently, network-level security (Kubernetes NetworkPolicies, service mesh) is recommended.

### Is data encrypted at rest?

Use S3 server-side encryption (SSE-S3 or SSE-KMS). KafScale writes standard S3 objects, so all S3 encryption options apply.

---

## Troubleshooting

### Brokers won't start

Check etcd connectivity and S3 credentials:

```bash
kubectl logs -n kafscale deployment/demo-broker
kubectl get secret kafscale-s3 -o yaml
```

Common issues: wrong etcd endpoints, expired AWS credentials, S3 bucket doesn't exist.

### High produce latency

Check S3 latency and broker resources:

```bash
kubectl exec -n kafscale deployment/demo-broker -- curl localhost:9093/metrics | grep s3_latency
kubectl top pods -n kafscale
```

If S3 latency is high, verify the bucket is in the same region as your cluster.

### Consumer group rebalancing constantly

Check session timeout and network stability:

```bash
kubectl logs -n kafscale deployment/demo-broker | grep -i rebalance
```

Increase `session.timeout.ms` on clients if pods are slow to respond to heartbeats.

---

## Contributing

### How can I contribute?

See [CONTRIBUTING.md](https://github.com/KafScale/platform/blob/main/CONTRIBUTING.md) in the repository. We welcome bug reports, feature requests, documentation improvements, and code contributions.

### Where do I report bugs?

Open an issue on [GitHub](https://github.com/KafScale/platform/issues). Include KafScale version, Kubernetes version, and relevant logs.

### Is there a community?

Join the discussion on [GitHub Discussions](https://github.com/KafScale/platform/discussions) or the `#kafscale` channel on the Kubernetes Slack.
