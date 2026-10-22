---
layout: doc
title: "KafScale Latency, Producer Batching and Trade-offs"
description: How KafScale produce and fetch latency works on S3, which workloads it fits, the batching settings that keep S3 cost low, and the deliberate trade-offs.
permalink: /latency/
nav_title: Latency and batching
nav_order: 5
schema:
  "@type": FAQPage
  mainEntity:
    - "@type": Question
      name: What problem does KafScale solve?
      acceptedAnswer:
        "@type": Answer
        text: "KafScale runs the Kafka protocol on stateless brokers and keeps all data in S3-compatible object storage. That removes broker disks, partition rebalancing and replica management from your operations work, and it gives you up to 90% lower storage cost than three-way replicated broker disks. It is open source under Apache 2.0, keeps metadata in etcd inside your own Kubernetes cluster, and stores data in the open, documented .kfs segment format, so processors can read it straight from S3."
    - "@type": Question
      name: What is KafScale latency?
      acceptedAnswer:
        "@type": Answer
        text: "We have not published a measured p50 or p99 latency benchmark yet, so we describe latency by its mechanism. By default the broker acknowledges a produce request after the batch is written to S3 and the segment metadata is committed to etcd. Produce latency is therefore about one S3 PUT round trip plus one etcd write. Recent data is served from the in-memory segment cache on the broker. This fits logs, telemetry, analytics, ETL, backup and AI agent workloads. It is not built for workloads that need sub-10ms produce latency."
    - "@type": Question
      name: Does KafScale support transactions?
      acceptedAnswer:
        "@type": Answer
        text: "No. Transactions, idempotent producers and compacted topics are deliberate trade-offs of the S3 design. Segments in object storage are immutable and brokers keep no state, so there is no producer sequence state, no transaction coordinator and no segment rewriting. Set enable.idempotence=false on producers. For CDC, key-value state topics or exactly-once pipelines, keep those topics on a regular Kafka cluster and run KafScale beside it for high-volume logs, telemetry and analytics."
---

# KafScale latency, batching and trade-offs

## What problem does KafScale solve?

KafScale runs the Kafka protocol on stateless brokers and keeps all data in S3-compatible object storage. That removes broker disks, partition rebalancing and replica management from your operations work, and it gives you up to 90% lower storage cost than three-way replicated broker disks. It is open source under Apache 2.0, keeps metadata in etcd inside your own Kubernetes cluster, and stores data in the open, documented [.kfs segment format](/storage-format/), so processors can read it straight from S3.

The storage figure comes from a [simple calculation with AWS list prices](/comparison/#storage-cost). The price for all of this is latency. A Kafka broker writes to a local disk. A KafScale broker writes to S3. This page explains what that means in practice, how to tune it, and what KafScale leaves out on purpose.

## What is KafScale latency?

We have not published a measured p50 or p99 latency benchmark yet, so we describe latency by its mechanism. By default the broker acknowledges a produce request after the batch is written to S3 and the segment metadata is committed to etcd. Produce latency is therefore about one S3 PUT round trip plus one etcd write. Recent data is served from the in-memory segment cache on the broker. This fits logs, telemetry, analytics, ETL, backup and AI agent workloads. It is not built for workloads that need sub-10ms produce latency.

You can measure it on your own setup. The broker exports `kafscale_produce_latency_ms` (histogram) and `kafscale_s3_latency_ms_avg` (see [Metrics](/metrics/)), and the [benchmark scenarios](/benchmarks/) show how to run repeatable tests.

### The produce path

With the defaults (`KAFSCALE_PRODUCE_SYNC_FLUSH=true` and producer `acks=1` or `acks=all`), every produce request goes through these steps before the client gets its ack:

1. The broker appends the record batch to the partition's in-memory buffer.
2. The buffer is sealed into a segment and a sparse index.
3. The segment and the index are uploaded to S3 in parallel.
4. The new segment metadata is committed to etcd.
5. The broker sends the ack.

So the ack waits for one S3 PUT round trip (the two uploads run at the same time) and one etcd write. Your S3 region, storage class and network decide most of that time. That is why the [supported storage](/supported/) page asks you to keep the bucket in the same region as the cluster.

Two other modes change this:

<div class="format-card">
  <div class="format-row header">
    <span>Mode</span>
    <span>When the producer gets its ack</span>
    <span>Durability</span>
  </div>
  <div class="format-row">
    <span><code>KAFSCALE_PRODUCE_SYNC_FLUSH=true</code> (default), <code>acks=1</code> or <code>all</code></span>
    <span>After the segment is in S3 and committed to etcd</span>
    <span>Acked data is in S3</span>
  </div>
  <div class="format-row">
    <span><code>KAFSCALE_PRODUCE_SYNC_FLUSH=false</code></span>
    <span>After the batch is in the broker buffer</span>
    <span>The buffer flushes at <code>KAFSCALE_SEGMENT_BYTES</code> or <code>KAFSCALE_FLUSH_INTERVAL_MS</code>. A broker crash loses the unflushed tail.</span>
  </div>
  <div class="format-row">
    <span><code>acks=0</code></span>
    <span>No ack</span>
    <span>Same background flush as above</span>
  </div>
</div>

### The fetch path

- **Recent data.** When a segment flushes, the broker keeps it in the hot segment cache (`KAFSCALE_CACHE_BYTES`, default 1 GB), so consumers that keep up read from memory.
- **Older data.** A cache miss is an S3 GET. The broker prefetches the next `KAFSCALE_READAHEAD_SEGMENTS` segments (default 2), so sequential consumers mostly hit memory after the first read.
- **Analytics and AI agents.** Processors can skip the broker and read `.kfs` segments from S3 directly. That keeps replays and backfills off the streaming path.

### What we have measured

The repo has one local baseline run, recorded on 2026-01-03 on a laptop with kind and the demo stack, 256 byte messages, using `kcat` (source: [Benchmarks](/benchmarks/), `docs/benchmarks` in the [platform repo](https://github.com/KafScale/platform/tree/main/docs/benchmarks)). It measured throughput, not latency percentiles:

| Scenario | Result |
|----------|--------|
| Produce and consume through the broker, 2000 messages | ~3.6 s wall time, ~553 msg/s end to end |
| Produce a backlog, 2000 messages | ~0.49 s, ~4115 msg/s |
| Consume that backlog from S3 | ~1.79 s, ~1117 msg/s |
| Consume after a broker restart (cold cache) | ~1.44 s, ~1388 msg/s |

Treat these as a smoke test of the S3 path on a laptop, not as production numbers. The [k6suite](https://github.com/KafScale/k6suite) repo checks correctness (no message loss, consumer groups, restarts, ACLs) and does not publish latency figures.

### S3 health thresholds

The broker watches S3 latency and error rate and changes state when S3 gets slow. These are configured limits, not measurements:

| Setting | Default | Effect |
|---------|---------|--------|
| `KAFSCALE_S3_LATENCY_WARN_MS` | `500` | Broker reports `DEGRADED` above this |
| `KAFSCALE_S3_LATENCY_CRIT_MS` | `2000` | Broker reports `UNAVAILABLE`, rejects produce, keeps serving cached fetches |

See [S3 health](/s3-health/) for profiles for cross-region and cost-optimized buckets.

## When latency matters and when it doesn't

Most Kafka topics are durable pipes. The data has to arrive, in order, and not get lost. A few hundred milliseconds rarely changes the result.

| Workload | Fit | Why |
|----------|-----|-----|
| Log aggregation | Good fit | Logs are read by search and alerting systems that batch anyway |
| Telemetry and IoT metrics | Good fit | High volume, append only, cost per GB matters more than milliseconds |
| Analytics and lakehouse feeds | Good fit | Processors read S3 directly, for example the [Iceberg processor](/processors/iceberg/) |
| AI agent context and replay | Good fit | Agents need the full ordered history, not sub-millisecond reads |
| ETL and CDC fan-out (append only) | Good fit | Downstream jobs run in batches or micro-batches |
| Backup and archive topics | Good fit | Retention is a bucket lifecycle rule |
| Order matching, trading, ad bidding | Not a fit | Needs sub-10ms produce latency. Use Apache Kafka or another disk-based platform |
| Request and reply over Kafka | Usually not a fit | Every round trip pays the S3 write |

## Producer batching settings

In the default mode every produce request becomes one segment upload plus one index upload per partition. So the number of produce requests decides your S3 PUT count. Batching on the producer is the main lever for both cost and throughput.

A quick calculation with the S3 Standard request price used on the [Operations](/operations/#s3-cost-estimation-example) page ($0.005 per 1,000 PUTs): one producer sending 100 small requests per second to one partition causes 200 PUTs per second. That is 17.28 million PUTs a day, or about $86 a day in request fees. The same data sent in 4 MB batches needs a tiny fraction of that.

Start with these producer settings. The `linger.ms`, `batch.size` and compression values are the high-throughput settings from the [developer guide](https://github.com/KafScale/platform/tree/main/examples/101_kafscale-dev-guide) in the repo:

```properties
enable.idempotence=false
acks=all
linger.ms=100
batch.size=65536
compression.type=snappy
```

- **`linger.ms`** waits up to that many milliseconds to fill a batch. It adds at most that much latency and cuts the number of requests. Raise it for logs and telemetry, lower it when you care about the last 100 ms.
- **`batch.size`** caps the batch per partition. Larger batches mean fewer, bigger S3 objects.
- **`compression.type`** shrinks what you store and upload. KafScale stores the client's record batches as they arrive, so compression on the producer is compression in S3.
- **`enable.idempotence=false`** is required. See the trade-offs below.

Broker-side settings (see [Runtime settings](/configuration/)):

| Setting | Default | What it controls |
|---------|---------|------------------|
| `KAFSCALE_PRODUCE_SYNC_FLUSH` | `true` | Flush to S3 before acking each produce with `acks != 0` |
| `KAFSCALE_SEGMENT_BYTES` | `4194304` (4 MB) | Buffer size that triggers a flush |
| `KAFSCALE_FLUSH_INTERVAL_MS` | `500` | Longest time data waits in the buffer |
| `KAFSCALE_CACHE_BYTES` | `1073741824` (1 GB) | Hot segment cache for consumers |
| `KAFSCALE_READAHEAD_SEGMENTS` | `2` | Segments prefetched for sequential reads |

On the `KafscaleCluster` resource the same buffer settings are `spec.config.segmentBytes` and `spec.config.flushIntervalMs`.

The two profiles documented in the repo:

| Profile | Settings | Result |
|---------|----------|--------|
| Durability first (default) | `SYNC_FLUSH=true`, `FLUSH_INTERVAL_MS=500`, `SEGMENT_BYTES=4MB` | Acked data is in S3. Cost depends on producer batching |
| Cost first | `SYNC_FLUSH=false`, `FLUSH_INTERVAL_MS=2000`, `SEGMENT_BYTES=32MB` | Far fewer PUTs, lower produce latency, larger loss window if a broker crashes |

## Trade-offs of the S3 design

KafScale does not support log compaction, transactions or idempotent producers. These are deliberate choices, not missing pieces. They follow from two decisions: segments in S3 are immutable, and brokers keep no state.

### Does KafScale support transactions?

No. Transactions, idempotent producers and compacted topics are deliberate trade-offs of the S3 design. Segments in object storage are immutable and brokers keep no state, so there is no producer sequence state, no transaction coordinator and no segment rewriting. Set `enable.idempotence=false` on producers. For CDC, key-value state topics or exactly-once pipelines, keep those topics on a regular Kafka cluster and run KafScale beside it for high-volume logs, telemetry and analytics.

### Why there are no compacted topics

Compaction rewrites old segments to keep only the latest value per key. KafScale never rewrites a segment once it is in S3, so there is nothing to compact. Retention is a bucket lifecycle rule.

This rules out topics that act as a key-value state store, such as CDC changelogs that rely on compaction or Kafka Streams state topics.

### Why there are no transactions or idempotent producers

Exactly-once delivery needs a transaction coordinator and per-producer sequence numbers that the brokers remember. KafScale brokers are stateless, so they keep neither, and the broker does not implement `InitProducerId`. Set `enable.idempotence=false` and do not set `transactional.id`.

This rules out exactly-once pipelines across topics, for example read-process-write jobs that commit offsets and output in one transaction.

### The cost of small writes

Every flush is an S3 object, and S3 charges per request. Many tiny produce requests mean many tiny objects and a higher request bill. Batch on the producer as shown above, or switch to the cost-first profile.

### What to do instead

- **Run both.** Keep compacted and transactional topics on a regular Kafka cluster, and run KafScale beside it for the high-volume logs, telemetry and analytics topics. Standard Kafka clients talk to both. [kaf-mirror](https://github.com/scalytics/kaf-mirror) can copy topics between them.
- **Dedupe on the consumer.** Give each record a stable key or ID and make the consumer idempotent, for example an upsert by key in the target database. The [Iceberg processor](/processors/iceberg/#record-ids-and-idempotency) adds a `record_id` of the form `<topic>:<partition>:<offset>` for exactly this.
- **Keep latest-value state outside the log.** If you need the current value per key, materialize the topic into a table or a key-value store and query that.

## Commercial support

Commercial support and an enterprise version of KafScale are available from Scalytics as [Lascaris](https://www.scalytics.io/lascaris).

## Related pages

- [Comparison](/comparison/) with other Kafka-compatible platforms
- [Migrate from WarpStream](/migrate-from-warpstream/)
- [Storage format](/storage-format/) and [flush triggers](/storage-format/#flush-triggers)
- [Runtime settings](/configuration/) and [Operations](/operations/)
- [FAQ](/faq/)
