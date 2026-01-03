# What is KafScale?

KafScale is a **Kafka-protocol compatible streaming platform** that separates compute from storage. Unlike traditional Kafka, KafScale uses **stateless brokers** and stores all data in **S3-compatible object storage**, making it simpler to operate and more cost-effective for many use cases.

## Key Characteristics

- **Kafka-Compatible**: Uses the standard Kafka wire protocol, so your existing Kafka clients work without modification
- **Stateless Brokers**: Brokers are ephemeral and can be scaled up or down without data movement
- **S3-Backed Storage**: All log segments are stored in S3 (or S3-compatible storage like MinIO)
- **etcd Metadata**: Topic configuration and consumer offsets are stored in etcd
- **Cloud-Native**: Designed for Kubernetes, but can run anywhere with Docker

## How KafScale Differs from Traditional Kafka

| Aspect | Traditional Kafka | KafScale |
|--------|------------------|----------|
| **Storage** | Local broker disks | S3 object storage |
| **Broker State** | Stateful (stores data) | Stateless (data in S3) |
| **Scaling** | Complex rebalancing | Simple pod scaling |
| **Durability** | Replication across brokers | S3 durability (11 9's) |
| **Recovery** | Rebuild from replicas | Read from S3 |
| **Cost Model** | Provision for peak + replicas | Pay for actual storage used |

## When to Use KafScale

### ✅ Good Use Cases

- **Development and Testing**: Quick setup without complex infrastructure
- **Cost-Sensitive Workloads**: Reduce storage costs by using S3 instead of provisioned disks
- **Cloud-Native Deployments**: Leverage Kubernetes for scaling and orchestration
- **Replay-Heavy Workloads**: S3 storage makes long-term retention affordable
- **Event Sourcing**: Durable, immutable event logs with cost-effective storage

### ❌ Not Suitable For

- **Transactional Workloads**: KafScale does not support exactly-once semantics or transactions
- **Log Compaction**: Compacted topics are not supported
- **Ultra-Low Latency**: S3 storage adds latency compared to local disks (typically 10-50ms additional)
- **High-Throughput Single Partition**: Traditional Kafka may be faster for very high throughput on a single partition

## Architecture Overview

Here's how KafScale works at a high level:

```
┌─────────────────────┐
│   Spring Boot App   │
│  (Kafka Producer/   │
│     Consumer)       │
└──────────┬──────────┘
           │ Kafka Protocol (port 9092)
           ▼
┌─────────────────────┐
│  KafScale Broker    │
│   (Stateless)       │
└────┬───────────┬────┘
     │           │
     │           │
     ▼           ▼
┌─────────┐  ┌─────────┐
│  etcd   │  │   S3    │
│ Metadata│  │  Data   │
└─────────┘  └─────────┘
```

### Components

1. **Your Application**: Uses standard Kafka client libraries (no changes needed!)
2. **KafScale Broker**: Handles Kafka protocol requests, buffers data in memory, writes to S3
3. **etcd**: Stores topic metadata, partition assignments, and consumer offsets
4. **S3 Storage**: Stores immutable log segments (MinIO for local development)

## What Makes KafScale Different

Traditional Kafka was designed when durable storage meant local disks. Brokers had to own both compute and data, requiring complex replication and rebalancing.

**Cloud object storage changes this entirely.**

With S3 providing durability, availability, and cost efficiency, brokers no longer need to be stateful. KafScale embraces this by making brokers simple protocol endpoints that read and write to S3.

This means:
- **No replication overhead**: S3 handles durability
- **No rebalancing**: Brokers are interchangeable
- **No disk management**: Storage is elastic and managed
- **Simpler operations**: Restart brokers without data movement

## Compatibility

KafScale implements the core Kafka APIs:

- ✅ **Produce** (API Key 0)
- ✅ **Fetch** (API Key 1)
- ✅ **Consumer Groups** (JoinGroup, SyncGroup, Heartbeat, etc.)
- ✅ **Offset Management** (OffsetCommit, OffsetFetch)
- ✅ **Topic Management** (CreateTopics, DeleteTopics)
- ✅ **Metadata** (topic/broker discovery)
- ✅ **Proto-compat**: Compatible with Kafka clients 2.x and 3.x (recommended: 3.4.x or older for best compatibility)

### Client Compatibility Notes

KafScale is compatible with standard Kafka clients, but stricter schema validation in newer clients (3.5+) may cause issues with certain requests (e.g., `ProduceResponse` schema mismatches).

**Recommendations:**
- **Java/Spring Boot**: Use Spring Boot 3.1.x (Kafka client 3.4.x) or ensure your client version is < 3.5.0 if you encounter protocol errors.
- **Idempotence**: Always set `enable.idempotence=false` in your producers. KafScale does not support the `InitProducerId` API or sequence number validation handling required for idempotent producers.
- **Transactions**: Config `isolation.level=read_uncommitted` (default) as transactions are not supported.

**Not supported** (by design):
- ❌ Transactions and exactly-once semantics
- ❌ Log compaction
- ❌ Kafka Streams embedded processing
- ❌ Flexible versions in some RPCs (may cause `recordErrors` serialization issues in newer clients)

For stream processing, use external engines like [Apache Flink](https://flink.apache.org) or [Apache Wayang](https://wayang.apache.org).

## Ready to Get Started?

Now that you understand what KafScale is and when to use it, let's get it running on your local machine!

**Next**: [Quick Start with Docker](02-quick-start.md) →
