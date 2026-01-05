---
layout: doc
title: Roadmap
description: Completed milestones, current work, and what is planned next.
permalink: /roadmap/
nav_title: Roadmap
nav_order: 8
---

# Roadmap

KafScale follows a milestone-based release process. This page summarizes what's shipped, what's in progress, and what's planned.

---

## Released (v1.0)

### Core protocol

- Kafka wire protocol parsing (17 APIs advertised)
- Produce path (v0-9) with S3-backed durability
- Fetch path (v11-13) with LRU caching and read-ahead
- Metadata API (v0-12) with topic/partition discovery
- ListOffsets (v0)

### Consumer groups

- FindCoordinator (v3)
- JoinGroup, SyncGroup, Heartbeat, LeaveGroup (v4)
- OffsetCommit (v3) and OffsetFetch (v5) with etcd persistence
- DescribeGroups (v5) and ListGroups (v5) for ops visibility
- OffsetForLeaderEpoch (v3) for consumer recovery

### Admin APIs

- CreateTopics (v0), DeleteTopics (v0)
- CreatePartitions (v0-3)
- DescribeConfigs (v4), AlterConfigs (v1)
- DeleteGroups (v0-2)

### Storage

- S3 segment format with sparse indexes
- Snappy, LZ4, ZSTD compression
- etcd-based topic and partition management
- Lifecycle-based retention via S3 policies

### Operations

- Kubernetes operator with CRDs (KafScaleCluster, KafScaleTopic)
- Managed etcd with automated snapshots to S3
- Prometheus metrics and Grafana dashboards
- Structured JSON logging
- S3 health state monitoring

### Testing

- End-to-end broker durability tests
- Multi-segment restart recovery tests
- Operator resilience tests
- Admin API e2e coverage

---

## Planned

| Feature | Target | Description |
|---------|--------|-------------|
| TLS enabled by default | v1.5 | Production Helm templates with TLS out of the box |
| SASL groundwork | v1.5 | Internal scaffolding for authentication |
| SASL/PLAIN authentication | v2.0 | Username/password auth for clients |
| SASL/SCRAM authentication | v2.0 | Secure credential storage |
| Topic-level ACLs | v2.0 | Read/write permissions per topic |
| Console improvements | v2.1 | Topic browser, consumer lag dashboard |
| Multi-cluster federation | v2.2 | Cross-cluster topic mirroring |
| Audit logging | v2.2 | Who did what, when |

---

## Explicitly not planned

Some features are intentionally out of scope for KafScale. These are architectural decisions, not missing features.

| Feature | Reason |
|---------|--------|
| Transactions (EOS) | Requires coordination complexity incompatible with stateless brokers |
| Compacted topics | Requires stateful compaction process; use a database instead |
| Kafka replication protocols | LeaderAndIsr, UpdateMetadata, etc. not needed with S3 as source of truth |
| Embedded stream processing | Out of scope; use Flink, Wayang, Spark, or other external engines |
| Tiered storage | S3 is already the primary tier; no local disk to tier from |
| Sub-10ms latency | Fundamental S3 round-trip constraint; use Kafka or Redpanda |
| KRaft / ZooKeeper | etcd is the metadata store; no Kafka controller needed |

If your use case requires these features, KafScale may not be the right fit. See [Comparison](/comparison/) for alternatives.

---

## How to request features

1. Check [GitHub Issues](https://github.com/novatechflow/kafscale/issues) for existing requests
2. Open a new issue with:
   - Your use case
   - Expected throughput and latency requirements
   - Operational constraints (cloud provider, compliance, etc.)
3. Join the discussion in [GitHub Discussions](https://github.com/novatechflow/kafscale/discussions)

We prioritize features based on community demand and alignment with KafScale's core mission: simple, stateless, S3-native Kafka compatibility.

---

## Release history

See [GitHub Releases](https://github.com/novatechflow/kafscale/releases) for full changelogs.
