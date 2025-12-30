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

# Kafscale Technical Specification

This document defines the system-level specifications for Kafscale. Narrative guidance lives in `docs/`.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Kubernetes Cluster                             │
│                                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                          │
│  │   Broker    │  │   Broker    │  │   Broker    │   Stateless pods         │
│  │   Pod 0     │  │   Pod 1     │  │   Pod 2     │   (HPA scaled)           │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                          │
│         │                │                │                                 │
│         └────────────────┼────────────────┘                                 │
│                          │                                                  │
│                          ▼                                                  │
│                   ┌─────────────┐                                           │
│                   │    etcd     │  Metadata store (topic config,            │
│                   │  (3 nodes)  │  consumer offsets, partition assignments) │
│                   └─────────────┘                                           │
│                                                                             │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
                            ┌─────────────┐
                            │     S3      │  Segment storage
                            │   Bucket    │  (source of truth)
                            └─────────────┘
```

---

## Data Model

### Topics and Partitions

```
Topic: "orders"
├── Partition 0
│   ├── segment-00000000000000000000.kfs
│   ├── segment-00000000000000050000.kfs
│   └── segment-00000000000000100000.kfs
├── Partition 1
│   ├── segment-00000000000000000000.kfs
│   └── segment-00000000000000050000.kfs
└── Partition 2
    └── segment-00000000000000000000.kfs
```

### S3 Key Structure

```
s3://{bucket}/{namespace}/{topic}/{partition}/segment-{base_offset}.kfs
s3://{bucket}/{namespace}/{topic}/{partition}/segment-{base_offset}.index
```

Example:
```
s3://kafscale-data/production/orders/0/segment-00000000000000000000.kfs
s3://kafscale-data/production/orders/0/segment-00000000000000000000.index
```

### Segment File Format

Each segment is a self-contained file with messages and metadata:

```
┌────────────────────────────────────────────────────────────────┐
│ Segment Header (32 bytes)                                      │
├────────────────────────────────────────────────────────────────┤
│ Magic Number        │ 4 bytes  │ 0x4B414653 ("KAFS")           │
│ Version             │ 2 bytes  │ Format version (1)            │
│ Flags               │ 2 bytes  │ Compression, etc.             │
│ Base Offset         │ 8 bytes  │ First offset in segment       │
│ Message Count       │ 4 bytes  │ Number of messages            │
│ Created Timestamp   │ 8 bytes  │ Unix millis                   │
│ Reserved            │ 4 bytes  │ Future use                    │
├────────────────────────────────────────────────────────────────┤
│ Message Batch 1                                                │
├────────────────────────────────────────────────────────────────┤
│ Message Batch 2                                                │
├────────────────────────────────────────────────────────────────┤
│ ...                                                            │
├────────────────────────────────────────────────────────────────┤
│ Segment Footer (16 bytes)                                      │
├────────────────────────────────────────────────────────────────┤
│ CRC32               │ 4 bytes  │ Checksum of all batches       │
│ Last Offset         │ 8 bytes  │ Last offset in segment        │
│ Footer Magic        │ 4 bytes  │ 0x454E4421 ("END!")           │
└────────────────────────────────────────────────────────────────┘
```

### Message Batch Format

Messages are grouped into batches for efficiency:

```
┌────────────────────────────────────────────────────────────────┐
│ Batch Header (49 bytes)                                        │
├────────────────────────────────────────────────────────────────┤
│ Base Offset         │ 8 bytes  │ First offset in batch         │
│ Batch Length        │ 4 bytes  │ Total bytes in batch          │
│ Partition Leader    │ 4 bytes  │ Epoch of leader               │
│ Magic               │ 1 byte   │ 2 (Kafka compat)              │
│ CRC32               │ 4 bytes  │ Checksum of batch             │
│ Attributes          │ 2 bytes  │ Compression, timestamp type   │
│ Last Offset Delta   │ 4 bytes  │ Offset of last msg - base     │
│ First Timestamp     │ 8 bytes  │ Timestamp of first message    │
│ Max Timestamp       │ 8 bytes  │ Max timestamp in batch        │
│ Producer ID         │ 8 bytes  │ -1 (no idempotence)           │
│ Producer Epoch      │ 2 bytes  │ -1                            │
│ Base Sequence       │ 4 bytes  │ -1                            │
│ Record Count        │ 4 bytes  │ Number of records in batch    │
├────────────────────────────────────────────────────────────────┤
│ Record 1                                                       │
│ Record 2                                                       │
│ ...                                                            │
└────────────────────────────────────────────────────────────────┘
```

### Individual Record Format

```
┌────────────────────────────────────────────────────────────────┐
│ Length              │ varint   │ Total record size             │
│ Attributes          │ 1 byte   │ Unused (0)                    │
│ Timestamp Delta     │ varint   │ Delta from batch timestamp    │
│ Offset Delta        │ varint   │ Delta from batch base offset  │
│ Key Length          │ varint   │ -1 for null, else byte count  │
│ Key                 │ bytes    │ Message key (optional)        │
│ Value Length        │ varint   │ Message value byte count      │
│ Value               │ bytes    │ Message payload               │
│ Headers Count       │ varint   │ Number of headers             │
│ Headers             │ bytes    │ Key-value header pairs        │
└────────────────────────────────────────────────────────────────┘
```

### Index File Format

Sparse index for offset-to-position lookups:

```
┌────────────────────────────────────────────────────────────────┐
│ Index Header (16 bytes)                                        │
├────────────────────────────────────────────────────────────────┤
│ Magic               │ 4 bytes  │ 0x494458 ("IDX")              │
│ Version             │ 2 bytes  │ 1                             │
│ Entry Count         │ 4 bytes  │ Number of index entries       │
│ Interval            │ 4 bytes  │ Messages between entries      │
│ Reserved            │ 2 bytes  │ Future use                    │
├────────────────────────────────────────────────────────────────┤
│ Entry 1: Offset (8 bytes) + Position (4 bytes)                 │
│ Entry 2: Offset (8 bytes) + Position (4 bytes)                 │
│ ...                                                            │
└────────────────────────────────────────────────────────────────┘
```

---

## Kafka Protocol Implementation

### Supported API Keys (v1.0)

Versions reflect what the broker advertises in ApiVersions today.

| API Key | Name | Version | Status | Notes |
|---------|------|---------|--------|-------|
| 0 | Produce | 0-9 | ✅ Full | Core produce path |
| 1 | Fetch | 11-13 | ✅ Full | Core consume path |
| 2 | ListOffsets | 0-4 | ✅ Full | Required for consumers |
| 3 | Metadata | 0-12 | ✅ Full | Topic/broker discovery |
| 8 | OffsetCommit | 3 | ✅ Full | Consumer group tracking (v3 only) |
| 9 | OffsetFetch | 5 | ✅ Full | Consumer group tracking (v5 only) |
| 10 | FindCoordinator | 3 | ✅ Full | Group coordinator lookup (v3 only) |
| 11 | JoinGroup | 4 | ✅ Full | Consumer group membership (v4 only) |
| 12 | Heartbeat | 4 | ✅ Full | Consumer liveness (v4 only) |
| 13 | LeaveGroup | 4 | ✅ Full | Graceful consumer shutdown (v4 only) |
| 14 | SyncGroup | 4 | ✅ Full | Partition assignment (v4 only) |
| 15 | DescribeGroups | 5 | ✅ Full | Ops debugging - `kafka-consumer-groups.sh --describe` |
| 16 | ListGroups | 5 | ✅ Full | Ops debugging - enumerate all consumer groups |
| 23 | OffsetForLeaderEpoch | 3 | ✅ Full | Safe consumer recovery after broker failover |
| 18 | ApiVersions | 0 | ✅ Full | Client capability negotiation (v0 only) |
| 19 | CreateTopics | 0 | ✅ Full | Topic management (v0 only) |
| 20 | DeleteTopics | 0 | ✅ Full | Topic management (v0 only) |
| 32 | DescribeConfigs | 4 | ✅ Full | Read topic/broker config |
| 33 | AlterConfigs | 1 | ✅ Full | Runtime config changes (whitelist) |
| 37 | CreatePartitions | 0-3 | ✅ Full | Scale partitions without topic recreation |
| 42 | DeleteGroups | 0-2 | ✅ Full | Consumer group cleanup |

### Explicitly Unsupported

| API Key | Name | Reason |
|---------|------|--------|
| 4 | LeaderAndIsr | Internal Kafka protocol, not client-facing |
| 5 | StopReplica | No replication - S3 handles durability |
| 6 | UpdateMetadata | Internal Kafka protocol |
| 7 | ControlledShutdown | Kubernetes handles pod lifecycle |
| 21 | DeleteRecords | S3 lifecycle handles retention |
| 22 | InitProducerId | Transactions not supported (by design) |
| 24 | AddPartitionsToTxn | Transactions not supported |
| 25 | AddOffsetsToTxn | Transactions not supported |
| 26 | EndTxn | Transactions not supported |
| 46 | ListPartitionReassignments | No manual reassignment - S3 is stateless |
| 47 | OffsetDelete | S3 lifecycle handles cleanup |
| 48-49 | DescribeClientQuotas/AlterClientQuotas | Quotas deferred to v2.0 |
| 50-56 | KRaft APIs | Using etcd, not KRaft |
| 57 | UpdateFeatures | Feature flags deferred |
| 65-67 | Transaction APIs (Describe/List/Abort) | Transactions not supported |

---

## Consumer Group Protocol

### State Machine

```
                    ┌─────────┐
           ┌───────►│  Empty  │◄───────┐
           │        └────┬────┘        │
           │             │             │
     All members     First member   All members
        leave          joins          expire
           │             │             │
           │             ▼             │
           │    ┌────────────────┐     │
           │    │ PreparingRe-   │     │
           │    │    balance     │     │
           │    └───────┬────────┘     │
           │            │              │
           │     All members           │
           │     have joined           │
           │            │              │
           │            ▼              │
           │    ┌────────────────┐     │
           │    │ CompletingRe-  │     │
           │    │    balance     │     │
           │    └───────┬────────┘     │
           │            │              │
           │     Leader sends          │
           │     assignments           │
           │            │              │
           │            ▼              │
           │    ┌────────────────┐     │
           └────┤    Stable      ├─────┘
                └───────┬────────┘
                        │
                  Heartbeat timeout
                  or member leaves
                        │
                        ▼
                ┌────────────────┐
                │ PreparingRe-   │
                │    balance     │
                └────────────────┘
```

### JoinGroup Flow

```
Consumer                    Broker                         etcd
   │                          │                              │
   │──JoinGroup Request──────►│                              │
   │                          │                              │
   │                          │◄───Get group state───────────│
   │                          │                              │
   │                          │ (If Empty or Stable)         │
   │                          │───Set PreparingRebalance────►│
   │                          │                              │
   │                          │ (Wait for all members        │
   │                          │  or rebalance timeout)       │
   │                          │                              │
   │◄─JoinGroup Response──────│                              │
   │  (leader gets member     │                              │
   │   list for assignment)   │                              │
   │                          │                              │
```

### SyncGroup Flow

```
Consumer (Leader)           Broker                         etcd
   │                          │                              │
   │──SyncGroup Request──────►│                              │
   │  (with assignments)      │                              │
   │                          │                              │
   │                          │───Store assignments─────────►│
   │                          │                              │
   │                          │───Set Stable────────────────►│
   │                          │                              │
   │◄─SyncGroup Response──────│                              │
   │  (my partition list)     │                              │
   │                          │                              │

Consumer (Follower)         Broker                         etcd
   │                          │                              │
   │──SyncGroup Request──────►│                              │
   │  (empty assignments)     │                              │
   │                          │                              │
   │                          │ (Wait for leader             │
   │                          │  or timeout)                 │
   │                          │                              │
   │◄─SyncGroup Response──────│                              │
   │  (my partition list)     │                              │
   │                          │                              │
```

---
