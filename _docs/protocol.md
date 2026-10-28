---
layout: doc
title: "KafScale Protocol Compatibility: Kafka APIs"
description: Supported Kafka protocol versions, API keys, and internal gRPC surfaces.
permalink: /protocol/
nav_title: Protocol
nav_order: 2
nav_group: References
---

# Kafka Protocol Support

Kafscale implements a focused subset of the Kafka protocol. Versions below reflect what the broker advertises in ApiVersions today.

## Supported

| API Key | Name | Version | Notes |
|---------|------|---------|-------|
| 0 | Produce | 0-9 | Core produce path |
| 1 | Fetch | 11-13 | Core consume path |
| 2 | ListOffsets | 0-4 | Required for consumers |
| 3 | Metadata | 0-12 | Topic/broker discovery |
| 8 | OffsetCommit | 3 | Consumer group tracking (v3 only) |
| 9 | OffsetFetch | 5 | Consumer group tracking (v5 only) |
| 10 | FindCoordinator | 3 | Group coordinator lookup (v3 only) |
| 11 | JoinGroup | 4 | Consumer group membership (v4 only) |
| 12 | Heartbeat | 4 | Consumer liveness (v4 only) |
| 13 | LeaveGroup | 4 | Graceful consumer shutdown (v4 only) |
| 14 | SyncGroup | 4 | Partition assignment (v4 only) |
| 15 | DescribeGroups | 5 | Ops visibility |
| 16 | ListGroups | 5 | Ops visibility |
| 23 | OffsetForLeaderEpoch | 3 | Safe consumer recovery |
| 18 | ApiVersions | 0-4 | Client capability negotiation |
| 19 | CreateTopics | 0-2 | Topic management |
| 20 | DeleteTopics | 0-2 | Topic management |
| 32 | DescribeConfigs | 4 | Read topic/broker config |
| 33 | AlterConfigs | 1 | Runtime config changes (whitelist) |
| 37 | CreatePartitions | 0-3 | Scale partitions |
| 42 | DeleteGroups | 0-2 | Consumer group cleanup |

`ListOffsets` does not auto-create topics, regardless of the broker's auto-create
setting. Earliest and latest lookups return `UNKNOWN_TOPIC_OR_PARTITION` for
missing topics or partitions. Deadline/cancellation errors return
`REQUEST_TIMED_OUT`; other failures return `UNKNOWN_SERVER_ERROR`. Failed lookups
emit warning logs with topic, partition, requested timestamp, error code, and cause.

## Explicitly Unsupported

| API Key | Name | Reason |
|---------|------|--------|
| 4 | LeaderAndIsr | Internal Kafka protocol |
| 5 | StopReplica | No replication (S3 durability) |
| 6 | UpdateMetadata | Internal Kafka protocol |
| 7 | ControlledShutdown | Kubernetes handles lifecycle |
| 21 | DeleteRecords | S3 lifecycle handles retention |
| 22 | InitProducerId | Transactions not supported |
| 24 | AddPartitionsToTxn | Transactions not supported |
| 25 | AddOffsetsToTxn | Transactions not supported |
| 26 | EndTxn | Transactions not supported |
| 46 | ListPartitionReassignments | No manual reassignment |
| 47 | OffsetDelete | S3 lifecycle handles cleanup |
| 48-49 | DescribeClientQuotas/AlterClientQuotas | Quotas deferred |
| 50-56 | KRaft APIs | Using etcd |
| 57 | UpdateFeatures | Feature flags deferred |
| 65-67 | Transaction APIs | Transactions not supported |

## Authentication

| Mechanism | Status |
|-----------|--------|
| mTLS at the proxy | Supported on main since PR #214. URI SAN mapped to a principal, enforced by broker ACLs. See [mTLS authentication](/kafka-authentication/) |
| ACL authorization | Supported. Per-topic, group and admin rules |
| SASL (PLAIN, SCRAM, OAUTHBEARER) | Not supported |

Kafscale rejects SASL handshake/auth requests as unsupported
API keys and closes the connection (no SASL-specific error payload is returned).
