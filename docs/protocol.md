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

# Kafka Protocol Support

Kafscale implements a focused subset of the Kafka protocol. Versions below reflect what the broker advertises in ApiVersions today.

## Supported

| API Key | Name | Version | Notes |
|---------|------|---------|-------|
| 0 | Produce | 0-9 | Core produce path |
| 1 | Fetch | 4-13 | Request-wide MaxWait and MinBytes across partitions |
| 2 | ListOffsets | 0-4 | Required for consumers |
| 3 | Metadata | 0-12 | Topic/broker discovery |
| 8 | OffsetCommit | 1-3 | Consumer group tracking |
| 9 | OffsetFetch | 1-5 | Consumer group tracking |
| 10 | FindCoordinator | 0-3 | Group coordinator lookup |
| 11 | JoinGroup | 0-4 | Consumer group membership |
| 12 | Heartbeat | 0-4 | Consumer liveness |
| 13 | LeaveGroup | 0-4 | Graceful consumer shutdown |
| 14 | SyncGroup | 0-4 | Partition assignment |
| 15 | DescribeGroups | 5 | Ops visibility |
| 16 | ListGroups | 0-5 | Ops visibility (wide range for older admin clients) |
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
`REQUEST_TIMED_OUT`. An earliest lookup that cannot load the partition from S3
returns `KAFKA_STORAGE_ERROR` on every version, as Kafka does. Other failures
return `UNKNOWN_SERVER_ERROR`. Failed lookups
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
| mTLS at the proxy | Supported. URI SAN mapped to a principal, enforced by broker ACLs. See [Kafka proxy authentication](kafka-authentication.md) |
| ACL authorization | Supported. Per-topic, group and admin rules |
| SASL (PLAIN, SCRAM, OAUTHBEARER) | Not supported |

Kafscale rejects SASL handshake/auth requests as unsupported
API keys and closes the connection (no SASL-specific error payload is returned).

Fetch v4 is advertised for librdkafka MsgVer2 negotiation. Produce accepts magic-2 record batches; legacy magic-0/1 MessageSets return `UNSUPPORTED_VERSION` (35), and truncated record batches return `CORRUPT_MESSAGE` (2).

Classic consumer-group APIs also advertise their older supported versions for librdkafka feature detection: FindCoordinator v0-v3, JoinGroup/SyncGroup/Heartbeat/LeaveGroup v0-v4, OffsetCommit v1-v3, and OffsetFetch v1-v5.
