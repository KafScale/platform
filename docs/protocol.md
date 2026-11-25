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
| 15 | DescribeGroups | 0-5 | Ops visibility |
| 16 | ListGroups | 0-5 | Ops visibility (wide range for older admin clients) |
| 23 | OffsetForLeaderEpoch | 3 | Safe consumer recovery |
| 18 | ApiVersions | 0-4 | Client capability negotiation |
| 19 | CreateTopics | 0-2 | Topic management |
| 20 | DeleteTopics | 0-2 | Topic management |
| 22 | InitProducerId | 0-4 | Nontransactional idempotent producers; fresh ID, epoch 0 |
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
| 24 | AddPartitionsToTxn | Transactions not supported |
| 25 | AddOffsetsToTxn | Transactions not supported |
| 26 | EndTxn | Transactions not supported |
| 27 | WriteTxnMarkers | Transactions not supported |
| 28 | TxnOffsetCommit | Transactions not supported |
| 46 | ListPartitionReassignments | No manual reassignment |
| 47 | OffsetDelete | S3 lifecycle handles cleanup |
| 48-49 | DescribeClientQuotas/AlterClientQuotas | Quotas deferred |
| 50-56 | KRaft APIs | Using etcd |
| 57 | UpdateFeatures | Feature flags deferred |
| 65-67 | Transaction APIs | Transactions not supported |

## Idempotent producers

Brokers and the Kubernetes proxy advertise `InitProducerId` v0-v4. Leave
`transactional.id` unset: every initialization allocates a fresh cluster-unique
producer ID from a linearizable etcd counter and returns epoch 0. In-memory
metadata mode allocates IDs only for that process lifetime; production durability
requires etcd. Producer epochs and sequences travel in the Kafka batch headers.

With `acks=all`, a partition append checks producer epoch and batch sequences
under the same lock that assigns offsets. The last five batch ranges per producer
and partition are retained for retries, matching the clients' usual maximum of
five in-flight requests. A duplicate returns its original offsets without adding
records. Sequence gaps and retries outside that window return
`OUT_OF_ORDER_SEQUENCE_NUMBER` (45); an older epoch returns
`INVALID_PRODUCER_EPOCH` (47). A higher epoch must start at sequence 0, and
sequences wrap after 2147483647. An unknown producer must also start at sequence 0.
Invalid producer metadata returns `INVALID_REQUEST` (42); idempotent requests
with other acknowledgement modes return `INVALID_REQUIRED_ACKS` (21).

New idempotent batches always flush to S3 before success, including with
`KAFSCALE_PRODUCE_SYNC_FLUSH=false`. Deduplication state is rebuilt from validated
immutable segment bytes on restart or partition handoff, so a crash between S3
upload and etcd offset publication cannot lose the durable sequence history.
Uncertain uploads fence the log until recovery. No independent sequence update
can get ahead of the records or fall behind an acknowledged write.

A multi-batch partition request must be entirely new or a retry of a contiguous
range still in the five-batch window. Mixed duplicate/new sets, noncontiguous
retry sets, and sets mixing idempotent and non-idempotent batches are rejected
before append (`OUT_OF_ORDER_SEQUENCE_NUMBER` or `INVALID_REQUEST`). Retention
bounds recoverable history; after all history for a producer is removed, a
nonzero sequence is rejected and the client must establish a new session.
Application resends under a new producer ID are new writes. This is retry
deduplication within a producer session, not cross-session exactly-once delivery.

Unsupported APIs are omitted from `ApiVersions`, rather than advertised with
negative version ranges. Known transaction requests receive schema-correct
`UNSUPPORTED_VERSION` (35) responses, including an error for each requested
partition or transaction where the schema has no top-level error. This covers
`InitProducerId` with a transactional ID, transaction coordinator lookup,
`AddPartitionsToTxn`, `AddOffsetsToTxn`, `EndTxn`, `WriteTxnMarkers`,
`TxnOffsetCommit`, `DescribeTransactions`, `ListTransactions`, and the internal
`AllocateProducerIds` API. Transactional/control batches, transactional Produce,
and `read_committed` Fetch/ListOffsets are rejected without appending or changing
consumer offsets. Produce with `acks=0` has no response, including on rejection.
Full transactions, commit markers, and transactional visibility remain unsupported.

Upgrade every broker and proxy before using idempotent producers. An older proxy
can hide the capability, and an older partition owner cannot deduplicate retries.
Downgrading after idempotent writes removes this guarantee. Back up the producer-ID
counter with the etcd data and retain matching S3 data; resetting the counter while
keeping old logs can reuse producer IDs.

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
