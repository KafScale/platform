---
layout: doc
title: API Documentation
description: Supported Kafka protocol versions, API keys, and internal gRPC surfaces.
---

# API Documentation

KafScale implements a focused subset of the Kafka protocol. Versions below reflect what the broker advertises in ApiVersions today.

## Supported Kafka protocol versions

| API Key | Name | Version | Notes |
| --- | --- | --- | --- |
| 0 | Produce | 0-9 | Core produce path |
| 1 | Fetch | 11-13 | Core consume path |
| 2 | ListOffsets | 0 | Required for consumers (v0 only) |
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
| 18 | ApiVersions | 0 | Client capability negotiation (v0 only) |
| 19 | CreateTopics | 0 | Topic management (v0 only) |
| 20 | DeleteTopics | 0 | Topic management (v0 only) |
| 23 | OffsetForLeaderEpoch | 3 | Safe consumer recovery |
| 32 | DescribeConfigs | 4 | Read topic/broker config |
| 33 | AlterConfigs | 1 | Runtime config changes (whitelist) |
| 37 | CreatePartitions | 0-3 | Scale partitions |
| 42 | DeleteGroups | 0-2 | Consumer group cleanup |

## Explicitly unsupported

| API Key | Name | Reason |
| --- | --- | --- |
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

## Consumer group state machine

<div class="diagram">
  <div class="diagram-label">Consumer group lifecycle</div>
  <svg viewBox="0 0 900 320" role="img" aria-label="Consumer group state machine">
    <defs>
      <marker id="arrow-sm" markerWidth="10" markerHeight="10" refX="6" refY="3" orient="auto">
        <path d="M0,0 L0,6 L6,3 z" fill="var(--text)"></path>
      </marker>
    </defs>
    <rect x="60" y="50" width="160" height="70" rx="12" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)"></rect>
    <text x="110" y="92" font-size="14" font-weight="600">Empty</text>

    <rect x="320" y="30" width="220" height="70" rx="12" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)"></rect>
    <text x="365" y="72" font-size="14" font-weight="600">PreparingRebalance</text>

    <rect x="620" y="50" width="200" height="70" rx="12" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)"></rect>
    <text x="655" y="92" font-size="14" font-weight="600">CompletingRebalance</text>

    <rect x="320" y="200" width="220" height="70" rx="12" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
    <text x="390" y="242" font-size="14" font-weight="600">Stable</text>

    <line x1="220" y1="85" x2="320" y2="70" stroke="var(--diagram-stroke)" stroke-width="2" marker-end="url(#arrow-sm)"></line>
    <line x1="540" y1="70" x2="620" y2="85" stroke="var(--diagram-stroke)" stroke-width="2" marker-end="url(#arrow-sm)"></line>
    <line x1="720" y1="120" x2="520" y2="200" stroke="var(--diagram-stroke)" stroke-width="2" marker-end="url(#arrow-sm)"></line>
    <line x1="430" y1="200" x2="430" y2="100" stroke="var(--diagram-stroke)" stroke-width="2" marker-end="url(#arrow-sm)"></line>
  </svg>
</div>

## JoinGroup and SyncGroup flow

<div class="diagram">
  <div class="diagram-label">Request flow</div>
  <svg viewBox="0 0 900 320" role="img" aria-label="JoinGroup and SyncGroup request flow">
    <defs>
      <marker id="arrow-flow" markerWidth="10" markerHeight="10" refX="6" refY="3" orient="auto">
        <path d="M0,0 L0,6 L6,3 z" fill="var(--text)"></path>
      </marker>
    </defs>
    <text x="90" y="40" font-size="13" font-weight="600">Consumer</text>
    <text x="410" y="40" font-size="13" font-weight="600">Broker</text>
    <text x="720" y="40" font-size="13" font-weight="600">etcd</text>

    <line x1="120" y1="55" x2="120" y2="280" stroke="var(--diagram-stroke)" stroke-width="2"></line>
    <line x1="450" y1="55" x2="450" y2="280" stroke="var(--diagram-stroke)" stroke-width="2"></line>
    <line x1="760" y1="55" x2="760" y2="280" stroke="var(--diagram-stroke)" stroke-width="2"></line>

    <line x1="120" y1="90" x2="450" y2="90" stroke="var(--diagram-stroke)" stroke-width="2" marker-end="url(#arrow-flow)"></line>
    <text x="180" y="80" font-size="12">JoinGroup</text>

    <line x1="450" y1="120" x2="760" y2="120" stroke="var(--diagram-stroke)" stroke-width="2" marker-end="url(#arrow-flow)"></line>
    <text x="520" y="110" font-size="12">Read group state</text>

    <line x1="760" y1="150" x2="450" y2="150" stroke="var(--diagram-stroke)" stroke-width="2" marker-end="url(#arrow-flow)"></line>
    <text x="520" y="140" font-size="12">State response</text>

    <line x1="450" y1="180" x2="120" y2="180" stroke="var(--diagram-stroke)" stroke-width="2" marker-end="url(#arrow-flow)"></line>
    <text x="190" y="170" font-size="12">JoinGroup response</text>

    <line x1="120" y1="220" x2="450" y2="220" stroke="var(--diagram-stroke)" stroke-width="2" marker-end="url(#arrow-flow)"></line>
    <text x="180" y="210" font-size="12">SyncGroup</text>

    <line x1="450" y1="250" x2="760" y2="250" stroke="var(--diagram-stroke)" stroke-width="2" marker-end="url(#arrow-flow)"></line>
    <text x="520" y="240" font-size="12">Store assignments</text>
  </svg>
</div>

## Unsupported APIs and error responses

Until auth lands, KafScale responds to SASL handshake attempts with `UNSUPPORTED_SASL_MECHANISM` (error code 33).

## gRPC internal API

The internal gRPC surface is used broker-to-operator and broker-to-broker for control plane workflows and snapshot coordination. It is not intended for external clients.
