<!--
Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

# Kafscale Ops API v1

This document defines the Kafka admin APIs Kafscale exposes to support operator
workflows. The scope stays within Kafscale’s architecture: stateless brokers,
S3 durability, and etcd-backed metadata. Transactions, compaction, and KRaft APIs remain
out of scope.

## Guiding Principles

- Prioritize operator parity (visibility, recovery, and cleanup).
- Keep config mutation narrow and explicit.
- Preserve a single source of truth in etcd metadata snapshots.
- Avoid UI write paths until authentication and authorization exist.

## API Coverage (v1)

| Category | API Key | Name | Version | Notes |
|---------|---------|------|---------|-------|
| Group visibility | 15 | DescribeGroups | v5 | Required for `kafka-consumer-groups.sh --describe`. |
| Group visibility | 16 | ListGroups | v5 | Required to enumerate consumer groups. |
| Group cleanup | 42 | DeleteGroups | v2 | Removes group metadata without touching topic data. |
| Recovery | 23 | OffsetForLeaderEpoch | v3 | Safe recovery after broker failover/truncation. |
| Config read | 32 | DescribeConfigs | v4 | Read-only visibility into topic/broker configuration. |
| Config write | 33 | AlterConfigs | v1 | Topic whitelist only; broker mutation rejected. |
| Scaling | 37 | CreatePartitions | v3 | Additive only; no partition reduction. |

## Selection Rationale

- These APIs unlock core ops workflows without requiring KRaft, transactions, or compaction.
- Write APIs are intentionally narrow to reduce blast radius until auth/ACLs exist.
- Anything outside this list is deferred and advertised as unsupported in ApiVersions.

## Versioning Notes

For v1 we implement the Kafka 3.7 CLI versions in active use; flexible versions are
supported where client tooling requires them. IncrementalAlterConfigs remains deferred.

Implemented versions:

- DescribeGroups v5
- ListGroups v5
- OffsetForLeaderEpoch v3
- DescribeConfigs v4
- AlterConfigs v1
- DeleteGroups v2
- CreatePartitions v3

## ApiVersions Advertising

Kafscale advertises supported ops APIs via ApiVersions so clients can select the
correct versions. Unsupported ops APIs are listed with `MinVersion = -1` and
`MaxVersion = -1` for clarity.

## Config Surface (Describe/AlterConfigs)

Kafscale will only expose topic/broker configs that map cleanly to existing metadata
and runtime behavior. Everything else returns `INVALID_CONFIG` (or the closest Kafka
error code supported by the request version).

### Topic Config Whitelist

| Config Key | Maps To | Notes |
|------------|---------|-------|
| `retention.ms` | `TopicConfig.retention_ms` | `-1` for unlimited, otherwise `>= 0`. |
| `retention.bytes` | `TopicConfig.retention_bytes` | `-1` for unlimited, otherwise `>= 0`. |
| `segment.bytes` | `TopicConfig.segment_bytes` | Must be `> 0`. |

AlterConfigs will accept only the topic keys above. Broker-level mutation is out of
scope for v1.

### Broker Config Whitelist (Read-Only)

Broker-level configs are read-only in v1 and are exposed under a Kafscale-specific
namespace so they do not conflict with upstream Kafka defaults.

| Config Key | Source |
|------------|--------|
| `broker.id` | Broker node ID |
| `advertised.listeners` | Kafka listener address |
| `kafscale.s3.bucket` | `KAFSCALE_S3_BUCKET` |
| `kafscale.s3.region` | `KAFSCALE_S3_REGION` |
| `kafscale.s3.endpoint` | `KAFSCALE_S3_ENDPOINT` |
| `kafscale.cache.bytes` | `KAFSCALE_CACHE_BYTES` |
| `kafscale.readahead.segments` | `KAFSCALE_READAHEAD_SEGMENTS` |
| `kafscale.segment.bytes` | `KAFSCALE_SEGMENT_BYTES` |
| `kafscale.flush.interval.ms` | `KAFSCALE_FLUSH_INTERVAL_MS` |

## Metadata + Etcd Integration

All topic configuration changes must be persisted to etcd using the existing snapshot
schema (`kafscale.metadata.TopicConfig`). The operator remains the source of truth:

- Kafka admin API updates are validated, written into etcd, and reconciled back into
  the operator-managed snapshot.
- Brokers consume the snapshot and adjust runtime behavior accordingly.
- The operator should reject config keys outside the whitelist.

## Topic/Partition Management in Etcd

Kafscale persists topic configuration and partition counts in etcd via the operator’s
metadata snapshot. The Kafka admin APIs will map to the same storage rules:

- CreateTopics writes a new `TopicConfig` entry and seeds partition metadata.
- AlterConfigs updates only the whitelisted config keys.
- CreatePartitions is additive only; reducing partition count is rejected.
- Partition creation should also pre-create empty partition metadata so brokers
  can begin serving immediately.

Until auth lands, write access should remain API-only (no UI mutation).

## Error Semantics

To align with Kafka client expectations:

- Unknown group: `GROUP_ID_NOT_FOUND`
- Coordinator unavailable/loading: `COORDINATOR_NOT_AVAILABLE` or `COORDINATOR_LOAD_IN_PROGRESS`
- Unknown topic/partition: `UNKNOWN_TOPIC_OR_PARTITION`
- Unsupported API/versions: `UNSUPPORTED_VERSION`
- Invalid config key/value: `INVALID_CONFIG`
- Unauthorized (when auth lands): `TOPIC_AUTHORIZATION_FAILED` or `GROUP_AUTHORIZATION_FAILED`

Request-specific notes:

- DescribeGroups/ListGroups return an empty result set on no matches rather than an error.
- DeleteGroups returns per-group errors; a missing group reports `GROUP_ID_NOT_FOUND`.
- DescribeConfigs returns only keys from the whitelist and ignores unknown keys.
- AlterConfigs rejects any broker resource mutation with `INVALID_CONFIG`.

## Ops Examples

These examples use the ops/admin APIs implemented in v1:

```bash
# List consumer groups
kafka-consumer-groups.sh --bootstrap-server <broker> --list

# Describe a consumer group
kafka-consumer-groups.sh --bootstrap-server <broker> --describe --group <group-id>

# Read topic configs
kafka-configs.sh --bootstrap-server <broker> --describe --entity-type topics --entity-name <topic>

# Update topic retention
kafka-configs.sh --bootstrap-server <broker> --alter --entity-type topics --entity-name <topic> \
  --add-config retention.ms=604800000

# Fetch offsets by leader epoch
kafka-run-class kafka.admin.OffsetForLeaderEpochClient \
  --bootstrap-server <broker> \
  --offset-for-leader-epoch-request "topic=<topic>,partition=0,leaderEpoch=0"

# Increase partition count for a topic (additive only)
kafka-topics.sh --bootstrap-server <broker> --alter --topic <topic> --partitions <count>

# Alter topic configs (whitelist only)
kafka-configs.sh --bootstrap-server <broker> --alter --entity-type topics --entity-name <topic> \
  --add-config retention.ms=120000

# Delete a consumer group
kafka-consumer-groups.sh --bootstrap-server <broker> --delete --group <group-id>
```

### Franz-go (Programmatic)

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, _ := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:9092"))
	defer client.Close()

	listReq := kmsg.NewPtrListGroupsRequest()
	listReq.Version = 5
	listReq.StatesFilter = []string{"Stable"}
	listReq.TypesFilter = []string{"classic"}
	listResp, _ := listReq.RequestWith(ctx, client)
	fmt.Println("groups:", listResp.Groups)

	describeReq := kmsg.NewPtrDescribeGroupsRequest()
	describeReq.Version = 5
	describeReq.Groups = []string{"my-group"}
	describeResp, _ := describeReq.RequestWith(ctx, client)
	fmt.Println("describe:", describeResp.Groups)

	offsetReq := kmsg.NewPtrOffsetForLeaderEpochRequest()
	offsetReq.Version = 3
	offsetReq.ReplicaID = -1
	offsetReq.Topics = []kmsg.OffsetForLeaderEpochRequestTopic{
		{
			Topic: "orders",
			Partitions: []kmsg.OffsetForLeaderEpochRequestTopicPartition{
				{Partition: 0, CurrentLeaderEpoch: -1, LeaderEpoch: 0},
			},
		},
	}
	offsetResp, _ := offsetReq.RequestWith(ctx, client)
	fmt.Println("offsets:", offsetResp.Topics)

	describeCfg := kmsg.NewPtrDescribeConfigsRequest()
	describeCfg.Version = 4
	describeCfg.Resources = []kmsg.DescribeConfigsRequestResource{
		{
			ResourceType: kmsg.ConfigResourceTypeTopic,
			ResourceName: "orders",
			ConfigNames:  []string{"retention.ms"},
		},
	}
	describeCfgResp, _ := describeCfg.RequestWith(ctx, client)
	fmt.Println("configs:", describeCfgResp.Resources)

	alterCfg := kmsg.NewPtrAlterConfigsRequest()
	alterCfg.Version = 1
	value := "120000"
	alterCfg.Resources = []kmsg.AlterConfigsRequestResource{
		{
			ResourceType: kmsg.ConfigResourceTypeTopic,
			ResourceName: "orders",
			Configs: []kmsg.AlterConfigsRequestResourceConfig{
				{Name: "retention.ms", Value: &value},
			},
		},
	}
	alterCfgResp, _ := alterCfg.RequestWith(ctx, client)
	fmt.Println("alter:", alterCfgResp.Resources)

	createReq := kmsg.NewPtrCreatePartitionsRequest()
	createReq.Version = 3
	createReq.Topics = []kmsg.CreatePartitionsRequestTopic{
		{Topic: "orders", Count: 6},
	}
	createResp, _ := createReq.RequestWith(ctx, client)
	fmt.Println("create partitions:", createResp.Topics)

	deleteReq := kmsg.NewPtrDeleteGroupsRequest()
	deleteReq.Version = 2
	deleteReq.Groups = []string{"my-group"}
	deleteResp, _ := deleteReq.RequestWith(ctx, client)
	fmt.Println("delete groups:", deleteResp.Groups)
}
```

### Local Testing Tip

If you want to test without external S3, run the broker with in-memory S3:

```bash
KAFSCALE_USE_MEMORY_S3=1 go run ./cmd/broker
```
