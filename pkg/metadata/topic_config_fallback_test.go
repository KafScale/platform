// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metadata

import (
	"context"
	"errors"
	"testing"

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestTopicConfigFallbackReplication(t *testing.T) {
	for _, backend := range []string{"memory", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			type fallbackCase struct {
				name       string
				partitions int
				replicas   []int32
				want       int32
			}
			cases := []fallbackCase{
				{"single-replica", 4, []int32{1}, 1},
				{"two-replicas", 4, []int32{1, 2}, 2},
				{"missing-replicas", 4, nil, 1},
				{"no-partitions", 0, nil, 1},
			}
			if backend == "memory" {
				cases = append(cases, fallbackCase{"large-partition-count", 1 << 15, []int32{1}, 1})
			}
			initial := ClusterMetadata{Brokers: []protocol.MetadataBroker{{NodeID: 1}, {NodeID: 2}}}
			for _, tc := range cases {
				topic := protocol.MetadataTopic{Topic: kmsg.StringPtr(tc.name)}
				for p := range tc.partitions {
					topic.Partitions = append(topic.Partitions, protocol.MetadataPartition{Partition: int32(p), Leader: 1, Replicas: tc.replicas})
				}
				initial.Topics = append(initial.Topics, topic)
			}
			var store Store = NewInMemoryStore(initial)
			if backend == "etcd" {
				endpoints := testutil.StartEmbeddedEtcd(t)
				putSnapshot(t, endpoints, initial)
				store = newTestEtcdStore(t, ctx, ClusterMetadata{}, endpoints)
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					got, err := store.FetchTopicConfig(ctx, tc.name)
					if err != nil {
						t.Fatal(err)
					}
					if got.ReplicationFactor != tc.want || got.Partitions != int32(tc.partitions) || got.Name != tc.name {
						t.Fatalf("config=%+v, want partitions=%d replication=%d", got, tc.partitions, tc.want)
					}
				})
			}
			// Expanding a topic with no stored config must not turn its new
			// partition count into a persisted replication factor.
			if err := store.CreatePartitions(ctx, "single-replica", 8); err != nil {
				t.Fatal(err)
			}
			got, err := store.FetchTopicConfig(ctx, "single-replica")
			if err != nil || got.Partitions != 8 || got.ReplicationFactor != 1 {
				t.Fatalf("expansion changed replication: %+v, %v", got, err)
			}
			// An explicitly stored replication factor takes precedence over defaults.
			if err := store.UpdateTopicConfig(ctx, &metadatapb.TopicConfig{Name: "single-replica", Partitions: 8, ReplicationFactor: 2}); err != nil {
				t.Fatal(err)
			}
			got, err = store.FetchTopicConfig(ctx, "single-replica")
			if err != nil || got.ReplicationFactor != 2 {
				t.Fatalf("stored config overwritten: %+v, %v", got, err)
			}
			if err := store.CreatePartitions(ctx, "single-replica", 12); err != nil {
				t.Fatal(err)
			}
			got, err = store.FetchTopicConfig(ctx, "single-replica")
			if err != nil || got.Partitions != 12 || got.ReplicationFactor != 2 {
				t.Fatalf("expansion left a stale stored config: %+v, %v", got, err)
			}
		})
	}
}

func TestExpansionPreservesConcurrentTopicConfig(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "created-during-expansion", true: "updated-during-expansion"}[existing], func(t *testing.T) {
			ctx := context.Background()
			endpoints := testutil.StartEmbeddedEtcd(t)
			base := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
			writeSnapshotFixture(t, base.client, snapshotFixture())
			cfg, err := base.FetchTopicConfig(ctx, "existing")
			if err != nil {
				t.Fatal(err)
			}
			if existing {
				if err := base.UpdateTopicConfig(ctx, cfg); err != nil {
					t.Fatal(err)
				}
			}
			store := snapshotMutationStore(t, base, &snapshotHookKV{KV: base.client, hook: func() {
				cfg.RetentionMs = 60000
				cfg.ReplicationFactor = 2
				cfg.Config["cleanup.policy"] = "compact"
				if err := base.UpdateTopicConfig(ctx, cfg); err != nil {
					t.Fatal(err)
				}
			}})
			if err := store.CreatePartitions(ctx, "existing", 8); err != nil {
				t.Fatal(err)
			}
			got, err := base.FetchTopicConfig(ctx, "existing")
			if err != nil || got.Partitions != 8 || got.ReplicationFactor != 2 || got.RetentionMs != 60000 || got.Config["cleanup.policy"] != "compact" {
				t.Fatalf("expansion lost a concurrent config edit: %+v, %v", got, err)
			}
		})
	}
}

type failedTopicConfigReadKV struct{ clientv3.KV }

func (k failedTopicConfigReadKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if key == TopicConfigKey("existing") {
		return nil, errors.New("config read unavailable")
	}
	return k.KV.Get(ctx, key, opts...)
}

func TestExpansionConfigFailureIsAtomic(t *testing.T) {
	for _, phase := range []string{"read", "decode", "commit"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			endpoints := testutil.StartEmbeddedEtcd(t)
			base := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
			writeSnapshotFixture(t, base.client, snapshotFixture())
			cfg, err := base.FetchTopicConfig(ctx, "existing")
			if err != nil {
				t.Fatal(err)
			}
			if err := base.UpdateTopicConfig(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			var kv clientv3.KV = base.client
			switch phase {
			case "read":
				kv = failedTopicConfigReadKV{kv}
			case "decode":
				if _, err := base.client.Put(ctx, TopicConfigKey("existing"), "invalid protobuf"); err != nil {
					t.Fatal(err)
				}
			case "commit":
				kv = snapshotFailKV{KV: kv, failWrite: true}
			}
			store := snapshotMutationStore(t, base, kv)
			if err := store.CreatePartitions(ctx, "existing", 8); err == nil {
				t.Fatal("expansion ignored config failure")
			}
			saved, err := loadSnapshot(endpoints)
			if err != nil || len(saved.Topics[0].Partitions) != 4 {
				t.Fatalf("failed expansion published metadata: %+v, %v", saved, err)
			}
			cached, err := store.Metadata(ctx, []string{"existing"})
			if err != nil || len(cached.Topics[0].Partitions) != 4 {
				t.Fatalf("failed expansion exposed speculative metadata: %+v, %v", cached, err)
			}
			if state, err := base.client.Get(ctx, PartitionStateKey("existing", 4)); err != nil || len(state.Kvs) != 0 {
				t.Fatalf("failed expansion published partition state: %+v, %v", state, err)
			}
		})
	}
}
