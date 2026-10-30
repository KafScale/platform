// Copyright 2026 KafScale team.
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

package broker

import (
	"context"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestEmptyConsumerGroupLifecycle(t *testing.T) {
	for _, backend := range []string{"memory", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var store metadata.Store = metadata.NewInMemoryStore(metadata.ClusterMetadata{})
			if backend == "etcd" {
				etcd, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = etcd.Close() })
				store = etcd
			}
			for _, exit := range []string{"leave", "session expiry", "rebalance timeout", "legacy offsets"} {
				t.Run(exit, func(t *testing.T) {
					c := newTestCoordinator(t, store)
					joined := joinTestGroup(t, c, "")
					syncTestGroup(t, c, joined.MemberID, joined.Generation, protocol.NONE)
					commit := kmsg.NewPtrOffsetCommitRequest()
					commit.Group, commit.MemberID, commit.Generation = "readers", joined.MemberID, joined.Generation
					commit.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "orders", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 42, Metadata: kmsg.StringPtr("checkpoint")}, {Partition: 1, Offset: 17}}}}
					committed, err := c.OffsetCommit(ctx, commit)
					if err != nil {
						t.Fatal(err)
					}
					for _, p := range committed.Topics[0].Partitions {
						if p.ErrorCode != 0 {
							t.Fatal(committed)
						}
					}
					before := c.groups["readers"].generationID
					switch exit {
					case "leave", "legacy offsets":
						leave := kmsg.NewPtrLeaveGroupRequest()
						leave.Group, leave.MemberID = "readers", joined.MemberID
						if resp := c.LeaveGroup(ctx, leave); resp.ErrorCode != 0 {
							t.Fatal(resp)
						}
					case "session expiry":
						c.groups["readers"].members[joined.MemberID].lastHeartbeat = time.Now().Add(-time.Hour)
						c.cleanupGroups()
					case "rebalance timeout":
						state := c.groups["readers"]
						state.state = groupStatePreparingRebalance
						state.rebalanceDeadline = time.Now().Add(-time.Second)
						state.members[joined.MemberID].joinGeneration = before - 1
						c.cleanupGroups()
					}
					group, err := store.FetchConsumerGroup(ctx, "readers")
					if err != nil || group == nil || group.State != groupStateEmptyStr || group.GenerationId != before+1 || group.Leader != "" || group.Protocol != "" || len(group.Members) != 0 {
						t.Fatalf("empty metadata: %+v, %v", group, err)
					}
					// Repeated sweeps must preserve Empty and must not bump its generation again.
					c.cleanupGroups()
					if c.groups["readers"].generationID != before+1 {
						t.Fatal("empty sweep changed generation")
					}
					if exit == "legacy offsets" {
						// Simulate old brokers losing metadata while retaining offsets.
						offsets, err := store.ListGroupConsumerOffsets(ctx, "readers")
						if err != nil {
							t.Fatal(err)
						}
						if err := store.DeleteConsumerGroup(ctx, "readers"); err != nil {
							t.Fatal(err)
						}
						for _, o := range offsets {
							if err := store.CommitConsumerOffset(ctx, o.Group, o.Topic, o.Partition, o.Offset, o.Metadata); err != nil {
								t.Fatal(err)
							}
						}
					}
					// All admin results must survive coordinator replacement.
					restarted := newTestCoordinator(t, store)
					describe := kmsg.NewPtrDescribeGroupsRequest()
					describe.Groups = []string{"readers", "missing"}
					described, err := restarted.DescribeGroups(ctx, describe)
					if err != nil || len(described.Groups) != 2 || described.Groups[0].ErrorCode != 0 || described.Groups[0].State != "Empty" || len(described.Groups[0].Members) != 0 || described.Groups[1].ErrorCode != 0 || described.Groups[1].State != "Dead" {
						t.Fatalf("describe: %+v, %v", described, err)
					}
					for _, state := range []string{"", "Empty", "Stable"} {
						list := kmsg.NewPtrListGroupsRequest()
						list.TypesFilter = []string{"classic"}
						if state != "" {
							list.StatesFilter = []string{state}
						}
						listed, err := restarted.ListGroups(ctx, list)
						want := 1
						if state == "Stable" {
							want = 0
						}
						if err != nil || listed.ErrorCode != 0 || len(listed.Groups) != want {
							t.Fatalf("list %q: %+v, %v", state, listed, err)
						}
						if want == 1 && (listed.Groups[0].Group != "readers" || listed.Groups[0].GroupState != "Empty") {
							t.Fatal(listed)
						}
					}
					fetch := kmsg.NewPtrOffsetFetchRequest()
					fetch.Version, fetch.Group = 5, "readers"
					fetched, err := restarted.OffsetFetch(ctx, fetch)
					if err != nil || fetched.ErrorCode != 0 || len(fetched.Topics) != 1 || len(fetched.Topics[0].Partitions) != 2 || fetched.Topics[0].Partitions[0].Offset != 42 || *fetched.Topics[0].Partitions[0].Metadata != "checkpoint" || fetched.Topics[0].Partitions[1].Offset != 17 {
						t.Fatalf("offset fetch: %+v, %v", fetched, err)
					}
					// Rejoining a persisted Empty group starts a fresh generation.
					if exit != "legacy offsets" {
						rejoined := joinTestGroup(t, restarted, "")
						if rejoined.ErrorCode != 0 || rejoined.Generation <= before+1 {
							t.Fatal(rejoined)
						}
						leave := kmsg.NewPtrLeaveGroupRequest()
						leave.Group, leave.MemberID = "readers", rejoined.MemberID
						if resp := restarted.LeaveGroup(ctx, leave); resp.ErrorCode != 0 {
							t.Fatal(resp)
						}
					}
					del := kmsg.NewPtrDeleteGroupsRequest()
					del.Groups = []string{"readers"}
					deleted, err := restarted.DeleteGroups(ctx, del)
					if err != nil || deleted.Groups[0].ErrorCode != 0 {
						t.Fatalf("delete: %+v, %v", deleted, err)
					}
					restarted.cleanupGroups()
					listed, err := restarted.ListGroups(ctx, kmsg.NewPtrListGroupsRequest())
					if err != nil || len(listed.Groups) != 0 {
						t.Fatalf("deleted group reappeared: %+v, %v", listed, err)
					}
					offsets, err := store.ListGroupConsumerOffsets(ctx, "readers")
					if err != nil || len(offsets) != 0 {
						t.Fatalf("deleted offsets: %+v, %v", offsets, err)
					}
					described, err = restarted.DescribeGroups(ctx, describe)
					if err != nil || described.Groups[0].ErrorCode != 0 || described.Groups[0].State != "Dead" {
						t.Fatalf("deleted describe: %+v, %v", described, err)
					}
				})
			}
		})
	}
}

type emptyGroupFaultStore struct {
	metadata.Store
}

func (s emptyGroupFaultStore) ListGroupConsumerOffsets(context.Context, string) ([]metadata.ConsumerOffset, error) {
	return nil, errCoordinatorStore
}

func (s emptyGroupFaultStore) ListConsumerGroups(context.Context) ([]*metadatapb.ConsumerGroup, error) {
	return nil, errCoordinatorStore
}

func TestEmptyGroupDiscoveryErrors(t *testing.T) {
	ctx := context.Background()
	c := newTestCoordinator(t, emptyGroupFaultStore{metadata.NewInMemoryStore(metadata.ClusterMetadata{})})
	describe := kmsg.NewPtrDescribeGroupsRequest()
	describe.Groups = []string{"readers"}
	if resp, err := c.DescribeGroups(ctx, describe); err != nil || resp.Groups[0].ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
		t.Fatal(resp, err)
	}
	if resp, err := c.ListGroups(ctx, kmsg.NewPtrListGroupsRequest()); err != nil || resp.ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
		t.Fatal(resp, err)
	}
	del := kmsg.NewPtrDeleteGroupsRequest()
	del.Groups = []string{"readers"}
	if resp, err := c.DeleteGroups(ctx, del); err != nil || resp.Groups[0].ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
		t.Fatal(resp, err)
	}
}

func TestDeleteGroupsRejectsActiveMembers(t *testing.T) {
	for _, source := range []string{"cached", "persisted", "cache ahead of store"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
			c := newTestCoordinator(t, store)
			joined := joinTestGroup(t, c, "")
			if err := store.CommitConsumerOffset(ctx, "readers", "orders", 0, 42, "checkpoint"); err != nil {
				t.Fatal(err)
			}
			if source == "persisted" {
				c = newTestCoordinator(t, store)
			}
			if source == "cache ahead of store" {
				if err := store.PutConsumerGroup(ctx, emptyConsumerGroup("readers")); err != nil {
					t.Fatal(err)
				}
			}
			del := kmsg.NewPtrDeleteGroupsRequest()
			del.Groups = []string{"readers"}
			resp, err := c.DeleteGroups(ctx, del)
			if err != nil || len(resp.Groups) != 1 || resp.Groups[0].ErrorCode != protocol.NON_EMPTY_GROUP {
				t.Fatalf("delete active: %+v, %v", resp, err)
			}
			offsets, err := store.ListGroupConsumerOffsets(ctx, "readers")
			if err != nil || len(offsets) != 1 || offsets[0].Offset != 42 || offsets[0].Metadata != "checkpoint" {
				t.Fatal(offsets, err)
			}
			if source != "cache ahead of store" {
				group, err := store.FetchConsumerGroup(ctx, "readers")
				if err != nil || group.Members[joined.MemberID] == nil {
					t.Fatal(group, err)
				}
			}
		})
	}
}
