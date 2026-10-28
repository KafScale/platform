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
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestGroupsWithOffsetsRemainVisibleWithoutMembers(t *testing.T) {
	for _, backend := range []string{"memory", "etcd"} {
		for _, departure := range []string{"leave", "expiry"} {
			t.Run(backend+"/"+departure, func(t *testing.T) {
				ctx := context.Background()
				initial := metadata.ClusterMetadata{Topics: []protocol.MetadataTopic{{Topic: kmsg.StringPtr("orders"), Partitions: []protocol.MetadataPartition{{Partition: 0}}}}}
				var store metadata.Store = metadata.NewInMemoryStore(initial)
				if backend == "etcd" {
					etcd, err := metadata.NewEtcdStore(ctx, initial, metadata.EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t)})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { etcd.Close() })
					store = etcd
				}
				c := newTestCoordinator(t, store)
				joined := joinTestGroup(t, c, "")
				syncTestGroup(t, c, joined.MemberID, joined.Generation, 0)
				commit := kmsg.NewPtrOffsetCommitRequest()
				commit.Group, commit.MemberID, commit.Generation = "readers", joined.MemberID, joined.Generation
				commit.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "orders", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 42, Metadata: kmsg.StringPtr("checkpoint")}}}}
				if resp, err := c.OffsetCommit(ctx, commit); err != nil || resp.Topics[0].Partitions[0].ErrorCode != 0 {
					t.Fatal(resp, err)
				}
				active, err := c.ListGroups(ctx, kmsg.NewPtrListGroupsRequest())
				if err != nil || len(active.Groups) != 1 || active.Groups[0].GroupState != "Stable" {
					t.Fatal("duplicate or changed active group", active, err)
				}
				if departure == "leave" {
					req := kmsg.NewPtrLeaveGroupRequest()
					req.Version, req.Group = 4, "readers"
					req.Members = []kmsg.LeaveGroupRequestMember{{MemberID: joined.MemberID}}
					if resp := c.LeaveGroup(ctx, req); resp.ErrorCode != 0 || resp.Members[0].ErrorCode != 0 {
						t.Fatal(resp)
					}
				} else {
					c.groups["readers"].members[joined.MemberID].lastHeartbeat = time.Now().Add(-time.Hour)
					c.cleanupGroups()
				}
				// Exercise recovery of an offset-only group, including existing installations.
				if record, err := store.FetchConsumerGroup(ctx, "readers"); err != nil || record != nil {
					t.Fatal(record, err)
				}
				for _, coordinator := range []*GroupCoordinator{c, newTestCoordinator(t, store)} {
					list := kmsg.NewPtrListGroupsRequest()
					list.StatesFilter = []string{"Empty"}
					list.TypesFilter = []string{"classic"}
					listed, err := coordinator.ListGroups(ctx, list)
					if err != nil || listed.ErrorCode != 0 || len(listed.Groups) != 1 || listed.Groups[0].Group != "readers" || listed.Groups[0].GroupState != "Empty" {
						t.Fatal(listed, err)
					}
					list.StatesFilter = []string{"Stable"}
					if resp, err := coordinator.ListGroups(ctx, list); err != nil || len(resp.Groups) != 0 {
						t.Fatal(resp, err)
					}
					describe := kmsg.NewPtrDescribeGroupsRequest()
					describe.Groups = []string{"readers", "missing"}
					described, err := coordinator.DescribeGroups(ctx, describe)
					if err != nil || described.Groups[0].ErrorCode != 0 || described.Groups[0].State != "Empty" || len(described.Groups[0].Members) != 0 || described.Groups[1].ErrorCode != protocol.GROUP_ID_NOT_FOUND {
						t.Fatal(described, err)
					}
					fetch := kmsg.NewPtrOffsetFetchRequest()
					fetch.Version, fetch.Group = 5, "readers"
					if fetched, err := coordinator.OffsetFetch(ctx, fetch); err != nil || fetched.Topics[0].Partitions[0].Offset != 42 || *fetched.Topics[0].Partitions[0].Metadata != "checkpoint" {
						t.Fatal(fetched, err)
					}
				}
				if err := store.CommitConsumerOffset(ctx, "readers-other", "orders", 0, 99, ""); err != nil {
					t.Fatal(err)
				}
				deletion := kmsg.NewPtrDeleteGroupsRequest()
				deletion.Groups = []string{"readers"}
				if resp, err := c.DeleteGroups(ctx, deletion); err != nil || resp.Groups[0].ErrorCode != 0 {
					t.Fatal(resp, err)
				}
				if offsets, err := store.ListGroupConsumerOffsets(ctx, "readers"); err != nil || len(offsets) != 0 {
					t.Fatal(offsets, err)
				}
				if offsets, err := store.ListGroupConsumerOffsets(ctx, "readers-other"); err != nil || len(offsets) != 1 {
					t.Fatal("deleted another group's offsets", offsets, err)
				}
				describe := kmsg.NewPtrDescribeGroupsRequest()
				describe.Groups = []string{"readers"}
				if resp, err := c.DescribeGroups(ctx, describe); err != nil || resp.Groups[0].ErrorCode != protocol.GROUP_ID_NOT_FOUND {
					t.Fatal("deleted group reappeared", resp, err)
				}
			})
		}
	}
}

type groupVisibilityFailureStore struct{ metadata.Store }

func (groupVisibilityFailureStore) ListConsumerOffsets(context.Context) ([]metadata.ConsumerOffset, error) {
	return nil, errCoordinatorStore
}

func TestOffsetOnlyGroupReadFailures(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	c := newTestCoordinator(t, groupVisibilityFailureStore{Store: store})
	if resp, err := c.ListGroups(context.Background(), kmsg.NewPtrListGroupsRequest()); err != nil || resp.ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
		t.Fatal(resp, err)
	}
	c = newTestCoordinator(t, groupOffsetFailureStore{Store: store})
	describe := kmsg.NewPtrDescribeGroupsRequest()
	describe.Groups = []string{"g"}
	if resp, err := c.DescribeGroups(context.Background(), describe); err != nil || resp.Groups[0].ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
		t.Fatal(resp, err)
	}
	deletion := kmsg.NewPtrDeleteGroupsRequest()
	deletion.Groups = []string{"g"}
	if resp, err := c.DeleteGroups(context.Background(), deletion); err != nil || resp.Groups[0].ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
		t.Fatal(resp, err)
	}
}

type storeWithoutAtomicDeletion struct{ metadata.Store }

func TestDeleteGroupsWithLegacyStore(t *testing.T) {
	ctx := context.Background()
	for _, offsets := range []bool{false, true} {
		base := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
		seedCoordinatorGroup(t, base, groupStateEmptyStr)
		if offsets {
			if err := base.CommitConsumerOffset(ctx, "readers", "orders", 0, 42, ""); err != nil {
				t.Fatal(err)
			}
		}
		legacy := storeWithoutAtomicDeletion{Store: base}
		if _, ok := any(legacy).(metadata.ConsumerGroupDeleter); ok {
			t.Fatal("Store interface requires atomic deletion")
		}
		c := newTestCoordinator(t, legacy)
		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"readers"}
		resp, err := c.DeleteGroups(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if offsets {
			if resp.Groups[0].ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
				t.Fatal(resp)
			}
			if group, _ := base.FetchConsumerGroup(ctx, "readers"); group == nil {
				t.Fatal("failed legacy deletion removed group")
			}
			if offset, _, _ := base.FetchConsumerOffset(ctx, "readers", "orders", 0); offset != 42 {
				t.Fatal("failed legacy deletion removed offsets")
			}
		} else if resp.Groups[0].ErrorCode != 0 {
			t.Fatal(resp)
		}
	}
}
