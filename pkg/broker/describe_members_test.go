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
	"bytes"
	"context"
	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	"testing"
)

func TestDescribeGroupMemberDetailsSurviveRestart(t *testing.T) {
	for _, backend := range []string{"memory", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			ctx := ContextWithConnInfo(context.Background(), &ConnContext{Principal: "service", RemoteAddr: "198.51.100.8:1234"})
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
			subscription := kmsg.NewConsumerMemberMetadata()
			subscription.Topics = []string{"orders"}
			subscription.UserData = []byte("client-specific-subscription")
			raw := subscription.AppendTo(nil)
			expected := append([]byte(nil), raw...)
			req := kmsg.NewPtrJoinGroupRequest()
			req.Version, req.Group, req.ProtocolType = 5, "readers", "consumer"
			req.SessionTimeoutMillis = 27000
			req.RebalanceTimeoutMillis = 17000
			req.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: raw}}
			joined, err := c.JoinGroupWithClient(ctx, req, "display-client")
			if err != nil || joined.ErrorCode != 0 || !bytes.Equal(joined.Members[0].ProtocolMetadata, expected) {
				t.Fatal(joined, err)
			}
			raw[len(raw)-1] ^= 1 // Joining must not retain the request buffer.
			sync := kmsg.NewPtrSyncGroupRequest()
			sync.Group, sync.MemberID, sync.Generation = "readers", joined.MemberID, joined.Generation
			synced, err := c.SyncGroup(ctx, sync)
			if err != nil || synced.ErrorCode != 0 {
				t.Fatal(synced, err)
			}
			for _, coordinator := range []*GroupCoordinator{c, newTestCoordinator(t, store)} {
				describe := kmsg.NewPtrDescribeGroupsRequest()
				describe.Groups = []string{"readers"}
				resp, err := coordinator.DescribeGroups(ctx, describe)
				if err != nil || len(resp.Groups) != 1 || len(resp.Groups[0].Members) != 1 {
					t.Fatal(resp, err)
				}
				member := resp.Groups[0].Members[0]
				if member.ClientID != "display-client" || member.ClientHost != "198.51.100.8" || !bytes.Equal(member.ProtocolMetadata, expected) || !bytes.Equal(member.MemberAssignment, synced.MemberAssignment) {
					t.Fatalf("member details: %+v", member)
				}
				var decoded kmsg.ConsumerMemberAssignment
				if err := decoded.ReadFrom(member.MemberAssignment); err != nil || len(decoded.Topics) != 1 || decoded.Topics[0].Topic != "orders" || len(decoded.Topics[0].Partitions) != 1 {
					t.Fatal(decoded, err)
				}
				// Exercise a persistence write after restore too.
				if after, err := coordinator.SyncGroup(ctx, sync); err != nil || after.ErrorCode != 0 || !bytes.Equal(after.MemberAssignment, synced.MemberAssignment) {
					t.Fatal(after, err)
				}
			}
			persisted, err := store.FetchConsumerGroup(ctx, "readers")
			if err != nil {
				t.Fatal(err)
			}
			if persisted.RebalanceTimeoutMs != 17000 || persisted.Members[joined.MemberID].SessionTimeoutMs != 27000 {
				t.Fatal("clone lost timeout fields", persisted)
			}
			persisted.Members[joined.MemberID].ProtocolMetadata[0] ^= 1
			persisted.Members[joined.MemberID].MemberAssignment[0] ^= 1
			untouched, err := store.FetchConsumerGroup(ctx, "readers")
			if err != nil || !bytes.Equal(untouched.Members[joined.MemberID].ProtocolMetadata, expected) || !bytes.Equal(untouched.Members[joined.MemberID].MemberAssignment, synced.MemberAssignment) {
				t.Fatal("store returned aliased bytes", err)
			}
			// Rebalancing must not report the old assignment as current ownership.
			other := kmsg.NewPtrJoinGroupRequest()
			other.Group, other.ProtocolType = "readers", "consumer"
			other.RebalanceTimeoutMillis = 1
			other.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: expected}}
			if _, err := c.JoinGroup(ctx, other); err != nil {
				t.Fatal(err)
			}
			describe := kmsg.NewPtrDescribeGroupsRequest()
			describe.Groups = []string{"readers"}
			rebalancing, err := c.DescribeGroups(ctx, describe)
			if err != nil {
				t.Fatal(err)
			}
			for _, member := range rebalancing.Groups[0].Members {
				if len(member.MemberAssignment) != 0 {
					t.Fatal("stale rebalance assignment", member)
				}
			}
		})
	}
}

func TestDescribeLegacyMemberDetails(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	seedCoordinatorGroup(t, store, groupStateStableStr)
	group, err := store.FetchConsumerGroup(context.Background(), "readers")
	if err != nil {
		t.Fatal(err)
	}
	group.Members["a"].Assignments = []*metadatapb.Assignment{{Topic: "orders", Partitions: []int32{0}}}
	described := buildDescribeGroup(group, false)
	var subscription kmsg.ConsumerMemberMetadata
	var assignment kmsg.ConsumerMemberAssignment
	if err := subscription.ReadFrom(described.Members[0].ProtocolMetadata); err != nil || len(subscription.Topics) != 1 {
		t.Fatal(subscription, err)
	}
	if err := assignment.ReadFrom(described.Members[0].MemberAssignment); err != nil || len(assignment.Topics) != 1 {
		t.Fatal(assignment, err)
	}
}
