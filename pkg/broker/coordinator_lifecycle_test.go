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
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func newTestCoordinator(t *testing.T, store metadata.Store) *GroupCoordinator {
	t.Helper()
	c := NewGroupCoordinator(store, protocol.MetadataBroker{NodeID: 7, Host: "broker", Port: 9092}, &CoordinatorConfig{CleanupInterval: time.Hour})
	t.Cleanup(c.Stop)
	return c
}

func joinTestGroup(t *testing.T, c *GroupCoordinator, member string) *kmsg.JoinGroupResponse {
	t.Helper()
	req := kmsg.NewPtrJoinGroupRequest()
	req.Group, req.MemberID, req.ProtocolType = "readers", member, "consumer"
	req.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: c.encodeSubscription([]string{"orders"})}}
	resp, err := c.JoinGroup(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func syncTestGroup(t *testing.T, c *GroupCoordinator, member string, generation int32, code int16) *kmsg.SyncGroupResponse {
	t.Helper()
	req := kmsg.NewPtrSyncGroupRequest()
	req.Group, req.MemberID, req.Generation = "readers", member, generation
	resp, err := c.SyncGroup(context.Background(), req)
	if err != nil || resp.ErrorCode != code {
		t.Fatalf("SyncGroup: response=%+v error=%v; want code %d", resp, err, code)
	}
	return resp
}

func TestCoordinatorConsumerLifecycle(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{Topics: []protocol.MetadataTopic{{
		Topic: kmsg.StringPtr("orders"), Partitions: []protocol.MetadataPartition{{Partition: 3}, {Partition: 1}, {Partition: 0}, {Partition: 2}},
	}}})
	c := newTestCoordinator(t, store)
	location := c.FindCoordinatorResponse(protocol.NONE)
	if location.NodeID != 7 || location.Host != "broker" || location.Port != 9092 || location.ErrorCode != protocol.NONE {
		t.Fatalf("coordinator location: %+v", location)
	}
	first := joinTestGroup(t, c, "")
	if first.ErrorCode != protocol.NONE || first.MemberID == "" || first.LeaderID != first.MemberID || len(first.Members) != 1 {
		t.Fatalf("first join: %+v", first)
	}
	initial := syncTestGroup(t, c, first.MemberID, first.Generation, protocol.NONE)
	var assignment kmsg.ConsumerMemberAssignment
	if err := assignment.ReadFrom(initial.MemberAssignment); err != nil || len(assignment.Topics) != 1 || len(assignment.Topics[0].Partitions) != 4 {
		t.Fatalf("initial assignment: %+v, %v", assignment, err)
	}
	second := joinTestGroup(t, c, "")
	if second.ErrorCode != protocol.REBALANCE_IN_PROGRESS || second.Generation <= first.Generation || len(second.Members) != 0 {
		t.Fatalf("new member must trigger rebalance: %+v", second)
	}
	syncTestGroup(t, c, second.MemberID, second.Generation, protocol.REBALANCE_IN_PROGRESS)
	rejoined := joinTestGroup(t, c, first.MemberID)
	if rejoined.ErrorCode != protocol.NONE || len(rejoined.Members) != 2 {
		t.Fatalf("leader rejoin: %+v", rejoined)
	}
	for _, m := range rejoined.Members {
		if got := c.parseSubscriptionTopics([]kmsg.JoinGroupRequestProtocol{{Metadata: m.ProtocolMetadata}}); !reflect.DeepEqual(got, []string{"orders"}) {
			t.Fatalf("member %s subscription: %v", m.MemberID, got)
		}
	}
	syncTestGroup(t, c, second.MemberID, second.Generation, protocol.REBALANCE_IN_PROGRESS)
	var assigned []int32
	for _, id := range []string{first.MemberID, second.MemberID} {
		resp := syncTestGroup(t, c, id, second.Generation, protocol.NONE)
		if resp.Protocol == nil || *resp.Protocol != "range" || resp.ProtocolType == nil || *resp.ProtocolType != "consumer" {
			t.Fatalf("sync protocol: %+v", resp)
		}
		var a kmsg.ConsumerMemberAssignment
		if err := a.ReadFrom(resp.MemberAssignment); err != nil || len(a.Topics) != 1 || len(a.Topics[0].Partitions) != 2 {
			t.Fatalf("balanced assignment: %+v, %v", a, err)
		}
		assigned = append(assigned, a.Topics[0].Partitions...)
	}
	sort.Slice(assigned, func(i, j int) bool { return assigned[i] < assigned[j] })
	if !reflect.DeepEqual(assigned, []int32{0, 1, 2, 3}) {
		t.Fatalf("partitions must be assigned exactly once: %v", assigned)
	}
	heartbeat := kmsg.NewPtrHeartbeatRequest()
	heartbeat.Group, heartbeat.MemberID, heartbeat.Generation = "readers", first.MemberID, second.Generation
	if resp := c.Heartbeat(ctx, heartbeat); resp.ErrorCode != protocol.NONE {
		t.Fatalf("heartbeat: %+v", resp)
	}
	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group, commit.MemberID, commit.Generation = "readers", first.MemberID, second.Generation
	commit.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "orders", Partitions: []kmsg.OffsetCommitRequestTopicPartition{
		{Partition: 0, Offset: 42, Metadata: kmsg.StringPtr("checkpoint")}, {Partition: 1, Offset: 17},
	}}}
	committed, err := c.OffsetCommit(ctx, commit)
	if err != nil || len(committed.Topics) != 1 || len(committed.Topics[0].Partitions) != 2 {
		t.Fatalf("commit: %+v, %v", committed, err)
	}
	for _, p := range committed.Topics[0].Partitions {
		if p.ErrorCode != protocol.NONE {
			t.Fatalf("commit partition: %+v", p)
		}
	}
	// A replacement coordinator must recover membership, assignments, and offsets from the store.
	restarted := newTestCoordinator(t, store)
	if resp := restarted.Heartbeat(ctx, heartbeat); resp.ErrorCode != protocol.NONE {
		t.Fatalf("restored heartbeat: %+v", resp)
	}
	syncTestGroup(t, restarted, first.MemberID, second.Generation, protocol.NONE)
	fetch := kmsg.NewPtrOffsetFetchRequest()
	fetch.Group, fetch.Topics = "readers", []kmsg.OffsetFetchRequestTopic{{Topic: "orders", Partitions: []int32{0, 1, 9}}}
	fetched, err := restarted.OffsetFetch(ctx, fetch)
	if err != nil || len(fetched.Topics) != 1 || len(fetched.Topics[0].Partitions) != 3 {
		t.Fatalf("fetch: %+v, %v", fetched, err)
	}
	for i, want := range []int64{42, 17, 0} {
		p := fetched.Topics[0].Partitions[i]
		if p.Offset != want || p.ErrorCode != protocol.NONE || p.LeaderEpoch != -1 || p.Metadata == nil {
			t.Fatalf("fetched partition: %+v, want offset %d", p, want)
		}
	}
	if *fetched.Topics[0].Partitions[0].Metadata != "checkpoint" || *fetched.Topics[0].Partitions[1].Metadata != "" {
		t.Fatal("offset metadata did not survive restart")
	}
	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Group, leave.MemberID = "readers", first.MemberID
	if resp := restarted.LeaveGroup(ctx, leave); resp.ErrorCode != protocol.NONE {
		t.Fatalf("leader leave: %+v", resp)
	}
	remaining := joinTestGroup(t, restarted, second.MemberID)
	if remaining.ErrorCode != protocol.NONE || remaining.LeaderID != second.MemberID {
		t.Fatalf("remaining member should become leader: %+v", remaining)
	}
	syncTestGroup(t, restarted, second.MemberID, remaining.Generation, protocol.NONE)
	leave.MemberID = second.MemberID
	if resp := restarted.LeaveGroup(ctx, leave); resp.ErrorCode != protocol.NONE {
		t.Fatalf("last member leave: %+v", resp)
	}
	if group, err := store.FetchConsumerGroup(ctx, "readers"); err != nil || group == nil || group.State != groupStateEmptyStr || len(group.Members) != 0 {
		t.Fatalf("empty group should be retained: %+v, %v", group, err)
	}
}

// Embedding the real store keeps failure tests on the same persistence contract.
type coordinatorFaultStore struct {
	metadata.Store
	fail string
}

var errCoordinatorStore = errors.New("coordinator store unavailable")

func (s coordinatorFaultStore) FetchConsumerGroup(ctx context.Context, group string) (*metadatapb.ConsumerGroup, error) {
	if s.fail == "fetch" {
		return nil, errCoordinatorStore
	}
	return s.Store.FetchConsumerGroup(ctx, group)
}

func (s coordinatorFaultStore) PutConsumerGroup(ctx context.Context, group *metadatapb.ConsumerGroup) error {
	if s.fail == "put" {
		return errCoordinatorStore
	}
	return s.Store.PutConsumerGroup(ctx, group)
}

func (s coordinatorFaultStore) DeleteConsumerGroup(ctx context.Context, group string) error {
	if s.fail == "delete" {
		return errCoordinatorStore
	}
	return s.Store.DeleteConsumerGroup(ctx, group)
}

func (s coordinatorFaultStore) ListConsumerGroups(ctx context.Context) ([]*metadatapb.ConsumerGroup, error) {
	if s.fail == "list" {
		return nil, errCoordinatorStore
	}
	return s.Store.ListConsumerGroups(ctx)
}

func (s coordinatorFaultStore) CommitConsumerOffset(ctx context.Context, group, topic string, partition int32, offset int64, meta string) error {
	if s.fail == "commit" {
		return errCoordinatorStore
	}
	return s.Store.CommitConsumerOffset(ctx, group, topic, partition, offset, meta)
}

func (s coordinatorFaultStore) FetchConsumerOffset(ctx context.Context, group, topic string, partition int32) (int64, string, error) {
	if s.fail == "offset" {
		return -1, "", errCoordinatorStore
	}
	return s.Store.FetchConsumerOffset(ctx, group, topic, partition)
}

func (s coordinatorFaultStore) Metadata(ctx context.Context, topics []string) (*metadata.ClusterMetadata, error) {
	if s.fail == "metadata" {
		return nil, errCoordinatorStore
	}
	if s.fail == "nil-metadata" {
		return nil, nil
	}
	return s.Store.Metadata(ctx, topics)
}

func seedCoordinatorGroup(t *testing.T, store metadata.Store, phase string) {
	t.Helper()
	group := &metadatapb.ConsumerGroup{
		GroupId: "readers", State: phase, ProtocolType: "consumer", Protocol: "range", GenerationId: 4, Leader: "a",
		Members: map[string]*metadatapb.GroupMember{
			"a": {Subscriptions: []string{"orders"}, HeartbeatAt: time.Now().UTC().Format(time.RFC3339Nano)},
		},
	}
	if err := store.PutConsumerGroup(context.Background(), group); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorRejectsInvalidMembership(t *testing.T) {
	for _, tc := range []struct {
		name, phase, member string
		generation          int32
		want                int16
	}{
		{"missing group", "", "a", 4, protocol.UNKNOWN_MEMBER_ID},
		{"unknown member", groupStateStableStr, "unknown", 4, protocol.UNKNOWN_MEMBER_ID},
		{"old generation", groupStateStableStr, "a", 3, protocol.ILLEGAL_GENERATION},
		{"preparing rebalance", groupStatePreparingStr, "a", 4, protocol.REBALANCE_IN_PROGRESS},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
			if tc.phase != "" {
				seedCoordinatorGroup(t, store, tc.phase)
			}
			c := newTestCoordinator(t, store)
			syncTestGroup(t, c, tc.member, tc.generation, tc.want)
			hb := kmsg.NewPtrHeartbeatRequest()
			hb.Group, hb.MemberID, hb.Generation = "readers", tc.member, tc.generation
			if resp := c.Heartbeat(context.Background(), hb); resp.ErrorCode != tc.want {
				t.Fatalf("heartbeat: %+v; want %d", resp, tc.want)
			}
			commit := kmsg.NewPtrOffsetCommitRequest()
			commit.Group, commit.MemberID, commit.Generation = "readers", tc.member, tc.generation
			commit.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "orders", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 99}}}}
			resp, err := c.OffsetCommit(context.Background(), commit)
			want := tc.want
			if tc.phase == groupStatePreparingStr {
				want = protocol.NONE // Existing generations may commit while rebalancing.
			}
			if err != nil || resp.Topics[0].Partitions[0].ErrorCode != want {
				t.Fatalf("commit: %+v, %v; want %d", resp, err, want)
			}
			if want != protocol.NONE {
				if offset, _, _ := store.FetchConsumerOffset(context.Background(), "readers", "orders", 0); offset != 0 {
					t.Fatalf("rejected commit changed stored offset to %d", offset)
				}
			}
			if tc.want == protocol.UNKNOWN_MEMBER_ID {
				leave := kmsg.NewPtrLeaveGroupRequest()
				leave.Group, leave.MemberID = "readers", tc.member
				if resp := c.LeaveGroup(context.Background(), leave); resp.ErrorCode != tc.want {
					t.Fatalf("leave: %+v", resp)
				}
			}
		})
	}
}

func TestCoordinatorStoreFailures(t *testing.T) {
	ctx := context.Background()
	for _, fail := range []string{"fetch", "put", "delete", "list", "commit", "offset"} {
		t.Run(fail, func(t *testing.T) {
			store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
			seedCoordinatorGroup(t, store, groupStateStableStr)
			c := newTestCoordinator(t, coordinatorFaultStore{Store: store, fail: fail})
			switch fail {
			case "fetch":
				join := kmsg.NewPtrJoinGroupRequest()
				join.Group = "readers"
				if _, err := c.JoinGroup(ctx, join); !errors.Is(err, errCoordinatorStore) {
					t.Fatalf("join error: %v", err)
				}
				syncReq := kmsg.NewPtrSyncGroupRequest()
				syncReq.Group = "readers"
				if _, err := c.SyncGroup(ctx, syncReq); !errors.Is(err, errCoordinatorStore) {
					t.Fatalf("sync error: %v", err)
				}
				commit := kmsg.NewPtrOffsetCommitRequest()
				commit.Group = "readers"
				if _, err := c.OffsetCommit(ctx, commit); !errors.Is(err, errCoordinatorStore) {
					t.Fatalf("commit error: %v", err)
				}
				hb := kmsg.NewPtrHeartbeatRequest()
				hb.Group = "readers"
				if resp := c.Heartbeat(ctx, hb); resp.ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
					t.Fatalf("heartbeat: %+v", resp)
				}
				leave := kmsg.NewPtrLeaveGroupRequest()
				leave.Group = "readers"
				if resp := c.LeaveGroup(ctx, leave); resp.ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
					t.Fatalf("leave: %+v", resp)
				}
				describe := kmsg.NewPtrDescribeGroupsRequest()
				describe.Groups = []string{"readers"}
				if resp, err := c.DescribeGroups(ctx, describe); err != nil || resp.Groups[0].ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
					t.Fatalf("describe: %+v, %v", resp, err)
				}
				fallthrough
			case "delete":
				req := kmsg.NewPtrDeleteGroupsRequest()
				req.Groups = []string{"readers"}
				if resp, err := c.DeleteGroups(ctx, req); err != nil || resp.Groups[0].ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
					t.Fatalf("delete: %+v, %v", resp, err)
				}
			case "put":
				leave := kmsg.NewPtrLeaveGroupRequest()
				leave.Group, leave.MemberID = "readers", "a"
				if resp := c.LeaveGroup(ctx, leave); resp.ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
					t.Fatalf("last member leave: %+v", resp)
				}
				// Reload the original persisted membership after the failed transition.
				c = newTestCoordinator(t, coordinatorFaultStore{Store: store, fail: fail})
				if resp := joinTestGroup(t, c, "a"); resp.ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
					t.Fatalf("join persistence: %+v", resp)
				}
				syncTestGroup(t, c, "a", 4, protocol.UNKNOWN_SERVER_ERROR)
				hb := kmsg.NewPtrHeartbeatRequest()
				hb.Group, hb.MemberID, hb.Generation = "readers", "a", 4
				if resp := c.Heartbeat(ctx, hb); resp.ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
					t.Fatalf("heartbeat persistence: %+v", resp)
				}
			case "list":
				if resp, err := c.ListGroups(ctx, kmsg.NewPtrListGroupsRequest()); err != nil || resp.ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
					t.Fatalf("list: %+v, %v", resp, err)
				}
			case "commit":
				req := kmsg.NewPtrOffsetCommitRequest()
				req.Group, req.MemberID, req.Generation = "readers", "a", 4
				req.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "orders", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 10}}}}
				if resp, err := c.OffsetCommit(ctx, req); err != nil || resp.Topics[0].Partitions[0].ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
					t.Fatalf("commit: %+v, %v", resp, err)
				}
				if offset, _, _ := store.FetchConsumerOffset(ctx, "readers", "orders", 0); offset != 0 {
					t.Fatalf("failed commit changed stored offset to %d", offset)
				}
			case "offset":
				req := kmsg.NewPtrOffsetFetchRequest()
				req.Group, req.Topics = "readers", []kmsg.OffsetFetchRequestTopic{{Topic: "orders", Partitions: []int32{0}}}
				if resp, err := c.OffsetFetch(ctx, req); err != nil || resp.Topics[0].Partitions[0].ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
					t.Fatalf("offset fetch: %+v, %v", resp, err)
				}
			}
		})
	}
}

func TestCoordinatorGroupDiscovery(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	c := newTestCoordinator(t, store)
	states := []struct{ stored, kafka string }{
		{groupStateEmptyStr, "Empty"}, {groupStatePreparingStr, "PreparingRebalance"},
		{groupStateCompletingStr, "CompletingRebalance"}, {groupStateStableStr, "Stable"},
		{groupStateDeadStr, "Dead"}, {"custom", "custom"},
	}
	for _, state := range states {
		if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: state.stored, State: state.stored}); err != nil {
			t.Fatal(err)
		}
	}
	listed, err := c.ListGroups(ctx, kmsg.NewPtrListGroupsRequest())
	if err != nil || len(listed.Groups) != len(states) {
		t.Fatalf("list: %+v, %v", listed, err)
	}
	for _, state := range states {
		t.Run(state.stored, func(t *testing.T) {
			req := kmsg.NewPtrDescribeGroupsRequest()
			req.Groups, req.IncludeAuthorizedOperations = []string{state.stored}, true
			resp, err := c.DescribeGroups(ctx, req)
			if err != nil || len(resp.Groups) != 1 {
				t.Fatalf("describe: %+v, %v", resp, err)
			}
			group := resp.Groups[0]
			if group.State != state.kafka || group.ProtocolType != "consumer" || group.AuthorizedOperations != 0 {
				t.Fatalf("described group: %+v", group)
			}
			for _, listed := range listed.Groups {
				if listed.Group == state.stored && (listed.GroupState != state.kafka || listed.ProtocolType != "consumer" || listed.GroupType != "classic") {
					t.Fatalf("listed group: %+v", listed)
				}
			}
		})
	}
	for _, tc := range []struct {
		states, types []string
		want          []string
	}{
		{[]string{"PREPARING_REBALANCE"}, []string{"CLASSIC"}, []string{groupStatePreparingStr}},
		{nil, []string{"consumer"}, []string{}},
		{[]string{"not-a-state"}, nil, []string{}},
	} {
		req := kmsg.NewPtrListGroupsRequest()
		req.StatesFilter, req.TypesFilter = tc.states, tc.types
		resp, err := c.ListGroups(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]string, 0, len(resp.Groups))
		for _, group := range resp.Groups {
			got = append(got, group.Group)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("filters %v/%v: got %v, want %v", tc.states, tc.types, got, tc.want)
		}
	}
	describe := kmsg.NewPtrDescribeGroupsRequest()
	describe.Groups = []string{"missing"}
	if resp, err := c.DescribeGroups(ctx, describe); err != nil || resp.Groups[0].ErrorCode != protocol.GROUP_ID_NOT_FOUND {
		t.Fatalf("missing group: %+v, %v", resp, err)
	}
	del := kmsg.NewPtrDeleteGroupsRequest()
	del.Groups = []string{" \t"}
	if resp, err := c.DeleteGroups(ctx, del); err != nil || resp.Groups[0].ErrorCode != protocol.INVALID_REQUEST {
		t.Fatalf("blank group ID: %+v, %v", resp, err)
	}
}

func TestCoordinatorAssignmentFallback(t *testing.T) {
	for _, failure := range []string{"metadata", "nil-metadata", "empty-partitions"} {
		t.Run(failure, func(t *testing.T) {
			store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
			c := newTestCoordinator(t, coordinatorFaultStore{Store: store, fail: failure})
			state := &groupState{members: map[string]*memberState{
				"a":    {topics: []string{"orders", "events"}},
				"b":    {topics: []string{"orders"}},
				"idle": {},
			}}
			got := c.assignPartitions(context.Background(), state)
			want := map[string][]assignmentTopic{
				"a": {{Name: "events", Partitions: []int32{0}}, {Name: "orders", Partitions: []int32{0}}},
				"b": nil, "idle": nil,
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("fallback must retain subscriptions and deterministic assignment: got %+v, want %+v", got, want)
			}
			if got := c.assignPartitions(context.Background(), &groupState{}); len(got) != 0 {
				t.Fatalf("empty group assignment: %v", got)
			}
		})
	}
}

func TestCoordinatorCleanupExpiredMembers(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	c := newTestCoordinator(t, store)
	// Control time through stored heartbeat timestamps rather than waiting for a session timeout.
	c.Stop()
	c.Stop() // Shutdown is idempotent.
	now := time.Now()
	for _, group := range []string{"expired", "mixed", "lagging", "healthy"} {
		seed := &metadatapb.ConsumerGroup{
			GroupId: group, State: groupStateStableStr, Leader: "a", GenerationId: 4,
			Members: map[string]*metadatapb.GroupMember{
				"a": {HeartbeatAt: now.Add(-time.Hour).UTC().Format(time.RFC3339Nano)},
			},
		}
		if group != "expired" {
			seed.Members["b"] = &metadatapb.GroupMember{HeartbeatAt: now.UTC().Format(time.RFC3339Nano)}
		}
		if group == "lagging" || group == "healthy" {
			seed.Members["a"].HeartbeatAt = now.UTC().Format(time.RFC3339Nano)
		}
		if err := store.PutConsumerGroup(ctx, seed); err != nil {
			t.Fatal(err)
		}
		state, err := c.loadGroupIfMissing(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		if group == "lagging" {
			state.state = groupStatePreparingRebalance
			state.rebalanceDeadline = now.Add(-time.Second)
			state.members["a"].joinGeneration = 3
		}
	}
	c.cleanupGroups()
	if group, err := store.FetchConsumerGroup(ctx, "expired"); err != nil || group == nil || group.State != groupStateEmptyStr || len(group.Members) != 0 || group.GenerationId != 5 || group.Leader != "" {
		t.Fatalf("expired group not Empty: %+v, %v", group, err)
	}
	for _, id := range []string{"mixed", "lagging"} {
		group, err := store.FetchConsumerGroup(ctx, id)
		if err != nil || group == nil || len(group.Members) != 1 || group.Members["b"] == nil || group.Leader != "b" || group.GenerationId != 5 || group.State != groupStatePreparingStr {
			t.Fatalf("%s must evict a, elect b, and rebalance: %+v, %v", id, group, err)
		}
	}
	if state := c.groups["healthy"]; state.generationID != 4 || state.state != groupStateStable || len(state.members) != 2 {
		t.Fatalf("healthy group changed: %+v", state)
	}
}

func TestCoordinatorBackgroundCleanup(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{
		GroupId: "expired", State: groupStateStableStr,
		Members: map[string]*metadatapb.GroupMember{"a": {HeartbeatAt: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)}},
	}); err != nil {
		t.Fatal(err)
	}
	c := NewGroupCoordinator(store, protocol.MetadataBroker{}, &CoordinatorConfig{CleanupInterval: time.Millisecond})
	t.Cleanup(c.Stop)
	c.mu.Lock()
	_, err := c.loadGroupIfMissing(ctx, "expired")
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		group, err := store.FetchConsumerGroup(ctx, "expired")
		if err != nil {
			t.Fatal(err)
		}
		if group != nil && group.State == groupStateEmptyStr && len(group.Members) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("background cleanup did not empty the expired group")
}
