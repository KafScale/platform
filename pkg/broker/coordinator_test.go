// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package broker

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

func TestConsumerGroupTimeoutPersistence(t *testing.T) {
	now := time.Now().UTC()
	state := &groupState{
		protocolName:     "range",
		protocolType:     "consumer",
		generationID:     2,
		leaderID:         "member-1",
		state:            groupStateStable,
		members:          make(map[string]*memberState),
		assignments:      make(map[string][]assignmentTopic),
		rebalanceTimeout: 45 * time.Second,
	}
	state.members["member-1"] = &memberState{
		topics:         []string{"orders"},
		sessionTimeout: 20 * time.Second,
		lastHeartbeat:  now,
		joinGeneration: 2,
	}

	group := buildConsumerGroup("group-1", state)
	if group.RebalanceTimeoutMs != 45000 {
		t.Fatalf("expected rebalance timeout 45000ms got %d", group.RebalanceTimeoutMs)
	}
	member := group.Members["member-1"]
	if member == nil {
		t.Fatalf("expected group member persisted")
	}
	if member.SessionTimeoutMs != 20000 {
		t.Fatalf("expected session timeout 20000ms got %d", member.SessionTimeoutMs)
	}

	restored := restoreGroupState(group)
	if restored.rebalanceTimeout != 45*time.Second {
		t.Fatalf("expected rebalance timeout 45s got %s", restored.rebalanceTimeout)
	}
	restoredMember := restored.members["member-1"]
	if restoredMember == nil {
		t.Fatalf("expected restored member")
	}
	if restoredMember.sessionTimeout != 20*time.Second {
		t.Fatalf("expected session timeout 20s got %s", restoredMember.sessionTimeout)
	}
}

func TestCoordinatorListDescribeGroups(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	group := &metadatapb.ConsumerGroup{
		GroupId:      "group-1",
		State:        "stable",
		ProtocolType: "consumer",
		Protocol:     "range",
		Members: map[string]*metadatapb.GroupMember{
			"member-1": {ClientId: "client-1", ClientHost: "127.0.0.1"},
		},
	}
	if err := store.PutConsumerGroup(context.Background(), group); err != nil {
		t.Fatalf("PutConsumerGroup: %v", err)
	}
	coord := NewGroupCoordinator(store, protocol.MetadataBroker{NodeID: 1, Host: "127.0.0.1", Port: 9092}, nil)

	listReq := kmsg.NewPtrListGroupsRequest()
	listReq.StatesFilter = []string{"Stable"}
	listReq.TypesFilter = []string{"classic"}
	listResp, err := coord.ListGroups(context.Background(), listReq)
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	if len(listResp.Groups) != 1 || listResp.Groups[0].Group != "group-1" {
		t.Fatalf("unexpected list response: %#v", listResp.Groups)
	}

	describeReq := kmsg.NewPtrDescribeGroupsRequest()
	describeReq.Groups = []string{"group-1"}
	describeResp, err := coord.DescribeGroups(context.Background(), describeReq)
	if err != nil {
		t.Fatalf("DescribeGroups: %v", err)
	}
	if len(describeResp.Groups) != 1 || describeResp.Groups[0].State != "Stable" {
		t.Fatalf("unexpected describe response: %#v", describeResp.Groups)
	}
}

func TestSubscriptionRoundTrip(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	coord := NewGroupCoordinator(store, protocol.MetadataBroker{}, nil)

	topics := []string{"orders", "events", "metrics"}
	encoded := coord.encodeSubscription(topics)
	parsed := coord.parseSubscriptionTopics([]kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: encoded},
	})
	if len(parsed) != len(topics) {
		t.Fatalf("topic count: got %d want %d", len(parsed), len(topics))
	}
	for i, topic := range topics {
		if parsed[i] != topic {
			t.Fatalf("topic[%d]: got %q want %q", i, parsed[i], topic)
		}
	}
}

func TestParseSubscriptionTopics_Malformed(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	coord := NewGroupCoordinator(store, protocol.MetadataBroker{}, nil)

	if got := coord.parseSubscriptionTopics(nil); got != nil {
		t.Fatalf("expected nil for empty protocols, got %v", got)
	}
	if got := coord.parseSubscriptionTopics([]kmsg.JoinGroupRequestProtocol{
		{Metadata: []byte{0}},
	}); got != nil {
		t.Fatalf("expected nil for too-short metadata, got %v", got)
	}
	if got := coord.parseSubscriptionTopics([]kmsg.JoinGroupRequestProtocol{
		{Metadata: []byte{0, 0, 0}},
	}); got != nil {
		t.Fatalf("expected nil for truncated count, got %v", got)
	}
}

func TestEncodeAssignment(t *testing.T) {
	topics := []assignmentTopic{
		{Name: "orders", Partitions: []int32{0, 1, 2}},
		{Name: "events", Partitions: []int32{0}},
	}
	encoded := encodeAssignment(topics)

	// Parse manually to verify wire format.
	off := 0
	version := int16(binary.BigEndian.Uint16(encoded[off:]))
	off += 2
	if version != 0 {
		t.Fatalf("expected version 0, got %d", version)
	}
	topicCount := int(binary.BigEndian.Uint32(encoded[off:]))
	off += 4
	if topicCount != 2 {
		t.Fatalf("expected 2 topics, got %d", topicCount)
	}
	for _, want := range topics {
		nameLen := int(binary.BigEndian.Uint16(encoded[off:]))
		off += 2
		name := string(encoded[off : off+nameLen])
		off += nameLen
		if name != want.Name {
			t.Fatalf("topic name: got %q want %q", name, want.Name)
		}
		partCount := int(binary.BigEndian.Uint32(encoded[off:]))
		off += 4
		if partCount != len(want.Partitions) {
			t.Fatalf("partition count for %s: got %d want %d", name, partCount, len(want.Partitions))
		}
		for j, wantPart := range want.Partitions {
			gotPart := int32(binary.BigEndian.Uint32(encoded[off:]))
			off += 4
			if gotPart != wantPart {
				t.Fatalf("%s partition[%d]: got %d want %d", name, j, gotPart, wantPart)
			}
		}
	}
}

func TestMemberSubscribes(t *testing.T) {
	member := &memberState{topics: []string{"orders", "events"}}
	if !memberSubscribes(member, "orders") {
		t.Fatal("expected true for subscribed topic")
	}
	if memberSubscribes(member, "metrics") {
		t.Fatal("expected false for unsubscribed topic")
	}
	if memberSubscribes(nil, "orders") {
		t.Fatal("expected false for nil member")
	}
}

func TestGroupState_RebalanceLifecycle(t *testing.T) {
	now := time.Now()
	state := &groupState{
		state:       groupStateStable,
		members:     make(map[string]*memberState),
		assignments: make(map[string][]assignmentTopic),
	}
	state.members["m-1"] = &memberState{
		topics: []string{"orders"}, lastHeartbeat: now, joinGeneration: 0,
	}
	state.members["m-2"] = &memberState{
		topics: []string{"orders"}, lastHeartbeat: now, joinGeneration: 0,
	}

	// startRebalance increments generation, sets preparing state, picks leader.
	state.startRebalance(10 * time.Second)
	if state.state != groupStatePreparingRebalance {
		t.Fatalf("expected PreparingRebalance, got %d", state.state)
	}
	if state.generationID != 1 {
		t.Fatalf("expected generation 1, got %d", state.generationID)
	}
	if state.leaderID == "" {
		t.Fatal("expected leader to be set")
	}
	// Both members should have joinGeneration reset to 0.
	for id, m := range state.members {
		if m.joinGeneration != 0 {
			t.Fatalf("member %s joinGeneration should be 0", id)
		}
	}

	// completeIfReady should return false — no members have joined this generation.
	if state.completeIfReady() {
		t.Fatal("expected completeIfReady=false before members join")
	}

	// Simulate both members joining.
	for _, m := range state.members {
		m.joinGeneration = state.generationID
	}
	if !state.completeIfReady() {
		t.Fatal("expected completeIfReady=true after all members join")
	}
	if state.state != groupStateCompletingRebalance {
		t.Fatalf("expected CompletingRebalance, got %d", state.state)
	}

	// markStable transitions to stable.
	state.markStable()
	if state.state != groupStateStable {
		t.Fatalf("expected Stable, got %d", state.state)
	}
}

func TestGroupState_RemoveExpiredMembers(t *testing.T) {
	now := time.Now()
	state := &groupState{
		state:       groupStateStable,
		leaderID:    "m-1",
		members:     make(map[string]*memberState),
		assignments: make(map[string][]assignmentTopic),
	}
	state.members["m-1"] = &memberState{
		lastHeartbeat: now.Add(-60 * time.Second), sessionTimeout: 30 * time.Second,
	}
	state.members["m-2"] = &memberState{
		lastHeartbeat: now, sessionTimeout: 30 * time.Second,
	}

	changed := state.removeExpiredMembers(now)
	if !changed {
		t.Fatal("expected change after removing expired member")
	}
	if _, ok := state.members["m-1"]; ok {
		t.Fatal("expired member m-1 should be removed")
	}
	if _, ok := state.members["m-2"]; !ok {
		t.Fatal("active member m-2 should remain")
	}
	if state.leaderID != "" {
		t.Fatalf("leader should be cleared when leader is removed, got %q", state.leaderID)
	}
}

func TestGroupState_DropRebalanceLaggers(t *testing.T) {
	now := time.Now()
	state := &groupState{
		state:             groupStatePreparingRebalance,
		generationID:      3,
		rebalanceDeadline: now.Add(-1 * time.Second),
		members:           make(map[string]*memberState),
		assignments:       make(map[string][]assignmentTopic),
	}
	state.members["m-joined"] = &memberState{joinGeneration: 3}
	state.members["m-lagging"] = &memberState{joinGeneration: 2}

	changed := state.dropRebalanceLaggers(now)
	if !changed {
		t.Fatal("expected laggers to be dropped")
	}
	if _, ok := state.members["m-lagging"]; ok {
		t.Fatal("lagging member should be removed")
	}
	if _, ok := state.members["m-joined"]; !ok {
		t.Fatal("joined member should remain")
	}

	// Before deadline: no drops.
	state.members["m-new-lagger"] = &memberState{joinGeneration: 2}
	state.rebalanceDeadline = now.Add(10 * time.Second)
	if state.dropRebalanceLaggers(now) {
		t.Fatal("should not drop before deadline")
	}
}

func TestGroupState_StartRebalance_EmptyGroup(t *testing.T) {
	state := &groupState{
		state:       groupStateStable,
		members:     make(map[string]*memberState),
		assignments: make(map[string][]assignmentTopic),
		leaderID:    "old-leader",
	}
	state.startRebalance(0)
	if state.state != groupStateEmpty {
		t.Fatalf("expected Empty for group with no members, got %d", state.state)
	}
	if state.leaderID != "" {
		t.Fatalf("expected leader cleared, got %q", state.leaderID)
	}
}

func TestSortedMembers(t *testing.T) {
	state := &groupState{
		members: map[string]*memberState{
			"charlie": {},
			"alpha":   {},
			"bravo":   {},
		},
	}
	got := state.sortedMembers()
	want := []string{"alpha", "bravo", "charlie"}
	if len(got) != len(want) {
		t.Fatalf("len: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("member[%d]: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestBumpRebalanceDeadline(t *testing.T) {
	state := &groupState{}
	state.bumpRebalanceDeadline(0)
	if state.rebalanceTimeout != defaultRebalanceTimeout {
		t.Fatalf("expected default timeout, got %s", state.rebalanceTimeout)
	}
	state.bumpRebalanceDeadline(15 * time.Second)
	if state.rebalanceTimeout != 15*time.Second {
		t.Fatalf("expected 15s, got %s", state.rebalanceTimeout)
	}
}

func TestCoordinatorDeleteGroups(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	group := &metadatapb.ConsumerGroup{GroupId: "group-1", State: "stable"}
	if err := store.PutConsumerGroup(context.Background(), group); err != nil {
		t.Fatalf("PutConsumerGroup: %v", err)
	}
	coord := NewGroupCoordinator(store, protocol.MetadataBroker{NodeID: 1, Host: "127.0.0.1", Port: 9092}, nil)

	deleteReq := kmsg.NewPtrDeleteGroupsRequest()
	deleteReq.Groups = []string{"group-1", "missing"}
	resp, err := coord.DeleteGroups(context.Background(), deleteReq)
	if err != nil {
		t.Fatalf("DeleteGroups: %v", err)
	}
	if len(resp.Groups) != 2 {
		t.Fatalf("unexpected delete response: %#v", resp.Groups)
	}
	if resp.Groups[0].ErrorCode != protocol.NONE {
		t.Fatalf("expected delete success: %#v", resp.Groups[0])
	}
	if resp.Groups[1].ErrorCode != protocol.GROUP_ID_NOT_FOUND {
		t.Fatalf("expected group not found: %#v", resp.Groups[1])
	}
	remaining, err := store.FetchConsumerGroup(context.Background(), "group-1")
	if err != nil {
		t.Fatalf("FetchConsumerGroup: %v", err)
	}
	if remaining != nil {
		t.Fatalf("expected group deleted, found: %#v", remaining)
	}
}
