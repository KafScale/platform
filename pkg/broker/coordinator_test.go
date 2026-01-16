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
	"testing"
	"time"

	metadatapb "github.com/novatechflow/kafscale/pkg/gen/metadata"
	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/protocol"
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

	listResp, err := coord.ListGroups(context.Background(), &protocol.ListGroupsRequest{
		StatesFilter: []string{"Stable"},
		TypesFilter:  []string{"classic"},
	}, 1)
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	if len(listResp.Groups) != 1 || listResp.Groups[0].GroupID != "group-1" {
		t.Fatalf("unexpected list response: %#v", listResp.Groups)
	}

	describeResp, err := coord.DescribeGroups(context.Background(), &protocol.DescribeGroupsRequest{
		Groups: []string{"group-1"},
	}, 2)
	if err != nil {
		t.Fatalf("DescribeGroups: %v", err)
	}
	if len(describeResp.Groups) != 1 || describeResp.Groups[0].State != "Stable" {
		t.Fatalf("unexpected describe response: %#v", describeResp.Groups)
	}
}

func TestCoordinatorDeleteGroups(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	group := &metadatapb.ConsumerGroup{GroupId: "group-1", State: "stable"}
	if err := store.PutConsumerGroup(context.Background(), group); err != nil {
		t.Fatalf("PutConsumerGroup: %v", err)
	}
	coord := NewGroupCoordinator(store, protocol.MetadataBroker{NodeID: 1, Host: "127.0.0.1", Port: 9092}, nil)

	resp, err := coord.DeleteGroups(context.Background(), &protocol.DeleteGroupsRequest{
		Groups: []string{"group-1", "missing"},
	}, 3)
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
