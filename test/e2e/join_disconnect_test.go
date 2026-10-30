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

//go:build e2e

package e2e

import (
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func testJoinDisconnect(t *testing.T, addr, topic string) {
	t.Helper()
	group := "disconnect-" + topic
	subscription := kmsg.NewConsumerMemberMetadata()
	subscription.Topics = []string{topic}
	join := kmsg.NewPtrJoinGroupRequest()
	join.Version, join.Group, join.ProtocolType = 4, group, "consumer"
	join.SessionTimeoutMillis, join.RebalanceTimeoutMillis = 30000, 30000
	join.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: subscription.AppendTo(nil)}}
	leader := directGroupRequest(t, addr, join).(*kmsg.JoinGroupResponse)
	if leader.ErrorCode != 0 {
		t.Fatalf("initial leader join: %+v", leader)
	}
	sync := kmsg.NewPtrSyncGroupRequest()
	sync.Group, sync.MemberID, sync.Generation = group, leader.MemberID, leader.Generation
	if response := directGroupRequest(t, addr, sync).(*kmsg.SyncGroupResponse); response.ErrorCode != 0 {
		t.Fatalf("initial sync: %+v", response)
	}
	pending, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Close()
	request := kmsg.NewRequestFormatter().AppendRequest(nil, join, 73)
	if _, err := pending.Write(request); err != nil {
		t.Fatal(err)
	}
	waitMembers := func(want int) {
		t.Helper()
		describe := kmsg.NewPtrDescribeGroupsRequest()
		describe.Groups = []string{group}
		for deadline := time.Now().Add(3 * time.Second); ; {
			response := directGroupRequest(t, addr, describe).(*kmsg.DescribeGroupsResponse)
			if len(response.Groups) == 1 && response.Groups[0].ErrorCode == 0 && len(response.Groups[0].Members) == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("expected %d members before session expiry: %+v", want, response)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitMembers(2)
	pending.Close()
	waitMembers(1)
	join.MemberID = leader.MemberID
	rejoined := directGroupRequest(t, addr, join).(*kmsg.JoinGroupResponse)
	if rejoined.ErrorCode != 0 || len(rejoined.Members) != 1 || rejoined.Members[0].MemberID != leader.MemberID {
		t.Fatalf("disconnected member entered generation: %+v", rejoined)
	}
	sync.Generation = rejoined.Generation
	response := directGroupRequest(t, addr, sync).(*kmsg.SyncGroupResponse)
	var assignment kmsg.ConsumerMemberAssignment
	if err := assignment.ReadFrom(response.MemberAssignment); response.ErrorCode != 0 || err != nil || len(assignment.Topics) != 1 || len(assignment.Topics[0].Partitions) != 1 || assignment.Topics[0].Partitions[0] != 0 {
		t.Fatalf("survivor assignment: %+v, error=%d, %v", assignment, response.ErrorCode, err)
	}
}
