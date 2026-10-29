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
	"reflect"
	"testing"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestLeaveGroupVersionedMembers(t *testing.T) {
	for _, version := range []int16{0, 1, 2, 3, 4} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
			c := newTestCoordinator(t, store)
			first := joinTestGroup(t, c, "")
			second := joinTestGroup(t, c, "")
			before := c.groups["readers"].generationID
			req := kmsg.NewPtrLeaveGroupRequest()
			req.Version, req.Group = version, "readers"
			if version < 3 {
				req.MemberID = first.MemberID
			} else {
				req.Members = []kmsg.LeaveGroupRequestMember{{MemberID: first.MemberID}, {MemberID: "missing"}, {MemberID: first.MemberID}}
			}
			resp := c.LeaveGroup(context.Background(), req)
			if resp.ErrorCode != 0 {
				t.Fatal(resp)
			}
			if version >= 3 {
				codes := []int16{}
				for _, member := range resp.Members {
					codes = append(codes, member.ErrorCode)
				}
				if !reflect.DeepEqual(codes, []int16{0, protocol.UNKNOWN_MEMBER_ID, protocol.UNKNOWN_MEMBER_ID}) {
					t.Fatal(resp.Members)
				}
				resp.Version = version
				var decoded kmsg.LeaveGroupResponse
				decoded.Version = version
				if err := decoded.ReadFrom(resp.AppendTo(nil)); err != nil || !reflect.DeepEqual(decoded.Members, resp.Members) {
					t.Fatal("wire response", err, decoded.Members)
				}
			} else if len(resp.Members) != 0 {
				t.Fatal(resp)
			}
			state := c.groups["readers"]
			if len(state.members) != 1 || state.members[second.MemberID] == nil || state.members[first.MemberID] != nil || state.leaderID != second.MemberID || state.generationID != before+1 {
				t.Fatalf("batch transition: %+v", state)
			}
			persisted, err := store.FetchConsumerGroup(context.Background(), "readers")
			if err != nil || len(persisted.Members) != 1 || persisted.Members[second.MemberID] == nil {
				t.Fatal(persisted, err)
			}
			req.MemberID = second.MemberID
			req.Members = []kmsg.LeaveGroupRequestMember{{MemberID: second.MemberID}}
			if resp := c.LeaveGroup(context.Background(), req); resp.ErrorCode != 0 {
				t.Fatal(resp)
			}
			if state := c.groups["readers"]; state == nil || len(state.members) != 0 || state.state != groupStateEmpty || state.generationID != before+2 {
				t.Fatalf("last member transition: %+v", state)
			}
			persisted, err = store.FetchConsumerGroup(context.Background(), "readers")
			if err != nil || persisted == nil || persisted.State != groupStateEmptyStr || len(persisted.Members) != 0 {
				t.Fatal(persisted, err)
			}
			resp = c.LeaveGroup(context.Background(), req)
			if version < 3 {
				if resp.ErrorCode != protocol.UNKNOWN_MEMBER_ID {
					t.Fatal(resp)
				}
			} else if resp.ErrorCode != 0 || len(resp.Members) != 1 || resp.Members[0].ErrorCode != protocol.UNKNOWN_MEMBER_ID {
				t.Fatal(resp)
			}
		})
	}
}

func TestLeaveGroupBatchWithoutKnownMembers(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	c := newTestCoordinator(t, store)
	joined := joinTestGroup(t, c, "")
	before := c.groups["readers"].generationID
	for _, members := range [][]kmsg.LeaveGroupRequestMember{nil, {{MemberID: "missing", InstanceID: kmsg.StringPtr("instance")}}} {
		req := kmsg.NewPtrLeaveGroupRequest()
		req.Version, req.Group, req.Members = 4, "readers", members
		resp := c.LeaveGroup(context.Background(), req)
		if resp.ErrorCode != 0 || len(resp.Members) != len(members) {
			t.Fatal(resp)
		}
		if len(members) > 0 && (resp.Members[0].InstanceID == nil || *resp.Members[0].InstanceID != "instance" || resp.Members[0].ErrorCode != protocol.UNKNOWN_MEMBER_ID) {
			t.Fatal(resp)
		}
		if state := c.groups["readers"]; state.generationID != before || state.members[joined.MemberID] == nil {
			t.Fatal("unknown members triggered rebalance")
		}
	}
}

func TestLeaveGroupRemovesWholeBatch(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	c := newTestCoordinator(t, store)
	first, second := joinTestGroup(t, c, ""), joinTestGroup(t, c, "")
	req := kmsg.NewPtrLeaveGroupRequest()
	req.Version, req.Group = 4, "readers"
	req.Members = []kmsg.LeaveGroupRequestMember{{MemberID: first.MemberID}, {MemberID: second.MemberID}}
	resp := c.LeaveGroup(context.Background(), req)
	if resp.ErrorCode != 0 || len(resp.Members) != 2 || resp.Members[0].ErrorCode != 0 || resp.Members[1].ErrorCode != 0 {
		t.Fatal(resp)
	}
	if state := c.groups["readers"]; state == nil || len(state.members) != 0 || state.state != groupStateEmpty {
		t.Fatalf("batch did not empty group: %+v", state)
	}
	if group, err := store.FetchConsumerGroup(context.Background(), "readers"); err != nil || group == nil || group.State != groupStateEmptyStr || len(group.Members) != 0 {
		t.Fatal(group, err)
	}
}
