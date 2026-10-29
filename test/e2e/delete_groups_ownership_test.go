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
	"context"
	"encoding/binary"
	"net"
	"os"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Send directly to a chosen broker so client-side coordinator retries cannot
// hide a wrong-broker response.
func directGroupRequest(t *testing.T, addr string, req kmsg.Request) kmsg.Response {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	header := binary.BigEndian.AppendUint16(nil, uint16(req.Key()))
	header = binary.BigEndian.AppendUint16(header, uint16(req.GetVersion()))
	header = binary.BigEndian.AppendUint32(header, 1)
	header = binary.BigEndian.AppendUint16(header, 0xffff) // null client ID
	if req.IsFlexible() {
		header = append(header, 0)
	}
	if err := protocol.WriteFrame(conn, req.AppendTo(header)); err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	body, ok := protocol.SkipResponseHeader(req.Key(), req.GetVersion(), frame.Payload)
	if !ok {
		t.Fatal("invalid response header")
	}
	resp := req.ResponseKind()
	if err := resp.ReadFrom(body); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestDeleteGroupsNonCoordinatorE2E(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E") != "1" {
		t.Skip("set KAFSCALE_E2E=1 to run integration harness")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	etcd, endpoints := startEmbeddedEtcd(t)
	defer etcd.Close()
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	a, logsA := startBrokerWithEtcd(t, ctx, aAddr, freeAddr(t), freeAddr(t), endpoints, "KAFSCALE_BROKER_ID=1")
	defer stopBroker(t, a)
	waitForBroker(t, logsA, aAddr)
	b, logsB := startBrokerWithEtcd(t, ctx, bAddr, freeAddr(t), freeAddr(t), endpoints, "KAFSCALE_BROKER_ID=2")
	defer stopBroker(t, b)
	waitForBroker(t, logsB, bAddr)
	const groupID = "healthy-multibroker"
	join := kmsg.NewPtrJoinGroupRequest()
	join.Group, join.ProtocolType, join.SessionTimeoutMillis = groupID, "consumer", 1000
	join.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range"}}
	joined := directGroupRequest(t, aAddr, join).(*kmsg.JoinGroupResponse)
	if joined.ErrorCode != 0 {
		t.Fatal(joined)
	}
	sync := kmsg.NewPtrSyncGroupRequest()
	sync.Group, sync.MemberID, sync.Generation = groupID, joined.MemberID, joined.Generation
	if resp := directGroupRequest(t, aAddr, sync).(*kmsg.SyncGroupResponse); resp.ErrorCode != 0 {
		t.Fatal(resp)
	}
	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Version, commit.Group, commit.MemberID, commit.Generation = 2, groupID, joined.MemberID, joined.Generation
	commit.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "orders", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 42}}}}
	committed := directGroupRequest(t, aAddr, commit).(*kmsg.OffsetCommitResponse)
	if len(committed.Topics) != 1 || committed.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatal(committed)
	}
	del := kmsg.NewPtrDeleteGroupsRequest()
	del.Groups = []string{groupID}
	if resp := directGroupRequest(t, bAddr, del).(*kmsg.DeleteGroupsResponse); resp.Groups[0].ErrorCode != protocol.NOT_COORDINATOR {
		t.Fatal(resp)
	}
	heartbeat := kmsg.NewPtrHeartbeatRequest()
	heartbeat.Group, heartbeat.MemberID, heartbeat.Generation = groupID, joined.MemberID, joined.Generation
	// Outlast B's five-second cleanup tick and multiple member session timeouts.
	// Only A receives heartbeats; B must never expire a copy of this group.
	until := time.Now().Add(6500 * time.Millisecond)
	for time.Now().Before(until) {
		if resp := directGroupRequest(t, aAddr, heartbeat).(*kmsg.HeartbeatResponse); resp.ErrorCode != 0 {
			t.Fatal(resp)
		}
		time.Sleep(100 * time.Millisecond)
	}
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	group, err := store.FetchConsumerGroup(ctx, groupID)
	if err != nil || group == nil || group.State != "stable" || group.GenerationId != joined.Generation || group.Members[joined.MemberID] == nil {
		t.Fatal(group, err)
	}
	offsets, err := store.ListGroupConsumerOffsets(ctx, groupID)
	if err != nil || len(offsets) != 1 || offsets[0].Offset != 42 {
		t.Fatal(offsets, err)
	}
	if resp := directGroupRequest(t, aAddr, del).(*kmsg.DeleteGroupsResponse); resp.Groups[0].ErrorCode != protocol.NON_EMPTY_GROUP {
		t.Fatal(resp)
	}
	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Group, leave.MemberID = groupID, joined.MemberID
	if resp := directGroupRequest(t, aAddr, leave).(*kmsg.LeaveGroupResponse); resp.ErrorCode != 0 {
		t.Fatal(resp)
	}
	if resp := directGroupRequest(t, aAddr, del).(*kmsg.DeleteGroupsResponse); resp.Groups[0].ErrorCode != 0 {
		t.Fatal(resp)
	}
}
