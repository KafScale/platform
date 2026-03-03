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

package main

import (
	"context"
	"net"
	"testing"

	"github.com/KafScale/platform/pkg/broker"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestACLProduceDenied(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[{"action":"fetch","resource":"topic","name":"orders"}]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &kmsg.ProduceRequest{
		Acks:          -1,
		TimeoutMillis: 1000,
		Topics: []kmsg.ProduceRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{Partition: 0, Records: testBatchBytes(0, 0, 1)},
				},
			},
		},
	}
	payload, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 1, APIVersion: 0, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	resp := decodeKmsgResponse(t, 0, payload, kmsg.NewPtrProduceResponse)
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("expected single topic/partition response")
	}
	if resp.Topics[0].Partitions[0].ErrorCode != protocol.TOPIC_AUTHORIZATION_FAILED {
		t.Fatalf("expected topic auth failed, got %d", resp.Topics[0].Partitions[0].ErrorCode)
	}
	offset, err := store.NextOffset(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("NextOffset: %v", err)
	}
	if offset != 0 {
		t.Fatalf("expected offset unchanged, got %d", offset)
	}
}

func TestACLJoinGroupDenied(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	joinReq := &kmsg.JoinGroupRequest{Group: "group-a"}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 5, APIVersion: 4, ClientID: &clientID}, joinReq)
	if err != nil {
		t.Fatalf("Handle JoinGroup: %v", err)
	}
	resp := decodeKmsgResponse(t, 4, payload, kmsg.NewPtrJoinGroupResponse)
	if resp.ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatalf("expected group auth failed, got %d", resp.ErrorCode)
	}
}

func TestACLListOffsetsDenied(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &kmsg.ListOffsetsRequest{
		Topics: []kmsg.ListOffsetsRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.ListOffsetsRequestTopicPartition{
					{Partition: 0, Timestamp: -1, MaxNumOffsets: 1},
				},
			},
		},
	}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 7, APIVersion: 4, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle ListOffsets: %v", err)
	}
	resp := decodeKmsgResponse(t, 4, payload, kmsg.NewPtrListOffsetsResponse)
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("expected single topic/partition response")
	}
	if resp.Topics[0].Partitions[0].ErrorCode != protocol.TOPIC_AUTHORIZATION_FAILED {
		t.Fatalf("expected topic auth failed, got %d", resp.Topics[0].Partitions[0].ErrorCode)
	}
}

func TestACLOffsetFetchDenied(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &kmsg.OffsetFetchRequest{
		Group: "group-a",
		Topics: []kmsg.OffsetFetchRequestTopic{
			{
				Topic:      "orders",
				Partitions: []int32{0},
			},
		},
	}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 8, APIVersion: 5, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle OffsetFetch: %v", err)
	}
	resp := decodeKmsgResponse(t, 5, payload, kmsg.NewPtrOffsetFetchResponse)
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("expected single topic/partition response")
	}
	if resp.Topics[0].Partitions[0].ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatalf("expected group auth failed, got %d", resp.Topics[0].Partitions[0].ErrorCode)
	}
	if resp.ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatalf("expected top-level group auth failed, got %d", resp.ErrorCode)
	}
}

func TestACLDescribeGroupsMixed(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[{"action":"group_read","resource":"group","name":"group-allowed"}]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &kmsg.DescribeGroupsRequest{Groups: []string{"group-allowed", "group-denied"}}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 20, APIVersion: 5, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle DescribeGroups: %v", err)
	}
	resp := decodeKmsgResponse(t, 5, payload, kmsg.NewPtrDescribeGroupsResponse)
	if len(resp.Groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(resp.Groups))
	}
	if resp.Groups[0].Group != "group-allowed" || resp.Groups[1].Group != "group-denied" {
		t.Fatalf("unexpected group order: %+v", resp.Groups)
	}
	if resp.Groups[0].ErrorCode == protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatalf("expected allowed group not to be auth failed, got %d", resp.Groups[0].ErrorCode)
	}
	if resp.Groups[1].ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatalf("expected denied group auth failed, got %d", resp.Groups[1].ErrorCode)
	}
}

func TestACLDeleteGroupsMixed(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[{"action":"group_admin","resource":"group","name":"group-allowed"}]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &kmsg.DeleteGroupsRequest{Groups: []string{"group-allowed", "group-denied"}}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 21, APIVersion: 2, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle DeleteGroups: %v", err)
	}
	resp := decodeKmsgResponse(t, 2, payload, kmsg.NewPtrDeleteGroupsResponse)
	if len(resp.Groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(resp.Groups))
	}
	if resp.Groups[0].Group != "group-allowed" || resp.Groups[1].Group != "group-denied" {
		t.Fatalf("unexpected group order: %+v", resp.Groups)
	}
	if resp.Groups[0].ErrorCode == protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatalf("expected allowed group not to be auth failed, got %d", resp.Groups[0].ErrorCode)
	}
	if resp.Groups[1].ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatalf("expected denied group auth failed, got %d", resp.Groups[1].ErrorCode)
	}
}

func TestACLProxyAddrProduceAllowed(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"10.0.0.1","allow":[{"action":"produce","resource":"topic","name":"orders"}]}]}`)
	t.Setenv("KAFSCALE_PRINCIPAL_SOURCE", "proxy_addr")
	t.Setenv("KAFSCALE_PROXY_PROTOCOL", "true")

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	go func() {
		_, _ = peer.Write([]byte("PROXY TCP4 10.0.0.1 10.0.0.2 12345 9092\r\n"))
	}()

	connCtx := buildConnContextFunc(testLogger())
	_, info, err := connCtx(conn)
	if err != nil {
		t.Fatalf("proxy conn context: %v", err)
	}
	ctx := broker.ContextWithConnInfo(context.Background(), info)

	clientID := "spoofed-client"
	req := &kmsg.ProduceRequest{
		Acks:          -1,
		TimeoutMillis: 1000,
		Topics: []kmsg.ProduceRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{Partition: 0, Records: testBatchBytes(0, 0, 1)},
				},
			},
		},
	}
	payload, err := handler.handleProduce(ctx, &protocol.RequestHeader{CorrelationID: 22, APIVersion: 0, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	resp := decodeKmsgResponse(t, 0, payload, kmsg.NewPtrProduceResponse)
	if resp.Topics[0].Partitions[0].ErrorCode != protocol.NONE {
		t.Fatalf("expected produce allowed, got %d", resp.Topics[0].Partitions[0].ErrorCode)
	}
}

func TestACLSyncGroupDenied(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &kmsg.SyncGroupRequest{Group: "group-a", Generation: 1, MemberID: "member-a"}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 9, APIVersion: 4, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle SyncGroup: %v", err)
	}
	resp := decodeKmsgResponse(t, 4, payload, kmsg.NewPtrSyncGroupResponse)
	if resp.ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatalf("expected group auth failed, got %d", resp.ErrorCode)
	}
}

func TestACLHeartbeatDenied(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &kmsg.HeartbeatRequest{Group: "group-a", Generation: 1, MemberID: "member-a"}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 10, APIVersion: 4, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle Heartbeat: %v", err)
	}
	resp := decodeKmsgResponse(t, 4, payload, kmsg.NewPtrHeartbeatResponse)
	if resp.ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatalf("expected group auth failed, got %d", resp.ErrorCode)
	}
}

func TestACLLeaveGroupDenied(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &kmsg.LeaveGroupRequest{Group: "group-a", MemberID: "member-a"}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 11, APIVersion: 0, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle LeaveGroup: %v", err)
	}
	resp := decodeKmsgResponse(t, 0, payload, kmsg.NewPtrLeaveGroupResponse)
	if resp.ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatalf("expected group auth failed, got %d", resp.ErrorCode)
	}
}

func TestACLOffsetCommitDenied(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &kmsg.OffsetCommitRequest{
		Group: "group-a",
		Topics: []kmsg.OffsetCommitRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.OffsetCommitRequestTopicPartition{
					{Partition: 0, Offset: 1, Metadata: kmsg.StringPtr("")},
				},
			},
		},
	}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 12, APIVersion: 3, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle OffsetCommit: %v", err)
	}
	resp := decodeKmsgResponse(t, 3, payload, kmsg.NewPtrOffsetCommitResponse)
	if len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) == 0 {
		t.Fatalf("expected offset commit response")
	}
	if resp.Topics[0].Partitions[0].ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatalf("expected group auth failed, got %d", resp.Topics[0].Partitions[0].ErrorCode)
	}
}

func TestACLCreateTopicsDenied(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &kmsg.CreateTopicsRequest{
		Topics: []kmsg.CreateTopicsRequestTopic{{Topic: "orders", NumPartitions: 1, ReplicationFactor: 1}},
	}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 13, APIVersion: 0, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle CreateTopics: %v", err)
	}
	resp := decodeKmsgResponse(t, 0, payload, kmsg.NewPtrCreateTopicsResponse)
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != protocol.TOPIC_AUTHORIZATION_FAILED {
		t.Fatalf("expected topic auth failed, got %+v", resp.Topics)
	}
}

func TestACLDeleteTopicsDenied(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &kmsg.DeleteTopicsRequest{TopicNames: []string{"orders"}}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 14, APIVersion: 0, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle DeleteTopics: %v", err)
	}
	resp := decodeKmsgResponse(t, 0, payload, kmsg.NewPtrDeleteTopicsResponse)
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != protocol.TOPIC_AUTHORIZATION_FAILED {
		t.Fatalf("expected topic auth failed, got %+v", resp.Topics)
	}
}

func TestACLAlterConfigsDenied(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &kmsg.AlterConfigsRequest{
		Resources: []kmsg.AlterConfigsRequestResource{
			{
				ResourceType: kmsg.ConfigResourceTypeTopic,
				ResourceName: "orders",
			},
		},
	}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 15, APIVersion: 1, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle AlterConfigs: %v", err)
	}
	resp := decodeKmsgResponse(t, 1, payload, kmsg.NewPtrAlterConfigsResponse)
	if len(resp.Resources) != 1 || resp.Resources[0].ErrorCode != protocol.TOPIC_AUTHORIZATION_FAILED {
		t.Fatalf("expected topic auth failed, got %+v", resp.Resources)
	}
}

func TestACLCreatePartitionsDenied(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &kmsg.CreatePartitionsRequest{
		Topics: []kmsg.CreatePartitionsRequestTopic{{Topic: "orders", Count: 2}},
	}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 16, APIVersion: 3, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle CreatePartitions: %v", err)
	}
	resp := decodeKmsgResponse(t, 3, payload, kmsg.NewPtrCreatePartitionsResponse)
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != protocol.TOPIC_AUTHORIZATION_FAILED {
		t.Fatalf("expected topic auth failed, got %+v", resp.Topics)
	}
}

func TestACLDeleteGroupsDenied(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &kmsg.DeleteGroupsRequest{Groups: []string{"group-a"}}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 17, APIVersion: 2, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle DeleteGroups: %v", err)
	}
	resp := decodeKmsgResponse(t, 2, payload, kmsg.NewPtrDeleteGroupsResponse)
	if len(resp.Groups) != 1 || resp.Groups[0].ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatalf("expected group auth failed, got %+v", resp.Groups)
	}
}
