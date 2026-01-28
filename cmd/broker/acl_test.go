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
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"

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
	req := &protocol.ProduceRequest{
		Acks:      -1,
		TimeoutMs: 1000,
		Topics: []protocol.ProduceTopic{
			{
				Name: "orders",
				Partitions: []protocol.ProducePartition{
					{Partition: 0, Records: testBatchBytes(0, 0, 1)},
				},
			},
		},
	}
	payload, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 1, APIVersion: 0, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	resp := decodeProduceResponse(t, payload, 0)
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
	joinReq := &protocol.JoinGroupRequest{GroupID: "group-a"}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 5, APIVersion: 4, ClientID: &clientID}, joinReq)
	if err != nil {
		t.Fatalf("Handle JoinGroup: %v", err)
	}
	resp := decodeJoinGroupResponse(t, payload, 4)
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
	req := &protocol.ListOffsetsRequest{
		Topics: []protocol.ListOffsetsTopic{
			{
				Name: "orders",
				Partitions: []protocol.ListOffsetsPartition{
					{Partition: 0, Timestamp: -1, MaxNumOffsets: 1},
				},
			},
		},
	}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 7, APIVersion: 4, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle ListOffsets: %v", err)
	}
	resp := decodeListOffsetsResponse(t, 4, payload)
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
	req := &protocol.OffsetFetchRequest{
		GroupID: "group-a",
		Topics: []protocol.OffsetFetchTopic{
			{
				Name: "orders",
				Partitions: []protocol.OffsetFetchPartition{
					{Partition: 0},
				},
			},
		},
	}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 8, APIVersion: 5, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle OffsetFetch: %v", err)
	}
	resp := decodeOffsetFetchResponse(t, payload, 5)
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

func TestACLSyncGroupDenied(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"client-a","allow":[]}]}`)

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	clientID := "client-a"
	req := &protocol.SyncGroupRequest{GroupID: "group-a", GenerationID: 1, MemberID: "member-a"}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 9, APIVersion: 4, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle SyncGroup: %v", err)
	}
	resp := decodeSyncGroupResponse(t, payload, 4)
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
	req := &protocol.HeartbeatRequest{GroupID: "group-a", GenerationID: 1, MemberID: "member-a"}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 10, APIVersion: 4, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle Heartbeat: %v", err)
	}
	resp := decodeHeartbeatResponse(t, payload, 4)
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
	req := &protocol.LeaveGroupRequest{GroupID: "group-a", MemberID: "member-a"}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 11, APIVersion: 0, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle LeaveGroup: %v", err)
	}
	resp := decodeLeaveGroupResponse(t, payload)
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
	req := &protocol.OffsetCommitRequest{
		GroupID: "group-a",
		Topics: []protocol.OffsetCommitTopic{
			{
				Name: "orders",
				Partitions: []protocol.OffsetCommitPartition{
					{Partition: 0, Offset: 1, Metadata: ""},
				},
			},
		},
	}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 12, APIVersion: 3, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle OffsetCommit: %v", err)
	}
	resp := decodeOffsetCommitResponse(t, payload, 3)
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
	req := &protocol.CreateTopicsRequest{
		Topics: []protocol.CreateTopicConfig{{Name: "orders", NumPartitions: 1, ReplicationFactor: 1}},
	}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 13, APIVersion: 0, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle CreateTopics: %v", err)
	}
	resp := decodeCreateTopicsResponse(t, payload, 0)
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
	req := &protocol.DeleteTopicsRequest{TopicNames: []string{"orders"}}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 14, APIVersion: 0, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle DeleteTopics: %v", err)
	}
	resp := decodeDeleteTopicsResponse(t, payload, 0)
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
	req := &protocol.AlterConfigsRequest{
		Resources: []protocol.AlterConfigsResource{
			{
				ResourceType: protocol.ConfigResourceTopic,
				ResourceName: "orders",
			},
		},
	}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 15, APIVersion: 1, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle AlterConfigs: %v", err)
	}
	resp := decodeAlterConfigsResponse(t, payload, 1)
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
	req := &protocol.CreatePartitionsRequest{
		Topics: []protocol.CreatePartitionsTopic{{Name: "orders", Count: 2}},
	}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 16, APIVersion: 3, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle CreatePartitions: %v", err)
	}
	resp := decodeCreatePartitionsResponse(t, payload, 3)
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
	req := &protocol.DeleteGroupsRequest{Groups: []string{"group-a"}}
	payload, err := handler.Handle(context.Background(), &protocol.RequestHeader{CorrelationID: 17, APIVersion: 2, ClientID: &clientID}, req)
	if err != nil {
		t.Fatalf("Handle DeleteGroups: %v", err)
	}
	resp := decodeDeleteGroupsResponse(t, payload, 2)
	if len(resp.Groups) != 1 || resp.Groups[0].ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatalf("expected group auth failed, got %+v", resp.Groups)
	}
}

func decodeOffsetFetchResponse(t *testing.T, payload []byte, version int16) *protocol.OffsetFetchResponse {
	t.Helper()
	reader := bytes.NewReader(payload)
	resp := &protocol.OffsetFetchResponse{}
	if err := binary.Read(reader, binary.BigEndian, &resp.CorrelationID); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	if version >= 3 {
		if err := binary.Read(reader, binary.BigEndian, &resp.ThrottleMs); err != nil {
			t.Fatalf("read throttle: %v", err)
		}
	}
	var topicCount int32
	if err := binary.Read(reader, binary.BigEndian, &topicCount); err != nil {
		t.Fatalf("read topic count: %v", err)
	}
	resp.Topics = make([]protocol.OffsetFetchTopicResponse, 0, topicCount)
	for i := 0; i < int(topicCount); i++ {
		name := readKafkaString(t, reader)
		var partCount int32
		if err := binary.Read(reader, binary.BigEndian, &partCount); err != nil {
			t.Fatalf("read partition count: %v", err)
		}
		topic := protocol.OffsetFetchTopicResponse{Name: name}
		topic.Partitions = make([]protocol.OffsetFetchPartitionResponse, 0, partCount)
		for j := 0; j < int(partCount); j++ {
			var part protocol.OffsetFetchPartitionResponse
			if err := binary.Read(reader, binary.BigEndian, &part.Partition); err != nil {
				t.Fatalf("read partition id: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &part.Offset); err != nil {
				t.Fatalf("read offset: %v", err)
			}
			if version >= 5 {
				if err := binary.Read(reader, binary.BigEndian, &part.LeaderEpoch); err != nil {
					t.Fatalf("read leader epoch: %v", err)
				}
			}
			part.Metadata = readKafkaNullableString(t, reader)
			if err := binary.Read(reader, binary.BigEndian, &part.ErrorCode); err != nil {
				t.Fatalf("read error code: %v", err)
			}
			topic.Partitions = append(topic.Partitions, part)
		}
		resp.Topics = append(resp.Topics, topic)
	}
	if version >= 2 {
		if err := binary.Read(reader, binary.BigEndian, &resp.ErrorCode); err != nil {
			t.Fatalf("read error code: %v", err)
		}
	}
	return resp
}

func decodeSyncGroupResponse(t *testing.T, payload []byte, version int16) *kmsg.SyncGroupResponse {
	t.Helper()
	reader := bytes.NewReader(payload)
	var corr int32
	if err := binary.Read(reader, binary.BigEndian, &corr); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	if version >= 4 {
		skipTaggedFields(t, reader)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	resp := kmsg.NewPtrSyncGroupResponse()
	resp.Version = version
	if err := resp.ReadFrom(body); err != nil {
		t.Fatalf("decode sync group response: %v", err)
	}
	return resp
}

func decodeHeartbeatResponse(t *testing.T, payload []byte, version int16) *kmsg.HeartbeatResponse {
	t.Helper()
	reader := bytes.NewReader(payload)
	var corr int32
	if err := binary.Read(reader, binary.BigEndian, &corr); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	if version >= 4 {
		skipTaggedFields(t, reader)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	resp := kmsg.NewPtrHeartbeatResponse()
	resp.Version = version
	if err := resp.ReadFrom(body); err != nil {
		t.Fatalf("decode heartbeat response: %v", err)
	}
	return resp
}

func decodeLeaveGroupResponse(t *testing.T, payload []byte) *protocol.LeaveGroupResponse {
	t.Helper()
	reader := bytes.NewReader(payload)
	resp := &protocol.LeaveGroupResponse{}
	if err := binary.Read(reader, binary.BigEndian, &resp.CorrelationID); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	if err := binary.Read(reader, binary.BigEndian, &resp.ErrorCode); err != nil {
		t.Fatalf("read error code: %v", err)
	}
	return resp
}

func decodeAlterConfigsResponse(t *testing.T, payload []byte, version int16) *protocol.AlterConfigsResponse {
	t.Helper()
	if version != 1 {
		t.Fatalf("alter configs decode only supports version 1")
	}
	reader := bytes.NewReader(payload)
	resp := &protocol.AlterConfigsResponse{}
	if err := binary.Read(reader, binary.BigEndian, &resp.CorrelationID); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	if err := binary.Read(reader, binary.BigEndian, &resp.ThrottleMs); err != nil {
		t.Fatalf("read throttle ms: %v", err)
	}
	var count int32
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		t.Fatalf("read resource count: %v", err)
	}
	resp.Resources = make([]protocol.AlterConfigsResponseResource, 0, count)
	for i := 0; i < int(count); i++ {
		var code int16
		if err := binary.Read(reader, binary.BigEndian, &code); err != nil {
			t.Fatalf("read error code: %v", err)
		}
		msg := readKafkaNullableString(t, reader)
		var rtype int8
		if err := binary.Read(reader, binary.BigEndian, &rtype); err != nil {
			t.Fatalf("read resource type: %v", err)
		}
		name := readKafkaString(t, reader)
		resp.Resources = append(resp.Resources, protocol.AlterConfigsResponseResource{
			ErrorCode:    code,
			ErrorMessage: msg,
			ResourceType: rtype,
			ResourceName: name,
		})
	}
	return resp
}

func readKafkaNullableString(t *testing.T, reader *bytes.Reader) *string {
	t.Helper()
	var length int16
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		t.Fatalf("read nullable string length: %v", err)
	}
	if length < 0 {
		return nil
	}
	buf := make([]byte, length)
	if _, err := reader.Read(buf); err != nil {
		t.Fatalf("read nullable string: %v", err)
	}
	value := string(buf)
	return &value
}
