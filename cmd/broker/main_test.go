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

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/KafScale/platform/pkg/broker"
	controlpb "github.com/KafScale/platform/pkg/gen/control"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
)

func TestHandleProduceAckAll(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	req := &kmsg.ProduceRequest{
		Acks:          -1,
		TimeoutMillis: 1000,
		Topics: []kmsg.ProduceRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{
						Partition: 0,
						Records:   testBatchBytes(0, 0, 1),
					},
				},
			},
		},
	}

	resp, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 1}, req)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected response for acks=-1")
	}

	offset, err := store.NextOffset(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("NextOffset: %v", err)
	}
	if offset != 1 {
		t.Fatalf("expected offset advanced to 1 got %d", offset)
	}
}

func TestHandleProduceEtcdUnavailable(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(unavailableMetadataStore{Store: store})

	req := &kmsg.ProduceRequest{
		Acks:          -1,
		TimeoutMillis: 1000,
		Topics: []kmsg.ProduceRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{
						Partition: 0,
						Records:   testBatchBytes(0, 0, 1),
					},
				},
			},
		},
	}

	resp, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 1}, req)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected response for acks=-1")
	}

	offset, err := store.NextOffset(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("NextOffset: %v", err)
	}
	if offset != 0 {
		t.Fatalf("expected offset unchanged got %d", offset)
	}
}

func TestHandleProduceAckZero(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	req := &kmsg.ProduceRequest{
		Acks:          0,
		TimeoutMillis: 1000,
		Topics: []kmsg.ProduceRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{
						Partition: 0,
						Records:   testBatchBytes(0, 0, 1),
					},
				},
			},
		},
	}

	resp, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 1}, req)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	if resp != nil {
		t.Fatalf("expected no response for acks=0")
	}
}

func TestHandlerApiVersionsUnsupported(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	header := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyApiVersion,
		APIVersion:    5,
		CorrelationID: 42,
	}
	payload, err := handler.Handle(context.Background(), header, kmsg.NewPtrApiVersionsRequest())
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	reader := bytes.NewReader(payload)
	var (
		corr      int32
		errorCode int16
	)
	if err := binary.Read(reader, binary.BigEndian, &corr); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	if err := binary.Read(reader, binary.BigEndian, &errorCode); err != nil {
		t.Fatalf("read error code: %v", err)
	}
	if corr != 42 {
		t.Fatalf("expected correlation id 42 got %d", corr)
	}
	if errorCode != protocol.UNSUPPORTED_VERSION {
		t.Fatalf("expected UNSUPPORTED_VERSION (%d) got %d", protocol.UNSUPPORTED_VERSION, errorCode)
	}
}

func TestHandleFetch(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	produceReq := &kmsg.ProduceRequest{
		Acks:          -1,
		TimeoutMillis: 1000,
		Topics: []kmsg.ProduceRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{
						Partition: 0,
						Records:   testBatchBytes(0, 0, 1),
					},
				},
			},
		},
	}
	if _, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 1}, produceReq); err != nil {
		t.Fatalf("handleProduce: %v", err)
	}

	fetchReq := &kmsg.FetchRequest{
		Topics: []kmsg.FetchRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.FetchRequestTopicPartition{
					{
						Partition:         0,
						FetchOffset:       0,
						PartitionMaxBytes: 1024,
					},
				},
			},
		},
	}

	resp, err := handler.handleFetch(context.Background(), &protocol.RequestHeader{CorrelationID: 2, APIVersion: 11}, fetchReq)
	if err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	if len(resp) == 0 {
		t.Fatalf("expected non-empty response for fetch")
	}
}

func TestHandleFetchByTopicID(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	produceReq := &kmsg.ProduceRequest{
		Acks:          -1,
		TimeoutMillis: 1000,
		Topics: []kmsg.ProduceRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{
						Partition: 0,
						Records:   testBatchBytes(0, 0, 1),
					},
				},
			},
		},
	}
	if _, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 1}, produceReq); err != nil {
		t.Fatalf("handleProduce: %v", err)
	}

	fetchReq := &kmsg.FetchRequest{
		Topics: []kmsg.FetchRequestTopic{
			{
				TopicID: metadata.TopicIDForName("orders"),
				Partitions: []kmsg.FetchRequestTopicPartition{
					{
						Partition:         0,
						FetchOffset:       0,
						PartitionMaxBytes: 1024,
					},
				},
			},
		},
	}

	resp, err := handler.handleFetch(context.Background(), &protocol.RequestHeader{CorrelationID: 2, APIVersion: 13}, fetchReq)
	if err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	fetchResp := decodeKmsgResponse(t, 13, resp, kmsg.NewPtrFetchResponse)
	if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
		t.Fatalf("expected topic partition in fetch response")
	}
	if len(fetchResp.Topics[0].Partitions[0].RecordBatches) == 0 {
		t.Fatalf("expected records for topic id fetch")
	}
}

func TestAutoCreateTopicOnProduce(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{
		ControllerID: 1,
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "localhost", Port: 19092},
		},
	})
	handler := newTestHandler(store)

	req := &kmsg.ProduceRequest{
		Acks:          -1,
		TimeoutMillis: 1000,
		Topics: []kmsg.ProduceRequestTopic{
			{
				Topic: "auto-created",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{
						Partition: 0,
						Records:   testBatchBytes(0, 0, 1),
					},
				},
			},
		},
	}
	if _, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 99}, req); err != nil {
		t.Fatalf("handleProduce auto-create: %v", err)
	}
	meta, err := store.Metadata(context.Background(), []string{"auto-created"})
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if len(meta.Topics) == 0 || meta.Topics[0].ErrorCode != protocol.NONE {
		t.Fatalf("expected topic metadata, got %+v", meta.Topics)
	}
	offset, err := store.NextOffset(context.Background(), "auto-created", 0)
	if err != nil {
		t.Fatalf("NextOffset: %v", err)
	}
	if offset != 1 {
		t.Fatalf("expected offset advanced to 1 got %d", offset)
	}
}

func TestHandleCreateDeleteTopics(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)
	createReq := &kmsg.CreateTopicsRequest{
		Topics: []kmsg.CreateTopicsRequestTopic{
			{Topic: "payments", NumPartitions: 1, ReplicationFactor: 1},
		},
	}
	respBytes, err := handler.handleCreateTopics(context.Background(), &protocol.RequestHeader{CorrelationID: 42}, createReq)
	if err != nil {
		t.Fatalf("handleCreateTopics: %v", err)
	}
	resp := decodeKmsgResponse(t, 0, respBytes, kmsg.NewPtrCreateTopicsResponse)
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != protocol.NONE {
		t.Fatalf("expected topic creation success: %#v", resp)
	}
	dupRespBytes, _ := handler.handleCreateTopics(context.Background(), &protocol.RequestHeader{CorrelationID: 43}, createReq)
	dupResp := decodeKmsgResponse(t, 0, dupRespBytes, kmsg.NewPtrCreateTopicsResponse)
	if dupResp.Topics[0].ErrorCode != protocol.TOPIC_ALREADY_EXISTS {
		t.Fatalf("expected duplicate error got %d", dupResp.Topics[0].ErrorCode)
	}
	deleteReq := &kmsg.DeleteTopicsRequest{TopicNames: []string{"payments", "missing"}}
	delBytes, err := handler.handleDeleteTopics(context.Background(), &protocol.RequestHeader{CorrelationID: 44}, deleteReq)
	if err != nil {
		t.Fatalf("handleDeleteTopics: %v", err)
	}
	delResp := decodeKmsgResponse(t, 0, delBytes, kmsg.NewPtrDeleteTopicsResponse)
	if len(delResp.Topics) != 2 || delResp.Topics[0].ErrorCode != protocol.NONE {
		t.Fatalf("expected delete success, got %#v", delResp)
	}
	if delResp.Topics[1].ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatalf("expected unknown topic error got %d", delResp.Topics[1].ErrorCode)
	}
}

func TestHandleCreateTopicsEtcdUnavailable(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(unavailableMetadataStore{Store: store})
	createReq := &kmsg.CreateTopicsRequest{
		Topics: []kmsg.CreateTopicsRequestTopic{
			{Topic: "payments", NumPartitions: 1, ReplicationFactor: 1},
		},
	}
	respBytes, err := handler.handleCreateTopics(context.Background(), &protocol.RequestHeader{CorrelationID: 45}, createReq)
	if err != nil {
		t.Fatalf("handleCreateTopics: %v", err)
	}
	resp := decodeKmsgResponse(t, 0, respBytes, kmsg.NewPtrCreateTopicsResponse)
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != protocol.REQUEST_TIMED_OUT {
		t.Fatalf("expected etcd unavailable error, got %#v", resp)
	}
	meta, err := store.Metadata(context.Background(), []string{"payments"})
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if len(meta.Topics) != 1 || meta.Topics[0].ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatalf("expected topic missing, got %+v", meta.Topics)
	}
}

func TestHandleCreatePartitions(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	req := &kmsg.CreatePartitionsRequest{
		Topics: []kmsg.CreatePartitionsRequestTopic{
			{Topic: "orders", Count: 2},
		},
		TimeoutMillis: 1000,
		ValidateOnly:  false,
	}
	payload, err := handler.handleCreatePartitions(context.Background(), &protocol.RequestHeader{
		CorrelationID: 51,
		APIVersion:    3,
	}, req)
	if err != nil {
		t.Fatalf("handleCreatePartitions: %v", err)
	}
	resp := decodeKmsgResponse(t, 3, payload, kmsg.NewPtrCreatePartitionsResponse)
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("unexpected create partitions response: %+v", resp.Topics)
	}
	meta, err := store.Metadata(context.Background(), []string{"orders"})
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if len(meta.Topics) != 1 || len(meta.Topics[0].Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got: %+v", meta.Topics)
	}
}

func TestHandleCreatePartitionsErrors(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	run := func(req *kmsg.CreatePartitionsRequest) *kmsg.CreatePartitionsResponse {
		t.Helper()
		payload, err := handler.handleCreatePartitions(context.Background(), &protocol.RequestHeader{
			CorrelationID: 52,
			APIVersion:    3,
		}, req)
		if err != nil {
			t.Fatalf("handleCreatePartitions: %v", err)
		}
		return decodeKmsgResponse(t, 3, payload, kmsg.NewPtrCreatePartitionsResponse)
	}

	resp := run(&kmsg.CreatePartitionsRequest{
		Topics:       []kmsg.CreatePartitionsRequestTopic{{Topic: "", Count: 2}},
		ValidateOnly: true,
	})
	if resp.Topics[0].ErrorCode != protocol.INVALID_TOPIC_EXCEPTION {
		t.Fatalf("expected invalid topic error, got %d", resp.Topics[0].ErrorCode)
	}

	resp = run(&kmsg.CreatePartitionsRequest{
		Topics: []kmsg.CreatePartitionsRequestTopic{
			{Topic: "orders", Count: 2},
			{Topic: "orders", Count: 3},
		},
		ValidateOnly: true,
	})
	if resp.Topics[0].ErrorCode != protocol.NONE {
		t.Fatalf("expected success for first orders, got %d", resp.Topics[0].ErrorCode)
	}
	if resp.Topics[1].ErrorCode != protocol.INVALID_REQUEST {
		t.Fatalf("expected duplicate topic error, got %d", resp.Topics[1].ErrorCode)
	}

	resp = run(&kmsg.CreatePartitionsRequest{
		Topics: []kmsg.CreatePartitionsRequestTopic{
			{Topic: "assignments", Count: 2, Assignment: []kmsg.CreatePartitionsRequestTopicAssignment{{Replicas: []int32{1}}}},
		},
		ValidateOnly: true,
	})
	if resp.Topics[0].ErrorCode != protocol.INVALID_REQUEST {
		t.Fatalf("expected assignment error, got %d", resp.Topics[0].ErrorCode)
	}

	resp = run(&kmsg.CreatePartitionsRequest{
		Topics:       []kmsg.CreatePartitionsRequestTopic{{Topic: "orders", Count: 1}},
		ValidateOnly: true,
	})
	if resp.Topics[0].ErrorCode != protocol.INVALID_PARTITIONS {
		t.Fatalf("expected invalid partitions error, got %d", resp.Topics[0].ErrorCode)
	}

	resp = run(&kmsg.CreatePartitionsRequest{
		Topics:       []kmsg.CreatePartitionsRequestTopic{{Topic: "missing", Count: 2}},
		ValidateOnly: true,
	})
	if resp.Topics[0].ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatalf("expected unknown topic error, got %d", resp.Topics[0].ErrorCode)
	}
}

func TestHandleDeleteGroups(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	group := &metadatapb.ConsumerGroup{GroupId: "group-1", State: "stable"}
	if err := store.PutConsumerGroup(context.Background(), group); err != nil {
		t.Fatalf("PutConsumerGroup: %v", err)
	}
	handler := newTestHandler(store)

	req := &kmsg.DeleteGroupsRequest{Groups: []string{"group-1", "missing", ""}}
	header := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyDeleteGroups,
		APIVersion:    2,
		CorrelationID: 53,
	}
	payload, err := handler.Handle(context.Background(), header, req)
	if err != nil {
		t.Fatalf("Handle DeleteGroups: %v", err)
	}
	resp := decodeKmsgResponse(t, 2, payload, kmsg.NewPtrDeleteGroupsResponse)
	if len(resp.Groups) != 3 {
		t.Fatalf("expected 3 delete group results, got %d", len(resp.Groups))
	}
	if resp.Groups[0].ErrorCode != protocol.NONE {
		t.Fatalf("expected success for group-1, got %d", resp.Groups[0].ErrorCode)
	}
	if resp.Groups[1].ErrorCode != protocol.GROUP_ID_NOT_FOUND {
		t.Fatalf("expected not found for missing, got %d", resp.Groups[1].ErrorCode)
	}
	if resp.Groups[2].ErrorCode != protocol.INVALID_REQUEST {
		t.Fatalf("expected invalid request for empty group, got %d", resp.Groups[2].ErrorCode)
	}
}

func TestHandleJoinGroupEtcdUnavailable(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(unavailableMetadataStore{Store: store})

	req := &kmsg.JoinGroupRequest{
		Group:                  "group-1",
		MemberID:               "member-1",
		ProtocolType:           "consumer",
		SessionTimeoutMillis:   1000,
		RebalanceTimeoutMillis: 1000,
		Protocols: []kmsg.JoinGroupRequestProtocol{
			{Name: "range", Metadata: []byte("meta")},
		},
	}
	header := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyJoinGroup,
		APIVersion:    4,
		CorrelationID: 99,
	}
	payload, err := handler.Handle(context.Background(), header, req)
	if err != nil {
		t.Fatalf("Handle JoinGroup: %v", err)
	}
	resp := decodeKmsgResponse(t, 4, payload, kmsg.NewPtrJoinGroupResponse)
	if resp.ErrorCode != protocol.REQUEST_TIMED_OUT {
		t.Fatalf("expected etcd unavailable error, got %d", resp.ErrorCode)
	}
}

func TestHandleOffsetCommitEtcdUnavailable(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(unavailableMetadataStore{Store: store})

	req := &kmsg.OffsetCommitRequest{
		Group:    "group-1",
		MemberID: "member-1",
		Topics: []kmsg.OffsetCommitRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.OffsetCommitRequestTopicPartition{
					{Partition: 0, Offset: 5},
				},
			},
		},
	}
	header := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyOffsetCommit,
		APIVersion:    3,
		CorrelationID: 100,
	}
	payload, err := handler.Handle(context.Background(), header, req)
	if err != nil {
		t.Fatalf("Handle OffsetCommit: %v", err)
	}
	resp := decodeKmsgResponse(t, 3, payload, kmsg.NewPtrOffsetCommitResponse)
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("expected commit response entries, got %+v", resp.Topics)
	}
	if resp.Topics[0].Partitions[0].ErrorCode != protocol.REQUEST_TIMED_OUT {
		t.Fatalf("expected etcd unavailable error, got %d", resp.Topics[0].Partitions[0].ErrorCode)
	}
}

func TestHandleListOffsets(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)
	if err := store.UpdateOffsets(context.Background(), "orders", 0, 9); err != nil {
		t.Fatalf("UpdateOffsets: %v", err)
	}
	tests := []struct {
		name        string
		version     int16
		timestamp   int64
		expected    int64
		leaderEpoch int32
	}{
		{name: "latest-v0", version: 0, timestamp: -1, expected: 10},
		{name: "earliest-v0", version: 0, timestamp: -2, expected: 0},
		{name: "latest-v1", version: 1, timestamp: -1, expected: 10},
		{name: "earliest-v1", version: 1, timestamp: -2, expected: 0},
		{name: "latest-v2", version: 2, timestamp: -1, expected: 10},
		{name: "earliest-v2", version: 2, timestamp: -2, expected: 0},
		{name: "latest-v4", version: 4, timestamp: -1, expected: 10, leaderEpoch: -1},
		{name: "earliest-v4", version: 4, timestamp: -2, expected: 0, leaderEpoch: -1},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			req := &kmsg.ListOffsetsRequest{
				Topics: []kmsg.ListOffsetsRequestTopic{
					{
						Topic:      "orders",
						Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: 0, Timestamp: tc.timestamp}},
					},
				},
			}
			if tc.version >= 2 {
				req.IsolationLevel = 1
			}
			header := &protocol.RequestHeader{CorrelationID: 55, APIVersion: tc.version}
			respBytes, err := handler.handleListOffsets(context.Background(), header, req)
			if err != nil {
				t.Fatalf("handleListOffsets: %v", err)
			}
			resp := decodeKmsgResponse(t, tc.version, respBytes, kmsg.NewPtrListOffsetsResponse)
			if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
				t.Fatalf("unexpected list offsets response: %#v", resp)
			}
			part := resp.Topics[0].Partitions[0]
			if tc.version == 0 {
				if len(part.OldStyleOffsets) != 1 || part.OldStyleOffsets[0] != tc.expected {
					t.Fatalf("expected old style offset %d got %#v", tc.expected, part.OldStyleOffsets)
				}
				return
			}
			if part.Offset != tc.expected || part.Timestamp != tc.timestamp {
				t.Fatalf("expected offset %d timestamp %d got offset %d timestamp %d", tc.expected, tc.timestamp, part.Offset, part.Timestamp)
			}
			if tc.version >= 4 && part.LeaderEpoch != tc.leaderEpoch {
				t.Fatalf("expected leader epoch %d got %d", tc.leaderEpoch, part.LeaderEpoch)
			}
		})
	}
}

func TestHandleListOffsetsRejectsUnsupportedVersion(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)
	req := &kmsg.ListOffsetsRequest{
		Topics: []kmsg.ListOffsetsRequestTopic{
			{
				Topic:      "orders",
				Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: 0, Timestamp: -1}},
			},
		},
	}
	header := &protocol.RequestHeader{CorrelationID: 55, APIVersion: 5}
	if _, err := handler.handleListOffsets(context.Background(), header, req); err == nil {
		t.Fatalf("expected error for unsupported list offsets version")
	}
}

func TestConsumerGroupLifecycle(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	joinReq := &kmsg.JoinGroupRequest{
		Group:                  "group-1",
		SessionTimeoutMillis:   10000,
		RebalanceTimeoutMillis: 10000,
		MemberID:               "",
		ProtocolType:           "consumer",
		Protocols: []kmsg.JoinGroupRequestProtocol{
			{Name: "range", Metadata: encodeJoinMetadata([]string{"orders"})},
		},
	}
	joinResp, err := handler.coordinator.JoinGroup(context.Background(), joinReq)
	if err != nil {
		t.Fatalf("JoinGroup: %v", err)
	}
	if joinResp.MemberID == "" {
		t.Fatalf("expected member id")
	}

	syncReq := &kmsg.SyncGroupRequest{
		Group:      "group-1",
		Generation: joinResp.Generation,
		MemberID:   joinResp.MemberID,
	}
	syncResp, err := handler.coordinator.SyncGroup(context.Background(), syncReq)
	if err != nil {
		t.Fatalf("SyncGroup: %v", err)
	}
	if len(syncResp.MemberAssignment) == 0 {
		t.Fatalf("expected assignment bytes")
	}

	hbResp := handler.coordinator.Heartbeat(context.Background(), &kmsg.HeartbeatRequest{
		Group:      "group-1",
		Generation: joinResp.Generation,
		MemberID:   joinResp.MemberID,
	})
	if hbResp.ErrorCode != protocol.NONE {
		t.Fatalf("heartbeat error: %d", hbResp.ErrorCode)
	}

	commitResp, err := handler.coordinator.OffsetCommit(context.Background(), &kmsg.OffsetCommitRequest{
		Group:      "group-1",
		Generation: joinResp.Generation,
		MemberID:   joinResp.MemberID,
		Topics: []kmsg.OffsetCommitRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.OffsetCommitRequestTopicPartition{
					{Partition: 0, Offset: 5, Metadata: kmsg.StringPtr("")},
				},
			},
		},
	})
	if err != nil || len(commitResp.Topics) == 0 {
		t.Fatalf("offset commit failed: %v", err)
	}

	fetchResp, err := handler.coordinator.OffsetFetch(context.Background(), &kmsg.OffsetFetchRequest{
		Group: "group-1",
		Topics: []kmsg.OffsetFetchRequestTopic{
			{
				Topic:      "orders",
				Partitions: []int32{0},
			},
		},
	})
	if err != nil {
		t.Fatalf("OffsetFetch: %v", err)
	}
	if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
		t.Fatalf("missing offset fetch data")
	}
	if fetchResp.Topics[0].Partitions[0].Offset != 5 {
		t.Fatalf("offset mismatch, got %d", fetchResp.Topics[0].Partitions[0].Offset)
	}
}

func TestProduceBackpressureDegraded(t *testing.T) {
	t.Setenv("KAFSCALE_S3_LATENCY_WARN_MS", "1")
	t.Setenv("KAFSCALE_S3_LATENCY_CRIT_MS", "60000")
	t.Setenv("KAFSCALE_S3_ERROR_RATE_CRIT", "2.0")
	store := metadata.NewInMemoryStore(defaultMetadata())
	brokerInfo := protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}
	handler := newHandler(store, &failingS3Client{}, brokerInfo, testLogger())
	handler.s3Health.RecordUpload(2*time.Millisecond, nil)

	req := &kmsg.ProduceRequest{
		Acks:          -1,
		TimeoutMillis: 100,
		Topics: []kmsg.ProduceRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{Partition: 0, Records: testBatchBytes(0, 0, 1)},
				},
			},
		},
	}
	resp, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 9}, req)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	produceResp := decodeKmsgResponse(t, 0, resp, kmsg.NewPtrProduceResponse)
	code := produceResp.Topics[0].Partitions[0].ErrorCode
	if code != protocol.REQUEST_TIMED_OUT {
		t.Fatalf("expected request timed out error, got %d", code)
	}
}

func TestProduceBackpressureUnavailable(t *testing.T) {
	t.Setenv("KAFSCALE_S3_ERROR_RATE_CRIT", "0.1")
	store := metadata.NewInMemoryStore(defaultMetadata())
	brokerInfo := protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}
	handler := newHandler(store, &failingS3Client{}, brokerInfo, testLogger())

	req := &kmsg.ProduceRequest{
		Acks:          -1,
		TimeoutMillis: 100,
		Topics: []kmsg.ProduceRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.ProduceRequestTopicPartition{
					{Partition: 0, Records: testBatchBytes(0, 0, 1)},
				},
			},
		},
	}
	resp, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 10}, req)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	produceResp := decodeKmsgResponse(t, 0, resp, kmsg.NewPtrProduceResponse)
	code := produceResp.Topics[0].Partitions[0].ErrorCode
	if code != protocol.UNKNOWN_SERVER_ERROR {
		t.Fatalf("expected unknown server error, got %d", code)
	}
}

func TestFetchBackpressureDegraded(t *testing.T) {
	t.Setenv("KAFSCALE_S3_LATENCY_WARN_MS", "1")
	t.Setenv("KAFSCALE_S3_LATENCY_CRIT_MS", "60000")
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)
	handler.s3Health.RecordOperation("download", 2*time.Millisecond, nil)

	req := &kmsg.FetchRequest{
		Topics: []kmsg.FetchRequestTopic{
			{
				Topic:      "orders",
				Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 0, FetchOffset: 0, PartitionMaxBytes: 1024}},
			},
		},
	}
	resp, err := handler.handleFetch(context.Background(), &protocol.RequestHeader{CorrelationID: 11, APIVersion: 11}, req)
	if err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	fetchResp := decodeKmsgResponse(t, 11, resp, kmsg.NewPtrFetchResponse)
	code := fetchResp.Topics[0].Partitions[0].ErrorCode
	if code != protocol.REQUEST_TIMED_OUT {
		t.Fatalf("expected request timed out error, got %d", code)
	}
}

func TestFetchBackpressureUnavailable(t *testing.T) {
	t.Setenv("KAFSCALE_S3_ERROR_RATE_CRIT", "0.1")
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)
	for i := 0; i < 2; i++ {
		handler.s3Health.RecordOperation("download", time.Millisecond, errors.New("boom"))
	}

	req := &kmsg.FetchRequest{
		Topics: []kmsg.FetchRequestTopic{
			{
				Topic:      "orders",
				Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 0, FetchOffset: 0, PartitionMaxBytes: 1024}},
			},
		},
	}
	resp, err := handler.handleFetch(context.Background(), &protocol.RequestHeader{CorrelationID: 12, APIVersion: 11}, req)
	if err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	fetchResp := decodeKmsgResponse(t, 11, resp, kmsg.NewPtrFetchResponse)
	code := fetchResp.Topics[0].Partitions[0].ErrorCode
	if code != protocol.UNKNOWN_SERVER_ERROR {
		t.Fatalf("expected unknown server error, got %d", code)
	}
}

func TestStartupChecksSuccess(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newHandler(store, storage.NewMemoryS3Client(), protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}, testLogger())
	if err := handler.runStartupChecks(context.Background()); err != nil {
		t.Fatalf("expected startup checks to pass: %v", err)
	}
}

func TestStartupChecksMetadataFailure(t *testing.T) {
	store := failingMetadataStore{
		Store: metadata.NewInMemoryStore(defaultMetadata()),
		err:   errors.New("metadata offline"),
	}
	handler := newHandler(store, storage.NewMemoryS3Client(), protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}, testLogger())
	err := handler.runStartupChecks(context.Background())
	if err == nil || !strings.Contains(err.Error(), "metadata") {
		t.Fatalf("expected metadata failure, got %v", err)
	}
}

func TestStartupChecksS3Failure(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newHandler(store, &failingS3Client{}, protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}, testLogger())
	err := handler.runStartupChecks(context.Background())
	if err == nil || !strings.Contains(err.Error(), "s3 readiness") {
		t.Fatalf("expected s3 failure, got %v", err)
	}
}

func encodeJoinMetadata(topics []string) []byte {
	buf := make([]byte, 0)
	writeInt16 := func(v int16) {
		tmp := make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(v))
		buf = append(buf, tmp...)
	}
	writeInt32 := func(v int32) {
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(v))
		buf = append(buf, tmp...)
	}
	writeInt16(0) // version
	writeInt32(int32(len(topics)))
	for _, topic := range topics {
		writeInt16(int16(len(topic)))
		buf = append(buf, []byte(topic)...)
	}
	writeInt32(0) // user data length
	return buf
}

func testBatchBytes(baseOffset int64, lastOffsetDelta int32, messageCount int32) []byte {
	data := make([]byte, 70)
	binary.BigEndian.PutUint64(data[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(data[23:27], uint32(lastOffsetDelta))
	binary.BigEndian.PutUint32(data[57:61], uint32(messageCount))
	return data
}

func newTestHandler(store metadata.Store) *handler {
	brokerInfo := protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}
	return newHandler(store, storage.NewMemoryS3Client(), brokerInfo, testLogger())
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
}

type failingS3Client struct{}

func (f *failingS3Client) UploadSegment(ctx context.Context, key string, body []byte) error {
	return errors.New("s3 unavailable")
}

func (f *failingS3Client) UploadIndex(ctx context.Context, key string, body []byte) error {
	return errors.New("s3 unavailable")
}

func (f *failingS3Client) DownloadSegment(ctx context.Context, key string, rng *storage.ByteRange) ([]byte, error) {
	return nil, errors.New("unsupported")
}

func (f *failingS3Client) DownloadIndex(ctx context.Context, key string) ([]byte, error) {
	return nil, errors.New("unsupported")
}

func (f *failingS3Client) ListSegments(ctx context.Context, prefix string) ([]storage.S3Object, error) {
	return nil, errors.New("unsupported")
}

func (f *failingS3Client) EnsureBucket(ctx context.Context) error {
	return errors.New("s3 unavailable")
}

type failingMetadataStore struct {
	metadata.Store
	err error
}

func (f failingMetadataStore) Metadata(ctx context.Context, topics []string) (*metadata.ClusterMetadata, error) {
	return nil, f.err
}

type unavailableMetadataStore struct {
	metadata.Store
}

func (u unavailableMetadataStore) Available() bool {
	return false
}

func TestFranzGoProduceConsumeLocal(t *testing.T) {
	if os.Getenv("KAFSCALE_LOCAL_FRANZ") != "1" {
		t.Skip("set KAFSCALE_LOCAL_FRANZ=1 to run the local franz-go test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	clusterID := "franz-local"
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{
		ControllerID: 1,
		ClusterID:    &clusterID,
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "127.0.0.1", Port: 39092},
		},
		Topics: []protocol.MetadataTopic{
			{
				ErrorCode: 0,
				Topic:     kmsg.StringPtr("orders"),
				Partitions: []protocol.MetadataPartition{
					{
						ErrorCode: 0,
						Partition: 0,
						Leader:    1,
						Replicas:  []int32{1},
						ISR:       []int32{1},
					},
				},
			},
		},
	})
	handler := newHandler(store, storage.NewMemoryS3Client(), protocol.MetadataBroker{NodeID: 1, Host: "127.0.0.1", Port: 39092}, testLogger())
	server := &broker.Server{Addr: "127.0.0.1:39092", Handler: handler}
	errCh := make(chan error, 1)

	go func() {
		if err := server.ListenAndServe(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	time.Sleep(150 * time.Millisecond)

	topic := "orders"
	producer, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:39092"),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableIdempotentWrite(),
		kgo.WithLogger(kgo.BasicLogger(io.Discard, kgo.LogLevelWarn, nil)),
	)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	for i := 0; i < 5; i++ {
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("message-%d", i))}).FirstErr(); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:39092"),
		kgo.ConsumerGroup("franz-local-consumer"),
		kgo.ConsumeTopics(topic),
		kgo.WithLogger(kgo.BasicLogger(io.Discard, kgo.LogLevelWarn, nil)),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	received := 0
	deadline := time.Now().Add(5 * time.Second)
	for received < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for records (got %d)", received)
		}
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %+v", errs)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			received++
		})
	}

	select {
	case err := <-errCh:
		t.Fatalf("broker listen failed: %v", err)
	default:
	}
}

// decodeKmsgResponse is a generic test helper that decodes any kmsg.Response
// from a wire-encoded response payload (correlation ID + optional tagged fields + body).
func decodeKmsgResponse[T kmsg.Response](t *testing.T, version int16, payload []byte, newFn func() T) T {
	t.Helper()
	resp := newFn()
	body, ok := protocol.SkipResponseHeader(resp.Key(), version, payload)
	if !ok {
		t.Fatalf("failed to skip response header for api key %d", resp.Key())
	}
	resp.SetVersion(version)
	if err := resp.ReadFrom(body); err != nil {
		t.Fatalf("decode response for api key %d v%d: %v", resp.Key(), version, err)
	}
	return resp
}

func TestMetricsHandlerExposesS3Health(t *testing.T) {
	t.Setenv("KAFSCALE_S3_LATENCY_WARN_MS", "1")
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)
	handler.s3Health.RecordOperation("upload", 2*time.Millisecond, nil)
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.metricsHandler(rec, req)
	body := rec.Body.String()
	if !strings.Contains(body, `kafscale_s3_health_state{state="degraded"} 1`) {
		t.Fatalf("expected degraded metric, got:\n%s", body)
	}
}

func TestMetricsHandlerExposesAdminMetrics(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)
	req := &kmsg.DescribeConfigsRequest{
		Resources: []kmsg.DescribeConfigsRequestResource{
			{ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: "orders"},
		},
	}
	header := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyDescribeConfigs,
		APIVersion:    4,
		CorrelationID: 1,
	}
	if _, err := handler.Handle(context.Background(), header, req); err != nil {
		t.Fatalf("handle describe configs: %v", err)
	}
	rec := httptest.NewRecorder()
	handler.metricsHandler(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	if !strings.Contains(body, `kafscale_admin_requests_total{api="DescribeConfigs"} 1`) {
		t.Fatalf("expected admin metrics, got:\n%s", body)
	}
}

func TestControlServerReportsHealth(t *testing.T) {
	t.Setenv("KAFSCALE_S3_ERROR_RATE_CRIT", "0.01")
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)
	handler.s3Health.RecordOperation("upload", time.Millisecond, errors.New("boom"))
	srv := &controlServer{handler: handler}
	resp, err := srv.GetStatus(context.Background(), &controlpb.BrokerStatusRequest{})
	if err != nil {
		t.Fatalf("GetStatus: %v", err)
	}
	if resp.Ready {
		t.Fatalf("expected broker not ready while S3 unavailable")
	}
	if len(resp.Partitions) == 0 || resp.Partitions[0].GetState() != string(broker.S3StateUnavailable) {
		t.Fatalf("unexpected partition state: %+v", resp.Partitions)
	}
}

func TestBrokerEnvConfigOverrides(t *testing.T) {
	t.Setenv("KAFSCALE_CACHE_BYTES", "10")
	t.Setenv("KAFSCALE_SEGMENT_BYTES", "2048")
	t.Setenv("KAFSCALE_FLUSH_INTERVAL_MS", "250")

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newHandler(store, storage.NewMemoryS3Client(), protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}, testLogger())

	if handler.logConfig.Buffer.MaxBytes != 2048 {
		t.Fatalf("expected segment bytes 2048 got %d", handler.logConfig.Buffer.MaxBytes)
	}
	if handler.logConfig.Buffer.FlushInterval != 250*time.Millisecond {
		t.Fatalf("expected flush interval 250ms got %s", handler.logConfig.Buffer.FlushInterval)
	}
	if handler.cache == nil {
		t.Fatalf("expected cache initialized")
	}
	handler.cache.SetSegment("topic", 0, 0, []byte("123456"))
	handler.cache.SetSegment("topic", 0, 1, []byte("abcdef"))
	if _, ok := handler.cache.GetSegment("topic", 0, 0); ok {
		t.Fatalf("expected cache eviction for base offset 0")
	}
}

func TestBrokerCacheSizeFallback(t *testing.T) {
	t.Setenv("KAFSCALE_CACHE_BYTES", "")
	t.Setenv("KAFSCALE_CACHE_SIZE", "8")

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newHandler(store, storage.NewMemoryS3Client(), protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}, testLogger())

	if handler.cache == nil {
		t.Fatalf("expected cache initialized")
	}
	handler.cache.SetSegment("topic", 0, 0, []byte("123456"))
	handler.cache.SetSegment("topic", 0, 1, []byte("abcdef"))
	if _, ok := handler.cache.GetSegment("topic", 0, 0); ok {
		t.Fatalf("expected cache eviction for base offset 0")
	}
}

func TestBuildStoreUsesEtcdEnv(t *testing.T) {
	etcd, endpoints := startEmbeddedEtcd(t)
	defer etcd.Close()

	t.Setenv("KAFSCALE_ETCD_ENDPOINTS", strings.Join(endpoints, ","))
	t.Setenv("KAFSCALE_ETCD_USERNAME", "user")
	t.Setenv("KAFSCALE_ETCD_PASSWORD", "pass")

	store := buildStore(context.Background(), buildBrokerInfo(), testLogger())
	if _, ok := store.(*metadata.EtcdStore); !ok {
		t.Fatalf("expected etcd store when KAFSCALE_ETCD_ENDPOINTS is set")
	}
}

func TestEtcdConfigFromEnv(t *testing.T) {
	t.Setenv("KAFSCALE_ETCD_ENDPOINTS", "http://a:2379,http://b:2379")
	t.Setenv("KAFSCALE_ETCD_USERNAME", "user")
	t.Setenv("KAFSCALE_ETCD_PASSWORD", "pass")
	cfg, ok := etcdConfigFromEnv()
	if !ok {
		t.Fatalf("expected config to be enabled")
	}
	if len(cfg.Endpoints) != 2 || cfg.Endpoints[0] != "http://a:2379" || cfg.Endpoints[1] != "http://b:2379" {
		t.Fatalf("unexpected endpoints: %v", cfg.Endpoints)
	}
	if cfg.Username != "user" || cfg.Password != "pass" {
		t.Fatalf("unexpected credentials: %+v", cfg)
	}
}

func TestLogLevelFromEnv(t *testing.T) {
	t.Setenv("KAFSCALE_LOG_LEVEL", "debug")
	if got := logLevelFromEnv(); got != slog.LevelDebug {
		t.Fatalf("expected debug level, got %v", got)
	}
	t.Setenv("KAFSCALE_LOG_LEVEL", "error")
	if got := logLevelFromEnv(); got != slog.LevelError {
		t.Fatalf("expected error level, got %v", got)
	}
	t.Setenv("KAFSCALE_LOG_LEVEL", "warning")
	if got := logLevelFromEnv(); got != slog.LevelWarn {
		t.Fatalf("expected warn level, got %v", got)
	}
}

func TestBuildBrokerInfoFromEnv(t *testing.T) {
	t.Setenv("KAFSCALE_BROKER_ID", "7")
	t.Setenv("KAFSCALE_BROKER_HOST", "broker.local")
	t.Setenv("KAFSCALE_BROKER_PORT", "9099")

	info := buildBrokerInfo()
	if info.NodeID != 7 || info.Host != "broker.local" || info.Port != 9099 {
		t.Fatalf("unexpected broker info: %+v", info)
	}

	t.Setenv("KAFSCALE_BROKER_ADDR", "override.local:1234")
	info = buildBrokerInfo()
	if info.Host != "override.local" || info.Port != 1234 {
		t.Fatalf("expected broker addr override, got %+v", info)
	}
}

func TestHandlerEnvOverridesAll(t *testing.T) {
	t.Setenv("KAFSCALE_READAHEAD_SEGMENTS", "5")
	t.Setenv("KAFSCALE_CACHE_BYTES", "128")
	t.Setenv("KAFSCALE_AUTO_CREATE_TOPICS", "false")
	t.Setenv("KAFSCALE_AUTO_CREATE_PARTITIONS", "0")
	t.Setenv("KAFSCALE_TRACE_KAFKA", "true")
	t.Setenv("KAFSCALE_THROUGHPUT_WINDOW_SEC", "10")
	t.Setenv("KAFSCALE_S3_NAMESPACE", "ns-1")
	t.Setenv("KAFSCALE_SEGMENT_BYTES", "4096")
	t.Setenv("KAFSCALE_FLUSH_INTERVAL_MS", "250")

	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newHandler(store, storage.NewMemoryS3Client(), protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}, testLogger())

	if handler.readAhead != 5 {
		t.Fatalf("expected readAhead 5 got %d", handler.readAhead)
	}
	if handler.cacheSize != 128 {
		t.Fatalf("expected cacheSize 128 got %d", handler.cacheSize)
	}
	if handler.autoCreateTopics {
		t.Fatalf("expected autoCreateTopics false")
	}
	if handler.autoCreatePartitions != 1 {
		t.Fatalf("expected autoCreatePartitions to clamp to 1, got %d", handler.autoCreatePartitions)
	}
	if !handler.traceKafka {
		t.Fatalf("expected traceKafka true")
	}
	if handler.produceRate.window != 10*time.Second {
		t.Fatalf("expected throughput window 10s got %s", handler.produceRate.window)
	}
	if handler.s3Namespace != "ns-1" {
		t.Fatalf("expected s3Namespace ns-1 got %q", handler.s3Namespace)
	}
	if handler.segmentBytes != 4096 {
		t.Fatalf("expected segmentBytes 4096 got %d", handler.segmentBytes)
	}
	if handler.flushInterval != 250*time.Millisecond {
		t.Fatalf("expected flushInterval 250ms got %s", handler.flushInterval)
	}
}

func TestCoordinatorBrokerPrefersMetadata(t *testing.T) {
	meta := metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 0, Host: "meta-0", Port: 9092},
			{NodeID: 2, Host: "meta-2", Port: 9092},
		},
	}
	brokerInfo := protocol.MetadataBroker{NodeID: 1, Host: "local", Port: 19092}
	handler := newHandler(metadata.NewInMemoryStore(meta), storage.NewMemoryS3Client(), brokerInfo, testLogger())

	got := handler.coordinatorBroker(context.Background())
	if got.NodeID != 0 || got.Host != "meta-0" || got.Port != 9092 {
		t.Fatalf("expected metadata broker 0, got %+v", got)
	}

	meta = metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "meta-1", Port: 19093},
			{NodeID: 0, Host: "meta-0", Port: 9092},
		},
	}
	handler = newHandler(metadata.NewInMemoryStore(meta), storage.NewMemoryS3Client(), brokerInfo, testLogger())
	got = handler.coordinatorBroker(context.Background())
	if got.NodeID != 1 || got.Host != "meta-1" || got.Port != 19093 {
		t.Fatalf("expected matching metadata broker, got %+v", got)
	}

	handler = newHandler(failingMetadataStore{
		Store: metadata.NewInMemoryStore(meta),
		err:   errors.New("metadata down"),
	}, storage.NewMemoryS3Client(), brokerInfo, testLogger())
	got = handler.coordinatorBroker(context.Background())
	if got.NodeID != brokerInfo.NodeID || got.Host != brokerInfo.Host || got.Port != brokerInfo.Port {
		t.Fatalf("expected broker info fallback, got %+v", got)
	}
}

func TestS3HealthConfigFromEnv(t *testing.T) {
	t.Setenv("KAFSCALE_S3_HEALTH_WINDOW_SEC", "15")
	t.Setenv("KAFSCALE_S3_LATENCY_WARN_MS", "25")
	t.Setenv("KAFSCALE_S3_LATENCY_CRIT_MS", "75")
	t.Setenv("KAFSCALE_S3_ERROR_RATE_WARN", "0.15")
	t.Setenv("KAFSCALE_S3_ERROR_RATE_CRIT", "0.35")

	cfg := s3HealthConfigFromEnv()
	if cfg.Window != 15*time.Second {
		t.Fatalf("expected window 15s got %s", cfg.Window)
	}
	if cfg.LatencyWarn != 25*time.Millisecond {
		t.Fatalf("expected latency warn 25ms got %s", cfg.LatencyWarn)
	}
	if cfg.LatencyCrit != 75*time.Millisecond {
		t.Fatalf("expected latency crit 75ms got %s", cfg.LatencyCrit)
	}
	if cfg.ErrorWarn != 0.15 {
		t.Fatalf("expected error warn 0.15 got %f", cfg.ErrorWarn)
	}
	if cfg.ErrorCrit != 0.35 {
		t.Fatalf("expected error crit 0.35 got %f", cfg.ErrorCrit)
	}
}

func TestBuildS3ConfigsFromEnv(t *testing.T) {
	t.Setenv("KAFSCALE_USE_MEMORY_S3", "")
	t.Setenv("KAFSCALE_S3_BUCKET", "bucket-a")
	t.Setenv("KAFSCALE_S3_REGION", "us-west-2")
	t.Setenv("KAFSCALE_S3_ENDPOINT", "http://s3.local")
	t.Setenv("KAFSCALE_S3_PATH_STYLE", "false")
	t.Setenv("KAFSCALE_S3_KMS_ARN", "kms-arn")
	t.Setenv("KAFSCALE_S3_ACCESS_KEY", "access")
	t.Setenv("KAFSCALE_S3_SECRET_KEY", "secret")
	t.Setenv("KAFSCALE_S3_SESSION_TOKEN", "token")
	t.Setenv("KAFSCALE_S3_READ_BUCKET", "bucket-r")
	t.Setenv("KAFSCALE_S3_READ_REGION", "us-east-2")
	t.Setenv("KAFSCALE_S3_READ_ENDPOINT", "http://s3-read.local")

	writeCfg, readCfg, useMemory, credsProvided, useReadReplica := buildS3ConfigsFromEnv()
	if useMemory {
		t.Fatalf("expected useMemory false")
	}
	if !credsProvided {
		t.Fatalf("expected credsProvided true")
	}
	if !useReadReplica {
		t.Fatalf("expected read replica enabled")
	}
	if writeCfg.Bucket != "bucket-a" || writeCfg.Region != "us-west-2" || writeCfg.Endpoint != "http://s3.local" {
		t.Fatalf("unexpected write config: %+v", writeCfg)
	}
	if writeCfg.ForcePathStyle {
		t.Fatalf("expected forcePathStyle false")
	}
	if writeCfg.KMSKeyARN != "kms-arn" {
		t.Fatalf("expected kms arn set")
	}
	if writeCfg.AccessKeyID != "access" || writeCfg.SecretAccessKey != "secret" || writeCfg.SessionToken != "token" {
		t.Fatalf("unexpected write credentials: %+v", writeCfg)
	}
	if readCfg.Bucket != "bucket-r" || readCfg.Region != "us-east-2" || readCfg.Endpoint != "http://s3-read.local" {
		t.Fatalf("unexpected read config: %+v", readCfg)
	}
}

func TestBuildS3ConfigsFromEnvDefaults(t *testing.T) {
	t.Setenv("KAFSCALE_USE_MEMORY_S3", "")
	t.Setenv("KAFSCALE_S3_BUCKET", "")
	t.Setenv("KAFSCALE_S3_REGION", "")
	t.Setenv("KAFSCALE_S3_ENDPOINT", "")
	t.Setenv("KAFSCALE_S3_PATH_STYLE", "")
	t.Setenv("KAFSCALE_S3_KMS_ARN", "")
	t.Setenv("KAFSCALE_S3_ACCESS_KEY", "")
	t.Setenv("KAFSCALE_S3_SECRET_KEY", "")
	t.Setenv("KAFSCALE_S3_SESSION_TOKEN", "")
	t.Setenv("KAFSCALE_S3_READ_BUCKET", "")
	t.Setenv("KAFSCALE_S3_READ_REGION", "")
	t.Setenv("KAFSCALE_S3_READ_ENDPOINT", "")

	writeCfg, readCfg, useMemory, credsProvided, useReadReplica := buildS3ConfigsFromEnv()
	if useMemory {
		t.Fatalf("expected useMemory false by default")
	}
	if credsProvided {
		t.Fatalf("expected no credentials when env vars are empty")
	}
	if useReadReplica {
		t.Fatalf("expected read replica disabled")
	}
	if writeCfg.Bucket != "" || writeCfg.Region != "" || writeCfg.Endpoint != "" {
		t.Fatalf("expected empty config when env vars are unset: %+v", writeCfg)
	}
	if writeCfg.ForcePathStyle {
		t.Fatalf("expected forcePathStyle false when no endpoint is set")
	}
	if readCfg.Bucket != "" || readCfg.Region != "" || readCfg.Endpoint != "" {
		t.Fatalf("expected empty read config when env vars are unset: %+v", readCfg)
	}
}

func TestBuildS3ConfigsFromEnvUseMemory(t *testing.T) {
	t.Setenv("KAFSCALE_USE_MEMORY_S3", "1")
	_, _, useMemory, credsProvided, useReadReplica := buildS3ConfigsFromEnv()
	if !useMemory {
		t.Fatalf("expected useMemory true")
	}
	if credsProvided || useReadReplica {
		t.Fatalf("unexpected flags for memory mode")
	}
}

func TestBuildS3ConfigsFromEnvPathStyleDefaultsToEndpoint(t *testing.T) {
	t.Setenv("KAFSCALE_S3_ENDPOINT", "http://minio.local:9000")
	t.Setenv("KAFSCALE_S3_PATH_STYLE", "")

	writeCfg, _, _, _, _ := buildS3ConfigsFromEnv()
	if !writeCfg.ForcePathStyle {
		t.Fatalf("expected forcePathStyle true when custom endpoint is set")
	}
}

func TestStartupTimeoutFromEnv(t *testing.T) {
	t.Setenv("KAFSCALE_STARTUP_TIMEOUT_SEC", "12")
	if got := startupTimeoutFromEnv(); got != 12*time.Second {
		t.Fatalf("expected 12s timeout got %s", got)
	}
}

func TestEnvOrDefaultAddresses(t *testing.T) {
	t.Setenv("KAFSCALE_METRICS_ADDR", "127.0.0.1:1111")
	t.Setenv("KAFSCALE_CONTROL_ADDR", "127.0.0.1:2222")
	t.Setenv("KAFSCALE_BROKER_ADDR", "127.0.0.1:3333")

	if got := envOrDefault("KAFSCALE_METRICS_ADDR", ":9093"); got != "127.0.0.1:1111" {
		t.Fatalf("expected metrics addr override, got %q", got)
	}
	if got := envOrDefault("KAFSCALE_CONTROL_ADDR", ":9094"); got != "127.0.0.1:2222" {
		t.Fatalf("expected control addr override, got %q", got)
	}
	if got := envOrDefault("KAFSCALE_BROKER_ADDR", ":9092"); got != "127.0.0.1:3333" {
		t.Fatalf("expected broker addr override, got %q", got)
	}
}

func startEmbeddedEtcd(t *testing.T) (*embed.Etcd, []string) {
	t.Helper()
	if err := ensureEtcdPortsFree(); err != nil {
		t.Skipf("skipping embedded etcd test: %v", err)
	}
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.Logger = "zap"
	setEtcdPorts(t, cfg, "12379", "12380")

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("skipping embedded etcd test: %v", err)
		}
		t.Fatalf("start embedded etcd: %v", err)
	}
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		e.Server.Stop()
		t.Fatalf("etcd server took too long to start")
	}
	clientURL := e.Clients[0].Addr().String()
	return e, []string{fmt.Sprintf("http://%s", clientURL)}
}

func ensureEtcdPortsFree() error {
	if err := killProcessesOnPort("12379"); err != nil {
		return err
	}
	if err := killProcessesOnPort("12380"); err != nil {
		return err
	}
	if err := portAvailable("127.0.0.1:12379"); err != nil {
		return err
	}
	if err := portAvailable("127.0.0.1:12380"); err != nil {
		return err
	}
	return nil
}

func setEtcdPorts(t *testing.T, cfg *embed.Config, clientPort, peerPort string) {
	t.Helper()
	clientURL, err := url.Parse("http://127.0.0.1:" + clientPort)
	if err != nil {
		t.Fatalf("parse client url: %v", err)
	}
	peerURL, err := url.Parse("http://127.0.0.1:" + peerPort)
	if err != nil {
		t.Fatalf("parse peer url: %v", err)
	}
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.Name = "default"
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
}

func killProcessesOnPort(port string) error {
	out, err := exec.Command("lsof", "-nP", "-iTCP:"+port, "-sTCP:LISTEN", "-t").Output()
	if err != nil {
		return nil
	}
	pids := strings.Fields(string(out))
	for _, pidStr := range pids {
		pid, convErr := strconv.Atoi(strings.TrimSpace(pidStr))
		if convErr != nil {
			continue
		}
		_ = syscall.Kill(pid, syscall.SIGTERM)
		time.Sleep(100 * time.Millisecond)
		if alive := syscall.Kill(pid, 0); alive == nil {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	return nil
}

func portAvailable(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("port %s already in use", addr)
	}
	_ = ln.Close()
	return nil
}
