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
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestFetchAdvertisesLibrdkafkaCompatibility(t *testing.T) {
	for _, api := range generateApiVersions() {
		if api.ApiKey == protocol.APIKeyFetch {
			if api.MinVersion != 4 || api.MaxVersion != 13 {
				t.Fatalf("Fetch range %d..%d excludes librdkafka's v4 MsgVer2 probe", api.MinVersion, api.MaxVersion)
			}
			return
		}
	}
	t.Fatal("Fetch missing from ApiVersions")
}

func TestFetchCompatibilityWireVersions(t *testing.T) {
	ctx := context.Background()
	topicID := metadata.TopicIDForName("orders")
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{Topics: []protocol.MetadataTopic{{Topic: kmsg.StringPtr("orders"), TopicID: topicID, Partitions: []protocol.MetadataPartition{{Partition: 0}}}}})
	h := newTestHandler(store)
	t.Cleanup(h.coordinator.Stop)
	produce := kmsg.NewPtrProduceRequest()
	produce.Version, produce.Acks = 9, -1
	produce.Topics = []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: testBatchBytes(0, 0, 1)}}}}
	wire, err := h.Handle(ctx, &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9}, produce)
	if err != nil {
		t.Fatal(err)
	}
	if resp := decodeKmsgResponse(t, 9, wire, kmsg.NewPtrProduceResponse); resp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatal(resp)
	}
	for version := int16(4); version <= 13; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			req := kmsg.NewPtrFetchRequest()
			req.Version, req.ReplicaID, req.MinBytes, req.MaxBytes = version, -1, 1, 1<<20
			req.Topics = []kmsg.FetchRequestTopic{{Topic: "orders", TopicID: topicID, Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 0, FetchOffset: 0, PartitionMaxBytes: 1 << 20}}}}
			formatter := kmsg.NewRequestFormatter(kmsg.FormatterClientID("compatibility"))
			framed := formatter.AppendRequest(nil, req, 42)
			header, parsed, err := protocol.ParseRequest(framed[4:])
			if err != nil {
				t.Fatal(err)
			}
			wire, err := h.Handle(ctx, header, parsed)
			if err != nil {
				t.Fatal(err)
			}
			resp := decodeKmsgResponse(t, version, wire, kmsg.NewPtrFetchResponse)
			if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
				t.Fatal(resp)
			}
			part := resp.Topics[0].Partitions[0]
			if part.ErrorCode != 0 || part.HighWatermark != 1 || len(part.RecordBatches) == 0 {
				t.Fatal(part)
			}
		})
	}
}

func TestProduceRejectsLegacyMessageSets(t *testing.T) {
	for _, magic := range []byte{0, 1} {
		for _, size := range []int{1, 128} {
			t.Run(fmt.Sprintf("magic%d/bytes%d", magic, size), func(t *testing.T) {
				// Legacy MessageSet: offset, message size, CRC, magic, attributes,
				// optional timestamp, nullable key and value.
				record := make([]byte, 18)
				record[16] = magic
				if magic == 1 {
					record = append(record, make([]byte, 8)...)
				}
				record = binary.BigEndian.AppendUint32(record, 0xffffffff)
				record = binary.BigEndian.AppendUint32(record, uint32(size))
				record = append(record, make([]byte, size)...)
				binary.BigEndian.PutUint32(record[8:12], uint32(len(record)-12))
				binary.BigEndian.PutUint32(record[12:16], crc32.ChecksumIEEE(record[16:]))
				store := metadata.NewInMemoryStore(defaultMetadata())
				h := newTestHandler(store)
				t.Cleanup(h.coordinator.Stop)
				req := kmsg.NewPtrProduceRequest()
				req.Version, req.Acks = 9, -1
				req.Topics = []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: record}}}}
				wire, err := h.handleProduce(context.Background(), &protocol.RequestHeader{APIVersion: 9}, req)
				if err != nil {
					t.Fatal(err)
				}
				resp := decodeKmsgResponse(t, 9, wire, kmsg.NewPtrProduceResponse)
				if resp.Topics[0].Partitions[0].ErrorCode != protocol.UNSUPPORTED_VERSION {
					t.Fatalf("legacy record error: %+v", resp)
				}
				if next, err := store.NextOffset(context.Background(), "orders", 0); err != nil || next != 0 {
					t.Fatal("legacy record advanced offset", next, err)
				}
			})
		}
	}
}

func TestProduceRejectsTruncatedRecordBatches(t *testing.T) {
	for _, size := range []int{0, 16, 60} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			store := metadata.NewInMemoryStore(defaultMetadata())
			h := newTestHandler(store)
			t.Cleanup(h.coordinator.Stop)
			records := testBatchBytes(0, 0, 1)[:size]
			req := kmsg.NewPtrProduceRequest()
			req.Version, req.Acks = 9, -1
			req.Topics = []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: records}}}}
			wire, err := h.handleProduce(context.Background(), &protocol.RequestHeader{APIVersion: 9}, req)
			if err != nil {
				t.Fatal(err)
			}
			resp := decodeKmsgResponse(t, 9, wire, kmsg.NewPtrProduceResponse)
			if resp.Topics[0].Partitions[0].ErrorCode != protocol.CORRUPT_MESSAGE {
				t.Fatal(resp)
			}
			if next, err := store.NextOffset(context.Background(), "orders", 0); err != nil || next != 0 {
				t.Fatal(next, err)
			}
		})
	}
}

func TestClassicGroupApiProbeCompatibility(t *testing.T) {
	probes := map[int16]int16{protocol.APIKeyFindCoordinator: 0, protocol.APIKeyJoinGroup: 0, protocol.APIKeySyncGroup: 0, protocol.APIKeyHeartbeat: 0, protocol.APIKeyLeaveGroup: 0, protocol.APIKeyOffsetCommit: 1, protocol.APIKeyOffsetFetch: 1}
	for _, api := range generateApiVersions() {
		if probe, ok := probes[api.ApiKey]; ok {
			if api.MinVersion > probe || api.MaxVersion < probe {
				t.Fatalf("API %d range %d..%d excludes librdkafka probe %d", api.ApiKey, api.MinVersion, api.MaxVersion, probe)
			}
			delete(probes, api.ApiKey)
		}
	}
	if len(probes) > 0 {
		t.Fatal("missing classic group APIs", probes)
	}
}

func TestClassicGroupCompatibilityWireVersions(t *testing.T) {
	for version := int16(0); version <= 4; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			h := newTestHandler(metadata.NewInMemoryStore(defaultMetadata()))
			t.Cleanup(h.coordinator.Stop)
			roundTrip := func(req kmsg.Request) kmsg.Response {
				t.Helper()
				formatter := kmsg.NewRequestFormatter(kmsg.FormatterClientID("classic-compatibility"))
				header, parsed, err := protocol.ParseRequest(formatter.AppendRequest(nil, req, 42)[4:])
				if err != nil {
					t.Fatal(err)
				}
				wire, err := h.Handle(context.Background(), header, parsed)
				if err != nil {
					t.Fatal(err)
				}
				body, ok := protocol.SkipResponseHeader(req.Key(), req.GetVersion(), wire)
				if !ok {
					t.Fatal("response header")
				}
				resp := req.ResponseKind()
				resp.SetVersion(req.GetVersion())
				if err := resp.ReadFrom(body); err != nil {
					t.Fatal(err)
				}
				return resp
			}
			find := kmsg.NewPtrFindCoordinatorRequest()
			find.Version, find.CoordinatorKey = min(version, 3), "classic"
			if resp := roundTrip(find).(*kmsg.FindCoordinatorResponse); resp.ErrorCode != 0 {
				t.Fatal(resp)
			}
			join := kmsg.NewPtrJoinGroupRequest()
			join.Version, join.Group, join.ProtocolType, join.SessionTimeoutMillis = version, "classic", "consumer", 60000
			join.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range"}}
			joined := roundTrip(join).(*kmsg.JoinGroupResponse)
			if joined.ErrorCode != 0 {
				t.Fatal(joined)
			}
			sync := kmsg.NewPtrSyncGroupRequest()
			sync.Version, sync.Group, sync.MemberID, sync.Generation = version, "classic", joined.MemberID, joined.Generation
			if resp := roundTrip(sync).(*kmsg.SyncGroupResponse); resp.ErrorCode != 0 {
				t.Fatal(resp)
			}
			heartbeat := kmsg.NewPtrHeartbeatRequest()
			heartbeat.Version, heartbeat.Group, heartbeat.MemberID, heartbeat.Generation = version, "classic", joined.MemberID, joined.Generation
			if resp := roundTrip(heartbeat).(*kmsg.HeartbeatResponse); resp.ErrorCode != 0 {
				t.Fatal(resp)
			}
			commit := kmsg.NewPtrOffsetCommitRequest()
			commit.Version, commit.Group, commit.MemberID, commit.Generation = 1+version%3, "classic", joined.MemberID, joined.Generation
			commit.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "orders", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 5}}}}
			if resp := roundTrip(commit).(*kmsg.OffsetCommitResponse); resp.Topics[0].Partitions[0].ErrorCode != 0 {
				t.Fatal(resp)
			}
			fetch := kmsg.NewPtrOffsetFetchRequest()
			fetch.Version, fetch.Group = 1+version, "classic"
			fetch.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "orders", Partitions: []int32{0}}}
			if resp := roundTrip(fetch).(*kmsg.OffsetFetchResponse); resp.Topics[0].Partitions[0].ErrorCode != 0 || resp.Topics[0].Partitions[0].Offset != 5 {
				t.Fatal(resp)
			}
			leave := kmsg.NewPtrLeaveGroupRequest()
			leave.Version, leave.Group, leave.MemberID = version, "classic", joined.MemberID
			leave.Members = []kmsg.LeaveGroupRequestMember{{MemberID: joined.MemberID}}
			left := roundTrip(leave).(*kmsg.LeaveGroupResponse)
			if left.ErrorCode != 0 || (version >= 3 && (len(left.Members) != 1 || left.Members[0].ErrorCode != 0)) {
				t.Fatal(left)
			}
		})
	}
}
