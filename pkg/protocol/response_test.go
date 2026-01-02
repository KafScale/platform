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

package protocol

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func strPtr(s string) *string {
	return &s
}

func TestEncodeApiVersionsResponseV0(t *testing.T) {
	payload, err := EncodeApiVersionsResponse(&ApiVersionsResponse{
		CorrelationID: 99,
		ErrorCode:     0,
		Versions: []ApiVersion{
			{APIKey: APIKeyMetadata, MinVersion: 0, MaxVersion: 1},
		},
	}, 0)
	if err != nil {
		t.Fatalf("EncodeApiVersionsResponse: %v", err)
	}
	reader := newByteReader(payload)
	corr, _ := reader.Int32()
	if corr != 99 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
}

func TestEncodeApiVersionsResponseV3(t *testing.T) {
	resp := &ApiVersionsResponse{
		CorrelationID: 101,
		ErrorCode:     0,
		Versions: []ApiVersion{
			{APIKey: APIKeyMetadata, MinVersion: 0, MaxVersion: 12},
		},
	}
	payload, err := EncodeApiVersionsResponse(resp, 3)
	if err != nil {
		t.Fatalf("EncodeApiVersionsResponse: %v", err)
	}
	reader := newByteReader(payload)
	corr, _ := reader.Int32()
	if corr != 101 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	body := payload[4:]
	kmsgResp := kmsg.NewPtrApiVersionsResponse()
	kmsgResp.Version = 3
	if err := kmsgResp.ReadFrom(body); err != nil {
		t.Fatalf("decode api versions response: %v", err)
	}
	if len(kmsgResp.ApiKeys) != 1 || kmsgResp.ApiKeys[0].ApiKey != APIKeyMetadata {
		t.Fatalf("unexpected api versions response: %#v", kmsgResp.ApiKeys)
	}
}

func TestEncodeMetadataResponse(t *testing.T) {
	clusterID := "cluster-1"
	payload, err := EncodeMetadataResponse(&MetadataResponse{
		CorrelationID: 5,
		ThrottleMs:    0,
		Brokers: []MetadataBroker{
			{NodeID: 1, Host: "localhost", Port: 9092},
		},
		ClusterID:    &clusterID,
		ControllerID: 1,
		Topics: []MetadataTopic{
			{
				ErrorCode: 0,
				Name:      "orders",
				Partitions: []MetadataPartition{
					{
						ErrorCode:      0,
						PartitionIndex: 0,
						LeaderID:       1,
						ReplicaNodes:   []int32{1},
						ISRNodes:       []int32{1},
					},
				},
			},
		},
	}, 0)
	if err != nil {
		t.Fatalf("EncodeMetadataResponse: %v", err)
	}
	reader := newByteReader(payload)
	corr, _ := reader.Int32()
	if corr != 5 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
}

func TestEncodeCreateTopicsResponseV2(t *testing.T) {
	resp := &CreateTopicsResponse{
		CorrelationID: 31,
		ThrottleMs:    0,
		Topics: []CreateTopicResult{
			{Name: "orders", ErrorCode: NONE},
		},
	}
	payload, err := EncodeCreateTopicsResponse(resp, 2)
	if err != nil {
		t.Fatalf("EncodeCreateTopicsResponse: %v", err)
	}
	body := payload[4:]
	kmsgResp := kmsg.NewPtrCreateTopicsResponse()
	kmsgResp.Version = 2
	if err := kmsgResp.ReadFrom(body); err != nil {
		t.Fatalf("decode create topics response: %v", err)
	}
	if len(kmsgResp.Topics) != 1 || kmsgResp.Topics[0].Topic != "orders" {
		t.Fatalf("unexpected create topics response: %#v", kmsgResp.Topics)
	}
}

func TestEncodeDeleteTopicsResponseV1(t *testing.T) {
	resp := &DeleteTopicsResponse{
		CorrelationID: 41,
		ThrottleMs:    0,
		Topics: []DeleteTopicResult{
			{Name: "orders", ErrorCode: NONE},
		},
	}
	payload, err := EncodeDeleteTopicsResponse(resp, 1)
	if err != nil {
		t.Fatalf("EncodeDeleteTopicsResponse: %v", err)
	}
	body := payload[4:]
	kmsgResp := kmsg.NewPtrDeleteTopicsResponse()
	kmsgResp.Version = 1
	if err := kmsgResp.ReadFrom(body); err != nil {
		t.Fatalf("decode delete topics response: %v", err)
	}
	if len(kmsgResp.Topics) != 1 || kmsgResp.Topics[0].Topic == nil || *kmsgResp.Topics[0].Topic != "orders" {
		t.Fatalf("unexpected delete topics response: %#v", kmsgResp.Topics)
	}
}

func TestEncodeMetadataResponseV10IncludesTopicID(t *testing.T) {
	clusterID := "cluster-1"
	var topicID [16]byte
	for i := range topicID {
		topicID[i] = byte(i + 1)
	}
	payload, err := EncodeMetadataResponse(&MetadataResponse{
		CorrelationID: 7,
		ThrottleMs:    0,
		Brokers: []MetadataBroker{
			{NodeID: 1, Host: "localhost", Port: 9092},
		},
		ClusterID:    &clusterID,
		ControllerID: 1,
		Topics: []MetadataTopic{
			{
				ErrorCode:  0,
				Name:       "orders",
				TopicID:    topicID,
				IsInternal: false,
				Partitions: []MetadataPartition{
					{
						ErrorCode:      0,
						PartitionIndex: 0,
						LeaderID:       1,
						ReplicaNodes:   []int32{1},
						ISRNodes:       []int32{1},
					},
				},
			},
		},
	}, 10)
	if err != nil {
		t.Fatalf("EncodeMetadataResponse v10: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 7 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero header tags got %d", tags)
	}
	if _, err := reader.Int32(); err != nil { // throttle
		t.Fatalf("read throttle: %v", err)
	}
	if brokers, _ := reader.CompactArrayLen(); brokers != 1 {
		t.Fatalf("expected 1 broker got %d", brokers)
	}
	if _, err := reader.Int32(); err != nil {
		t.Fatalf("read broker id: %v", err)
	}
	if host, _ := reader.CompactString(); host != "localhost" {
		t.Fatalf("unexpected broker host %q", host)
	}
	reader.Int32() // port
	if _, err := reader.CompactNullableString(); err != nil {
		t.Fatalf("read rack: %v", err)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero broker tags got %d", tags)
	}
	if _, err := reader.CompactNullableString(); err != nil {
		t.Fatalf("read cluster id: %v", err)
	}
	reader.Int32() // controller id
	if topics, _ := reader.CompactArrayLen(); topics != 1 {
		t.Fatalf("expected 1 topic got %d", topics)
	}
	reader.Int16() // error code
	if name, _ := reader.CompactNullableString(); name == nil || *name != "orders" {
		t.Fatalf("unexpected topic name %v", name)
	}
	id, err := reader.UUID()
	if err != nil {
		t.Fatalf("read topic id: %v", err)
	}
	if id != topicID {
		t.Fatalf("unexpected topic id %v", id)
	}
	if internal, _ := reader.Bool(); internal {
		t.Fatalf("expected non-internal topic")
	}
	if parts, _ := reader.CompactArrayLen(); parts != 1 {
		t.Fatalf("expected 1 partition got %d", parts)
	}
	reader.Int16() // partition error
	reader.Int32() // partition index
	reader.Int32() // leader
	reader.Int32() // leader epoch
	if replicas, _ := reader.CompactArrayLen(); replicas != 1 {
		t.Fatalf("expected 1 replica got %d", replicas)
	}
	reader.Int32()
	if isr, _ := reader.CompactArrayLen(); isr != 1 {
		t.Fatalf("expected 1 isr got %d", isr)
	}
	reader.Int32()
	if offline, _ := reader.CompactArrayLen(); offline != 0 {
		t.Fatalf("expected 0 offline replicas got %d", offline)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero partition tags got %d", tags)
	}
	reader.Int32() // authorized ops
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero topic tags got %d", tags)
	}
	reader.Int32() // cluster authorized ops
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero response tags got %d", tags)
	}
	if reader.remaining() != 0 {
		t.Fatalf("unexpected trailing bytes: %d", reader.remaining())
	}
}

func TestEncodeProduceResponse(t *testing.T) {
	payload, err := EncodeProduceResponse(&ProduceResponse{
		CorrelationID: 7,
		Topics: []ProduceTopicResponse{
			{
				Name: "orders",
				Partitions: []ProducePartitionResponse{
					{Partition: 0, ErrorCode: 0, BaseOffset: 10, LogAppendTimeMs: 1234, LogStartOffset: 10},
				},
			},
		},
		ThrottleMs: 5,
	}, 8)
	if err != nil {
		t.Fatalf("EncodeProduceResponse: %v", err)
	}
	reader := newByteReader(payload)
	corr, _ := reader.Int32()
	if corr != 7 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	topicCount, _ := reader.Int32()
	if topicCount != 1 {
		t.Fatalf("expected 1 topic got %d", topicCount)
	}
	if name, _ := reader.String(); name != "orders" {
		t.Fatalf("unexpected topic %q", name)
	}
	partCount, _ := reader.Int32()
	if partCount != 1 {
		t.Fatalf("expected 1 partition got %d", partCount)
	}
	reader.Int32() // partition
	reader.Int16() // error code
	reader.Int64() // base offset
	reader.Int64() // log append time
	reader.Int64() // log start offset
	if errCount, _ := reader.Int32(); errCount != 0 {
		t.Fatalf("expected 0 record errors got %d", errCount)
	}
	if msg, _ := reader.NullableString(); msg != nil {
		t.Fatalf("expected nil record error message got %v", msg)
	}
}

func TestEncodeProduceResponseFlexible(t *testing.T) {
	payload, err := EncodeProduceResponse(&ProduceResponse{
		CorrelationID: 9,
		Topics: []ProduceTopicResponse{
			{
				Name: "orders",
				Partitions: []ProducePartitionResponse{
					{Partition: 0, ErrorCode: 0, BaseOffset: 42, LogAppendTimeMs: 11, LogStartOffset: 5},
				},
			},
		},
		ThrottleMs: 3,
	}, 9)
	if err != nil {
		t.Fatalf("EncodeProduceResponse flexible: %v", err)
	}
	reader := newByteReader(payload)
	corr, _ := reader.Int32()
	if corr != 9 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero header tags got %d", tags)
	}
	topicCount, _ := reader.CompactArrayLen()
	if topicCount != 1 {
		t.Fatalf("expected 1 topic got %d", topicCount)
	}
	name, _ := reader.CompactString()
	if name != "orders" {
		t.Fatalf("unexpected topic %q", name)
	}
	partCount, _ := reader.CompactArrayLen()
	if partCount != 1 {
		t.Fatalf("expected 1 partition got %d", partCount)
	}
	if partition, _ := reader.Int32(); partition != 0 {
		t.Fatalf("unexpected partition %d", partition)
	}
	if errCode, _ := reader.Int16(); errCode != 0 {
		t.Fatalf("unexpected error code %d", errCode)
	}
	if base, _ := reader.Int64(); base != 42 {
		t.Fatalf("unexpected base offset %d", base)
	}
	reader.Int64() // log append time
	reader.Int64() // log start offset
	if errCount, _ := reader.CompactArrayLen(); errCount != 0 {
		t.Fatalf("expected 0 record errors got %d", errCount)
	}
	if msg, _ := reader.CompactNullableString(); msg != nil {
		t.Fatalf("expected nil record error message got %v", msg)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero partition tags got %d", tags)
	}
	if topicTags, _ := reader.UVarint(); topicTags != 0 {
		t.Fatalf("expected zero topic tags got %d", topicTags)
	}
	if throttle, _ := reader.Int32(); throttle != 3 {
		t.Fatalf("unexpected throttle %d", throttle)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero response tags got %d", tags)
	}
	if reader.remaining() != 0 {
		t.Fatalf("unexpected trailing bytes: %d", reader.remaining())
	}
}

func TestEncodeProduceResponseLegacyVersions(t *testing.T) {
	resp := &ProduceResponse{
		CorrelationID: 7,
		Topics: []ProduceTopicResponse{
			{
				Name: "orders",
				Partitions: []ProducePartitionResponse{
					{Partition: 0, ErrorCode: 0, BaseOffset: 10, LogAppendTimeMs: 123, LogStartOffset: 5},
				},
			},
		},
		ThrottleMs: 0,
	}

	tests := []struct {
		name    string
		version int16
	}{
		{name: "v0", version: 0},
		{name: "v7", version: 7},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			payload, err := EncodeProduceResponse(resp, tc.version)
			if err != nil {
				t.Fatalf("EncodeProduceResponse v%d: %v", tc.version, err)
			}
			reader := newByteReader(payload)
			if _, err := reader.Int32(); err != nil {
				t.Fatalf("read correlation: %v", err)
			}
			topicCount, err := reader.Int32()
			if err != nil {
				t.Fatalf("read topic count: %v", err)
			}
			for i := int32(0); i < topicCount; i++ {
				if _, err := reader.String(); err != nil {
					t.Fatalf("read topic name: %v", err)
				}
				partCount, err := reader.Int32()
				if err != nil {
					t.Fatalf("read partition count: %v", err)
				}
				for j := int32(0); j < partCount; j++ {
					if _, err := reader.Int32(); err != nil {
						t.Fatalf("read partition id: %v", err)
					}
					if _, err := reader.Int16(); err != nil {
						t.Fatalf("read error code: %v", err)
					}
					if _, err := reader.Int64(); err != nil {
						t.Fatalf("read base offset: %v", err)
					}
					if tc.version >= 3 {
						if _, err := reader.Int64(); err != nil {
							t.Fatalf("read log append time: %v", err)
						}
					}
					if tc.version >= 5 {
						if _, err := reader.Int64(); err != nil {
							t.Fatalf("read log start offset: %v", err)
						}
					}
					if tc.version >= 8 {
						if _, err := reader.Int32(); err != nil {
							t.Fatalf("read log offset delta: %v", err)
						}
					}
				}
			}
			if tc.version >= 1 {
				if _, err := reader.Int32(); err != nil {
					t.Fatalf("read throttle ms: %v", err)
				}
			}
			if reader.remaining() != 0 {
				t.Fatalf("unexpected trailing bytes: %d", reader.remaining())
			}
		})
	}
}

func TestEncodeListOffsetsResponseV0(t *testing.T) {
	payload, err := EncodeListOffsetsResponse(0, &ListOffsetsResponse{
		CorrelationID: 15,
		Topics: []ListOffsetsTopicResponse{
			{
				Name: "orders",
				Partitions: []ListOffsetsPartitionResponse{
					{Partition: 0, ErrorCode: 0, OldStyleOffsets: []int64{42}},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("EncodeListOffsetsResponse: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 15 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	if topics, _ := reader.Int32(); topics != 1 {
		t.Fatalf("unexpected topic count %d", topics)
	}
	if name, _ := reader.String(); name != "orders" {
		t.Fatalf("unexpected topic name %q", name)
	}
	if parts, _ := reader.Int32(); parts != 1 {
		t.Fatalf("unexpected partition count %d", parts)
	}
	if part, _ := reader.Int32(); part != 0 {
		t.Fatalf("unexpected partition %d", part)
	}
	if errCode, _ := reader.Int16(); errCode != 0 {
		t.Fatalf("unexpected error code %d", errCode)
	}
	if count, _ := reader.Int32(); count != 1 {
		t.Fatalf("unexpected offset count %d", count)
	}
	if offset, _ := reader.Int64(); offset != 42 {
		t.Fatalf("unexpected offset %d", offset)
	}
	if reader.remaining() != 0 {
		t.Fatalf("expected no remaining bytes, got %d", reader.remaining())
	}
}

func TestEncodeFetchResponse(t *testing.T) {
	payload, err := EncodeFetchResponse(&FetchResponse{
		CorrelationID: 3,
		ThrottleMs:    9,
		ErrorCode:     NONE,
		SessionID:     7,
		Topics: []FetchTopicResponse{
			{
				Name: "orders",
				Partitions: []FetchPartitionResponse{
					{
						Partition:            0,
						ErrorCode:            NONE,
						HighWatermark:        10,
						LastStableOffset:     10,
						LogStartOffset:       0,
						PreferredReadReplica: -1,
						RecordSet:            []byte("records"),
					},
				},
			},
		},
	}, 11)
	if err != nil {
		t.Fatalf("EncodeFetchResponse: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 3 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	if throttle, _ := reader.Int32(); throttle != 9 {
		t.Fatalf("unexpected throttle %d", throttle)
	}
	if errCode, _ := reader.Int16(); errCode != 0 {
		t.Fatalf("unexpected error code %d", errCode)
	}
	if session, _ := reader.Int32(); session != 7 {
		t.Fatalf("unexpected session id %d", session)
	}
	if topicCount, _ := reader.Int32(); topicCount != 1 {
		t.Fatalf("unexpected topic count %d", topicCount)
	}
	name, _ := reader.String()
	if name != "orders" {
		t.Fatalf("unexpected topic %q", name)
	}
	if partCount, _ := reader.Int32(); partCount != 1 {
		t.Fatalf("unexpected partition count %d", partCount)
	}
	if partition, _ := reader.Int32(); partition != 0 {
		t.Fatalf("unexpected partition %d", partition)
	}
	if perr, _ := reader.Int16(); perr != 0 {
		t.Fatalf("unexpected partition error %d", perr)
	}
	if hw, _ := reader.Int64(); hw != 10 {
		t.Fatalf("unexpected high watermark %d", hw)
	}
	if lso, _ := reader.Int64(); lso != 10 {
		t.Fatalf("unexpected lso %d", lso)
	}
	if lsoff, _ := reader.Int64(); lsoff != 0 {
		t.Fatalf("unexpected log start offset %d", lsoff)
	}
	if abortedCount, _ := reader.Int32(); abortedCount != 0 {
		t.Fatalf("unexpected aborted txns %d", abortedCount)
	}
	if pref, _ := reader.Int32(); pref != -1 {
		t.Fatalf("unexpected preferred replica %d", pref)
	}
	recordLen, _ := reader.Int32()
	if recordLen != int32(len("records")) {
		t.Fatalf("unexpected record set length %d", recordLen)
	}
	if _, err := reader.read(int(recordLen)); err != nil {
		t.Fatalf("read record set: %v", err)
	}
	if reader.remaining() != 0 {
		t.Fatalf("unexpected trailing bytes %d", reader.remaining())
	}
}

func TestEncodeFetchResponseV13(t *testing.T) {
	var topicID [16]byte
	for i := range topicID {
		topicID[i] = byte(i + 1)
	}
	payload, err := EncodeFetchResponse(&FetchResponse{
		CorrelationID: 11,
		ThrottleMs:    1,
		ErrorCode:     NONE,
		SessionID:     2,
		Topics: []FetchTopicResponse{
			{
				TopicID: topicID,
				Partitions: []FetchPartitionResponse{
					{
						Partition:            0,
						ErrorCode:            NONE,
						HighWatermark:        5,
						LastStableOffset:     5,
						LogStartOffset:       0,
						PreferredReadReplica: -1,
						RecordSet:            []byte("records"),
					},
				},
			},
		},
	}, 13)
	if err != nil {
		t.Fatalf("EncodeFetchResponse v13: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 11 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero header tags got %d", tags)
	}
	if throttle, _ := reader.Int32(); throttle != 1 {
		t.Fatalf("unexpected throttle %d", throttle)
	}
	if errCode, _ := reader.Int16(); errCode != 0 {
		t.Fatalf("unexpected error code %d", errCode)
	}
	if session, _ := reader.Int32(); session != 2 {
		t.Fatalf("unexpected session id %d", session)
	}
	if topicCount, _ := reader.CompactArrayLen(); topicCount != 1 {
		t.Fatalf("unexpected topic count %d", topicCount)
	}
	gotID, err := reader.UUID()
	if err != nil {
		t.Fatalf("read topic id: %v", err)
	}
	if gotID != topicID {
		t.Fatalf("unexpected topic id %v", gotID)
	}
	if partCount, _ := reader.CompactArrayLen(); partCount != 1 {
		t.Fatalf("unexpected partition count %d", partCount)
	}
	if partition, _ := reader.Int32(); partition != 0 {
		t.Fatalf("unexpected partition %d", partition)
	}
	if perr, _ := reader.Int16(); perr != 0 {
		t.Fatalf("unexpected partition error %d", perr)
	}
	if hw, _ := reader.Int64(); hw != 5 {
		t.Fatalf("unexpected high watermark %d", hw)
	}
	if lso, _ := reader.Int64(); lso != 5 {
		t.Fatalf("unexpected lso %d", lso)
	}
	if lsoff, _ := reader.Int64(); lsoff != 0 {
		t.Fatalf("unexpected log start offset %d", lsoff)
	}
	if abortedCount, _ := reader.CompactArrayLen(); abortedCount != 0 {
		t.Fatalf("unexpected aborted txns %d", abortedCount)
	}
	if pref, _ := reader.Int32(); pref != -1 {
		t.Fatalf("unexpected preferred replica %d", pref)
	}
	recordSet, err := reader.CompactBytes()
	if err != nil {
		t.Fatalf("read record set: %v", err)
	}
	if string(recordSet) != "records" {
		t.Fatalf("unexpected record set %q", recordSet)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero partition tags got %d", tags)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero topic tags got %d", tags)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero response tags got %d", tags)
	}
	if reader.remaining() != 0 {
		t.Fatalf("unexpected trailing bytes %d", reader.remaining())
	}
}

func TestEncodeFetchResponseV13KmsgRoundTrip(t *testing.T) {
	var topicID [16]byte
	for i := range topicID {
		topicID[i] = byte(i + 1)
	}
	recordSet := makeTestRecordBatch(2, 0)
	payload, err := EncodeFetchResponse(&FetchResponse{
		CorrelationID: 21,
		ThrottleMs:    0,
		ErrorCode:     NONE,
		SessionID:     0,
		Topics: []FetchTopicResponse{
			{
				TopicID: topicID,
				Partitions: []FetchPartitionResponse{
					{
						Partition:            0,
						ErrorCode:            NONE,
						HighWatermark:        2,
						LastStableOffset:     2,
						LogStartOffset:       0,
						PreferredReadReplica: -1,
						RecordSet:            recordSet,
					},
				},
			},
		},
	}, 13)
	if err != nil {
		t.Fatalf("EncodeFetchResponse v13: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 21 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	if err := reader.SkipTaggedFields(); err != nil {
		t.Fatalf("skip response header tags: %v", err)
	}
	body := payload[reader.pos:]
	kmsgResp := kmsg.NewPtrFetchResponse()
	kmsgResp.Version = 13
	if err := kmsgResp.ReadFrom(body); err != nil {
		t.Fatalf("kmsg decode: %v", err)
	}
	if len(kmsgResp.Topics) != 1 || len(kmsgResp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected topic/partition counts: %+v", kmsgResp.Topics)
	}
	if kmsgResp.Topics[0].TopicID != topicID {
		t.Fatalf("unexpected topic id %v", kmsgResp.Topics[0].TopicID)
	}
	part := kmsgResp.Topics[0].Partitions[0]
	if part.ErrorCode != 0 {
		t.Fatalf("unexpected partition error %d", part.ErrorCode)
	}
	if len(part.RecordBatches) != len(recordSet) {
		t.Fatalf("unexpected record batch length %d", len(part.RecordBatches))
	}
}

func TestEncodeFindCoordinatorResponseFlexible(t *testing.T) {
	payload, err := EncodeFindCoordinatorResponse(&FindCoordinatorResponse{
		CorrelationID: 4,
		ThrottleMs:    7,
		ErrorCode:     0,
		NodeID:        1,
		Host:          "127.0.0.1",
		Port:          39092,
	}, 3)
	if err != nil {
		t.Fatalf("EncodeFindCoordinatorResponse: %v", err)
	}
	reader := newByteReader(payload)
	corr, _ := reader.Int32()
	if corr != 4 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero header tags got %d", tags)
	}
	if throttle, _ := reader.Int32(); throttle != 7 {
		t.Fatalf("unexpected throttle %d", throttle)
	}
	if errCode, _ := reader.Int16(); errCode != 0 {
		t.Fatalf("unexpected error code %d", errCode)
	}
	if errMsg, _ := reader.CompactNullableString(); errMsg != nil {
		t.Fatalf("expected nil error message got %q", *errMsg)
	}
	if nodeID, _ := reader.Int32(); nodeID != 1 {
		t.Fatalf("unexpected node id %d", nodeID)
	}
	host, _ := reader.CompactString()
	if host != "127.0.0.1" {
		t.Fatalf("unexpected host %q", host)
	}
	if port, _ := reader.Int32(); port != 39092 {
		t.Fatalf("unexpected port %d", port)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero response tags got %d", tags)
	}
	if reader.remaining() != 0 {
		t.Fatalf("unexpected trailing bytes %d", reader.remaining())
	}
}

func TestEncodeDescribeGroupsResponseV5KmsgRoundTrip(t *testing.T) {
	payload, err := EncodeDescribeGroupsResponse(&DescribeGroupsResponse{
		CorrelationID: 55,
		ThrottleMs:    0,
		Groups: []DescribeGroupsResponseGroup{
			{
				ErrorCode:            NONE,
				GroupID:              "group-1",
				State:                "Stable",
				ProtocolType:         "consumer",
				Protocol:             "range",
				AuthorizedOperations: 0,
				Members: []DescribeGroupsResponseGroupMember{
					{
						MemberID:         "member-1",
						ClientID:         "client-1",
						ClientHost:       "127.0.0.1",
						ProtocolMetadata: []byte{0x01},
						MemberAssignment: []byte{0x02},
					},
				},
			},
		},
	}, 5)
	if err != nil {
		t.Fatalf("EncodeDescribeGroupsResponse: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 55 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	if err := reader.SkipTaggedFields(); err != nil {
		t.Fatalf("skip response header tags: %v", err)
	}
	body := payload[reader.pos:]
	kmsgResp := kmsg.NewPtrDescribeGroupsResponse()
	kmsgResp.Version = 5
	if err := kmsgResp.ReadFrom(body); err != nil {
		t.Fatalf("kmsg decode: %v", err)
	}
	if len(kmsgResp.Groups) != 1 {
		t.Fatalf("unexpected groups: %+v", kmsgResp.Groups)
	}
	group := kmsgResp.Groups[0]
	if group.Group != "group-1" || group.State != "Stable" {
		t.Fatalf("unexpected group data: %+v", group)
	}
	if len(group.Members) != 1 || group.Members[0].MemberID != "member-1" {
		t.Fatalf("unexpected member data: %+v", group.Members)
	}
}

func TestEncodeListGroupsResponseV5KmsgRoundTrip(t *testing.T) {
	payload, err := EncodeListGroupsResponse(&ListGroupsResponse{
		CorrelationID: 77,
		ThrottleMs:    0,
		ErrorCode:     NONE,
		Groups: []ListGroupsResponseGroup{
			{
				GroupID:      "group-1",
				ProtocolType: "consumer",
				GroupState:   "Stable",
				GroupType:    "classic",
			},
		},
	}, 5)
	if err != nil {
		t.Fatalf("EncodeListGroupsResponse: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 77 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	if err := reader.SkipTaggedFields(); err != nil {
		t.Fatalf("skip response header tags: %v", err)
	}
	body := payload[reader.pos:]
	kmsgResp := kmsg.NewPtrListGroupsResponse()
	kmsgResp.Version = 5
	if err := kmsgResp.ReadFrom(body); err != nil {
		t.Fatalf("kmsg decode: %v", err)
	}
	if len(kmsgResp.Groups) != 1 || kmsgResp.Groups[0].Group != "group-1" {
		t.Fatalf("unexpected list groups: %+v", kmsgResp.Groups)
	}
}

func TestEncodeOffsetForLeaderEpochResponseV3KmsgRoundTrip(t *testing.T) {
	payload, err := EncodeOffsetForLeaderEpochResponse(&OffsetForLeaderEpochResponse{
		CorrelationID: 13,
		ThrottleMs:    0,
		Topics: []OffsetForLeaderEpochTopicResponse{
			{
				Name: "orders",
				Partitions: []OffsetForLeaderEpochPartitionResponse{
					{Partition: 0, ErrorCode: NONE, LeaderEpoch: 1, EndOffset: 12},
				},
			},
		},
	}, 3)
	if err != nil {
		t.Fatalf("EncodeOffsetForLeaderEpochResponse: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 13 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	body := payload[reader.pos:]
	kmsgResp := kmsg.NewPtrOffsetForLeaderEpochResponse()
	kmsgResp.Version = 3
	if err := kmsgResp.ReadFrom(body); err != nil {
		t.Fatalf("kmsg decode: %v", err)
	}
	if len(kmsgResp.Topics) != 1 || kmsgResp.Topics[0].Topic != "orders" {
		t.Fatalf("unexpected response: %+v", kmsgResp.Topics)
	}
}

func TestEncodeDescribeConfigsResponseV4KmsgRoundTrip(t *testing.T) {
	payload, err := EncodeDescribeConfigsResponse(&DescribeConfigsResponse{
		CorrelationID: 19,
		ThrottleMs:    0,
		Resources: []DescribeConfigsResponseResource{
			{
				ErrorCode:    NONE,
				ResourceType: ConfigResourceTopic,
				ResourceName: "orders",
				Configs: []DescribeConfigsResponseConfig{
					{
						Name:        "retention.ms",
						Value:       strPtr("1000"),
						ReadOnly:    false,
						IsDefault:   false,
						Source:      ConfigSourceDynamicTopic,
						IsSensitive: false,
						ConfigType:  ConfigTypeLong,
					},
				},
			},
		},
	}, 4)
	if err != nil {
		t.Fatalf("EncodeDescribeConfigsResponse: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 19 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	if err := reader.SkipTaggedFields(); err != nil {
		t.Fatalf("skip response header tags: %v", err)
	}
	body := payload[reader.pos:]
	kmsgResp := kmsg.NewPtrDescribeConfigsResponse()
	kmsgResp.Version = 4
	if err := kmsgResp.ReadFrom(body); err != nil {
		t.Fatalf("kmsg decode: %v", err)
	}
	if len(kmsgResp.Resources) != 1 || kmsgResp.Resources[0].ResourceName != "orders" {
		t.Fatalf("unexpected resources: %+v", kmsgResp.Resources)
	}
}

func TestEncodeAlterConfigsResponseV1KmsgRoundTrip(t *testing.T) {
	payload, err := EncodeAlterConfigsResponse(&AlterConfigsResponse{
		CorrelationID: 27,
		ThrottleMs:    0,
		Resources: []AlterConfigsResponseResource{
			{
				ErrorCode:    NONE,
				ResourceType: ConfigResourceTopic,
				ResourceName: "orders",
			},
		},
	}, 1)
	if err != nil {
		t.Fatalf("EncodeAlterConfigsResponse: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 27 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	body := payload[reader.pos:]
	kmsgResp := kmsg.NewPtrAlterConfigsResponse()
	kmsgResp.Version = 1
	if err := kmsgResp.ReadFrom(body); err != nil {
		t.Fatalf("kmsg decode: %v", err)
	}
	if len(kmsgResp.Resources) != 1 || kmsgResp.Resources[0].ResourceName != "orders" {
		t.Fatalf("unexpected response: %+v", kmsgResp.Resources)
	}
}

func TestEncodeCreatePartitionsResponseV3KmsgRoundTrip(t *testing.T) {
	payload, err := EncodeCreatePartitionsResponse(&CreatePartitionsResponse{
		CorrelationID: 33,
		ThrottleMs:    0,
		Topics: []CreatePartitionsResponseTopic{
			{Name: "orders", ErrorCode: NONE},
		},
	}, 3)
	if err != nil {
		t.Fatalf("EncodeCreatePartitionsResponse: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 33 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	if err := reader.SkipTaggedFields(); err != nil {
		t.Fatalf("skip response header tags: %v", err)
	}
	body := payload[reader.pos:]
	kmsgResp := kmsg.NewPtrCreatePartitionsResponse()
	kmsgResp.Version = 3
	if err := kmsgResp.ReadFrom(body); err != nil {
		t.Fatalf("kmsg decode: %v", err)
	}
	if len(kmsgResp.Topics) != 1 || kmsgResp.Topics[0].Topic != "orders" {
		t.Fatalf("unexpected response: %+v", kmsgResp.Topics)
	}
}

func TestEncodeDeleteGroupsResponseV2KmsgRoundTrip(t *testing.T) {
	payload, err := EncodeDeleteGroupsResponse(&DeleteGroupsResponse{
		CorrelationID: 35,
		ThrottleMs:    0,
		Groups: []DeleteGroupsResponseGroup{
			{Group: "group-1", ErrorCode: NONE},
		},
	}, 2)
	if err != nil {
		t.Fatalf("EncodeDeleteGroupsResponse: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 35 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	if err := reader.SkipTaggedFields(); err != nil {
		t.Fatalf("skip response header tags: %v", err)
	}
	body := payload[reader.pos:]
	kmsgResp := kmsg.NewPtrDeleteGroupsResponse()
	kmsgResp.Version = 2
	if err := kmsgResp.ReadFrom(body); err != nil {
		t.Fatalf("kmsg decode: %v", err)
	}
	if len(kmsgResp.Groups) != 1 || kmsgResp.Groups[0].Group != "group-1" {
		t.Fatalf("unexpected response: %+v", kmsgResp.Groups)
	}
}

func TestEncodeFindCoordinatorResponseLegacy(t *testing.T) {
	errMsg := "ok"
	payload, err := EncodeFindCoordinatorResponse(&FindCoordinatorResponse{
		CorrelationID: 2,
		ThrottleMs:    9,
		ErrorCode:     1,
		ErrorMessage:  &errMsg,
		NodeID:        5,
		Host:          "node-1",
		Port:          9092,
	}, 2)
	if err != nil {
		t.Fatalf("EncodeFindCoordinatorResponse legacy: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 2 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	if throttle, _ := reader.Int32(); throttle != 9 {
		t.Fatalf("unexpected throttle %d", throttle)
	}
	if code, _ := reader.Int16(); code != 1 {
		t.Fatalf("unexpected error code %d", code)
	}
	msg, _ := reader.NullableString()
	if msg == nil || *msg != "ok" {
		t.Fatalf("unexpected error message %v", msg)
	}
	if node, _ := reader.Int32(); node != 5 {
		t.Fatalf("unexpected node %d", node)
	}
	host, _ := reader.String()
	if host != "node-1" {
		t.Fatalf("unexpected host %q", host)
	}
	if port, _ := reader.Int32(); port != 9092 {
		t.Fatalf("unexpected port %d", port)
	}
}

func TestEncodeJoinGroupResponseV4(t *testing.T) {
	payload, err := EncodeJoinGroupResponse(&JoinGroupResponse{
		CorrelationID: 5,
		ThrottleMs:    7,
		ErrorCode:     0,
		GenerationID:  3,
		ProtocolName:  "range",
		LeaderID:      "member-1",
		MemberID:      "member-2",
		Members: []JoinGroupMember{
			{MemberID: "member-1", Metadata: []byte{0x01}},
			{MemberID: "member-2", Metadata: []byte{0x02}},
		},
	}, 4)
	if err != nil {
		t.Fatalf("EncodeJoinGroupResponse: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 5 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
	if throttle, _ := reader.Int32(); throttle != 7 {
		t.Fatalf("unexpected throttle %d", throttle)
	}
	if errCode, _ := reader.Int16(); errCode != 0 {
		t.Fatalf("unexpected error code %d", errCode)
	}
	if gen, _ := reader.Int32(); gen != 3 {
		t.Fatalf("unexpected generation %d", gen)
	}
	if proto, _ := reader.String(); proto != "range" {
		t.Fatalf("unexpected protocol %q", proto)
	}
	if leader, _ := reader.String(); leader != "member-1" {
		t.Fatalf("unexpected leader %q", leader)
	}
	if member, _ := reader.String(); member != "member-2" {
		t.Fatalf("unexpected member %q", member)
	}
	if count, _ := reader.Int32(); count != 2 {
		t.Fatalf("unexpected member count %d", count)
	}
	for i := 0; i < 2; i++ {
		id, _ := reader.String()
		if id != fmt.Sprintf("member-%d", i+1) {
			t.Fatalf("unexpected member id %q", id)
		}
		length, _ := reader.Int32()
		if length != 1 {
			t.Fatalf("unexpected metadata length %d", length)
		}
		reader.read(int(length))
	}
	if reader.remaining() != 0 {
		t.Fatalf("unexpected trailing bytes %d", reader.remaining())
	}
}

func TestEncodeSyncGroupResponseV2(t *testing.T) {
	payload, err := EncodeSyncGroupResponse(&SyncGroupResponse{
		CorrelationID: 11,
		ThrottleMs:    8,
		ErrorCode:     NONE,
		Assignment:    []byte{0x01, 0x02},
	}, 2)
	if err != nil {
		t.Fatalf("EncodeSyncGroupResponse v2: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 11 {
		t.Fatalf("unexpected correlation %d", corr)
	}
	if throttle, _ := reader.Int32(); throttle != 8 {
		t.Fatalf("unexpected throttle %d", throttle)
	}
	if errCode, _ := reader.Int16(); errCode != 0 {
		t.Fatalf("unexpected error code %d", errCode)
	}
	length, _ := reader.Int32()
	if length != 2 {
		t.Fatalf("unexpected assignment length %d", length)
	}
	if data, _ := reader.read(int(length)); len(data) != 2 || data[0] != 0x01 || data[1] != 0x02 {
		t.Fatalf("unexpected assignment payload %v", data)
	}
	if reader.remaining() != 0 {
		t.Fatalf("unexpected trailing bytes %d", reader.remaining())
	}
}

func TestEncodeSyncGroupResponseFlexibleV4(t *testing.T) {
	payload, err := EncodeSyncGroupResponse(&SyncGroupResponse{
		CorrelationID: 13,
		ThrottleMs:    4,
		ErrorCode:     NONE,
		Assignment:    []byte{0xaa},
	}, 4)
	if err != nil {
		t.Fatalf("EncodeSyncGroupResponse flexible: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 13 {
		t.Fatalf("unexpected correlation %d", corr)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero header tags got %d", tags)
	}
	if throttle, _ := reader.Int32(); throttle != 4 {
		t.Fatalf("unexpected throttle %d", throttle)
	}
	if errCode, _ := reader.Int16(); errCode != 0 {
		t.Fatalf("unexpected error code %d", errCode)
	}
	if b, _ := reader.CompactBytes(); len(b) != 1 || b[0] != 0xaa {
		t.Fatalf("unexpected assignment %v", b)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero response tags got %d", tags)
	}
	if reader.remaining() != 0 {
		t.Fatalf("unexpected trailing bytes %d", reader.remaining())
	}
}

func TestEncodeHeartbeatResponseV2(t *testing.T) {
	payload, err := EncodeHeartbeatResponse(&HeartbeatResponse{
		CorrelationID: 21,
		ThrottleMs:    9,
		ErrorCode:     NONE,
	}, 2)
	if err != nil {
		t.Fatalf("EncodeHeartbeatResponse v2: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 21 {
		t.Fatalf("unexpected correlation %d", corr)
	}
	if throttle, _ := reader.Int32(); throttle != 9 {
		t.Fatalf("unexpected throttle %d", throttle)
	}
	if errCode, _ := reader.Int16(); errCode != 0 {
		t.Fatalf("unexpected error code %d", errCode)
	}
	if reader.remaining() != 0 {
		t.Fatalf("unexpected trailing bytes %d", reader.remaining())
	}
}

func TestEncodeHeartbeatResponseFlexibleV4(t *testing.T) {
	payload, err := EncodeHeartbeatResponse(&HeartbeatResponse{
		CorrelationID: 22,
		ThrottleMs:    3,
		ErrorCode:     NONE,
	}, 4)
	if err != nil {
		t.Fatalf("EncodeHeartbeatResponse flexible: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 22 {
		t.Fatalf("unexpected correlation %d", corr)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero header tags got %d", tags)
	}
	if throttle, _ := reader.Int32(); throttle != 3 {
		t.Fatalf("unexpected throttle %d", throttle)
	}
	if errCode, _ := reader.Int16(); errCode != 0 {
		t.Fatalf("unexpected error code %d", errCode)
	}
	if tags, _ := reader.UVarint(); tags != 0 {
		t.Fatalf("expected zero response tags got %d", tags)
	}
	if reader.remaining() != 0 {
		t.Fatalf("unexpected trailing bytes %d", reader.remaining())
	}
}

func TestEncodeOffsetFetchResponse(t *testing.T) {
	resp := &OffsetFetchResponse{
		CorrelationID: 31,
		ThrottleMs:    12,
		Topics: []OffsetFetchTopicResponse{
			{
				Name: "orders",
				Partitions: []OffsetFetchPartitionResponse{
					{Partition: 0, Offset: 42, LeaderEpoch: -1, Metadata: strPtr("meta"), ErrorCode: NONE},
				},
			},
		},
		ErrorCode: NONE,
	}
	payload, err := EncodeOffsetFetchResponse(resp, 5)
	if err != nil {
		t.Fatalf("EncodeOffsetFetchResponse: %v", err)
	}
	reader := newByteReader(payload)
	if corr, _ := reader.Int32(); corr != 31 {
		t.Fatalf("unexpected correlation %d", corr)
	}
	if throttle, _ := reader.Int32(); throttle != 12 {
		t.Fatalf("unexpected throttle %d", throttle)
	}
	if topics, _ := reader.Int32(); topics != 1 {
		t.Fatalf("unexpected topic count %d", topics)
	}
	name, _ := reader.String()
	if name != "orders" {
		t.Fatalf("unexpected topic %q", name)
	}
	if partitions, _ := reader.Int32(); partitions != 1 {
		t.Fatalf("unexpected partition count %d", partitions)
	}
	if part, _ := reader.Int32(); part != 0 {
		t.Fatalf("unexpected partition %d", part)
	}
	if offset, _ := reader.Int64(); offset != 42 {
		t.Fatalf("unexpected offset %d", offset)
	}
	if leader, _ := reader.Int32(); leader != -1 {
		t.Fatalf("unexpected leader epoch %d", leader)
	}
	metaStr, _ := reader.NullableString()
	if metaStr == nil || *metaStr != "meta" {
		t.Fatalf("unexpected metadata %v", metaStr)
	}
	if perr, _ := reader.Int16(); perr != 0 {
		t.Fatalf("unexpected partition error %d", perr)
	}
	if errCode, _ := reader.Int16(); errCode != 0 {
		t.Fatalf("unexpected response error %d", errCode)
	}
	if reader.remaining() != 0 {
		t.Fatalf("unexpected trailing bytes %d", reader.remaining())
	}
}

func makeTestRecordBatch(count int32, baseOffset int64) []byte {
	const size = 90
	data := make([]byte, size)
	binary.BigEndian.PutUint64(data[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(data[8:12], uint32(size-12))
	binary.BigEndian.PutUint32(data[23:27], uint32(count-1))
	binary.BigEndian.PutUint32(data[57:61], uint32(count))
	return data
}
