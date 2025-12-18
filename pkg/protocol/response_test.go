package protocol

import (
	"fmt"
	"testing"
)

func strPtr(s string) *string {
	return &s
}

func TestEncodeApiVersionsResponse(t *testing.T) {
	payload, err := EncodeApiVersionsResponse(&ApiVersionsResponse{
		CorrelationID: 99,
		ErrorCode:     0,
		Versions: []ApiVersion{
			{APIKey: APIKeyMetadata, MinVersion: 0, MaxVersion: 1},
		},
	})
	if err != nil {
		t.Fatalf("EncodeApiVersionsResponse: %v", err)
	}
	reader := newByteReader(payload)
	corr, _ := reader.Int32()
	if corr != 99 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
}

func TestEncodeMetadataResponse(t *testing.T) {
	clusterID := "cluster-1"
	payload, err := EncodeMetadataResponse(&MetadataResponse{
		CorrelationID: 5,
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
	reader.Int32() // log_offset_delta placeholder
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
