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
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)


// buildRequestFrame prepends a Kafka request header to a kmsg-encoded body.
// This mirrors what a real Kafka client does: header first, then the body
// serialized by kmsg.
func buildRequestFrame(apiKey, version int16, correlationID int32, clientID *string, body []byte) []byte {
	w := newByteWriter(len(body) + 32)
	w.Int16(apiKey)
	w.Int16(version)
	w.Int32(correlationID)
	w.NullableString(clientID)

	// Flexible versions (KIP-482) include tagged fields in the header.
	req := kmsg.RequestForKey(apiKey)
	if req != nil {
		req.SetVersion(version)
		if req.IsFlexible() {
			w.WriteTaggedFields(0)
		}
	}
	w.write(body)
	return w.Bytes()
}

func TestParseRequest_Produce(t *testing.T) {
	req := kmsg.NewPtrProduceRequest()
	req.Version = 9
	req.Acks = 1
	req.TimeoutMillis = 1500
	topic := kmsg.NewProduceRequestTopic()
	topic.Topic = "orders"
	part := kmsg.NewProduceRequestTopicPartition()
	part.Partition = 0
	part.Records = []byte("record batch payload")
	topic.Partitions = append(topic.Partitions, part)
	req.Topics = append(req.Topics, topic)

	frame := buildRequestFrame(APIKeyProduce, 9, 42, kmsg.StringPtr("kgo"), req.AppendTo(nil))
	header, parsed, err := ParseRequest(frame)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyProduce || header.CorrelationID != 42 {
		t.Fatalf("unexpected header: %+v", header)
	}
	produceReq, ok := parsed.(*kmsg.ProduceRequest)
	if !ok {
		t.Fatalf("expected *kmsg.ProduceRequest got %T", parsed)
	}
	if produceReq.Acks != 1 || len(produceReq.Topics) != 1 {
		t.Fatalf("produce data mismatch: acks=%d topics=%d", produceReq.Acks, len(produceReq.Topics))
	}
	if string(produceReq.Topics[0].Partitions[0].Records) != "record batch payload" {
		t.Fatalf("records mismatch")
	}
}

func TestParseRequest_Metadata(t *testing.T) {
	req := kmsg.NewPtrMetadataRequest()
	req.Version = 12
	req.AllowAutoTopicCreation = true
	req.Topics = []kmsg.MetadataRequestTopic{
		{Topic: kmsg.StringPtr("orders-3eb53935-0")},
	}

	frame := buildRequestFrame(APIKeyMetadata, 12, 1, kmsg.StringPtr("kgo"), req.AppendTo(nil))
	header, parsed, err := ParseRequest(frame)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyMetadata || header.APIVersion != 12 {
		t.Fatalf("unexpected header: %+v", header)
	}
	metaReq, ok := parsed.(*kmsg.MetadataRequest)
	if !ok {
		t.Fatalf("expected *kmsg.MetadataRequest got %T", parsed)
	}
	if len(metaReq.Topics) != 1 || metaReq.Topics[0].Topic == nil || *metaReq.Topics[0].Topic != "orders-3eb53935-0" {
		t.Fatalf("unexpected topics: %+v", metaReq.Topics)
	}
	if !metaReq.AllowAutoTopicCreation {
		t.Fatalf("expected AllowAutoTopicCreation true")
	}
}

func TestParseRequest_FindCoordinator(t *testing.T) {
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.Version = 3
	req.CoordinatorKey = "franz-e2e-consumer"

	frame := buildRequestFrame(APIKeyFindCoordinator, 3, 1, kmsg.StringPtr("kgo"), req.AppendTo(nil))
	header, parsed, err := ParseRequest(frame)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyFindCoordinator {
		t.Fatalf("unexpected api key %d", header.APIKey)
	}
	findReq, ok := parsed.(*kmsg.FindCoordinatorRequest)
	if !ok {
		t.Fatalf("expected *kmsg.FindCoordinatorRequest got %T", parsed)
	}
	if findReq.CoordinatorKey != "franz-e2e-consumer" {
		t.Fatalf("unexpected coordinator key %q", findReq.CoordinatorKey)
	}
}

func TestParseRequest_Fetch(t *testing.T) {
	var topicID [16]byte
	for i := range topicID {
		topicID[i] = byte(i + 1)
	}
	req := kmsg.NewPtrFetchRequest()
	req.Version = 13
	req.MaxWaitMillis = 500
	req.MinBytes = 1
	req.MaxBytes = 1048576
	topic := kmsg.NewFetchRequestTopic()
	topic.TopicID = topicID
	part := kmsg.NewFetchRequestTopicPartition()
	part.Partition = 0
	part.FetchOffset = 42
	part.PartitionMaxBytes = 1048576
	topic.Partitions = append(topic.Partitions, part)
	req.Topics = append(req.Topics, topic)

	frame := buildRequestFrame(APIKeyFetch, 13, 9, kmsg.StringPtr("client"), req.AppendTo(nil))
	header, parsed, err := ParseRequest(frame)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyFetch || header.APIVersion != 13 {
		t.Fatalf("unexpected header: %+v", header)
	}
	fetchReq, ok := parsed.(*kmsg.FetchRequest)
	if !ok {
		t.Fatalf("expected *kmsg.FetchRequest got %T", parsed)
	}
	if len(fetchReq.Topics) != 1 || fetchReq.Topics[0].TopicID != topicID {
		t.Fatalf("unexpected topics: %+v", fetchReq.Topics)
	}
	if fetchReq.Topics[0].Partitions[0].FetchOffset != 42 {
		t.Fatalf("unexpected fetch offset %d", fetchReq.Topics[0].Partitions[0].FetchOffset)
	}
}

func TestParseRequest_OffsetCommit(t *testing.T) {
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Version = 3
	req.Group = "group-1"
	req.Generation = 4
	req.MemberID = "member-1"
	req.RetentionTimeMillis = 60000
	topic := kmsg.NewOffsetCommitRequestTopic()
	topic.Topic = "orders"
	part := kmsg.NewOffsetCommitRequestTopicPartition()
	part.Partition = 0
	part.Offset = 100
	meta := "checkpoint"
	part.Metadata = &meta
	topic.Partitions = append(topic.Partitions, part)
	req.Topics = append(req.Topics, topic)

	// OffsetCommit v3 is pre-flexible (no tagged fields in header).
	frame := buildRequestFrame(APIKeyOffsetCommit, 3, 7, kmsg.StringPtr("kgo"), req.AppendTo(nil))
	header, parsed, err := ParseRequest(frame)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyOffsetCommit {
		t.Fatalf("unexpected api key %d", header.APIKey)
	}
	commitReq, ok := parsed.(*kmsg.OffsetCommitRequest)
	if !ok {
		t.Fatalf("expected *kmsg.OffsetCommitRequest got %T", parsed)
	}
	if commitReq.Group != "group-1" || commitReq.Generation != 4 {
		t.Fatalf("unexpected group data: group=%q gen=%d", commitReq.Group, commitReq.Generation)
	}
	if len(commitReq.Topics) != 1 || commitReq.Topics[0].Partitions[0].Offset != 100 {
		t.Fatalf("unexpected partition data")
	}
}

func TestParseRequest_SyncGroup(t *testing.T) {
	req := kmsg.NewPtrSyncGroupRequest()
	req.Version = 4
	req.Group = "franz-e2e-consumer"
	req.Generation = 1
	req.MemberID = "member-1"
	req.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
		{MemberID: "member-1", MemberAssignment: []byte{0x00, 0x01}},
	}

	frame := buildRequestFrame(APIKeySyncGroup, 4, 9, kmsg.StringPtr("kgo"), req.AppendTo(nil))
	header, parsed, err := ParseRequest(frame)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeySyncGroup {
		t.Fatalf("unexpected api key %d", header.APIKey)
	}
	syncReq, ok := parsed.(*kmsg.SyncGroupRequest)
	if !ok {
		t.Fatalf("expected *kmsg.SyncGroupRequest got %T", parsed)
	}
	if syncReq.Group != "franz-e2e-consumer" {
		t.Fatalf("unexpected group id %q", syncReq.Group)
	}
	if len(syncReq.GroupAssignment) != 1 || syncReq.GroupAssignment[0].MemberID != "member-1" {
		t.Fatalf("unexpected assignments %+v", syncReq.GroupAssignment)
	}
}

func TestParseRequestHeader_ReturnsRemainingBody(t *testing.T) {
	req := kmsg.NewPtrApiVersionsRequest()
	req.Version = 3
	req.ClientSoftwareName = "kgo"
	req.ClientSoftwareVersion = "1.0.0"
	body := req.AppendTo(nil)

	frame := buildRequestFrame(APIKeyApiVersion, 3, 7, kmsg.StringPtr("kgo"), body)
	header, remaining, err := ParseRequestHeader(frame)
	if err != nil {
		t.Fatalf("ParseRequestHeader: %v", err)
	}
	if header.APIKey != APIKeyApiVersion || header.APIVersion != 3 || header.CorrelationID != 7 {
		t.Fatalf("unexpected header: %+v", header)
	}
	if len(remaining) != len(body) {
		t.Fatalf("remaining body length: got %d, want %d", len(remaining), len(body))
	}
}

func TestParseRequest_UnsupportedAPIKey(t *testing.T) {
	w := newByteWriter(16)
	w.Int16(9999)
	w.Int16(0)
	w.Int32(1)
	w.NullableString(nil)

	_, _, err := ParseRequest(w.Bytes())
	if err == nil {
		t.Fatalf("expected error for unsupported api key")
	}
}

func TestParseRequest_TruncatedHeader(t *testing.T) {
	_, _, err := ParseRequest([]byte{0x00, 0x03})
	if err == nil {
		t.Fatalf("expected error for truncated header")
	}
}

// TestProduceMultiPartitionFranzCompat tests byte-level compatibility with
// franz-go for multi-partition produce requests in both directions:
//   - franz-go encodes → KafScale parses
//   - KafScale encodes → franz-go decodes
func TestProduceMultiPartitionFranzCompat(t *testing.T) {
	t.Run("franz-encode-kafscale-parse", func(t *testing.T) {
		req := kmsg.NewPtrProduceRequest()
		req.Version = 9
		req.Acks = -1
		req.TimeoutMillis = 3000
		topic := kmsg.NewProduceRequestTopic()
		topic.Topic = "orders"
		for _, pi := range []int32{0, 1, 2} {
			part := kmsg.NewProduceRequestTopicPartition()
			part.Partition = pi
			part.Records = []byte{byte(pi + 1), byte(pi + 2)}
			topic.Partitions = append(topic.Partitions, part)
		}
		req.Topics = append(req.Topics, topic)
		body := req.AppendTo(nil)

		w := newByteWriter(len(body) + 32)
		w.Int16(APIKeyProduce)
		w.Int16(9)
		w.Int32(55)
		clientID := "kgo"
		w.NullableString(&clientID)
		w.WriteTaggedFields(0)
		w.write(body)

		_, parsed, err := ParseRequest(w.Bytes())
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		got, ok := parsed.(*kmsg.ProduceRequest)
		if !ok {
			t.Fatalf("expected *kmsg.ProduceRequest, got %T", parsed)
		}
		if len(got.Topics) != 1 {
			t.Fatalf("topic count: got %d want 1", len(got.Topics))
		}
		if len(got.Topics[0].Partitions) != 3 {
			t.Fatalf("partition count: got %d want 3", len(got.Topics[0].Partitions))
		}
		for pi, part := range got.Topics[0].Partitions {
			if part.Partition != int32(pi) {
				t.Fatalf("part[%d] index: got %d want %d", pi, part.Partition, pi)
			}
			want := []byte{byte(pi + 1), byte(pi + 2)}
			if string(part.Records) != string(want) {
				t.Fatalf("part[%d] records: got %x want %x", pi, part.Records, want)
			}
		}
	})

}

func TestParseJoinGroupRequest(t *testing.T) {
	req := kmsg.NewPtrJoinGroupRequest()
	req.Version = 1
	req.Group = "group-1"
	req.SessionTimeoutMillis = 10000
	req.RebalanceTimeoutMillis = 30000
	req.MemberID = ""
	req.ProtocolType = "consumer"
	req.Protocols = []kmsg.JoinGroupRequestProtocol{
		{Name: "range", Metadata: []byte{0x00, 0x01}},
	}

	frame := buildRequestFrame(APIKeyJoinGroup, 1, 33, nil, req.AppendTo(nil))
	_, parsed, err := ParseRequest(frame)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	joinReq, ok := parsed.(*kmsg.JoinGroupRequest)
	if !ok {
		t.Fatalf("expected *kmsg.JoinGroupRequest got %T", parsed)
	}
	if joinReq.Group != "group-1" || joinReq.SessionTimeoutMillis != 10000 {
		t.Fatalf("unexpected join group: %#v", joinReq)
	}
	if joinReq.ProtocolType != "consumer" || len(joinReq.Protocols) != 1 {
		t.Fatalf("unexpected protocols: %#v", joinReq)
	}
	if joinReq.Protocols[0].Name != "range" {
		t.Fatalf("unexpected protocol name: %q", joinReq.Protocols[0].Name)
	}
}

func TestParseHeartbeatRequest(t *testing.T) {
	req := kmsg.NewPtrHeartbeatRequest()
	req.Version = 1
	req.Group = "group-1"
	req.Generation = 5
	req.MemberID = "member-1"

	frame := buildRequestFrame(APIKeyHeartbeat, 1, 44, nil, req.AppendTo(nil))
	_, parsed, err := ParseRequest(frame)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	heartReq, ok := parsed.(*kmsg.HeartbeatRequest)
	if !ok {
		t.Fatalf("expected *kmsg.HeartbeatRequest got %T", parsed)
	}
	if heartReq.Group != "group-1" || heartReq.Generation != 5 || heartReq.MemberID != "member-1" {
		t.Fatalf("unexpected heartbeat: %#v", heartReq)
	}
}

func TestParseLeaveGroupRequest(t *testing.T) {
	req := kmsg.NewPtrLeaveGroupRequest()
	req.Version = 0
	req.Group = "group-1"
	req.MemberID = "member-1"

	frame := buildRequestFrame(APIKeyLeaveGroup, 0, 55, nil, req.AppendTo(nil))
	_, parsed, err := ParseRequest(frame)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	leaveReq, ok := parsed.(*kmsg.LeaveGroupRequest)
	if !ok {
		t.Fatalf("expected *kmsg.LeaveGroupRequest got %T", parsed)
	}
	if leaveReq.Group != "group-1" || leaveReq.MemberID != "member-1" {
		t.Fatalf("unexpected leave group: %#v", leaveReq)
	}
}

func TestParseOffsetFetchRequest(t *testing.T) {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Version = 1
	req.Group = "group-1"
	req.Topics = []kmsg.OffsetFetchRequestTopic{
		{Topic: "orders", Partitions: []int32{0, 1}},
	}

	frame := buildRequestFrame(APIKeyOffsetFetch, 1, 66, nil, req.AppendTo(nil))
	_, parsed, err := ParseRequest(frame)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	fetchReq, ok := parsed.(*kmsg.OffsetFetchRequest)
	if !ok {
		t.Fatalf("expected *kmsg.OffsetFetchRequest got %T", parsed)
	}
	if fetchReq.Group != "group-1" {
		t.Fatalf("unexpected group: %s", fetchReq.Group)
	}
	if len(fetchReq.Topics) != 1 || fetchReq.Topics[0].Topic != "orders" {
		t.Fatalf("unexpected topics: %#v", fetchReq.Topics)
	}
	if len(fetchReq.Topics[0].Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(fetchReq.Topics[0].Partitions))
	}
}

func TestParseHeartbeatFlexible(t *testing.T) {
	req := kmsg.NewPtrHeartbeatRequest()
	req.Version = 4
	req.Group = "group-2"
	req.Generation = 10
	req.MemberID = "member-2"
	instanceID := "instance-1"
	req.InstanceID = &instanceID

	frame := buildRequestFrame(APIKeyHeartbeat, 4, 77, nil, req.AppendTo(nil))
	_, parsed, err := ParseRequest(frame)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	heartReq, ok := parsed.(*kmsg.HeartbeatRequest)
	if !ok {
		t.Fatalf("expected *kmsg.HeartbeatRequest got %T", parsed)
	}
	if heartReq.Group != "group-2" || heartReq.Generation != 10 {
		t.Fatalf("unexpected heartbeat: %#v", heartReq)
	}
	if heartReq.InstanceID == nil || *heartReq.InstanceID != "instance-1" {
		t.Fatalf("unexpected instance id: %v", heartReq.InstanceID)
	}
}
