package metadata

import (
	"testing"

	metadatapb "github.com/novatechflow/kafscale/pkg/gen/metadata"
)

func TestTopicConfigCodec(t *testing.T) {
	cfg := &metadatapb.TopicConfig{
		Name:              "orders",
		Partitions:        3,
		ReplicationFactor: 1,
		RetentionMs:       600000,
		SegmentBytes:      1024,
		Config:            map[string]string{"cleanup.policy": "delete"},
		RetentionBytes:    -1,
		CreatedAt:         "2024-01-15T10:30:00Z",
	}

	raw, err := EncodeTopicConfig(cfg)
	if err != nil {
		t.Fatalf("EncodeTopicConfig: %v", err)
	}

	decoded, err := DecodeTopicConfig(raw)
	if err != nil {
		t.Fatalf("DecodeTopicConfig: %v", err)
	}

	if decoded.GetName() != cfg.GetName() || decoded.GetPartitions() != cfg.GetPartitions() {
		t.Fatalf("decoded config mismatch: %#v vs %#v", decoded, cfg)
	}
	if decoded.GetConfig()["cleanup.policy"] != "delete" {
		t.Fatalf("missing config map values: %#v", decoded.GetConfig())
	}
}

func TestPartitionStateCodec(t *testing.T) {
	state := &metadatapb.PartitionState{
		Topic:          "orders",
		Partition:      0,
		LeaderBroker:   "broker-0",
		LeaderEpoch:    5,
		LogStartOffset: 0,
		LogEndOffset:   1200,
		HighWatermark:  1100,
		Segments: []*metadatapb.SegmentInfo{
			{BaseOffset: 0, SizeBytes: 1024},
			{BaseOffset: 500, SizeBytes: 1024},
		},
	}

	raw, err := EncodePartitionState(state)
	if err != nil {
		t.Fatalf("EncodePartitionState: %v", err)
	}

	decoded, err := DecodePartitionState(raw)
	if err != nil {
		t.Fatalf("DecodePartitionState: %v", err)
	}

	if decoded.GetLeaderBroker() != state.GetLeaderBroker() || len(decoded.GetSegments()) != len(state.GetSegments()) {
		t.Fatalf("decoded partition state mismatch: %#v vs %#v", decoded, state)
	}
}

func TestConsumerGroupCodec(t *testing.T) {
	group := &metadatapb.ConsumerGroup{
		GroupId:      "order-processor",
		State:        "Stable",
		ProtocolType: "consumer",
		Protocol:     "range",
		Leader:       "consumer-1",
		GenerationId: 42,
		Members: map[string]*metadatapb.GroupMember{
			"consumer-1": {
				ClientId:   "svc-1",
				ClientHost: "10.0.0.1",
				Assignments: []*metadatapb.Assignment{
					{Topic: "orders", Partitions: []int32{0, 1}},
				},
			},
		},
	}

	raw, err := EncodeConsumerGroup(group)
	if err != nil {
		t.Fatalf("EncodeConsumerGroup: %v", err)
	}

	decoded, err := DecodeConsumerGroup(raw)
	if err != nil {
		t.Fatalf("DecodeConsumerGroup: %v", err)
	}

	if decoded.GetLeader() != group.GetLeader() || decoded.GetMembers()["consumer-1"].GetClientId() != "svc-1" {
		t.Fatalf("decoded consumer group mismatch: %#v vs %#v", decoded, group)
	}
}

func TestKeyBuilders(t *testing.T) {
	if got := TopicConfigKey("orders"); got != "/kafscale/topics/orders/config" {
		t.Fatalf("unexpected topic config key: %s", got)
	}

	if got := PartitionStateKey("orders", 1); got != "/kafscale/topics/orders/partitions/1" {
		t.Fatalf("unexpected partition key: %s", got)
	}

	if got := ConsumerGroupKey("group-1"); got != "/kafscale/consumers/group-1/metadata" {
		t.Fatalf("unexpected consumer group key: %s", got)
	}

	if got := ConsumerOffsetKey("group-1", "orders", 0); got != "/kafscale/consumers/group-1/offsets/orders/0" {
		t.Fatalf("unexpected consumer offset key: %s", got)
	}

	if got := BrokerRegistrationKey("broker-1"); got != "/kafscale/brokers/broker-1" {
		t.Fatalf("unexpected broker key: %s", got)
	}

	if got := PartitionAssignmentKey("orders", 2); got != "/kafscale/assignments/orders/2" {
		t.Fatalf("unexpected assignment key: %s", got)
	}
}
