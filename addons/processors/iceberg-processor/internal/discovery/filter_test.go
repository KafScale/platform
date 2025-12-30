package discovery

import (
	"testing"

	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/protocol"
)

func TestBuildTopicPartitionFilter(t *testing.T) {
	snapshot := metadata.ClusterMetadata{
		Topics: []protocol.MetadataTopic{
			{
				Name: "orders",
				Partitions: []protocol.MetadataPartition{
					{PartitionIndex: 0},
					{PartitionIndex: 1},
				},
			},
			{
				Name:      "bad-topic",
				ErrorCode: 3,
				Partitions: []protocol.MetadataPartition{
					{PartitionIndex: 0},
				},
			},
			{
				Name: "payments",
				Partitions: []protocol.MetadataPartition{
					{PartitionIndex: 2, ErrorCode: 2},
					{PartitionIndex: 3},
				},
			},
		},
	}

	filter := buildTopicPartitionFilter(snapshot)
	if !filterAllows(filter, "orders", 0) || !filterAllows(filter, "orders", 1) {
		t.Fatalf("expected orders partitions to be included")
	}
	if filterAllows(filter, "bad-topic", 0) {
		t.Fatalf("expected bad-topic to be excluded")
	}
	if filterAllows(filter, "payments", 2) {
		t.Fatalf("expected errored partition to be excluded")
	}
	if !filterAllows(filter, "payments", 3) {
		t.Fatalf("expected payments partition 3 to be included")
	}
}
