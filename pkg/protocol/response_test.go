package protocol

import "testing"

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
	})
	if err != nil {
		t.Fatalf("EncodeProduceResponse: %v", err)
	}
	reader := newByteReader(payload)
	corr, _ := reader.Int32()
	if corr != 7 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
}

func TestEncodeFetchResponse(t *testing.T) {
	payload, err := EncodeFetchResponse(&FetchResponse{
		CorrelationID: 3,
		Topics: []FetchTopicResponse{
			{
				Name: "orders",
				Partitions: []FetchPartitionResponse{
					{
						Partition:     0,
						ErrorCode:     0,
						HighWatermark: 10,
						RecordSet:     []byte("records"),
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("EncodeFetchResponse: %v", err)
	}
	reader := newByteReader(payload)
	corr, _ := reader.Int32()
	if corr != 3 {
		t.Fatalf("unexpected correlation id %d", corr)
	}
}
