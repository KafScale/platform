package decoder

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/config"
)

func TestDecodeSegmentFromMinIO(t *testing.T) {
	endpoint := os.Getenv("ICEBERG_PROCESSOR_MINIO_ENDPOINT")
	bucket := os.Getenv("ICEBERG_PROCESSOR_S3_BUCKET")
	segmentKey := os.Getenv("ICEBERG_PROCESSOR_SEGMENT_KEY")
	indexKey := os.Getenv("ICEBERG_PROCESSOR_INDEX_KEY")
	topic := os.Getenv("ICEBERG_PROCESSOR_TOPIC")
	partitionStr := os.Getenv("ICEBERG_PROCESSOR_PARTITION")
	if endpoint == "" || bucket == "" || segmentKey == "" || indexKey == "" || topic == "" || partitionStr == "" {
		t.Skip("MinIO env not set for integration test")
	}
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		t.Fatalf("invalid ICEBERG_PROCESSOR_PARTITION: %v", err)
	}

	cfg := config.Config{
		S3: config.S3Config{
			Bucket:    bucket,
			Endpoint:  endpoint,
			PathStyle: true,
		},
	}

	dec, err := New(cfg)
	if err != nil {
		t.Fatalf("New decoder: %v", err)
	}

	if _, err := dec.Decode(context.Background(), segmentKey, indexKey, topic, int32(partition)); err != nil {
		t.Fatalf("Decode: %v", err)
	}
}
