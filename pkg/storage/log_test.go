package storage

import (
	"context"
	"testing"
	"time"

	"github.com/novatechflow/kafscale/pkg/cache"
)

func TestPartitionLogAppendFlush(t *testing.T) {
	s3 := NewMemoryS3Client()
	c := cache.NewSegmentCache(1024)
	var flushCount int
	log := NewPartitionLog("orders", 0, 0, s3, c, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes:      1,
			FlushInterval: time.Millisecond,
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
	}, func(ctx context.Context, artifact *SegmentArtifact) {
		flushCount++
	}, nil)

	batchData := make([]byte, 70)
	batch, err := NewRecordBatchFromBytes(batchData)
	if err != nil {
		t.Fatalf("NewRecordBatchFromBytes: %v", err)
	}

	res, err := log.AppendBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if res.BaseOffset != 0 {
		t.Fatalf("expected base offset 0 got %d", res.BaseOffset)
	}
	// Force flush
	time.Sleep(2 * time.Millisecond)
	if err := log.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if flushCount == 0 {
		t.Fatalf("expected flush callback invoked")
	}
}

func TestPartitionLogRead(t *testing.T) {
	s3 := NewMemoryS3Client()
	c := cache.NewSegmentCache(1024)
	log := NewPartitionLog("orders", 0, 0, s3, c, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes:      1,
			FlushInterval: time.Millisecond,
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
		ReadAheadSegments: 1,
		CacheEnabled:      true,
	}, nil, nil)

	batchData := make([]byte, 70)
	batch, _ := NewRecordBatchFromBytes(batchData)
	if _, err := log.AppendBatch(context.Background(), batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := log.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	data, err := log.Read(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("expected data from read")
	}
}

func TestPartitionLogReportsS3Uploads(t *testing.T) {
	s3 := NewMemoryS3Client()
	c := cache.NewSegmentCache(1024)
	var uploads int
	log := NewPartitionLog("orders", 0, 0, s3, c, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes:      1,
			FlushInterval: time.Millisecond,
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
	}, nil, func(op string, d time.Duration, err error) {
		uploads++
	})

	batchData := make([]byte, 70)
	batch, _ := NewRecordBatchFromBytes(batchData)
	if _, err := log.AppendBatch(context.Background(), batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := log.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if uploads < 2 {
		t.Fatalf("expected upload callback for segment + index, got %d", uploads)
	}
}
