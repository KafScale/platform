package storage

import (
	"context"
	"testing"
	"time"

	"github.com/alo/kafscale/pkg/cache"
)

func TestPartitionLogAppendFlush(t *testing.T) {
	s3 := NewMemoryS3Client()
	c := cache.NewSegmentCache(1024)
	log := NewPartitionLog("orders", 0, s3, c, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes:      1,
			FlushInterval: time.Millisecond,
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
	})

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
	if err := log.flushLocked(context.Background()); err != nil {
		t.Fatalf("flushLocked: %v", err)
	}
}
