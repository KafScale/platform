package storage

import (
	"testing"
	"time"
)

func TestWriteBufferThresholds(t *testing.T) {
	cfg := WriteBufferConfig{
		MaxBytes:      10,
		MaxMessages:   5,
		MaxBatches:    3,
		FlushInterval: 50 * time.Millisecond,
	}
	buf := NewWriteBuffer(cfg)

	if buf.ShouldFlush(time.Now()) {
		t.Fatalf("empty buffer should not flush")
	}

	buf.Append(RecordBatch{Bytes: make([]byte, 8), MessageCount: 4})
	if buf.ShouldFlush(time.Now()) {
		t.Fatalf("below thresholds")
	}

	buf.Append(RecordBatch{Bytes: make([]byte, 4), MessageCount: 2})
	if !buf.ShouldFlush(time.Now()) {
		t.Fatalf("expected flush by bytes")
	}

	drained := buf.Drain()
	if len(drained) != 2 {
		t.Fatalf("expected 2 drained batches")
	}

	buf.Append(RecordBatch{Bytes: make([]byte, 1), MessageCount: 1})
	time.Sleep(cfg.FlushInterval)
	if !buf.ShouldFlush(time.Now()) {
		t.Fatalf("expected flush by time")
	}
}
