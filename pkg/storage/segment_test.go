package storage

import (
	"encoding/binary"
	"testing"
	"time"
)

func TestBuildSegment(t *testing.T) {
	batches := []RecordBatch{
		{
			BaseOffset:      0,
			LastOffsetDelta: 1,
			MessageCount:    2,
			Bytes:           []byte("batch-1"),
		},
		{
			BaseOffset:      2,
			LastOffsetDelta: 0,
			MessageCount:    1,
			Bytes:           []byte("batch-2"),
		},
	}
	cfg := SegmentWriterConfig{IndexIntervalMessages: 2}
	created := time.UnixMilli(1700000000000)
	artifact, err := BuildSegment(cfg, batches, created)
	if err != nil {
		t.Fatalf("BuildSegment: %v", err)
	}
	if artifact.MessageCount != 3 {
		t.Fatalf("expected 3 messages got %d", artifact.MessageCount)
	}
	if len(artifact.IndexBytes) == 0 {
		t.Fatalf("index bytes missing")
	}

	// Validate header fields
	if string(artifact.SegmentBytes[:4]) != segmentMagic {
		t.Fatalf("segment magic mismatch")
	}
	baseOffset := int64(binary.BigEndian.Uint64(artifact.SegmentBytes[8:16]))
	if baseOffset != 0 {
		t.Fatalf("base offset mismatch: %d", baseOffset)
	}
	messageCount := int32(binary.BigEndian.Uint32(artifact.SegmentBytes[16:20]))
	if messageCount != 3 {
		t.Fatalf("message count mismatch %d", messageCount)
	}
}

func TestBuildSegmentNoBatches(t *testing.T) {
	if _, err := BuildSegment(SegmentWriterConfig{}, nil, time.Now()); err == nil {
		t.Fatalf("expected error when no batches supplied")
	}
}
