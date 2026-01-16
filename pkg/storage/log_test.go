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

package storage

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/cache"
)

func TestPartitionLogAppendFlush(t *testing.T) {
	s3 := NewMemoryS3Client()
	c := cache.NewSegmentCache(1024)
	var flushCount int
	log := NewPartitionLog("default", "orders", 0, 0, s3, c, PartitionLogConfig{
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
	log := NewPartitionLog("default", "orders", 0, 0, s3, c, PartitionLogConfig{
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

func TestPartitionLogReadUsesIndexRange(t *testing.T) {
	s3 := NewMemoryS3Client()
	log := NewPartitionLog("default", "orders", 0, 0, s3, nil, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes:      1,
			FlushInterval: time.Millisecond,
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
	}, nil, nil)

	batch1 := makeBatchBytes(0, 0, 1, 0x11)
	batch2 := makeBatchBytes(1, 0, 1, 0x22)
	record1, err := NewRecordBatchFromBytes(batch1)
	if err != nil {
		t.Fatalf("NewRecordBatchFromBytes batch1: %v", err)
	}
	record2, err := NewRecordBatchFromBytes(batch2)
	if err != nil {
		t.Fatalf("NewRecordBatchFromBytes batch2: %v", err)
	}
	if _, err := log.AppendBatch(context.Background(), record1); err != nil {
		t.Fatalf("AppendBatch batch1: %v", err)
	}
	if _, err := log.AppendBatch(context.Background(), record2); err != nil {
		t.Fatalf("AppendBatch batch2: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := log.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	data, err := log.Read(context.Background(), 1, int32(len(batch2)))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(data) <= 12 {
		t.Fatalf("expected data length > 12, got %d", len(data))
	}
	if data[12] != 0x22 {
		t.Fatalf("expected range read from second batch, got marker %x", data[12])
	}
}

func TestPartitionLogReportsS3Uploads(t *testing.T) {
	s3 := NewMemoryS3Client()
	c := cache.NewSegmentCache(1024)
	var uploads int
	log := NewPartitionLog("default", "orders", 0, 0, s3, c, PartitionLogConfig{
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

func TestPartitionLogRestoreFromS3(t *testing.T) {
	s3 := NewMemoryS3Client()
	c := cache.NewSegmentCache(1024)
	log := NewPartitionLog("default", "orders", 0, 0, s3, c, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes:      1,
			FlushInterval: time.Millisecond,
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
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

	recovered := NewPartitionLog("default", "orders", 0, 0, s3, c, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes:      1,
			FlushInterval: time.Millisecond,
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
	}, nil, nil)
	lastOffset, err := recovered.RestoreFromS3(context.Background())
	if err != nil {
		t.Fatalf("RestoreFromS3: %v", err)
	}
	if lastOffset != 0 {
		t.Fatalf("expected last offset 0, got %d", lastOffset)
	}
	if earliest := recovered.EarliestOffset(); earliest != 0 {
		t.Fatalf("expected earliest offset 0, got %d", earliest)
	}
	if entries, ok := recovered.indexEntries[0]; !ok || len(entries) == 0 {
		t.Fatalf("expected index entries for base offset 0")
	}

	res, err := recovered.AppendBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("AppendBatch after restore: %v", err)
	}
	if res.BaseOffset != 1 {
		t.Fatalf("expected base offset 1 after restore, got %d", res.BaseOffset)
	}
}

func makeBatchBytes(baseOffset int64, lastOffsetDelta int32, messageCount int32, marker byte) []byte {
	const size = 70
	data := make([]byte, size)
	binary.BigEndian.PutUint64(data[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(data[23:27], uint32(lastOffsetDelta))
	binary.BigEndian.PutUint32(data[57:61], uint32(messageCount))
	data[12] = marker
	return data
}
