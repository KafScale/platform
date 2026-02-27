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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/cache"
	"golang.org/x/sync/semaphore"
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
	}, nil, nil)

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
	}, nil, nil, nil)

	batchData := make([]byte, 70)
	batch, _ := NewRecordBatchFromBytes(batchData)
	if _, err := log.AppendBatch(context.Background(), batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

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
	}, nil, nil, nil)

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
	var uploads atomic.Int64
	log := NewPartitionLog("default", "orders", 0, 0, s3, c, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes:      1,
			FlushInterval: time.Millisecond,
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
	}, nil, func(op string, d time.Duration, err error) {
		uploads.Add(1)
	}, nil)

	batchData := make([]byte, 70)
	batch, _ := NewRecordBatchFromBytes(batchData)
	if _, err := log.AppendBatch(context.Background(), batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	if err := log.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if uploads.Load() < 2 {
		t.Fatalf("expected upload callback for segment + index, got %d", uploads.Load())
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
	}, nil, nil, nil)

	batchData := make([]byte, 70)
	batch, _ := NewRecordBatchFromBytes(batchData)
	if _, err := log.AppendBatch(context.Background(), batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

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
	}, nil, nil, nil)
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

func TestPartitionLogPrefetchSkippedWhenSemaphoreFull(t *testing.T) {
	s3mem := NewMemoryS3Client()
	// First, write two segments without semaphore constraint.
	writer := NewPartitionLog("default", "orders", 0, 0, s3mem, nil, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes:      1,
			FlushInterval: time.Millisecond,
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
	}, nil, nil, nil)
	for i := 0; i < 2; i++ {
		batch, _ := NewRecordBatchFromBytes(makeBatchBytes(int64(i), 0, 1, byte(i+1)))
		if _, err := writer.AppendBatch(context.Background(), batch); err != nil {
			t.Fatalf("AppendBatch %d: %v", i, err)
		}
		if err := writer.Flush(context.Background()); err != nil {
			t.Fatalf("Flush %d: %v", i, err)
		}
	}

	// Now create a reader with a full semaphore and a fresh cache.
	sem := semaphore.NewWeighted(1)
	sem.Acquire(context.Background(), 1) // exhaust the semaphore
	c := cache.NewSegmentCache(1 << 20)
	reader := NewPartitionLog("default", "orders", 0, 0, s3mem, c, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes:      1,
			FlushInterval: time.Millisecond,
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
		ReadAheadSegments: 2,
		CacheEnabled:      true,
	}, nil, nil, sem)
	// Restore segments so the reader knows about them.
	sem.Release(1) // temporarily release for RestoreFromS3
	if _, err := reader.RestoreFromS3(context.Background()); err != nil {
		t.Fatalf("RestoreFromS3: %v", err)
	}
	sem.Acquire(context.Background(), 1) // re-exhaust

	// Trigger prefetch — should be skipped because TryAcquire fails.
	reader.startPrefetch(context.Background(), 0)
	time.Sleep(5 * time.Millisecond)

	if _, ok := c.GetSegment("default/orders", 0, 0); ok {
		t.Fatalf("expected prefetch to be skipped when semaphore is full")
	}
	sem.Release(1)
}

func TestPartitionLogFlushWaitsForInflight(t *testing.T) {
	// Verify that a concurrent Flush blocks until an in-flight flush completes
	// and then flushes any data that accumulated in the meantime.
	uploadStarted := make(chan struct{})
	uploadRelease := make(chan struct{})
	slowS3 := &slowUploadS3{
		MemoryS3Client: NewMemoryS3Client(),
		started:        uploadStarted,
		release:        uploadRelease,
	}
	flushCh := make(chan int64, 4)
	log := NewPartitionLog("default", "orders", 0, 0, slowS3, nil, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes: 1 << 20, // large — manual flush only
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
	}, func(_ context.Context, a *SegmentArtifact) {
		flushCh <- a.LastOffset
	}, nil, nil)

	// Append batch 0 and start a slow flush in the background.
	batch0, _ := NewRecordBatchFromBytes(makeBatchBytes(0, 0, 1, 0x01))
	if _, err := log.AppendBatch(context.Background(), batch0); err != nil {
		t.Fatalf("AppendBatch 0: %v", err)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- log.Flush(context.Background())
	}()
	<-uploadStarted // first flush is now blocked in UploadSegment

	// Append batch 1 while the first flush is in progress.
	batch1, _ := NewRecordBatchFromBytes(makeBatchBytes(1, 0, 1, 0x02))
	if _, err := log.AppendBatch(context.Background(), batch1); err != nil {
		t.Fatalf("AppendBatch 1: %v", err)
	}

	// Start second Flush — it must block until first completes.
	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- log.Flush(context.Background())
	}()
	time.Sleep(5 * time.Millisecond) // give second Flush time to reach the wait

	// Release the slow upload — both flushes should complete.
	close(uploadRelease)

	if err := <-errCh; err != nil {
		t.Fatalf("first Flush: %v", err)
	}
	if err := <-errCh2; err != nil {
		t.Fatalf("second Flush: %v", err)
	}

	// Both batches must have been flushed (two onFlush callbacks).
	// Collect results from the channel now that both goroutines have returned.
	close(flushCh)
	var flushedOffsets []int64
	for off := range flushCh {
		flushedOffsets = append(flushedOffsets, off)
	}
	if len(flushedOffsets) < 2 {
		t.Fatalf("expected 2 flush callbacks, got %d", len(flushedOffsets))
	}
}

// slowUploadS3 blocks UploadSegment until release is closed.
type slowUploadS3 struct {
	*MemoryS3Client
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *slowUploadS3) UploadSegment(ctx context.Context, key string, body []byte) error {
	s.once.Do(func() { close(s.started) })
	<-s.release
	return s.MemoryS3Client.UploadSegment(ctx, key, body)
}

func TestPartitionLogFlushErrorClearsFlushing(t *testing.T) {
	failingS3 := &failingUploadS3{MemoryS3Client: NewMemoryS3Client()}
	log := NewPartitionLog("default", "orders", 0, 0, failingS3, nil, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes: 1 << 20, // large threshold so AppendBatch won't auto-flush
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
	}, nil, nil, nil)

	batchData := make([]byte, 70)
	batch, _ := NewRecordBatchFromBytes(batchData)
	if _, err := log.AppendBatch(context.Background(), batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	err := log.Flush(context.Background())
	if err == nil {
		t.Fatalf("expected flush to fail")
	}

	// flushing flag should be cleared so a subsequent flush can proceed.
	log.mu.Lock()
	if log.flushing {
		t.Fatalf("expected flushing flag to be false after failed upload")
	}
	log.mu.Unlock()
}

// failingUploadS3 wraps MemoryS3Client but fails all uploads.
type failingUploadS3 struct {
	*MemoryS3Client
}

func (f *failingUploadS3) UploadSegment(ctx context.Context, key string, body []byte) error {
	return fmt.Errorf("simulated S3 upload failure")
}

func (f *failingUploadS3) UploadIndex(ctx context.Context, key string, body []byte) error {
	return fmt.Errorf("simulated S3 upload failure")
}

func TestPartitionLogRestoreFromS3SkipsOrphanedSegment(t *testing.T) {
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
	}, nil, nil, nil)

	batchData := make([]byte, 70)
	batch, _ := NewRecordBatchFromBytes(batchData)
	if _, err := log.AppendBatch(context.Background(), batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	if err := log.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	s3.mu.Lock()
	for key := range s3.index {
		delete(s3.index, key)
	}
	s3.mu.Unlock()

	recovered := NewPartitionLog("default", "orders", 0, 0, s3, c, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes:      1,
			FlushInterval: time.Millisecond,
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
	}, nil, nil, nil)
	lastOffset, err := recovered.RestoreFromS3(context.Background())
	if err != nil {
		t.Fatalf("RestoreFromS3 should not fail for orphaned .kfs: %v", err)
	}
	if lastOffset != -1 {
		t.Fatalf("expected last offset -1 (no valid segments), got %d", lastOffset)
	}
	if len(recovered.segments) != 0 {
		t.Fatalf("expected no segments, got %d", len(recovered.segments))
	}
	// nextOffset is not advanced past the orphan because the metadata store
	// controls the starting offset. New writes at offset 0 will overwrite
	// the orphaned .kfs with a valid segment+index pair (self-healing).
	res, err := recovered.AppendBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("AppendBatch after restore: %v", err)
	}
	if res.BaseOffset != 0 {
		t.Fatalf("expected base offset 0 (metadata store controls offset), got %d", res.BaseOffset)
	}
}

func TestPartitionLogRestoreFromS3TransientErrorPropagates(t *testing.T) {
	// A transient S3 error (not ErrNotFound) during DownloadIndex must
	// propagate as a hard error even for uncommitted segments, because we
	// cannot distinguish "orphaned .kfs" from "S3 is temporarily down."
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
	}, nil, nil, nil)

	batchData := make([]byte, 70)
	batch, _ := NewRecordBatchFromBytes(batchData)
	if _, err := log.AppendBatch(context.Background(), batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	if err := log.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	transientS3 := &transientIndexErrorS3{MemoryS3Client: s3}

	// The segment's baseOffset (0) >= nextOffset (0), so the offset condition
	// for orphan-skip is satisfied. But the error is not ErrNotFound, so the
	// skip must not trigger — we can't tell if .index is missing or S3 is down.
	recovered := NewPartitionLog("default", "orders", 0, 0, transientS3, c, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes:      1,
			FlushInterval: time.Millisecond,
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
	}, nil, nil, nil)
	_, err := recovered.RestoreFromS3(context.Background())
	if err == nil {
		t.Fatalf("RestoreFromS3 should fail on transient DownloadIndex error")
	}
	if err.Error() == "" || errors.Is(err, ErrNotFound) {
		t.Fatalf("expected transient error, not ErrNotFound, got: %v", err)
	}
}

func TestPartitionLogReadSkipsGap(t *testing.T) {
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
	}, nil, nil, nil)

	batch1, _ := NewRecordBatchFromBytes(makeBatchBytes(0, 0, 1, 0xAA))
	if _, err := log.AppendBatch(context.Background(), batch1); err != nil {
		t.Fatalf("AppendBatch 1: %v", err)
	}

	if err := log.Flush(context.Background()); err != nil {
		t.Fatalf("Flush 1: %v", err)
	}

	batch2, _ := NewRecordBatchFromBytes(makeBatchBytes(0, 0, 1, 0xBB))
	if _, err := log.AppendBatch(context.Background(), batch2); err != nil {
		t.Fatalf("AppendBatch 2: %v", err)
	}

	if err := log.Flush(context.Background()); err != nil {
		t.Fatalf("Flush 2: %v", err)
	}

	firstIndexKey := "default/orders/0/segment-00000000000000000000.index"
	s3.mu.Lock()
	delete(s3.index, firstIndexKey)
	s3.mu.Unlock()

	recovered := NewPartitionLog("default", "orders", 0, 0, s3, c, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes:      1,
			FlushInterval: time.Millisecond,
		},
		Segment: SegmentWriterConfig{
			IndexIntervalMessages: 1,
		},
	}, nil, nil, nil)
	lastOffset, err := recovered.RestoreFromS3(context.Background())
	if err != nil {
		t.Fatalf("RestoreFromS3: %v", err)
	}
	if len(recovered.segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(recovered.segments))
	}
	if recovered.segments[0].baseOffset != 1 {
		t.Fatalf("expected surviving segment base offset 1, got %d", recovered.segments[0].baseOffset)
	}
	if lastOffset != 1 {
		t.Fatalf("expected last offset 1, got %d", lastOffset)
	}

	data, err := recovered.Read(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("Read at gap offset 0 should snap forward, got error: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("expected non-empty data from snapped-forward read")
	}
}

type transientIndexErrorS3 struct {
	*MemoryS3Client
}

func (t *transientIndexErrorS3) DownloadIndex(ctx context.Context, key string) ([]byte, error) {
	return nil, fmt.Errorf("connection reset by peer")
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
