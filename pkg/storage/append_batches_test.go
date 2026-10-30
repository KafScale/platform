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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type atomicAppendS3 struct {
	*MemoryS3Client
	fail            string
	failDelete      bool
	cancelOnFailure context.CancelFunc
	puts            atomic.Int32
}

func (s *atomicAppendS3) UploadIndex(ctx context.Context, key string, body []byte) error {
	s.puts.Add(1)
	if s.fail == "index" {
		return errors.New("index upload failed")
	}
	if err := s.MemoryS3Client.UploadIndex(ctx, key, body); err != nil {
		return err
	}
	if s.fail == "index-after-write" {
		return errors.New("index response lost")
	}
	return nil
}

func (s *atomicAppendS3) UploadSegment(ctx context.Context, key string, body []byte) error {
	s.puts.Add(1)
	if s.fail == "segment" {
		if s.cancelOnFailure != nil {
			s.cancelOnFailure()
		}
		return errors.New("segment upload failed")
	}
	if err := s.MemoryS3Client.UploadSegment(ctx, key, body); err != nil {
		return err
	}
	if s.fail == "segment-after-write" {
		return errors.New("segment response lost")
	}
	return nil
}

func (s *atomicAppendS3) DeleteIndex(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.failDelete {
		return errors.New("delete index failed")
	}
	return s.MemoryS3Client.DeleteIndex(ctx, key)
}

func (s *atomicAppendS3) DeleteSegment(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.failDelete {
		return errors.New("delete segment failed")
	}
	return s.MemoryS3Client.DeleteSegment(ctx, key)
}

func TestAppendBatchesFailureAndRetry(t *testing.T) {
	ctx := context.Background()
	for _, force := range []bool{false, true} {
		for _, fail := range []string{"index", "segment", "index-after-write", "segment-after-write"} {
			t.Run(fmt.Sprintf("force=%t/%s", force, fail), func(t *testing.T) {
				s3 := &atomicAppendS3{MemoryS3Client: NewMemoryS3Client()}
				var committed int64 = -1
				cfg := PartitionLogConfig{Buffer: WriteBufferConfig{MaxBatches: 3}, Segment: SegmentWriterConfig{IndexIntervalMessages: 1}}
				log := NewPartitionLog("default", "orders", 0, 0, s3, nil, cfg, func(_ context.Context, a *SegmentArtifact) { committed = a.LastOffset }, nil, nil)
				old, _ := NewRecordBatchFromBytes(makeBatchBytes(0, 0, 1, 1))
				if _, err := log.AppendBatch(ctx, old); err != nil {
					t.Fatal(err)
				}
				first, _ := NewRecordBatchFromBytes(makeBatchBytes(99, 1, 2, 2))
				second, _ := NewRecordBatchFromBytes(makeBatchBytes(99, 2, 3, 3))
				batches := []RecordBatch{first, second}
				s3.fail = fail
				if _, err := log.AppendBatches(ctx, batches, force); err == nil {
					t.Fatal("expected upload failure")
				}
				if next := log.BufferedHighWatermark(); next != 1 || committed != -1 {
					t.Fatalf("failed append changed offsets: next=%d committed=%d", next, committed)
				}
				got, err := log.Read(ctx, 0, 1<<20)
				if err != nil || !bytes.Equal(got, old.Bytes) {
					t.Fatalf("failed append lost old buffer: %x, %v", got, err)
				}
				if _, err := log.Read(ctx, 1, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
					t.Fatalf("failed append became readable: %v", err)
				}
				recovered := NewPartitionLog("default", "orders", 0, 0, s3, nil, cfg, nil, nil, nil)
				if last, err := recovered.RestoreFromS3(ctx); err != nil || last != -1 {
					t.Fatalf("failed append survived restart: %d, %v", last, err)
				}
				s3.fail = ""
				result, err := log.AppendBatches(ctx, batches, force)
				if err != nil || result.BaseOffset != 1 || result.LastOffset != 5 || committed != 5 {
					t.Fatalf("retry: %+v, committed=%d, %v", result, committed, err)
				}
				if first.BaseOffset != 99 || second.BaseOffset != 99 {
					t.Fatal("append mutated caller batches")
				}
				recovered = NewPartitionLog("default", "orders", 0, 0, s3, nil, cfg, nil, nil, nil)
				if last, err := recovered.RestoreFromS3(ctx); err != nil || last != 5 {
					t.Fatalf("restore retry: %d, %v", last, err)
				}
				got, err = recovered.Read(ctx, 0, 1<<20)
				PatchRecordBatchBaseOffset(&first, 1)
				PatchRecordBatchBaseOffset(&second, 3)
				want := append(append(bytes.Clone(old.Bytes), first.Bytes...), second.Bytes...)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("retry duplicated or lost batches: got %x want %x, %v", got, want, err)
				}
			})
		}
	}
}

func TestAppendBatchesStagingVisibility(t *testing.T) {
	ctx := context.Background()
	s3 := &slowUploadS3{MemoryS3Client: NewMemoryS3Client(), started: make(chan struct{}), release: make(chan struct{})}
	log := NewPartitionLog("default", "orders", 0, 0, s3, nil, PartitionLogConfig{}, nil, nil, nil)
	old, _ := NewRecordBatchFromBytes(makeBatchBytes(0, 0, 1, 1))
	if _, err := log.AppendBatch(ctx, old); err != nil {
		t.Fatal(err)
	}
	first, _ := NewRecordBatchFromBytes(makeBatchBytes(0, 0, 1, 2))
	second, _ := NewRecordBatchFromBytes(makeBatchBytes(0, 0, 1, 3))
	done := make(chan error, 1)
	go func() { _, err := log.AppendBatches(ctx, []RecordBatch{first, second}, true); done <- err }()
	<-s3.started
	// Always release the upload, including after a failed visibility assertion.
	func() {
		defer close(s3.release)
		if next := log.BufferedHighWatermark(); next != 1 {
			t.Errorf("staged offsets visible: %d", next)
		}
		if got, err := log.Read(ctx, 0, 1<<20); err != nil || !bytes.Equal(got, old.Bytes) {
			t.Errorf("old buffered data unavailable during upload: %x, %v", got, err)
		}
		if _, err := log.Read(ctx, 1, 1<<20); !errors.Is(err, ErrOffsetOutOfRange) {
			t.Errorf("staged batches became readable: %v", err)
		}
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if next := log.BufferedHighWatermark(); next != 3 {
		t.Fatal(next)
	}
}

func TestAppendBatchesRejectsBeforeCommit(t *testing.T) {
	for _, tc := range []string{"empty", "invalid-second", "canceled"} {
		t.Run(tc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			log := NewPartitionLog("default", "orders", 0, 0, NewMemoryS3Client(), nil, PartitionLogConfig{}, nil, nil, nil)
			batch, _ := NewRecordBatchFromBytes(makeBatchBytes(0, 0, 1, 1))
			batches := []RecordBatch{batch}
			switch tc {
			case "empty":
				batches = nil
			case "invalid-second":
				batches = append(batches, RecordBatch{})
			case "canceled":
				cancel()
			}
			if _, err := log.AppendBatches(ctx, batches, true); err == nil {
				t.Fatal("expected rejection")
			}
			if next := log.BufferedHighWatermark(); next != 0 {
				t.Fatal(next)
			}
		})
	}
}

func TestAppendBatchesConcurrentRequests(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(fmt.Sprintf("force=%t", force), func(t *testing.T) {
			ctx := context.Background()
			log := NewPartitionLog("default", "orders", 0, 0, NewMemoryS3Client(), nil, PartitionLogConfig{}, nil, nil, nil)
			const requests = 16
			type outcome struct {
				result *AppendResult
				err    error
			}
			done := make(chan outcome, requests)
			for range requests {
				go func() {
					batch, _ := NewRecordBatchFromBytes(makeBatchBytes(0, 0, 1, 1))
					result, err := log.AppendBatches(ctx, []RecordBatch{batch, batch}, force)
					done <- outcome{result, err}
				}()
			}
			seen := make(map[int64]bool)
			for range requests {
				out := <-done
				if out.err != nil {
					t.Fatal(out.err)
				}
				result := out.result
				if result.BaseOffset%2 != 0 || result.LastOffset != result.BaseOffset+1 || seen[result.BaseOffset] {
					t.Fatalf("overlapping or interleaved record set: %+v", result)
				}
				seen[result.BaseOffset] = true
			}
			if next := log.BufferedHighWatermark(); next != requests*2 {
				t.Fatal(next)
			}
			if err := log.Flush(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Each PUT must enter before either is allowed to finish. This fails for
// sequential uploads without depending on a latency benchmark.
type overlappingUploadS3 struct {
	*MemoryS3Client
	entered chan string
	release chan struct{}
}

func (s *overlappingUploadS3) UploadSegment(ctx context.Context, key string, body []byte) error {
	s.entered <- "segment"
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.MemoryS3Client.UploadSegment(ctx, key, body)
}

func (s *overlappingUploadS3) UploadIndex(ctx context.Context, key string, body []byte) error {
	s.entered <- "index"
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.MemoryS3Client.UploadIndex(ctx, key, body)
}

func TestAppendBatchesUploadsOverlap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s3 := &overlappingUploadS3{MemoryS3Client: NewMemoryS3Client(), entered: make(chan string, 2), release: make(chan struct{})}
	var commits atomic.Int32
	log := NewPartitionLog("default", "orders", 0, 0, s3, nil, PartitionLogConfig{}, func(context.Context, *SegmentArtifact) { commits.Add(1) }, nil, nil)
	batch, _ := NewRecordBatchFromBytes(makeBatchBytes(0, 0, 1, 1))
	done := make(chan error, 1)
	go func() { _, err := log.AppendBatches(ctx, []RecordBatch{batch, batch}, true); done <- err }()
	entered := map[string]bool{}
	for range 2 {
		select {
		case op := <-s3.entered:
			entered[op] = true
		case <-ctx.Done():
			t.Fatal("segment and index uploads did not overlap")
		}
	}
	if len(entered) != 2 || commits.Load() != 0 || log.BufferedHighWatermark() != 0 {
		t.Fatal("state published before both uploads completed")
	}
	close(s3.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if commits.Load() != 1 || log.BufferedHighWatermark() != 2 {
		t.Fatal("complete upload was not committed")
	}
}

func TestAppendBatchesFailedCleanupFencesRetry(t *testing.T) {
	ctx := context.Background()
	s3 := &atomicAppendS3{MemoryS3Client: NewMemoryS3Client(), fail: "segment", failDelete: true}
	log := NewPartitionLog("default", "orders", 0, 0, s3, nil, PartitionLogConfig{}, nil, nil, nil)
	batch, _ := NewRecordBatchFromBytes(makeBatchBytes(0, 0, 1, 1))
	if _, err := log.AppendBatches(ctx, []RecordBatch{batch}, true); err == nil {
		t.Fatal("expected upload and cleanup failure")
	}
	s3.fail = "index"
	if _, err := log.AppendBatches(ctx, []RecordBatch{batch}, true); err == nil {
		t.Fatal("retry bypassed failed cleanup")
	}
	if s3.puts.Load() != 2 || log.BufferedHighWatermark() != 0 {
		t.Fatal("dirty offset was reused before cleanup")
	}
	// An opposite upload failure must not pair the stale index with a new segment.
	s3.failDelete = false
	if _, err := log.AppendBatches(ctx, []RecordBatch{batch}, true); err == nil {
		t.Fatal("expected index failure")
	}
	recovered := NewPartitionLog("default", "orders", 0, 0, s3, nil, PartitionLogConfig{}, nil, nil, nil)
	if last, err := recovered.RestoreFromS3(ctx); err != nil || last != -1 {
		t.Fatalf("recovered a rejected upload: %d, %v", last, err)
	}
	s3.fail = ""
	if result, err := log.AppendBatches(ctx, []RecordBatch{batch}, true); err != nil || result.BaseOffset != 0 {
		t.Fatalf("retry: %+v, %v", result, err)
	}
}

func TestAppendBatchFailureDoesNotReserveOffsets(t *testing.T) {
	ctx := context.Background()
	s3 := &atomicAppendS3{MemoryS3Client: NewMemoryS3Client(), fail: "index"}
	log := NewPartitionLog("default", "orders", 0, 0, s3, nil, PartitionLogConfig{Buffer: WriteBufferConfig{MaxBatches: 1}}, nil, nil, nil)
	batch, _ := NewRecordBatchFromBytes(makeBatchBytes(0, 0, 1, 1))
	if _, err := log.AppendBatch(ctx, batch); err == nil {
		t.Fatal("expected upload failure")
	}
	if log.BufferedHighWatermark() != 0 {
		t.Fatal("single-batch failure reserved an offset")
	}
	s3.fail = ""
	if result, err := log.AppendBatch(ctx, batch); err != nil || result.BaseOffset != 0 {
		t.Fatalf("single-batch retry: %+v, %v", result, err)
	}
}

func TestAppendBatchesCanceledUploadCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s3 := &atomicAppendS3{MemoryS3Client: NewMemoryS3Client(), fail: "segment", cancelOnFailure: cancel}
	log := NewPartitionLog("default", "orders", 0, 0, s3, nil, PartitionLogConfig{}, nil, nil, nil)
	batch, _ := NewRecordBatchFromBytes(makeBatchBytes(0, 0, 1, 1))
	if _, err := log.AppendBatches(ctx, []RecordBatch{batch}, true); err == nil {
		t.Fatal("expected canceled upload failure")
	}
	if ctx.Err() == nil {
		t.Fatal("test did not cancel the upload context")
	}
	if log.failedUploadBase != nil {
		t.Fatal("cleanup reused the canceled upload context")
	}
	if log.BufferedHighWatermark() != 0 {
		t.Fatal("canceled upload reserved offsets")
	}
	recovered := NewPartitionLog("default", "orders", 0, 0, s3, nil, PartitionLogConfig{}, nil, nil, nil)
	if last, err := recovered.RestoreFromS3(context.Background()); err != nil || last != -1 {
		t.Fatalf("canceled upload survived cleanup: %d, %v", last, err)
	}
}
