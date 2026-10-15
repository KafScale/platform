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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
)

type restoreProbeS3 struct {
	*MemoryS3Client
	active atomic.Int64
	peak   atomic.Int64
	calls  atomic.Int64
	before func(context.Context, string, string) error
}

func (s *restoreProbeS3) ListSegments(ctx context.Context, prefix string) ([]S3Object, error) {
	objects, err := s.MemoryS3Client.ListSegments(ctx, prefix)
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key > objects[j].Key })
	return objects, err
}

func (s *restoreProbeS3) enter(ctx context.Context, operation, key string) error {
	active := s.active.Add(1)
	for peak := s.peak.Load(); active > peak; peak = s.peak.Load() {
		if s.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	s.calls.Add(1)
	if s.before != nil {
		return s.before(ctx, operation, key)
	}
	return ctx.Err()
}

func (s *restoreProbeS3) DownloadSegment(ctx context.Context, key string, rng *ByteRange) ([]byte, error) {
	defer s.active.Add(-1)
	if err := s.enter(ctx, "footer", key); err != nil {
		return nil, err
	}
	return s.MemoryS3Client.DownloadSegment(ctx, key, rng)
}

func (s *restoreProbeS3) DownloadIndex(ctx context.Context, key string) ([]byte, error) {
	defer s.active.Add(-1)
	if err := s.enter(ctx, "index", key); err != nil {
		return nil, err
	}
	return s.MemoryS3Client.DownloadIndex(ctx, key)
}

type restoreLogWriter chan string

func (w restoreLogWriter) Write(p []byte) (int, error) {
	w <- string(p)
	return len(p), nil
}

func restoreFixture(t *testing.T, count int) *MemoryS3Client {
	t.Helper()
	s3 := NewMemoryS3Client()
	for i := 0; i < count; i++ {
		uploadRecoverySegment(t, s3, "default", "orders", 0, int64(i), time.Unix(1, 0))
	}
	return s3
}

func TestRestoreBoundedConcurrencyAndProgress(t *testing.T) {
	for _, limit := range []int{0, 2} {
		t.Run(fmt.Sprintf("semaphore_%d", limit), func(t *testing.T) {
			const count = 40
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			gate := make(chan struct{})
			entered := make(chan struct{}, count*2)
			s3 := &restoreProbeS3{MemoryS3Client: restoreFixture(t, count)}
			s3.data["default/orders/0/unrelated.kfs"] = make([]byte, segmentFooterLen)
			s3.data["default/orders/0/segment-invalid.kfs"] = make([]byte, segmentFooterLen)
			s3.data["default/orders/0/segment-00000000000000000099.kfs"] = []byte("short")
			s3.data["default/orders/0/segment-00000000000000000099.index"] = make([]byte, segmentFooterLen)
			s3.before = func(ctx context.Context, _, _ string) error {
				select {
				case entered <- struct{}{}:
				case <-ctx.Done():
					return ctx.Err()
				}
				select {
				case <-gate:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			logs := make(restoreLogWriter, count+10)
			cfg := PartitionLogConfig{Logger: slog.New(slog.NewJSONHandler(logs, nil))}
			var sem *semaphore.Weighted
			expected := restoreConcurrency
			if limit > 0 {
				sem = semaphore.NewWeighted(int64(limit))
				expected = limit
			}
			log := NewPartitionLog("default", "orders", 0, 0, s3, nil, cfg, nil, nil, sem)
			done := make(chan error, 1)
			go func() {
				last, err := log.RestoreFromS3(ctx)
				if err == nil && last != count-1 {
					err = fmt.Errorf("last offset = %d", last)
				}
				done <- err
			}()
			joined := false
			defer func() {
				cancel()
				if !joined {
					<-done
				}
			}()
			for i := 0; i < expected; i++ {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("restore did not start concurrent requests")
				}
			}
			// A blocked store must still emit progress, and must not exceed either limit.
			for {
				select {
				case line := <-logs:
					if strings.Contains(line, "partition restore progress") {
						if !strings.Contains(line, `"segments":40`) || !strings.Contains(line, `"processed":0`) || !strings.Contains(line, `"topic":"orders"`) || !strings.Contains(line, `"partition":0`) {
							t.Fatalf("incomplete progress log: %s", line)
						}
						goto progressed
					}
				case <-ctx.Done():
					t.Fatal("no restore progress log")
				}
			}
		progressed:
			select {
			case <-entered:
				t.Fatal("restore exceeded concurrency limit")
			default:
			}
			close(gate)
			select {
			case err := <-done:
				joined = true
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("restore did not finish")
			}
			if peak := s3.peak.Load(); peak != int64(expected) {
				t.Fatalf("peak = %d, want %d", peak, expected)
			}
			if s3.active.Load() != 0 || s3.calls.Load() != 2*count {
				t.Fatalf("active=%d calls=%d", s3.active.Load(), s3.calls.Load())
			}
			if len(log.segments) != count || len(log.indexEntries) != count {
				t.Fatalf("incomplete restore: segments=%d indexes=%d", len(log.segments), len(log.indexEntries))
			}
			s3.before = nil
			for i, seg := range log.segments {
				if seg.baseOffset != int64(i) || seg.lastOffset != int64(i) {
					t.Fatalf("unordered segment %d: %+v", i, seg)
				}
				entries := log.indexEntries[int64(i)]
				if len(entries) != 1 || entries[0].Offset != int64(i) {
					t.Fatalf("wrong index at %d: %+v", i, entries)
				}
				if _, err := log.Read(ctx, int64(i), 0); err != nil {
					t.Fatalf("read offset %d: %v", i, err)
				}
			}
			select {
			case line := <-logs:
				if !strings.Contains(line, "partition restore completed") || !strings.Contains(line, `"restored":40`) {
					t.Fatalf("missing completion log: %s", line)
				}
			default:
				t.Fatal("missing completion log")
			}
		})
	}
}

func TestRestoreFailureCancelsWorkersAndLeavesStateUnchanged(t *testing.T) {
	for _, operation := range []string{"footer", "index", "cancel"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			sentinel := errors.New("store unavailable")
			s3 := &restoreProbeS3{MemoryS3Client: restoreFixture(t, 32)}
			sibling := make(chan struct{}, 32)
			s3.before = func(ctx context.Context, op, key string) error {
				base, _ := parseSegmentBaseOffset(strings.TrimSuffix(strings.TrimSuffix(key, ".index"), ".kfs") + ".kfs")
				if base == 31 {
					if op != operation && operation != "cancel" {
						return nil
					}
					select {
					case <-sibling:
					case <-ctx.Done():
						return ctx.Err()
					}
					if operation == "cancel" {
						cancel()
						return ctx.Err()
					}
					return sentinel
				}
				sibling <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			}
			sem := semaphore.NewWeighted(restoreConcurrency)
			logs := make(restoreLogWriter, 10)
			log := NewPartitionLog("default", "orders", 0, 7, s3, nil, PartitionLogConfig{Logger: slog.New(slog.NewJSONHandler(logs, nil))}, nil, nil, sem)
			log.segments = []segmentRange{{baseOffset: -2, lastOffset: -1}}
			log.indexEntries[-2] = []*IndexEntry{{Offset: -2}}
			_, err := log.RestoreFromS3(ctx)
			want := sentinel
			if operation == "cancel" {
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Fatalf("error = %v, want %v", err, want)
			}
			if s3.active.Load() != 0 {
				t.Fatal("workers still active after return")
			}
			if !sem.TryAcquire(restoreConcurrency) {
				t.Fatal("S3 semaphore token leaked")
			}
			sem.Release(restoreConcurrency)
			if log.nextOffset != 7 || len(log.segments) != 1 || log.segments[0].baseOffset != -2 || len(log.indexEntries) != 1 {
				t.Fatal("failed restore mutated live state")
			}
			sawFailure := false
			for len(logs) > 0 {
				if strings.Contains(<-logs, "partition restore failed") {
					sawFailure = true
				}
			}
			if !sawFailure {
				t.Fatal("missing failure log")
			}
			s3.before = nil
			if last, err := log.RestoreFromS3(context.Background()); err != nil || last != 31 {
				t.Fatalf("retry: last=%d err=%v", last, err)
			}
		})
	}
}

func TestRestoreInvalidIndexesPreserveCommitBoundary(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		for _, committed := range []bool{false, true} {
			t.Run(fmt.Sprintf("corrupt_%t_committed_%t", corrupt, committed), func(t *testing.T) {
				s3 := restoreFixture(t, 20)
				key := segmentIndexKey("default", "orders", 0, 5)
				if corrupt {
					s3.index[key] = []byte("invalid")
				} else {
					delete(s3.index, key)
				}
				next := int64(0)
				if committed {
					next = 20
				}
				log := NewPartitionLog("default", "orders", 0, next, s3, nil, PartitionLogConfig{}, nil, nil, nil)
				last, err := log.RestoreFromS3(context.Background())
				if committed {
					if err == nil || len(log.segments) != 0 || log.nextOffset != next {
						t.Fatalf("committed invalid index: last=%d err=%v", last, err)
					}
					return
				}
				if err != nil || last != 19 || len(log.segments) != 19 {
					t.Fatalf("orphan handling: last=%d segments=%d err=%v", last, len(log.segments), err)
				}
				if _, exists := log.indexEntries[5]; exists {
					t.Fatal("orphan index retained")
				}
				batch := buildRecoveryBatch(0, 1000, []int64{0})
				result, err := log.AppendBatch(context.Background(), batch)
				if err != nil || result.BaseOffset != 20 {
					t.Fatalf("append after restore: result=%+v err=%v", result, err)
				}
			})
		}
	}
}

func BenchmarkPartitionRestore(b *testing.B) {
	const count = 1024
	for _, limit := range []int{1, restoreConcurrency} {
		b.Run(fmt.Sprintf("concurrency_%d", limit), func(b *testing.B) {
			memory := NewMemoryS3Client()
			ctx := context.Background()
			for i := 0; i < count; i++ {
				artifact, err := BuildSegment(SegmentWriterConfig{IndexIntervalMessages: 1}, []RecordBatch{buildRecoveryBatch(int64(i), 1000, []int64{0})}, time.Unix(1, 0))
				if err != nil {
					b.Fatal(err)
				}
				if err := memory.UploadSegment(ctx, segmentObjectKey("default", "orders", 0, int64(i)), artifact.SegmentBytes); err != nil {
					b.Fatal(err)
				}
				if err := memory.UploadIndex(ctx, segmentIndexKey("default", "orders", 0, int64(i)), artifact.IndexBytes); err != nil {
					b.Fatal(err)
				}
			}
			s3 := &restoreProbeS3{MemoryS3Client: memory, before: func(ctx context.Context, _, _ string) error {
				timer := time.NewTimer(time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}
			cfg := PartitionLogConfig{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				log := NewPartitionLog("default", "orders", 0, 0, s3, nil, cfg, nil, nil, semaphore.NewWeighted(int64(limit)))
				if last, err := log.RestoreFromS3(ctx); err != nil || last != count-1 {
					b.Fatalf("last=%d err=%v", last, err)
				}
			}
		})
	}
}

func TestRestoreCancellationWhileWaitingForS3(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sem := semaphore.NewWeighted(1)
	if err := sem.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	defer sem.Release(1)
	s3 := &restoreProbeS3{MemoryS3Client: restoreFixture(t, 1)}
	logs := make(restoreLogWriter, 10)
	log := NewPartitionLog("default", "orders", 0, 0, s3, nil, PartitionLogConfig{Logger: slog.New(slog.NewJSONHandler(logs, nil))}, nil, nil, sem)
	done := make(chan error, 1)
	go func() { _, err := log.RestoreFromS3(ctx); done <- err }()
	select {
	case <-logs:
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not cancel while waiting for S3")
	}
	if s3.calls.Load() != 0 || len(log.segments) != 0 {
		t.Fatal("cancelled restore performed I/O or installed state")
	}
}

func TestRestoreRejectsCorruptFooterWithoutPartialState(t *testing.T) {
	s3 := restoreFixture(t, 20)
	key := segmentObjectKey("default", "orders", 0, 5)
	s3.data[key][len(s3.data[key])-1] ^= 0xff
	log := NewPartitionLog("default", "orders", 0, 0, s3, nil, PartitionLogConfig{}, nil, nil, nil)
	if _, err := log.RestoreFromS3(context.Background()); err == nil || !strings.Contains(err.Error(), "invalid footer magic") {
		t.Fatalf("error = %v", err)
	}
	if len(log.segments) != 0 || len(log.indexEntries) != 0 || log.nextOffset != 0 {
		t.Fatal("corrupt footer installed partial state")
	}
}
