// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/cache"
	"golang.org/x/sync/semaphore"
)

func indexRepairFixture(t *testing.T) *SegmentArtifact {
	t.Helper()
	a, _ := NewRecordBatchFromBytes(makeBatchBytes(5, 1, 2, 1))
	b, _ := NewRecordBatchFromBytes(makeBatchBytes(7, 0, 1, 2))
	artifact, err := BuildSegment(SegmentWriterConfig{IndexIntervalMessages: 1}, []RecordBatch{a, b}, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}

func TestRebuildSegmentIndexValidation(t *testing.T) {
	artifact := indexRepairFixture(t)
	fixCRC := func(data []byte) {
		binary.BigEndian.PutUint32(data[len(data)-segmentFooterLen:], crc32.Checksum(segmentBody(data), crcTable))
	}
	withBody := func(body []byte) []byte {
		data := append([]byte(nil), artifact.SegmentBytes[:segmentHeaderLen]...)
		data = append(data, body...)
		data = append(data, artifact.SegmentBytes[len(artifact.SegmentBytes)-segmentFooterLen:]...)
		fixCRC(data)
		return data
	}
	for _, tc := range []struct {
		name       string
		mutate     func([]byte) []byte
		base, last int64
		want       string
	}{
		{name: "valid", base: 5, last: 7},
		{name: "short segment", mutate: func(d []byte) []byte { return d[:segmentHeaderLen] }, base: 5, last: 7, want: "bounds"},
		{name: "negative base", base: -1, last: 7, want: "bounds"},
		{name: "last below base", base: 5, last: 4, want: "bounds"},
		{name: "overflow", base: 5, last: math.MaxInt64, want: "bounds"},
		{name: "magic", mutate: func(d []byte) []byte { d[0] ^= 1; return d }, base: 5, last: 7, want: "magic"},
		{name: "version", mutate: func(d []byte) []byte { binary.BigEndian.PutUint16(d[4:6], 2); return d }, base: 5, last: 7, want: "header"},
		{name: "header base", mutate: func(d []byte) []byte { binary.BigEndian.PutUint64(d[8:16], 6); return d }, base: 5, last: 7, want: "header"},
		{name: "footer magic", mutate: func(d []byte) []byte { d[len(d)-1] ^= 1; return d }, base: 5, last: 7, want: "footer magic"},
		{name: "footer changed", base: 5, last: 8, want: "footer changed"},
		{name: "checksum", mutate: func(d []byte) []byte { d[segmentHeaderLen+12] ^= 1; return d }, base: 5, last: 7, want: "checksum"},
		{name: "trailing frame", mutate: func([]byte) []byte { return withBody([]byte{1}) }, base: 5, last: 7, want: "truncated"},
		{name: "frame length", mutate: func(d []byte) []byte { return withBody(segmentBody(d)[:12]) }, base: 5, last: 7, want: "exceeds"},
		{name: "short batch", mutate: func([]byte) []byte { return withBody(make([]byte, 12)) }, base: 5, last: 7, want: "batch too small"},
		{name: "batch base", mutate: func(d []byte) []byte { binary.BigEndian.PutUint64(d[segmentHeaderLen:], 6); fixCRC(d); return d }, base: 5, last: 7, want: "batch offsets"},
		{name: "negative delta", mutate: func(d []byte) []byte {
			binary.BigEndian.PutUint32(d[segmentHeaderLen+23:], math.MaxUint32)
			fixCRC(d)
			return d
		}, base: 5, last: 7, want: "batch offsets"},
		{name: "delta exceeds footer", mutate: func(d []byte) []byte { binary.BigEndian.PutUint32(d[segmentHeaderLen+23:], 3); fixCRC(d); return d }, base: 5, last: 7, want: "batch offsets"},
		{name: "noncontiguous batches", mutate: func(d []byte) []byte { binary.BigEndian.PutUint64(d[segmentHeaderLen+70:], 8); fixCRC(d); return d }, base: 5, last: 7, want: "batch offsets"},
		{name: "incomplete body", mutate: func(d []byte) []byte { return withBody(segmentBody(d)[:70]) }, base: 5, last: 7, want: "advertised last"},
		{name: "empty body", mutate: func([]byte) []byte { return withBody(nil) }, base: 5, last: 7, want: "advertised last"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := append([]byte(nil), artifact.SegmentBytes...)
			if tc.mutate != nil {
				data = tc.mutate(data)
			}
			before := append([]byte(nil), data...)
			entries, err := rebuildSegmentIndex(data, tc.base, tc.last, 1)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("want %s, got %v", tc.want, err)
				}
			} else if err != nil || len(entries) != 2 || entries[0].Offset != 5 || entries[0].Position != segmentHeaderLen || entries[1].Offset != 7 || entries[1].Position != segmentHeaderLen+70 {
				t.Fatalf("entries=%+v err=%v", entries, err)
			}
			if !bytes.Equal(data, before) {
				t.Fatal("rebuild changed segment bytes")
			}
		})
	}
}

type indexRepairS3 struct {
	*MemoryS3Client
	fullError error
}

func (s *indexRepairS3) DownloadSegment(ctx context.Context, key string, rng *ByteRange) ([]byte, error) {
	if rng == nil && s.fullError != nil {
		return nil, s.fullError
	}
	return s.MemoryS3Client.DownloadSegment(ctx, key, rng)
}

func TestRestoreSegmentIndexIO(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			ctx := context.Background()
			s3 := &indexRepairS3{MemoryS3Client: NewMemoryS3Client()}
			artifact := indexRepairFixture(t)
			key := segmentObjectKey("default", "orders", 0, 5)
			if err := s3.UploadSegment(ctx, key, artifact.SegmentBytes); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("S3 offline")
			if failure {
				s3.fullError = injected
			}
			sem := semaphore.NewWeighted(1)
			calls := 0
			log := NewPartitionLog("default", "orders", 0, 5, s3, nil, PartitionLogConfig{}, nil, func(op string, _ time.Duration, err error) {
				calls++
				if op != "download_segment_rebuild_index" || errors.Is(err, injected) != failure {
					t.Fatalf("op=%s err=%v", op, err)
				}
			}, sem)
			entries, err := log.restoreSegmentIndex(ctx, S3Object{Key: key, Size: int64(len(artifact.SegmentBytes))}, 5, 7)
			if failure {
				if !errors.Is(err, injected) {
					t.Fatal(err)
				}
			} else if err != nil || len(entries) != 2 {
				t.Fatalf("entries=%+v err=%v", entries, err)
			}
			if calls != 1 || !sem.TryAcquire(1) {
				t.Fatal("metrics missing or semaphore leaked")
			}
			sem.Release(1)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sem := semaphore.NewWeighted(1)
	if err := sem.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	log := NewPartitionLog("default", "orders", 0, 0, NewMemoryS3Client(), nil, PartitionLogConfig{}, nil, nil, sem)
	if _, err := log.restoreSegmentIndex(ctx, S3Object{}, 0, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestRestoreRejectsOverlappingSegments(t *testing.T) {
	ctx := context.Background()
	s3 := NewMemoryS3Client()
	for _, bounds := range [][2]int64{{0, 3}, {2, 2}} {
		batch, _ := NewRecordBatchFromBytes(makeBatchBytes(bounds[0], int32(bounds[1]-bounds[0]), int32(bounds[1]-bounds[0]+1), 1))
		artifact, err := BuildSegment(SegmentWriterConfig{}, []RecordBatch{batch}, time.Unix(1, 0))
		if err != nil {
			t.Fatal(err)
		}
		if err := s3.UploadSegment(ctx, segmentObjectKey("default", "orders", 0, bounds[0]), artifact.SegmentBytes); err != nil {
			t.Fatal(err)
		}
		// Both sidecars are absent: adoption must not hide one writer's data.
	}
	log := NewPartitionLog("default", "orders", 0, 0, s3, nil, PartitionLogConfig{}, nil, nil, nil)
	if _, err := log.RestoreFromS3(ctx); err == nil || !strings.Contains(err.Error(), "overlapping") {
		t.Fatal(err)
	}
	if log.nextOffset != 0 || len(log.segments) != 0 {
		t.Fatal("failed restore published overlapping data")
	}
}

func TestRestoreRebuildsMisleadingIndexEntries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries []*IndexEntry
	}{
		{"valid", []*IndexEntry{{Offset: 5, Position: 32}, {Offset: 7, Position: 102}}},
		{"invalid first entry", []*IndexEntry{{Offset: 6, Position: 33}}},
		{"wrong secondary offset", []*IndexEntry{{Offset: 5, Position: 32}, {Offset: 6, Position: 102}}},
		{"wrong secondary position", []*IndexEntry{{Offset: 5, Position: 32}, {Offset: 7, Position: 103}}},
	} {
		for _, cached := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cached=%v", tc.name, cached), func(t *testing.T) {
				ctx := context.Background()
				s3 := NewMemoryS3Client()
				artifact := indexRepairFixture(t)
				key := segmentObjectKey("default", "orders", 0, 5)
				if err := s3.UploadSegment(ctx, key, artifact.SegmentBytes); err != nil {
					t.Fatal(err)
				}
				index := NewIndexBuilder(1)
				for _, entry := range tc.entries {
					index.MaybeAdd(entry.Offset, entry.Position, 1)
				}
				body, err := index.BuildBytes()
				if err != nil {
					t.Fatal(err)
				}
				indexKey := segmentIndexKey("default", "orders", 0, 5)
				if err := s3.UploadIndex(ctx, indexKey, body); err != nil {
					t.Fatal(err)
				}
				c := cache.NewSegmentCache(1 << 20)
				log := NewPartitionLog("default", "orders", 0, 5, s3, c, PartitionLogConfig{CacheEnabled: cached}, nil, nil, nil)
				if last, err := log.RestoreFromS3(ctx); err != nil || last != 7 {
					t.Fatalf("last=%d err=%v", last, err)
				}
				if cached {
					c.SetSegment(log.cacheTopicKey(), 0, 5, artifact.SegmentBytes)
				}
				for _, offset := range []int64{5, 6, 7} {
					data, err := log.Read(ctx, offset, 0)
					want := int64(5)
					if offset == 7 {
						want = 7
					}
					if err != nil || len(data) < recordBatchHeaderMinSize || int64(binary.BigEndian.Uint64(data[:8])) != want {
						t.Fatalf("offset %d skipped containing batch: %x err=%v", offset, data, err)
					}
				}
				got, err := s3.DownloadIndex(ctx, indexKey)
				if err != nil || !bytes.Equal(got, body) {
					t.Fatal("recovery rewrote the sidecar")
				}
			})
		}
	}
}

func TestRestoreRejectsInvalidSegmentData(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%v", indexed), func(t *testing.T) {
			ctx := context.Background()
			s3 := NewMemoryS3Client()
			artifact := indexRepairFixture(t)
			data := bytes.Clone(artifact.SegmentBytes)
			data[segmentHeaderLen+12] ^= 1
			if err := s3.UploadSegment(ctx, segmentObjectKey("default", "orders", 0, 5), data); err != nil {
				t.Fatal(err)
			}
			if indexed {
				if err := s3.UploadIndex(ctx, segmentIndexKey("default", "orders", 0, 5), artifact.IndexBytes); err != nil {
					t.Fatal(err)
				}
			}
			log := NewPartitionLog("default", "orders", 0, 5, s3, nil, PartitionLogConfig{}, nil, nil, nil)
			if _, err := log.RestoreFromS3(ctx); err == nil || !strings.Contains(err.Error(), "checksum") {
				t.Fatal(err)
			}
			if log.nextOffset != 5 || len(log.segments) != 0 {
				t.Fatal("invalid segment was published")
			}
		})
	}
}

func TestCheckOwnershipPreservesLeasePriority(t *testing.T) {
	leaseErr := errors.New("lease lost")
	for _, lost := range []bool{false, true} {
		for _, fenced := range []bool{false, true} {
			log := NewPartitionLogWithOptions("default", "orders", 0, 0, NewMemoryS3Client(), nil, PartitionLogOptions{CheckOwnership: func() error {
				if lost {
					return leaseErr
				}
				return nil
			}}, nil, nil, nil)
			if fenced {
				log.uploadConflict.Store(&partitionLogFailure{err: ErrLogFenced})
			}
			err := log.CheckOwnership()
			switch {
			case lost:
				if !errors.Is(err, leaseErr) {
					t.Fatal(err)
				}
			case fenced:
				if !errors.Is(err, ErrLogFenced) {
					t.Fatal(err)
				}
			default:
				if err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

type unreadableIndexS3 struct {
	*MemoryS3Client
	indexReads int
}

func (s *unreadableIndexS3) DownloadIndex(context.Context, string) ([]byte, error) {
	s.indexReads++
	return nil, errors.New("sidecar read forbidden")
}

func TestRestoreDoesNotRequireSidecarAccess(t *testing.T) {
	ctx := context.Background()
	s3 := &unreadableIndexS3{MemoryS3Client: NewMemoryS3Client()}
	artifact := indexRepairFixture(t)
	if err := s3.UploadSegment(ctx, segmentObjectKey("default", "orders", 0, 5), artifact.SegmentBytes); err != nil {
		t.Fatal(err)
	}
	log := NewPartitionLog("default", "orders", 0, 5, s3, nil, PartitionLogConfig{}, nil, nil, nil)
	if last, err := log.RestoreFromS3(ctx); err != nil || last != 7 {
		t.Fatalf("last=%d err=%v", last, err)
	}
	if s3.indexReads != 0 {
		t.Fatal("restore still reads an untrusted sidecar")
	}
	data, err := log.Read(ctx, 6, 0)
	if err != nil || len(data) < 8 || binary.BigEndian.Uint64(data[:8]) != 5 {
		t.Fatalf("offset 6 skipped its containing batch: %x %v", data, err)
	}
}
