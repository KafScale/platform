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
	"sync"
	"testing"

	"github.com/KafScale/platform/pkg/cache"
)

// TestPartitionLogReadAfterAckBeforeFlush exercises read-after-ack at the
// storage layer: an acknowledged record that is still in the in-memory write
// buffer (not yet flushed to a segment) must be readable through
// PartitionLog.Read.
//
// AppendBatch returns an AppendResult (with assigned offsets) as soon as the
// batch is buffered; that return value is the basis for ACKing the produce to
// the client. Flushing to a segment happens only when a WriteBuffer threshold
// trips (MaxBytes/MaxMessages/MaxBatches/FlushInterval), evaluated inside
// AppendBatch (there is no background flusher). Before this change Read served
// only flushed segments and returned ErrOffsetOutOfRange for anything still in
// the buffer.
//
// Consequence at the storage layer: when produce to a partition stops below the
// flush threshold, the just-acknowledged tail stays in the buffer and Read
// returns ErrOffsetOutOfRange for it. This test asserts Read serves that
// buffered tail.
//
// Scope: this is the storage-layer half of read-after-ack. Whether a real Kafka
// consumer reaches these offsets through the fetch path additionally depends on
// the broker advertising a high-watermark that includes the buffered tail; that
// end-to-end path is covered by the broker test
// TestHandleFetchReadAfterAckFlushDisabled. The buffer is in-memory, so these
// records are readable while the process lives but lost on restart; this is not
// a durability guarantee.
//
// Existing tests use MaxBytes:1 so every append flushes immediately, which is
// why they never exercised this path.
func TestPartitionLogReadAfterAckBeforeFlush(t *testing.T) {
	s3 := NewMemoryS3Client()
	c := cache.NewSegmentCache(1024)
	// Thresholds set so NOTHING auto-flushes: records stay in the buffer.
	log := NewPartitionLog("default", "orders", 0, 0, s3, c, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBytes:      1 << 30, // 1 GiB; never tripped by this test
			MaxMessages:   0,
			MaxBatches:    0,
			FlushInterval: 0, // time-based flush disabled
		},
		Segment: SegmentWriterConfig{IndexIntervalMessages: 1},
	}, nil, nil, nil)

	const n = 10
	for i := 0; i < n; i++ {
		batch, err := NewRecordBatchFromBytes(make([]byte, 70))
		if err != nil {
			t.Fatalf("NewRecordBatchFromBytes: %v", err)
		}
		res, err := log.AppendBatch(context.Background(), batch)
		if err != nil {
			t.Fatalf("AppendBatch %d: %v", i, err)
		}
		// A non-error AppendResult is the basis for ACKing this offset to the
		// client: from the producer's point of view, offset res.BaseOffset is
		// now committed.
		if res.BaseOffset != int64(i) {
			t.Fatalf("append %d: expected base offset %d, got %d", i, i, res.BaseOffset)
		}
	}

	// Every acked offset MUST be readable. On v1.6.0 nothing flushed (no
	// threshold tripped), Read finds no segment and returns ErrOffsetOutOfRange.
	for off := int64(0); off < n; off++ {
		data, err := log.Read(context.Background(), off, 1<<20)
		if err != nil {
			if errors.Is(err, ErrOffsetOutOfRange) {
				t.Fatalf("acks-but-unreadable: acked offset %d is not consumable "+
					"(buffered, not flushed; Read serves only segments): %v", off, err)
			}
			t.Fatalf("Read acked offset %d: %v", off, err)
		}
		if len(data) == 0 {
			t.Fatalf("Read acked offset %d returned no data", off)
		}
	}
}

func TestPartitionLogConcurrentReadDuringBufferedAppend(t *testing.T) {
	s3 := NewMemoryS3Client()
	c := cache.NewSegmentCache(1 << 20)
	log := NewPartitionLog("default", "orders", 0, 0, s3, c, PartitionLogConfig{
		Buffer:  WriteBufferConfig{MaxBytes: 1 << 30},
		Segment: SegmentWriterConfig{IndexIntervalMessages: 1},
	}, nil, nil, nil)

	const readers = 8
	var wg sync.WaitGroup
	errCh := make(chan error, readers)

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for off := int64(0); off < 20; off++ {
				data, err := log.Read(context.Background(), off, 1<<20)
				if err != nil && !errors.Is(err, ErrOffsetOutOfRange) {
					errCh <- err
					return
				}
				if err == nil && len(data) == 0 {
					errCh <- errors.New("read returned empty body without error")
					return
				}
			}
		}()
	}

	for i := 0; i < 20; i++ {
		batch, err := NewRecordBatchFromBytes(make([]byte, 70))
		if err != nil {
			t.Fatalf("NewRecordBatchFromBytes: %v", err)
		}
		if _, err := log.AppendBatch(context.Background(), batch); err != nil {
			t.Fatalf("AppendBatch %d: %v", i, err)
		}
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}
