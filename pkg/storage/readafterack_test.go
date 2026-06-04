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
	"testing"

	"github.com/KafScale/platform/pkg/cache"
)

// TestPartitionLogReadAfterAckBeforeFlush reproduces the "acks-but-unreadable"
// data-consistency bug.
//
// AppendBatch returns an AppendResult (with assigned offsets) as soon as the
// batch is buffered; that return value is what the broker uses to ACK the
// produce to the client. But flushing to a segment happens only when a
// WriteBuffer threshold trips (MaxBytes/MaxMessages/MaxBatches/FlushInterval),
// and flushing is only ever evaluated inside AppendBatch (there is no background
// flusher). Read serves only flushed segments and returns ErrOffsetOutOfRange
// for anything still in the buffer.
//
// Consequence: when produce to a partition stops below the flush threshold, the
// just-acked tail stays in the in-memory buffer and is NOT consumable, violating
// Kafka's read-after-ack contract (and, because the buffer is in-memory, the
// acked records are lost on a broker restart). Observed end-to-end as
// 1015 acked -> 588 readable against KafScale v1.6.0.
//
// Existing tests use MaxBytes:1 so every append flushes immediately, which is
// why they never exercised this path.
//
// This test FAILS on v1.6.0 and must pass once Read serves the in-memory write
// buffer (or produce flushes before acking under acks=all).
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
