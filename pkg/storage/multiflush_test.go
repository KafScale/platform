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

// TestPartitionLogMultiFlushAllOffsetsReadable appends many batches across many
// flush rotations (MaxBatches=3 -> a flush roughly every 3 appends) and asserts
// that EVERY acknowledged offset is still readable afterwards: the offsets that
// rotated into segments via the flushed-segment path, and the buffered tail via
// the write-buffer fallback. It checks this in pure storage logic over a
// MemoryS3 backend (no network/S3 flakiness).
//
// If segments overwrite each other, are not registered in l.segments, the flush
// path drops drained batches, or the buffered tail is not served, mid-stream
// offsets become ErrOffsetOutOfRange and this test fails.
func TestPartitionLogMultiFlushAllOffsetsReadable(t *testing.T) {
	s3 := NewMemoryS3Client()
	c := cache.NewSegmentCache(1 << 20)
	log := NewPartitionLog("default", "orders", 0, 0, s3, c, PartitionLogConfig{
		Buffer: WriteBufferConfig{
			MaxBatches: 3, // force frequent flush rotations
		},
		Segment:      SegmentWriterConfig{IndexIntervalMessages: 1},
		CacheEnabled: true,
	}, nil, nil, nil)

	const n = 30 // ~10 flush rotations + a buffered tail
	for i := 0; i < n; i++ {
		batch, err := NewRecordBatchFromBytes(make([]byte, 70))
		if err != nil {
			t.Fatalf("NewRecordBatchFromBytes: %v", err)
		}
		res, err := log.AppendBatch(context.Background(), batch)
		if err != nil {
			t.Fatalf("AppendBatch %d: %v", i, err)
		}
		if res.BaseOffset != int64(i) {
			t.Fatalf("append %d: expected base offset %d, got %d", i, i, res.BaseOffset)
		}
	}

	missing := []int64{}
	for off := int64(0); off < n; off++ {
		data, err := log.Read(context.Background(), off, 1<<20)
		if err != nil {
			if errors.Is(err, ErrOffsetOutOfRange) {
				missing = append(missing, off)
				continue
			}
			t.Fatalf("Read offset %d: %v", off, err)
		}
		if len(data) == 0 {
			missing = append(missing, off)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("read-after-ack across flush rotations: %d/%d acknowledged offsets unreadable: %v",
			len(missing), n, missing)
	}
}
