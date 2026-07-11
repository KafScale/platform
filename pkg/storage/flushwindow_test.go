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

// TestPartitionLogFlushWindowOffsetReadable documents and guards the flush
// window: prepareFlush drains the buffer and builds a segment artifact, but the
// segment is not registered in l.segments until uploadFlush commits it after the
// S3 upload completes. Between those two steps an acknowledged offset is in
// neither the buffer (drained) nor a committed segment.
//
// This test drives that exact window by calling prepareFlush directly and then
// reading a drained offset before uploadFlush commits. It asserts the drained
// batches stay readable during the window so an in-flight flush never makes an
// acknowledged record briefly unreadable.
func TestPartitionLogFlushWindowOffsetReadable(t *testing.T) {
	s3 := NewMemoryS3Client()
	c := cache.NewSegmentCache(1 << 20)
	// Thresholds that never auto-trip; we drive prepareFlush by hand to land
	// precisely in the window.
	log := NewPartitionLog("default", "orders", 0, 0, s3, c, PartitionLogConfig{
		Buffer:  WriteBufferConfig{MaxBytes: 1 << 30},
		Segment: SegmentWriterConfig{IndexIntervalMessages: 1},
	}, nil, nil, nil)

	const n = 4
	for i := 0; i < n; i++ {
		batch, err := NewRecordBatchFromBytes(make([]byte, 70))
		if err != nil {
			t.Fatalf("NewRecordBatchFromBytes: %v", err)
		}
		if _, err := log.AppendBatch(context.Background(), batch); err != nil {
			t.Fatalf("AppendBatch %d: %v", i, err)
		}
	}

	// Enter the flush window: drain the buffer into an artifact, mark flushing,
	// but do NOT yet commit the segment to l.segments (that is uploadFlush's job).
	log.mu.Lock()
	artifact, err := log.prepareFlush()
	log.mu.Unlock()
	if err != nil {
		t.Fatalf("prepareFlush: %v", err)
	}
	if artifact == nil {
		t.Fatalf("prepareFlush returned no artifact; nothing was buffered")
	}

	// During the window every acknowledged offset must still be readable.
	for off := int64(0); off < n; off++ {
		data, err := log.Read(context.Background(), off, 1<<20)
		if err != nil {
			if errors.Is(err, ErrOffsetOutOfRange) {
				t.Fatalf("flush window: acknowledged offset %d is unreadable while a flush is in flight "+
					"(drained from buffer, segment not yet committed)", off)
			}
			t.Fatalf("Read offset %d during flush window: %v", off, err)
		}
		if len(data) == 0 {
			t.Fatalf("flush window: Read offset %d returned no data while a flush is in flight", off)
		}
	}

	// Complete the flush; the offsets must remain readable afterwards.
	if err := log.uploadFlush(context.Background(), artifact); err != nil {
		t.Fatalf("uploadFlush: %v", err)
	}
	for off := int64(0); off < n; off++ {
		data, err := log.Read(context.Background(), off, 1<<20)
		if err != nil {
			t.Fatalf("Read offset %d after flush committed: %v", off, err)
		}
		if len(data) == 0 {
			t.Fatalf("Read offset %d after flush committed returned no data", off)
		}
	}
}
