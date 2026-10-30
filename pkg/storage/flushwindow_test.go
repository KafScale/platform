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

// An explicit Flush must keep acknowledged buffered offsets readable for the
// entire upload, before publishing the staged segment.
func TestPartitionLogFlushWindowOffsetReadable(t *testing.T) {
	s3 := &slowUploadS3{MemoryS3Client: NewMemoryS3Client(), started: make(chan struct{}), release: make(chan struct{})}
	c := cache.NewSegmentCache(1 << 20)
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

	done := make(chan error, 1)
	go func() { done <- log.Flush(context.Background()) }()
	<-s3.started
	defer func() {
		select {
		case <-s3.release:
		default:
			close(s3.release)
		}
	}()

	// During the window every acknowledged offset must still be readable.
	for off := int64(0); off < n; off++ {
		data, err := log.Read(context.Background(), off, 1<<20)
		if err != nil {
			if errors.Is(err, ErrOffsetOutOfRange) {
				t.Fatalf("flush window: acknowledged offset %d is unreadable while a flush is in flight "+
					"(segment not yet committed)", off)
			}
			t.Fatalf("Read offset %d during flush window: %v", off, err)
		}
		if len(data) == 0 {
			t.Fatalf("flush window: Read offset %d returned no data while a flush is in flight", off)
		}
	}

	// Complete the flush; the offsets must remain readable afterwards.
	close(s3.release)
	if err := <-done; err != nil {
		t.Fatalf("Flush: %v", err)
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
