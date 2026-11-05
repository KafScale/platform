// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"github.com/KafScale/platform/pkg/cache"
	"testing"
	"time"
)

func TestPersistedFetchReturnsWholeBatches(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, interval := range []int32{1, 1000} {
			s3 := NewMemoryS3Client()
			c := cache.NewSegmentCache(1 << 20)
			log := NewPartitionLog("test", "orders", 0, 0, s3, c, PartitionLogConfig{Buffer: WriteBufferConfig{MaxBytes: 1 << 20, FlushInterval: time.Hour}, Segment: SegmentWriterConfig{IndexIntervalMessages: interval}, CacheEnabled: cached}, nil, nil, nil)
			first, _ := NewRecordBatchFromBytes(makeRecordBatch(1, 0))
			second, _ := NewRecordBatchFromBytes(makeRecordBatch(1, 1))
			for _, batch := range []RecordBatch{first, second} {
				if _, err := log.AppendBatch(context.Background(), batch); err != nil {
					t.Fatal(err)
				}
			}
			if err := log.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			for _, max := range []int32{1, 0, -1, int32(len(first.Bytes) + 10), int32(len(first.Bytes) * 2)} {
				data, err := log.Read(context.Background(), 0, max)
				if err != nil {
					t.Fatal(err)
				}
				want := first.Bytes
				if max >= int32(len(first.Bytes)*2) {
					want = append(append([]byte{}, first.Bytes...), second.Bytes...)
				}
				if !bytes.Equal(data, want) {
					t.Fatalf("cache=%v index=%d max=%d: %d bytes, want %d", cached, interval, max, len(data), len(want))
				}
			}
			for _, noIndex := range []bool{false, true} {
				if noIndex {
					log.indexEntries = nil
				}
				data, err := log.Read(context.Background(), 1, 1)
				if err != nil || !bytes.Equal(data, second.Bytes) {
					t.Fatalf("read at second batch cache=%v index=%d noIndex=%v: %d bytes: %v", cached, interval, noIndex, len(data), err)
				}
			}
		}
	}
}

func TestFetchLimitsRejectPartialBatches(t *testing.T) {
	batch := makeRecordBatch(1, 0)
	if got := limitRecordBatches(batch[:len(batch)-1], 1<<20); len(got) != 0 {
		t.Fatal("sent a partial first batch")
	}
	data := append(append([]byte{}, batch...), batch[:len(batch)-1]...)
	if got := limitRecordBatches(data, 1<<20); !bytes.Equal(got, batch) {
		t.Fatal("sent a partial later batch")
	}
}
