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
	"testing"
	"time"
)

func TestWriteBufferThresholds(t *testing.T) {
	cfg := WriteBufferConfig{
		MaxBytes:      10,
		MaxMessages:   5,
		MaxBatches:    3,
		FlushInterval: 50 * time.Millisecond,
	}
	buf := NewWriteBuffer(cfg)

	if buf.ShouldFlush(time.Now()) {
		t.Fatalf("empty buffer should not flush")
	}

	buf.Append(RecordBatch{Bytes: make([]byte, 8), MessageCount: 4})
	if buf.ShouldFlush(time.Now()) {
		t.Fatalf("below thresholds")
	}

	buf.Append(RecordBatch{Bytes: make([]byte, 4), MessageCount: 2})
	if !buf.ShouldFlush(time.Now()) {
		t.Fatalf("expected flush by bytes")
	}

	drained := buf.Drain()
	if len(drained) != 2 {
		t.Fatalf("expected 2 drained batches")
	}

	buf.Append(RecordBatch{Bytes: make([]byte, 1), MessageCount: 1})
	time.Sleep(cfg.FlushInterval)
	if !buf.ShouldFlush(time.Now()) {
		t.Fatalf("expected flush by time")
	}
}
