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

func TestWriteBufferSize(t *testing.T) {
	buf := NewWriteBuffer(WriteBufferConfig{})
	if buf.Size() != 0 {
		t.Fatalf("expected initial size 0, got %d", buf.Size())
	}
	buf.Append(RecordBatch{Bytes: make([]byte, 10), MessageCount: 1})
	if buf.Size() != 10 {
		t.Fatalf("expected size 10, got %d", buf.Size())
	}
	buf.Append(RecordBatch{Bytes: make([]byte, 5), MessageCount: 2})
	if buf.Size() != 15 {
		t.Fatalf("expected size 15, got %d", buf.Size())
	}
	buf.Drain()
	if buf.Size() != 0 {
		t.Fatalf("expected size 0 after drain, got %d", buf.Size())
	}
}

func TestWriteBufferFlushByMessages(t *testing.T) {
	buf := NewWriteBuffer(WriteBufferConfig{MaxMessages: 5})
	buf.Append(RecordBatch{Bytes: make([]byte, 1), MessageCount: 3})
	if buf.ShouldFlush(time.Now()) {
		t.Fatal("3 messages should not trigger flush at threshold 5")
	}
	buf.Append(RecordBatch{Bytes: make([]byte, 1), MessageCount: 3})
	if !buf.ShouldFlush(time.Now()) {
		t.Fatal("6 messages should trigger flush at threshold 5")
	}
}

func TestWriteBufferFlushByBatches(t *testing.T) {
	buf := NewWriteBuffer(WriteBufferConfig{MaxBatches: 2})
	buf.Append(RecordBatch{Bytes: make([]byte, 1), MessageCount: 1})
	if buf.ShouldFlush(time.Now()) {
		t.Fatal("1 batch should not trigger flush at threshold 2")
	}
	buf.Append(RecordBatch{Bytes: make([]byte, 1), MessageCount: 1})
	if !buf.ShouldFlush(time.Now()) {
		t.Fatal("2 batches should trigger flush at threshold 2")
	}
}

func TestWriteBufferDrainEmpty(t *testing.T) {
	buf := NewWriteBuffer(WriteBufferConfig{})
	drained := buf.Drain()
	if len(drained) != 0 {
		t.Fatalf("expected 0 drained batches, got %d", len(drained))
	}
}
