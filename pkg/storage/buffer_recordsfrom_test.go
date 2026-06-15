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

import "testing"

// bufferWithBatches appends n single-record batches of the given size, assigning
// sequential base offsets, and returns the buffer plus the per-batch byte size.
func bufferWithBatches(t *testing.T, n int, payload int) (*WriteBuffer, int) {
	t.Helper()
	b := NewWriteBuffer(WriteBufferConfig{MaxBytes: 1 << 30})
	var batchLen int
	for i := 0; i < n; i++ {
		batch, err := NewRecordBatchFromBytes(make([]byte, payload))
		if err != nil {
			t.Fatalf("NewRecordBatchFromBytes: %v", err)
		}
		PatchRecordBatchBaseOffset(&batch, int64(i))
		batchLen = len(batch.Bytes)
		b.Append(batch)
	}
	return b, batchLen
}

// TestWriteBufferRecordsFromMaxBytesGuard pins the maxBytes contract, including
// the non-positive case, so a malformed or zero PartitionMaxBytes can never
// produce an unbounded response and a read always makes progress.
func TestWriteBufferRecordsFromMaxBytesGuard(t *testing.T) {
	const n = 5
	b, batchLen := bufferWithBatches(t, n, 70)
	allLen := n * batchLen

	t.Run("positive cap stops after the budget but returns at least one batch", func(t *testing.T) {
		// A cap smaller than a single batch still returns exactly one batch.
		got := b.RecordsFrom(0, 1)
		if len(got) != batchLen {
			t.Fatalf("maxBytes=1: expected first batch in full (%d bytes), got %d", batchLen, len(got))
		}
		// A cap that fits two batches returns two, not more.
		got = b.RecordsFrom(0, int32(2*batchLen))
		if len(got) != 2*batchLen {
			t.Fatalf("maxBytes=2*batch: expected %d bytes, got %d", 2*batchLen, len(got))
		}
	})

	t.Run("maxBytes zero returns the first matching batch only", func(t *testing.T) {
		got := b.RecordsFrom(0, 0)
		if len(got) != batchLen {
			t.Fatalf("maxBytes=0: expected first matching batch only (%d bytes), got %d", batchLen, len(got))
		}
	})

	t.Run("maxBytes negative returns the first matching batch only", func(t *testing.T) {
		got := b.RecordsFrom(0, -1)
		if len(got) != batchLen {
			t.Fatalf("maxBytes<0: expected first matching batch only (%d bytes), got %d", batchLen, len(got))
		}
	})

	t.Run("large positive cap returns the whole matching tail", func(t *testing.T) {
		got := b.RecordsFrom(0, 1<<20)
		if len(got) != allLen {
			t.Fatalf("large cap: expected whole tail (%d bytes), got %d", allLen, len(got))
		}
	})

	t.Run("offset past the tail returns nil", func(t *testing.T) {
		if got := b.RecordsFrom(int64(n), 1<<20); got != nil {
			t.Fatalf("offset past tail: expected nil, got %d bytes", len(got))
		}
	})

	t.Run("offset mid-tail starts at the matching batch", func(t *testing.T) {
		got := b.RecordsFrom(2, 1<<20)
		if len(got) != (n-2)*batchLen {
			t.Fatalf("mid-tail: expected %d bytes from offset 2, got %d", (n-2)*batchLen, len(got))
		}
	})
}

// TestWriteBufferDrainEmptiesBuffer backs the broker-side no-op-in-default claim
// at the storage layer: after a flush drains the buffer, RecordsFrom serves
// nothing, so the buffer fallback can never be the source of a read once the
// data is in a segment. In the default flushOnAck=true path every ack drains the
// buffer immediately, so this is the post-flush state at read time.
func TestWriteBufferDrainEmptiesBuffer(t *testing.T) {
	b, _ := bufferWithBatches(t, 3, 70)
	if got := b.RecordsFrom(0, 1<<20); len(got) == 0 {
		t.Fatalf("precondition: expected buffered bytes before drain")
	}
	drained := b.Drain()
	if len(drained) != 3 {
		t.Fatalf("expected 3 drained batches, got %d", len(drained))
	}
	for off := int64(0); off < 3; off++ {
		if got := b.RecordsFrom(off, 1<<20); got != nil {
			t.Fatalf("post-drain: buffer fallback served offset %d (%d bytes); buffer must be empty after a flush", off, len(got))
		}
	}
	if b.Size() != 0 {
		t.Fatalf("post-drain: expected buffer size 0, got %d", b.Size())
	}
}
