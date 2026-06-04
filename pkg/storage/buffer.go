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
	"sync"
	"time"
)

// WriteBufferConfig controls flush thresholds.
type WriteBufferConfig struct {
	MaxBytes      int
	MaxMessages   int
	MaxBatches    int
	FlushInterval time.Duration
}

// WriteBuffer accumulates record batches prior to segment serialization.
type WriteBuffer struct {
	cfg          WriteBufferConfig
	mu           sync.Mutex
	batches      []RecordBatch
	sizeBytes    int
	messageCount int
	lastFlush    time.Time
}

// NewWriteBuffer creates an empty buffer.
func NewWriteBuffer(cfg WriteBufferConfig) *WriteBuffer {
	return &WriteBuffer{
		cfg:       cfg,
		lastFlush: time.Now(),
	}
}

// Append adds a batch to the buffer.
func (b *WriteBuffer) Append(batch RecordBatch) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.batches = append(b.batches, batch)
	b.sizeBytes += len(batch.Bytes)
	b.messageCount += int(batch.MessageCount)
}

// ShouldFlush checks if size thresholds or time elapsed require a flush.
func (b *WriteBuffer) ShouldFlush(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sizeBytes == 0 {
		return false
	}
	if b.cfg.MaxBytes > 0 && b.sizeBytes >= b.cfg.MaxBytes {
		return true
	}
	if b.cfg.MaxMessages > 0 && b.messageCount >= b.cfg.MaxMessages {
		return true
	}
	if b.cfg.MaxBatches > 0 && len(b.batches) >= b.cfg.MaxBatches {
		return true
	}
	if b.cfg.FlushInterval > 0 && now.Sub(b.lastFlush) >= b.cfg.FlushInterval {
		return true
	}
	return false
}

// Drain returns all buffered batches and resets counters.
func (b *WriteBuffer) Drain() []RecordBatch {
	b.mu.Lock()
	defer b.mu.Unlock()
	drained := make([]RecordBatch, len(b.batches))
	copy(drained, b.batches)
	b.batches = b.batches[:0]
	b.sizeBytes = 0
	b.messageCount = 0
	b.lastFlush = time.Now()
	return drained
}

// RecordsFrom returns the raw bytes of buffered batches needed to serve a read
// starting at offset, concatenated, non-destructively. A batch is included when
// its last offset (BaseOffset+LastOffsetDelta) is >= offset, i.e. the batch that
// contains the requested offset plus every batch after it. maxBytes caps the
// result but always returns at least the first matching batch (so a read can
// always make progress). Returns nil when no buffered batch reaches offset.
//
// This makes acked-but-not-yet-flushed records readable (Kafka read-after-ack):
// flush is append-triggered, so a partition that goes quiet leaves its tail in
// the buffer; without this the fetch path (segments only) returns
// ErrOffsetOutOfRange for those acked offsets.
func (b *WriteBuffer) RecordsFrom(offset int64, maxBytes int32) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []byte
	for i := range b.batches {
		batch := b.batches[i]
		if batch.BaseOffset+int64(batch.LastOffsetDelta) < offset {
			continue
		}
		if len(out) > 0 && maxBytes > 0 && len(out)+len(batch.Bytes) > int(maxBytes) {
			break
		}
		out = append(out, batch.Bytes...)
	}
	return out
}

// Size returns the accumulated byte count (for tests/metrics).
func (b *WriteBuffer) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sizeBytes
}
