// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"sync"
	"sync/atomic"
	"testing"
)

func idempotentBatch(t *testing.T, epoch int16, sequence, count int32) RecordBatch {
	t.Helper()
	data := makeBatchBytes(0, count-1, count, 1)
	data[16] = 2
	binary.BigEndian.PutUint64(data[43:51], 123)
	binary.BigEndian.PutUint16(data[51:53], uint16(epoch))
	binary.BigEndian.PutUint32(data[53:57], uint32(sequence))
	binary.BigEndian.PutUint32(data[17:21], crc32.Checksum(data[21:], crcTable))
	batch, err := NewRecordBatchFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func producerTestLog(s3 S3Client) *PartitionLog {
	return NewPartitionLog("default", "orders", 0, 0, s3, nil, PartitionLogConfig{}, nil, nil, nil)
}

func TestIdempotentAppendConcurrentAndRecovery(t *testing.T) {
	ctx := context.Background()
	s3 := NewMemoryS3Client()
	log := producerTestLog(s3)
	batch := idempotentBatch(t, 0, 0, 2)
	var wg sync.WaitGroup
	var appended atomic.Int32
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := log.AppendIdempotentBatches(ctx, []RecordBatch{batch})
			if err != nil {
				t.Error(err)
				return
			}
			if result.BaseOffset != 0 || result.LastOffset != 1 {
				t.Errorf("retry offsets: %+v", result)
			}
			if !result.Duplicate {
				appended.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := appended.Load(); got != 1 {
		t.Fatalf("reported %d new appends for one batch", got)
	}
	if got := log.BufferedHighWatermark(); got != 2 {
		t.Fatalf("duplicates advanced watermark to %d", got)
	}
	recovered := producerTestLog(s3)
	if _, err := recovered.RestoreFromS3(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := recovered.AppendIdempotentBatches(ctx, []RecordBatch{batch})
	if err != nil || !result.Duplicate || result.BaseOffset != 0 || recovered.BufferedHighWatermark() != 2 {
		t.Fatalf("recovered retry: %+v %v", result, err)
	}
	result, err = recovered.AppendIdempotentBatches(ctx, []RecordBatch{idempotentBatch(t, 0, 2, 1)})
	if err != nil || result.BaseOffset != 2 {
		t.Fatalf("recovered continuation: %+v %v", result, err)
	}
}

func TestRestoreRejectsLegacyTransactionalBatches(t *testing.T) {
	for _, flags := range []uint16{0x10, 0x20, 0x30} {
		ctx := context.Background()
		s3 := NewMemoryS3Client()
		log := producerTestLog(s3)
		batch := idempotentBatch(t, 0, 0, 1)
		binary.BigEndian.PutUint16(batch.Bytes[21:23], flags)
		binary.BigEndian.PutUint32(batch.Bytes[17:21], crc32.Checksum(batch.Bytes[21:], crcTable))
		// Simulate a segment written by an older broker that did not check flags.
		if _, err := log.AppendBatches(ctx, []RecordBatch{batch}, true); err != nil {
			t.Fatal(err)
		}
		if _, err := producerTestLog(s3).RestoreFromS3(ctx); err == nil {
			t.Fatalf("restored unsupported transactional flags %x", flags)
		}
	}
}

func TestIdempotentSequencesAndEpochs(t *testing.T) {
	ctx := context.Background()
	log := producerTestLog(NewMemoryS3Client())
	appendBatch := func(epoch int16, seq int32) error {
		_, err := log.AppendIdempotentBatches(ctx, []RecordBatch{idempotentBatch(t, epoch, seq, 1)})
		return err
	}
	if err := appendBatch(0, 1); !errors.Is(err, ErrOutOfOrderSequence) {
		t.Fatal(err)
	}
	for seq := int32(0); seq < 6; seq++ {
		if err := appendBatch(0, seq); err != nil {
			t.Fatal(err)
		}
	}
	for seq := int32(1); seq < 6; seq++ {
		if err := appendBatch(0, seq); err != nil {
			t.Fatalf("retry %d: %v", seq, err)
		}
	}
	if err := appendBatch(0, 0); !errors.Is(err, ErrOutOfOrderSequence) {
		t.Fatalf("expired retry: %v", err)
	}
	if err := appendBatch(0, 8); !errors.Is(err, ErrOutOfOrderSequence) {
		t.Fatalf("gap: %v", err)
	}
	if err := appendBatch(1, 1); !errors.Is(err, ErrOutOfOrderSequence) {
		t.Fatalf("epoch must restart at zero: %v", err)
	}
	if err := appendBatch(1, 0); err != nil {
		t.Fatal(err)
	}
	if err := appendBatch(0, 5); !errors.Is(err, ErrInvalidProducerEpoch) {
		t.Fatalf("stale epoch: %v", err)
	}
	if got := log.BufferedHighWatermark(); got != 7 {
		t.Fatalf("watermark %d", got)
	}
	// Sequence numbers wrap at MaxInt32, not at the signed integer boundary.
	log.producers[123] = producerState{epoch: 1, batches: []producerBatch{{lastSequence: math.MaxInt32}}}
	if err := appendBatch(1, 0); err != nil {
		t.Fatalf("wrap: %v", err)
	}
}

func TestIdempotentMultiBatchValidationIsAtomic(t *testing.T) {
	ctx := context.Background()
	log := producerTestLog(NewMemoryS3Client())
	first, second := idempotentBatch(t, 0, 0, 2), idempotentBatch(t, 0, 2, 1)
	if _, err := log.AppendIdempotentBatches(ctx, []RecordBatch{first, idempotentBatch(t, 0, 4, 1)}); !errors.Is(err, ErrOutOfOrderSequence) {
		t.Fatal(err)
	}
	if log.BufferedHighWatermark() != 0 {
		t.Fatal("invalid request assigned offsets")
	}
	for i := 0; i < 2; i++ {
		result, err := log.AppendIdempotentBatches(ctx, []RecordBatch{first, second})
		if err != nil || result.BaseOffset != 0 || result.LastOffset != 2 {
			t.Fatalf("append/retry: %+v %v", result, err)
		}
	}
	if _, err := log.AppendIdempotentBatches(ctx, []RecordBatch{second, idempotentBatch(t, 0, 3, 1)}); !errors.Is(err, ErrOutOfOrderSequence) {
		t.Fatalf("mixed retry/new: %v", err)
	}
	if log.BufferedHighWatermark() != 3 {
		t.Fatal("mixed retry assigned offsets")
	}
}

func TestIdempotentAmbiguousUploadRecovery(t *testing.T) {
	for _, failure := range []string{"segment", "segment-after-write", "index", "index-after-write"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			s3 := &atomicAppendS3{MemoryS3Client: NewMemoryS3Client(), fail: failure}
			log := producerTestLog(s3)
			batch := idempotentBatch(t, 0, 0, 1)
			if _, err := log.AppendIdempotentBatches(ctx, []RecordBatch{batch}); !errors.Is(err, ErrLogFenced) {
				t.Fatalf("uncertain append must fence: %v", err)
			}
			s3.fail = ""
			recovered := producerTestLog(s3)
			if _, err := recovered.RestoreFromS3(ctx); err != nil {
				t.Fatal(err)
			}
			result, err := recovered.AppendIdempotentBatches(ctx, []RecordBatch{batch})
			if err != nil || result.BaseOffset != 0 || recovered.BufferedHighWatermark() != 1 {
				t.Fatalf("retry: %+v %v", result, err)
			}
		})
	}
}
