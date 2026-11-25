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
	"math"
)

var (
	ErrOutOfOrderSequence   = errors.New("out of order producer sequence")
	ErrInvalidProducerEpoch = errors.New("invalid producer epoch")
	ErrInvalidProducerBatch = errors.New("invalid idempotent record batch")
)

const producerBatchWindow = 5

type producerBatch struct {
	firstSequence int32
	lastSequence  int32
	baseOffset    int64
	lastOffset    int64
}

type producerState struct {
	epoch   int16
	batches []producerBatch
}

type producerStates map[int64]producerState

func producerFields(batch RecordBatch) (id int64, epoch int16, sequence int32) {
	if len(batch.Bytes) < recordBatchHeaderMinSize || batch.Bytes[16] != 2 {
		return -1, -1, -1
	}
	return int64(binary.BigEndian.Uint64(batch.Bytes[43:51])),
		int16(binary.BigEndian.Uint16(batch.Bytes[51:53])),
		int32(binary.BigEndian.Uint32(batch.Bytes[53:57]))
}

func sequenceAfter(sequence, delta int32) int32 {
	return int32((int64(sequence) + int64(delta)) & math.MaxInt32)
}

func (s producerStates) add(id int64, epoch int16, batch producerBatch) {
	state, ok := s[id]
	if !ok || state.epoch != epoch {
		state = producerState{epoch: epoch}
	}
	state.batches = append(state.batches, batch)
	if len(state.batches) > producerBatchWindow {
		state.batches = state.batches[len(state.batches)-producerBatchWindow:]
	}
	s[id] = state
}

func (s producerStates) recoverBatch(batch RecordBatch) {
	id, epoch, first := producerFields(batch)
	if id < 0 || epoch < 0 || first < 0 {
		return
	}
	s.add(id, epoch, producerBatch{
		firstSequence: first, lastSequence: sequenceAfter(first, batch.LastOffsetDelta),
		baseOffset: batch.BaseOffset, lastOffset: batch.BaseOffset + int64(batch.LastOffsetDelta),
	})
}

// IdempotentAppendResult distinguishes newly persisted records from a retry
// acknowledged at its original offsets. The published AppendResult stays intact.
type IdempotentAppendResult struct {
	AppendResult
	Duplicate bool
}

// AppendIdempotentBatches validates sequences and deduplicates under the same
// append lock that assigns offsets. Every accepted new set is flushed, even if
// ordinary writes are configured to buffer, so no acknowledged sequence exists
// only in process memory. Recovery derives this state from the same S3 bytes.
func (l *PartitionLog) AppendIdempotentBatches(ctx context.Context, batches []RecordBatch) (*IdempotentAppendResult, error) {
	if len(batches) == 0 {
		return nil, ErrInvalidProducerBatch
	}
	result, duplicate, err := l.appendBatches(ctx, batches, true, true)
	if err != nil {
		return nil, err
	}
	return &IdempotentAppendResult{AppendResult: *result, Duplicate: duplicate}, nil
}

// stageProducerState never mutates live state. A partition record set must be
// entirely new or an entirely duplicate contiguous range; acknowledging a mixed
// set could make clients derive incorrect per-record offsets from BaseOffset.
func (l *PartitionLog) stageProducerState(batches []RecordBatch, base int64) (producerStates, *AppendResult, error) {
	updates := make(producerStates)
	var duplicate *AppendResult
	newCount, duplicateCount := 0, 0
	for _, batch := range batches {
		id, epoch, first := producerFields(batch)
		if id < 0 || epoch < 0 || first < 0 || batch.MessageCount <= 0 || batch.LastOffsetDelta != batch.MessageCount-1 {
			return nil, nil, ErrInvalidProducerBatch
		}
		state, ok := updates[id]
		if !ok {
			state, ok = l.producers[id]
			state.batches = append([]producerBatch(nil), state.batches...)
		}
		if ok && epoch < state.epoch {
			return nil, nil, ErrInvalidProducerEpoch
		}
		last := sequenceAfter(first, batch.LastOffsetDelta)
		isDuplicate := false
		if ok && epoch == state.epoch {
			for _, old := range state.batches {
				if old.firstSequence == first && old.lastSequence == last {
					if duplicate != nil && duplicate.LastOffset+1 != old.baseOffset {
						return nil, nil, ErrOutOfOrderSequence
					}
					if duplicate == nil {
						duplicate = &AppendResult{BaseOffset: old.baseOffset}
					}
					duplicate.LastOffset = old.lastOffset
					duplicateCount++
					isDuplicate = true
					break
				}
			}
		}
		if isDuplicate {
			continue
		}
		if !ok || epoch > state.epoch {
			if first != 0 {
				return nil, nil, ErrOutOfOrderSequence
			}
			state = producerState{epoch: epoch}
		} else if len(state.batches) == 0 || first != sequenceAfter(state.batches[len(state.batches)-1].lastSequence, 1) {
			return nil, nil, ErrOutOfOrderSequence
		}
		updates[id] = state
		updates.add(id, epoch, producerBatch{first, last, base, base + int64(batch.LastOffsetDelta)})
		base += int64(batch.LastOffsetDelta) + 1
		newCount++
	}
	if newCount > 0 && duplicateCount > 0 {
		return nil, nil, ErrOutOfOrderSequence
	}
	return updates, duplicate, nil
}
