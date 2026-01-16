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
	"encoding/binary"
	"fmt"
)

const recordBatchHeaderMinSize = 61

// NewRecordBatchFromBytes parses Kafka record batch metadata and returns a RecordBatch struct.
func NewRecordBatchFromBytes(data []byte) (RecordBatch, error) {
	if len(data) < recordBatchHeaderMinSize {
		return RecordBatch{}, fmt.Errorf("record batch too small: %d", len(data))
	}
	baseOffset := int64(binary.BigEndian.Uint64(data[0:8]))
	lastOffsetDelta := int32(binary.BigEndian.Uint32(data[23:27]))
	messageCount := int32(binary.BigEndian.Uint32(data[57:61]))
	return RecordBatch{
		BaseOffset:      baseOffset,
		LastOffsetDelta: lastOffsetDelta,
		MessageCount:    messageCount,
		Bytes:           append([]byte(nil), data...),
	}, nil
}

// PatchRecordBatchBaseOffset overwrites the base offset field in the Kafka record batch header.
func PatchRecordBatchBaseOffset(batch *RecordBatch, baseOffset int64) {
	binary.BigEndian.PutUint64(batch.Bytes[0:8], uint64(baseOffset))
	batch.BaseOffset = baseOffset
}

// CountRecordBatchMessages sums the message counts encoded in a record set. The
// record set is expected to be a concatenation of Kafka record batches as
// produced by the broker.
func CountRecordBatchMessages(recordSet []byte) int {
	const frameHeaderLen = 12
	if len(recordSet) < recordBatchHeaderMinSize {
		return 0
	}
	total := 0
	offset := 0
	for offset+frameHeaderLen <= len(recordSet) {
		batchLen := int(binary.BigEndian.Uint32(recordSet[offset+8 : offset+12]))
		if batchLen <= 0 {
			break
		}
		frameLen := frameHeaderLen + batchLen
		if offset+frameLen > len(recordSet) {
			break
		}
		batch := recordSet[offset : offset+frameLen]
		if len(batch) < recordBatchHeaderMinSize {
			break
		}
		total += int(binary.BigEndian.Uint32(batch[57:61]))
		offset += frameLen
	}
	return total
}
