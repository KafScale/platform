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
