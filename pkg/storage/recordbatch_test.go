package storage

import (
	"encoding/binary"
	"testing"
)

func TestNewRecordBatchFromBytes(t *testing.T) {
	data := make([]byte, 70)
	binary.BigEndian.PutUint64(data[0:8], uint64(5))
	binary.BigEndian.PutUint32(data[23:27], uint32(3))
	binary.BigEndian.PutUint32(data[57:61], uint32(10))

	batch, err := NewRecordBatchFromBytes(data)
	if err != nil {
		t.Fatalf("NewRecordBatchFromBytes: %v", err)
	}
	if batch.BaseOffset != 5 || batch.LastOffsetDelta != 3 || batch.MessageCount != 10 {
		t.Fatalf("unexpected batch metadata: %#v", batch)
	}

	PatchRecordBatchBaseOffset(&batch, 100)
	if batch.BaseOffset != 100 {
		t.Fatalf("base offset not patched: %d", batch.BaseOffset)
	}
	if uint64(100) != binary.BigEndian.Uint64(batch.Bytes[0:8]) {
		t.Fatalf("bytes not patched")
	}
}
