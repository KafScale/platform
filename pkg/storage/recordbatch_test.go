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

func TestCountRecordBatchMessages(t *testing.T) {
	first := makeRecordBatch(5, 0)
	second := makeRecordBatch(3, 1)
	recordSet := append(first, second...)
	if got := CountRecordBatchMessages(recordSet); got != 8 {
		t.Fatalf("expected 8 messages, got %d", got)
	}
}

func TestCountRecordBatchMessagesIgnoresPartialBatch(t *testing.T) {
	full := makeRecordBatch(4, 2)
	partial := makeRecordBatch(6, 3)
	recordSet := append(full, partial[:40]...)
	if got := CountRecordBatchMessages(recordSet); got != 4 {
		t.Fatalf("expected 4 messages from full batch, got %d", got)
	}
}

func makeRecordBatch(count int32, baseOffset int64) []byte {
	const size = 90
	data := make([]byte, size)
	binary.BigEndian.PutUint64(data[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(data[8:12], uint32(size-12))
	binary.BigEndian.PutUint32(data[23:27], uint32(count-1))
	binary.BigEndian.PutUint32(data[57:61], uint32(count))
	return data
}
