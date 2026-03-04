// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package main

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func lfsEncodeRecords(records []kmsg.Record) []byte {
	if len(records) == 0 {
		return nil
	}
	out := make([]byte, 0, 256)
	for _, record := range records {
		out = append(out, lfsEncodeRecord(record)...)
	}
	return out
}

func lfsEncodeRecord(record kmsg.Record) []byte {
	body := make([]byte, 0, 128)
	body = append(body, byte(record.Attributes))
	body = lfsAppendVarlong(body, record.TimestampDelta64)
	body = lfsAppendVarint(body, record.OffsetDelta)
	body = lfsAppendVarintBytes(body, record.Key)
	body = lfsAppendVarintBytes(body, record.Value)
	body = lfsAppendVarint(body, int32(len(record.Headers)))
	for _, header := range record.Headers {
		body = lfsAppendVarintString(body, header.Key)
		body = lfsAppendVarintBytes(body, header.Value)
	}

	cap64 := int64(len(body)) + int64(binary.MaxVarintLen32)
	out := make([]byte, 0, cap64)
	out = lfsAppendVarint(out, int32(len(body)))
	out = append(out, body...)
	return out
}

func lfsAppendVarint(dst []byte, v int32) []byte {
	var tmp [binary.MaxVarintLen32]byte
	n := binary.PutVarint(tmp[:], int64(v))
	return append(dst, tmp[:n]...)
}

func lfsAppendVarlong(dst []byte, v int64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], v)
	return append(dst, tmp[:n]...)
}

func lfsAppendVarintBytes(dst []byte, b []byte) []byte {
	if b == nil {
		dst = lfsAppendVarint(dst, -1)
		return dst
	}
	dst = lfsAppendVarint(dst, int32(len(b)))
	return append(dst, b...)
}

func lfsAppendVarintString(dst []byte, s string) []byte {
	dst = lfsAppendVarint(dst, int32(len(s)))
	return append(dst, s...)
}

func lfsVarint(buf []byte) (int32, int) {
	val, n := binary.Varint(buf)
	if n <= 0 {
		return 0, 0
	}
	return int32(val), n
}

// lfsBuildRecordBatch constructs a full RecordBatch from records.
// Used by the HTTP API produce path.
func lfsBuildRecordBatch(records []kmsg.Record) []byte {
	encoded := lfsEncodeRecords(records)
	batch := kmsg.RecordBatch{
		FirstOffset:          0,
		PartitionLeaderEpoch: -1,
		Magic:                2,
		Attributes:           0,
		LastOffsetDelta:      int32(len(records) - 1),
		FirstTimestamp:       0,
		MaxTimestamp:         0,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		FirstSequence:        0,
		NumRecords:           int32(len(records)),
		Records:              encoded,
	}
	batchBytes := batch.AppendTo(nil)
	batch.Length = int32(len(batchBytes) - 12)
	batchBytes = batch.AppendTo(nil)
	batch.CRC = int32(crc32.Checksum(batchBytes[21:], lfsCRC32cTable))
	return batch.AppendTo(nil)
}
