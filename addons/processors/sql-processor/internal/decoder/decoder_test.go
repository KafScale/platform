// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package decoder

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestParseIndex(t *testing.T) {
	data := make([]byte, 16+12)
	copy(data[0:4], []byte(indexMagic))
	binary.BigEndian.PutUint32(data[6:10], 1)
	binary.BigEndian.PutUint64(data[16:24], 42)
	binary.BigEndian.PutUint32(data[24:28], 99)

	entries, err := parseIndex(data)
	if err != nil {
		t.Fatalf("parse index: %v", err)
	}
	if len(entries) != 1 || entries[0].Offset != 42 || entries[0].Position != 99 {
		t.Fatalf("unexpected entries: %+v", entries)
	}
}

func TestParseIndexInvalid(t *testing.T) {
	if _, err := parseIndex([]byte("bad")); err == nil {
		t.Fatalf("expected error for invalid index")
	}
	data := make([]byte, 16)
	copy(data[0:4], []byte("BAD!"))
	if _, err := parseIndex(data); err == nil {
		t.Fatalf("expected error for invalid magic")
	}
}

func TestDecodeSegmentInvalidMagic(t *testing.T) {
	_, err := decodeSegment([]byte("bad"), "orders", 0)
	if err == nil {
		t.Fatalf("expected error for invalid magic")
	}
}

func TestDecodeRecordBatch(t *testing.T) {
	record := buildRecord(0, 0, []byte("k"), []byte("v"))
	batch := buildBatch(5, 1000, record)
	segment := buildSegment(batch)

	records, err := decodeSegment(segment, "orders", 0)
	if err != nil {
		t.Fatalf("decode segment: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Offset != 5 || string(records[0].Key) != "k" || string(records[0].Value) != "v" {
		t.Fatalf("unexpected record: %+v", records[0])
	}
}

func TestDecodeBatchCompressed(t *testing.T) {
	record := buildRecord(0, 0, []byte("k"), []byte("v"))
	batch := buildBatch(5, 1000, record)
	batch[21] = 0
	batch[22] = 1
	if _, err := decodeBatchRecords(batch, "orders", 0); err == nil {
		t.Fatalf("expected error for compressed batch")
	}
}

func buildSegment(batch []byte) []byte {
	segment := make([]byte, 0, segmentHeaderLen+len(batch)+segmentFooterLen)
	header := make([]byte, segmentHeaderLen)
	copy(header[0:4], []byte(segmentMagic))
	segment = append(segment, header...)
	segment = append(segment, batch...)
	footer := make([]byte, segmentFooterLen)
	copy(footer[12:16], []byte("END!"))
	segment = append(segment, footer...)
	return segment
}

func buildBatch(baseOffset int64, firstTimestamp int64, record []byte) []byte {
	headerRest := make([]byte, recordBatchHeaderLen-12)
	binary.BigEndian.PutUint64(headerRest[27-12:35-12], uint64(firstTimestamp))
	binary.BigEndian.PutUint32(headerRest[57-12:61-12], 1)
	batchLen := len(headerRest) + len(record)
	frame := make([]byte, 12+batchLen)
	binary.BigEndian.PutUint64(frame[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(frame[8:12], uint32(batchLen))
	copy(frame[12:], headerRest)
	copy(frame[12+len(headerRest):], record)
	return frame
}

func buildRecord(tsDelta int32, offsetDelta int32, key []byte, value []byte) []byte {
	payload := makeRecordPayload(tsDelta, offsetDelta, key, value)
	encoded := encodeVarint(int32(len(payload)))
	out := make([]byte, 0, len(encoded)+len(payload))
	out = append(out, encoded...)
	out = append(out, payload...)
	return out
}

func writeVarint(buf *bytes.Buffer, value int32) {
	encoded := encodeVarint(value)
	buf.Write(encoded)
}

func encodeVarint(value int32) []byte {
	zigzag := uint32((value << 1) ^ (value >> 31))
	out := make([]byte, 0, 5)
	for {
		b := byte(zigzag & 0x7f)
		zigzag >>= 7
		if zigzag != 0 {
			b |= 0x80
		}
		out = append(out, b)
		if zigzag == 0 {
			break
		}
	}
	return out
}

func makeRecordPayload(tsDelta int32, offsetDelta int32, key []byte, value []byte) []byte {
	var body bytes.Buffer
	body.WriteByte(0)
	writeVarint(&body, tsDelta)
	writeVarint(&body, offsetDelta)
	writeVarint(&body, int32(len(key)))
	body.Write(key)
	writeVarint(&body, int32(len(value)))
	body.Write(value)
	writeVarint(&body, 0)
	return body.Bytes()
}
