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

package decoder

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestDecodeSegment(t *testing.T) {
	key := []byte("k1")
	value := []byte("v1")
	baseOffset := int64(10)
	baseTimestamp := time.Now().UnixMilli()

	batch := buildRecordBatch(baseOffset, baseTimestamp, key, value)
	segment := buildSegmentBytes(baseOffset, 1, baseTimestamp, batch)

	records, err := decodeSegment(segment, "orders", 0)
	if err != nil {
		t.Fatalf("decodeSegment: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	record := records[0]
	if record.Offset != baseOffset {
		t.Fatalf("unexpected offset: %d", record.Offset)
	}
	if record.Timestamp != baseTimestamp {
		t.Fatalf("unexpected timestamp: %d", record.Timestamp)
	}
	if !bytes.Equal(record.Key, key) || !bytes.Equal(record.Value, value) {
		t.Fatalf("unexpected key/value: %q %q", record.Key, record.Value)
	}
}

func TestParseIndex(t *testing.T) {
	data := buildIndexBytes(1)
	entries, err := parseIndex(data)
	if err != nil {
		t.Fatalf("parseIndex: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
}

func buildIndexBytes(count int32) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteString(indexMagic)
	binary.Write(buf, binary.BigEndian, uint16(1))
	binary.Write(buf, binary.BigEndian, count)
	binary.Write(buf, binary.BigEndian, int32(1))
	binary.Write(buf, binary.BigEndian, uint16(0))
	for i := int32(0); i < count; i++ {
		binary.Write(buf, binary.BigEndian, int64(i))
		binary.Write(buf, binary.BigEndian, int32(i*10))
	}
	return buf.Bytes()
}

func buildSegmentBytes(baseOffset int64, messageCount int32, created int64, body []byte) []byte {
	header := bytes.NewBuffer(make([]byte, 0, segmentHeaderLen))
	header.WriteString(segmentMagic)
	binary.Write(header, binary.BigEndian, uint16(1))
	binary.Write(header, binary.BigEndian, uint16(0))
	binary.Write(header, binary.BigEndian, baseOffset)
	binary.Write(header, binary.BigEndian, messageCount)
	binary.Write(header, binary.BigEndian, created)
	binary.Write(header, binary.BigEndian, uint32(0))

	footer := bytes.NewBuffer(make([]byte, 0, segmentFooterLen))
	binary.Write(footer, binary.BigEndian, uint32(0))
	binary.Write(footer, binary.BigEndian, baseOffset)
	footer.WriteString("END!")

	segment := bytes.NewBuffer(make([]byte, 0, segmentHeaderLen+len(body)+segmentFooterLen))
	segment.Write(header.Bytes())
	segment.Write(body)
	segment.Write(footer.Bytes())
	return segment.Bytes()
}

func buildRecordBatch(baseOffset int64, baseTimestamp int64, key, value []byte) []byte {
	record := buildRecord(key, value)
	totalSize := recordBatchHeaderLen + len(record)
	batch := make([]byte, totalSize)

	binary.BigEndian.PutUint64(batch[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(batch[8:12], uint32(totalSize-12))
	binary.BigEndian.PutUint32(batch[12:16], uint32(0))
	batch[16] = 2
	binary.BigEndian.PutUint32(batch[17:21], uint32(0))
	binary.BigEndian.PutUint16(batch[21:23], uint16(0))
	binary.BigEndian.PutUint32(batch[23:27], uint32(0))
	binary.BigEndian.PutUint64(batch[27:35], uint64(baseTimestamp))
	binary.BigEndian.PutUint64(batch[35:43], uint64(baseTimestamp))
	binary.BigEndian.PutUint64(batch[43:51], uint64(^uint64(0)))
	binary.BigEndian.PutUint16(batch[51:53], uint16(^uint16(0)))
	binary.BigEndian.PutUint32(batch[53:57], uint32(^uint32(0)))
	binary.BigEndian.PutUint32(batch[57:61], uint32(1))

	copy(batch[recordBatchHeaderLen:], record)
	return batch
}

func buildRecord(key, value []byte) []byte {
	payload := bytes.NewBuffer(nil)
	payload.WriteByte(0)
	payload.Write(encodeVarint(0))
	payload.Write(encodeVarint(0))
	payload.Write(encodeVarint(int64(len(key))))
	payload.Write(key)
	payload.Write(encodeVarint(int64(len(value))))
	payload.Write(value)
	payload.Write(encodeVarint(0))

	record := bytes.NewBuffer(nil)
	record.Write(encodeVarint(int64(payload.Len())))
	record.Write(payload.Bytes())
	return record.Bytes()
}

func encodeVarint(value int64) []byte {
	zigzag := uint64(value<<1) ^ uint64(value>>63)
	var out []byte
	for {
		b := byte(zigzag & 0x7f)
		zigzag >>= 7
		if zigzag == 0 {
			out = append(out, b)
			break
		}
		out = append(out, b|0x80)
	}
	return out
}
