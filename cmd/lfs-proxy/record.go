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
	"errors"
	"hash/crc32"

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type byteWriter struct {
	buf []byte
}

func newByteWriter(capacity int) *byteWriter {
	return &byteWriter{buf: make([]byte, 0, capacity)}
}

func (w *byteWriter) write(b []byte) {
	w.buf = append(w.buf, b...)
}

func (w *byteWriter) Int16(v int16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(v))
	w.write(tmp[:])
}

func (w *byteWriter) Int32(v int32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(v))
	w.write(tmp[:])
}

func (w *byteWriter) Int64(v int64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	w.write(tmp[:])
}

func (w *byteWriter) String(v string) {
	w.Int16(int16(len(v)))
	if len(v) > 0 {
		w.write([]byte(v))
	}
}

func (w *byteWriter) NullableString(v *string) {
	if v == nil {
		w.Int16(-1)
		return
	}
	w.String(*v)
}

func (w *byteWriter) CompactString(v string) {
	w.compactLength(len(v))
	if len(v) > 0 {
		w.write([]byte(v))
	}
}

func (w *byteWriter) CompactNullableString(v *string) {
	if v == nil {
		w.compactLength(-1)
		return
	}
	w.CompactString(*v)
}

func (w *byteWriter) BytesWithLength(b []byte) {
	w.Int32(int32(len(b)))
	w.write(b)
}

func (w *byteWriter) CompactBytes(b []byte) {
	if b == nil {
		w.compactLength(-1)
		return
	}
	w.compactLength(len(b))
	w.write(b)
}

func (w *byteWriter) UVarint(v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	w.write(tmp[:n])
}

func (w *byteWriter) CompactArrayLen(length int) {
	if length < 0 {
		w.UVarint(0)
		return
	}
	w.UVarint(uint64(length) + 1)
}

func (w *byteWriter) WriteTaggedFields(count int) {
	if count == 0 {
		w.UVarint(0)
		return
	}
	w.UVarint(uint64(count))
}

func (w *byteWriter) compactLength(length int) {
	if length < 0 {
		w.UVarint(0)
		return
	}
	w.UVarint(uint64(length) + 1)
}

func (w *byteWriter) Bytes() []byte {
	return w.buf
}

func encodeProduceRequest(header *protocol.RequestHeader, req *protocol.ProduceRequest) ([]byte, error) {
	if header == nil || req == nil {
		return nil, errors.New("nil header or request")
	}
	flexible := isFlexibleRequest(header.APIKey, header.APIVersion)
	w := newByteWriter(0)
	w.Int16(header.APIKey)
	w.Int16(header.APIVersion)
	w.Int32(header.CorrelationID)
	w.NullableString(header.ClientID)
	if flexible {
		w.WriteTaggedFields(0)
	}

	if header.APIVersion >= 3 {
		if flexible {
			w.CompactNullableString(req.TransactionalID)
		} else {
			w.NullableString(req.TransactionalID)
		}
	}
	w.Int16(req.Acks)
	w.Int32(req.TimeoutMs)
	if flexible {
		w.CompactArrayLen(len(req.Topics))
	} else {
		w.Int32(int32(len(req.Topics)))
	}
	for _, topic := range req.Topics {
		if flexible {
			w.CompactString(topic.Name)
			w.CompactArrayLen(len(topic.Partitions))
		} else {
			w.String(topic.Name)
			w.Int32(int32(len(topic.Partitions)))
		}
		for _, partition := range topic.Partitions {
			w.Int32(partition.Partition)
			if flexible {
				w.CompactBytes(partition.Records)
				w.WriteTaggedFields(0)
			} else {
				w.BytesWithLength(partition.Records)
			}
		}
		if flexible {
			w.WriteTaggedFields(0)
		}
	}
	if flexible {
		w.WriteTaggedFields(0)
	}

	return w.Bytes(), nil
}

func isFlexibleRequest(apiKey, version int16) bool {
	switch apiKey {
	case protocol.APIKeyApiVersion:
		return version >= 3
	case protocol.APIKeyProduce:
		return version >= 9
	case protocol.APIKeyMetadata:
		return version >= 9
	case protocol.APIKeyFetch:
		return version >= 12
	case protocol.APIKeyFindCoordinator:
		return version >= 3
	case protocol.APIKeySyncGroup:
		return version >= 4
	case protocol.APIKeyHeartbeat:
		return version >= 4
	case protocol.APIKeyListGroups:
		return version >= 3
	case protocol.APIKeyDescribeGroups:
		return version >= 5
	case protocol.APIKeyOffsetForLeaderEpoch:
		return version >= 4
	case protocol.APIKeyDescribeConfigs:
		return version >= 4
	case protocol.APIKeyAlterConfigs:
		return version >= 2
	case protocol.APIKeyCreatePartitions:
		return version >= 2
	case protocol.APIKeyDeleteGroups:
		return version >= 2
	default:
		return false
	}
}

func encodeRecords(records []kmsg.Record) []byte {
	if len(records) == 0 {
		return nil
	}
	out := make([]byte, 0, 256)
	for _, record := range records {
		out = append(out, encodeRecord(record)...)
	}
	return out
}

func encodeRecord(record kmsg.Record) []byte {
	body := make([]byte, 0, 128)
	body = append(body, byte(record.Attributes))
	body = appendVarlong(body, record.TimestampDelta64)
	body = appendVarint(body, record.OffsetDelta)
	body = appendVarintBytes(body, record.Key)
	body = appendVarintBytes(body, record.Value)
	body = appendVarint(body, int32(len(record.Headers)))
	for _, header := range record.Headers {
		body = appendVarintString(body, header.Key)
		body = appendVarintBytes(body, header.Value)
	}

	out := make([]byte, 0, len(body)+binary.MaxVarintLen32)
	out = appendVarint(out, int32(len(body)))
	out = append(out, body...)
	return out
}

func appendVarint(dst []byte, v int32) []byte {
	var tmp [binary.MaxVarintLen32]byte
	n := binary.PutVarint(tmp[:], int64(v))
	return append(dst, tmp[:n]...)
}

func appendVarlong(dst []byte, v int64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], v)
	return append(dst, tmp[:n]...)
}

func appendVarintBytes(dst []byte, b []byte) []byte {
	if b == nil {
		dst = appendVarint(dst, -1)
		return dst
	}
	dst = appendVarint(dst, int32(len(b)))
	return append(dst, b...)
}

func appendVarintString(dst []byte, s string) []byte {
	dst = appendVarint(dst, int32(len(s)))
	return append(dst, s...)
}

func varint(buf []byte) (int32, int) {
	val, n := binary.Varint(buf)
	if n <= 0 {
		return 0, 0
	}
	return int32(val), n
}

func buildRecordBatch(records []kmsg.Record) []byte {
	encoded := encodeRecords(records)
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
	batch.CRC = int32(crc32.Checksum(batchBytes[21:], crc32cTable))
	return batch.AppendTo(nil)
}
