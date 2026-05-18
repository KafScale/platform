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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

const (
	recordBatchHeaderLen = 61
	batchFrameHeaderLen  = 12
)

type segmentRestorePlan struct {
	segmentBytes []byte
	indexBytes   []byte
	baseOffset   int64
	lastOffset   int64
	keep         bool
}

func buildRestorePlan(segmentBytes, indexBytes []byte, restoreTo time.Time, createdAt time.Time) (*segmentRestorePlan, error) {
	indexInterval, _, err := parseIndexMetadata(indexBytes)
	if err != nil {
		return nil, fmt.Errorf("parse index: %w", err)
	}
	batches, err := collectRecoverableBatches(segmentBytes, restoreTo.UnixMilli())
	if err != nil {
		return nil, err
	}
	if len(batches) == 0 {
		return &segmentRestorePlan{keep: false}, nil
	}
	artifact, err := BuildSegment(SegmentWriterConfig{IndexIntervalMessages: indexInterval}, batches, createdAt)
	if err != nil {
		return nil, err
	}
	return &segmentRestorePlan{
		segmentBytes: artifact.SegmentBytes,
		indexBytes:   artifact.IndexBytes,
		baseOffset:   artifact.BaseOffset,
		lastOffset:   artifact.LastOffset,
		keep:         true,
	}, nil
}

func collectRecoverableBatches(segmentBytes []byte, cutoffMs int64) ([]RecordBatch, error) {
	if len(segmentBytes) < segmentHeaderLen+segmentFooterLen {
		return nil, fmt.Errorf("segment too small")
	}
	if string(segmentBytes[:4]) != segmentMagic {
		return nil, fmt.Errorf("invalid segment magic")
	}
	body := segmentBytes[segmentHeaderLen : len(segmentBytes)-segmentFooterLen]
	batches := make([]RecordBatch, 0)
	for offset := 0; offset+batchFrameHeaderLen <= len(body); {
		batchLen := int(binary.BigEndian.Uint32(body[offset+8 : offset+12]))
		if batchLen <= 0 {
			break
		}
		frameLen := batchFrameHeaderLen + batchLen
		if offset+frameLen > len(body) {
			return nil, fmt.Errorf("record batch exceeds segment bounds")
		}
		batch := append([]byte(nil), body[offset:offset+frameLen]...)
		truncated, keep, done, err := truncateRecordBatchToTimestamp(batch, cutoffMs)
		if err != nil {
			return nil, err
		}
		if keep {
			batches = append(batches, truncated)
		}
		offset += frameLen
		if done {
			break
		}
	}
	return batches, nil
}

func truncateRecordBatchToTimestamp(batch []byte, cutoffMs int64) (RecordBatch, bool, bool, error) {
	if len(batch) < recordBatchHeaderLen {
		return RecordBatch{}, false, false, fmt.Errorf("record batch too small: %d", len(batch))
	}
	firstTimestamp := int64(binary.BigEndian.Uint64(batch[27:35]))
	maxTimestamp := int64(binary.BigEndian.Uint64(batch[35:43]))
	if maxTimestamp <= cutoffMs {
		parsed, err := NewRecordBatchFromBytes(batch)
		return parsed, true, false, err
	}
	if firstTimestamp > cutoffMs {
		return RecordBatch{}, false, true, nil
	}
	attributes := int16(binary.BigEndian.Uint16(batch[21:23]))
	if compressionType(attributes) != 0 {
		return RecordBatch{}, false, false, fmt.Errorf("exact PITR does not support compressed record batches")
	}

	recordCount := int32(binary.BigEndian.Uint32(batch[57:61]))
	reader := bytes.NewReader(batch[recordBatchHeaderLen:])
	keptCount := int32(0)
	keptBytes := 0
	lastOffsetDelta := int32(0)
	maxIncludedTimestamp := firstTimestamp
	recordDataLen := len(batch[recordBatchHeaderLen:])
	for i := int32(0); i < recordCount; i++ {
		timestampDelta, offsetDelta, err := scanRecord(reader)
		if err != nil {
			return RecordBatch{}, false, false, err
		}
		recordTimestamp := firstTimestamp + timestampDelta
		if recordTimestamp > cutoffMs {
			break
		}
		keptCount++
		keptBytes = recordDataLen - reader.Len()
		lastOffsetDelta = offsetDelta
		if recordTimestamp > maxIncludedTimestamp {
			maxIncludedTimestamp = recordTimestamp
		}
	}
	if keptCount == 0 {
		return RecordBatch{}, false, true, nil
	}
	if keptCount == recordCount {
		parsed, err := NewRecordBatchFromBytes(batch)
		return parsed, true, true, err
	}

	truncated := append([]byte(nil), batch[:recordBatchHeaderLen+keptBytes]...)
	binary.BigEndian.PutUint32(truncated[8:12], uint32(len(truncated)-batchFrameHeaderLen))
	binary.BigEndian.PutUint32(truncated[23:27], uint32(lastOffsetDelta))
	binary.BigEndian.PutUint64(truncated[35:43], uint64(maxIncludedTimestamp))
	binary.BigEndian.PutUint32(truncated[57:61], uint32(keptCount))
	binary.BigEndian.PutUint32(truncated[17:21], crc32.Checksum(truncated[21:], crcTable))

	parsed, err := NewRecordBatchFromBytes(truncated)
	return parsed, true, true, err
}

func scanRecord(reader *bytes.Reader) (timestampDelta int64, offsetDelta int32, err error) {
	recordLen, err := readVarint(reader)
	if err != nil {
		return 0, 0, err
	}
	if recordLen < 0 {
		return 0, 0, fmt.Errorf("invalid record length")
	}
	recordData := make([]byte, recordLen)
	if _, err := io.ReadFull(reader, recordData); err != nil {
		return 0, 0, err
	}
	buf := bytes.NewReader(recordData)
	if _, err := buf.ReadByte(); err != nil {
		return 0, 0, err
	}
	timestampDelta, err = readVarint(buf)
	if err != nil {
		return 0, 0, err
	}
	offsetDelta64, err := readVarint(buf)
	if err != nil {
		return 0, 0, err
	}
	return timestampDelta, int32(offsetDelta64), nil
}

func readVarint(reader *bytes.Reader) (int64, error) {
	var value uint64
	var shift uint
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		value |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if shift > 63 {
			return 0, fmt.Errorf("varint too long")
		}
	}
	return int64((value >> 1) ^ uint64((int64(value&1)<<63)>>63)), nil
}

func compressionType(attributes int16) int16 {
	return attributes & 0x07
}
