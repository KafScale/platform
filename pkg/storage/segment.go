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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

const (
	segmentMagic     = "KAFS"
	footerMagic      = "END!"
	segmentFooterLen = 16
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

// BuildSegment assembles segment + index bytes from buffered batches.
func BuildSegment(cfg SegmentWriterConfig, batches []RecordBatch, created time.Time) (*SegmentArtifact, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to serialize")
	}
	body := &bytes.Buffer{}
	index := NewIndexBuilder(cfg.IndexIntervalMessages)

	headerLen := 32
	var totalMessages int32
	lastOffset := batches[len(batches)-1].BaseOffset + int64(batches[len(batches)-1].LastOffsetDelta)

	for _, batch := range batches {
		if len(batch.Bytes) == 0 {
			return nil, fmt.Errorf("batch payload empty")
		}
		position := headerLen + body.Len()
		index.MaybeAdd(batch.BaseOffset, int32(position), batch.MessageCount)
		if _, err := body.Write(batch.Bytes); err != nil {
			return nil, err
		}
		totalMessages += batch.MessageCount
	}

	bodyBytes := body.Bytes()
	crc := crc32.Checksum(bodyBytes, crcTable)

	header := buildHeader(batches[0].BaseOffset, totalMessages, created)
	footer := buildFooter(crc, lastOffset)

	segment := bytes.NewBuffer(make([]byte, 0, len(header)+len(bodyBytes)+len(footer)))
	segment.Write(header)
	segment.Write(bodyBytes)
	segment.Write(footer)

	indexBytes, err := index.BuildBytes()
	if err != nil {
		return nil, err
	}

	return &SegmentArtifact{
		BaseOffset:    batches[0].BaseOffset,
		LastOffset:    lastOffset,
		MessageCount:  totalMessages,
		CreatedAt:     created,
		SegmentBytes:  segment.Bytes(),
		IndexBytes:    indexBytes,
		RelativeIndex: index.Entries(),
	}, nil
}

func buildHeader(baseOffset int64, messageCount int32, created time.Time) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	buf.WriteString(segmentMagic)
	binary.Write(buf, binary.BigEndian, uint16(1))  // version
	binary.Write(buf, binary.BigEndian, uint16(0))  // flags
	binary.Write(buf, binary.BigEndian, baseOffset) // base offset
	binary.Write(buf, binary.BigEndian, messageCount)
	binary.Write(buf, binary.BigEndian, created.UnixMilli())
	binary.Write(buf, binary.BigEndian, uint32(0)) // reserved
	return buf.Bytes()
}

func buildFooter(crc uint32, lastOffset int64) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	binary.Write(buf, binary.BigEndian, crc)
	binary.Write(buf, binary.BigEndian, lastOffset)
	buf.WriteString(footerMagic)
	return buf.Bytes()
}

func parseSegmentFooter(data []byte) (int64, error) {
	if len(data) < segmentFooterLen {
		return 0, fmt.Errorf("footer too small")
	}
	reader := bytes.NewReader(data)
	var crc uint32
	if err := binary.Read(reader, binary.BigEndian, &crc); err != nil {
		return 0, err
	}
	var lastOffset int64
	if err := binary.Read(reader, binary.BigEndian, &lastOffset); err != nil {
		return 0, err
	}
	footer := make([]byte, 4)
	if _, err := reader.Read(footer); err != nil {
		return 0, err
	}
	if string(footer) != footerMagic {
		return 0, fmt.Errorf("invalid footer magic")
	}
	_ = crc
	return lastOffset, nil
}
