package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

const (
	segmentMagic = "KAFS"
	footerMagic  = "END!"
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
