package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	indexMagic = "IDX\x00"
)

// IndexBuilder tracks offsets and file positions for sparse indexing.
type IndexBuilder struct {
	interval  int32
	sinceLast int32
	entries   []*IndexEntry
}

// NewIndexBuilder creates a builder that emits an entry every interval messages.
func NewIndexBuilder(interval int32) *IndexBuilder {
	if interval <= 0 {
		interval = 1
	}
	return &IndexBuilder{interval: interval}
}

// MaybeAdd records an index entry when the interval has elapsed or no entry exists yet.
func (b *IndexBuilder) MaybeAdd(offset int64, position int32, batchMessages int32) {
	if len(b.entries) == 0 || b.sinceLast >= b.interval {
		b.entries = append(b.entries, &IndexEntry{Offset: offset, Position: position})
		b.sinceLast = 0
	}
	b.sinceLast += batchMessages
}

// Entries returns the recorded index entries.
func (b *IndexBuilder) Entries() []*IndexEntry {
	out := make([]*IndexEntry, len(b.entries))
	copy(out, b.entries)
	return out
}

// BuildBytes encodes the index header and entries.
func (b *IndexBuilder) BuildBytes() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	if _, err := buf.WriteString(indexMagic); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(1)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int32(len(b.entries))); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, b.interval); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(0)); err != nil { // reserved
		return nil, err
	}
	for _, entry := range b.entries {
		if err := binary.Write(buf, binary.BigEndian, entry.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, entry.Position); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// ParseIndex validates and returns entries from serialized bytes.
func ParseIndex(data []byte) ([]*IndexEntry, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("index too small")
	}
	if string(data[:4]) != indexMagic {
		return nil, fmt.Errorf("invalid index magic")
	}
	reader := bytes.NewReader(data[4:])
	var version uint16
	if err := binary.Read(reader, binary.BigEndian, &version); err != nil {
		return nil, err
	}
	if version != 1 {
		return nil, fmt.Errorf("unsupported index version %d", version)
	}
	var count int32
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return nil, err
	}
	var interval int32
	if err := binary.Read(reader, binary.BigEndian, &interval); err != nil {
		return nil, err
	}
	var reserved uint16
	if err := binary.Read(reader, binary.BigEndian, &reserved); err != nil {
		return nil, err
	}
	entries := make([]*IndexEntry, count)
	for i := int32(0); i < count; i++ {
		var offset int64
		var position int32
		if err := binary.Read(reader, binary.BigEndian, &offset); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.BigEndian, &position); err != nil {
			return nil, err
		}
		entries[i] = &IndexEntry{Offset: offset, Position: position}
	}
	_ = interval
	return entries, nil
}
