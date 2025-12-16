package storage

import "time"

// RecordBatch carries a Kafka record batch blob plus metadata required for indexing.
type RecordBatch struct {
	BaseOffset      int64
	LastOffsetDelta int32
	MessageCount    int32
	Bytes           []byte
}

// SegmentWriterConfig controls serialization.
type SegmentWriterConfig struct {
	IndexIntervalMessages int32
}

// SegmentArtifact contains serialized segment + index bytes ready for upload.
type SegmentArtifact struct {
	BaseOffset    int64
	LastOffset    int64
	MessageCount  int32
	CreatedAt     time.Time
	SegmentBytes  []byte
	IndexBytes    []byte
	RelativeIndex []*IndexEntry
}

// IndexEntry mirrors a sparse index row.
type IndexEntry struct {
	Offset   int64
	Position int32
}
