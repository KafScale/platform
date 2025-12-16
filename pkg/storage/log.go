package storage

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/alo/kafscale/pkg/cache"
)

// PartitionLogConfig configures per-partition log behavior.
type PartitionLogConfig struct {
	Buffer  WriteBufferConfig
	Segment SegmentWriterConfig
}

// PartitionLog coordinates buffering, segment serialization, S3 uploads, and caching.
type PartitionLog struct {
	topic     string
	partition int32
	s3        S3Client
	cache     *cache.SegmentCache
	cfg       PartitionLogConfig

	buffer     *WriteBuffer
	nextOffset int64
	mu         sync.Mutex
}

// NewPartitionLog constructs a log for a topic partition.
func NewPartitionLog(topic string, partition int32, s3Client S3Client, cache *cache.SegmentCache, cfg PartitionLogConfig) *PartitionLog {
	return &PartitionLog{
		topic:     topic,
		partition: partition,
		s3:        s3Client,
		cache:     cache,
		cfg:       cfg,
		buffer:    NewWriteBuffer(cfg.Buffer),
	}
}

// AppendBatch writes a record batch to the log, updating offsets and flushing as needed.
func (l *PartitionLog) AppendBatch(ctx context.Context, batch RecordBatch) (*AppendResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	baseOffset := l.nextOffset
	PatchRecordBatchBaseOffset(&batch, baseOffset)
	l.nextOffset = baseOffset + int64(batch.LastOffsetDelta) + 1

	l.buffer.Append(batch)
	result := &AppendResult{
		BaseOffset: baseOffset,
		LastOffset: l.nextOffset - 1,
	}
	if l.buffer.ShouldFlush(time.Now()) {
		if err := l.flushLocked(ctx); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (l *PartitionLog) flushLocked(ctx context.Context) error {
	batches := l.buffer.Drain()
	if len(batches) == 0 {
		return nil
	}
	artifact, err := BuildSegment(l.cfg.Segment, batches, time.Now())
	if err != nil {
		return fmt.Errorf("build segment: %w", err)
	}
	segmentKey := l.segmentKey(artifact.BaseOffset)
	indexKey := l.indexKey(artifact.BaseOffset)

	if err := l.s3.UploadSegment(ctx, segmentKey, artifact.SegmentBytes); err != nil {
		return err
	}
	if err := l.s3.UploadIndex(ctx, indexKey, artifact.IndexBytes); err != nil {
		return err
	}
	if l.cache != nil {
		l.cache.SetSegment(l.topic, l.partition, artifact.BaseOffset, artifact.SegmentBytes)
	}
	return nil
}

func (l *PartitionLog) segmentKey(baseOffset int64) string {
	return path.Join(l.topic, fmt.Sprintf("%d", l.partition), fmt.Sprintf("segment-%020d.kfs", baseOffset))
}

func (l *PartitionLog) indexKey(baseOffset int64) string {
	return path.Join(l.topic, fmt.Sprintf("%d", l.partition), fmt.Sprintf("segment-%020d.index", baseOffset))
}

// AppendResult contains offsets for a flushed batch.
type AppendResult struct {
	BaseOffset int64
	LastOffset int64
}
