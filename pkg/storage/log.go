package storage

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/novatechflow/kafscale/pkg/cache"
)

// PartitionLogConfig configures per-partition log behavior.
type PartitionLogConfig struct {
	Buffer            WriteBufferConfig
	Segment           SegmentWriterConfig
	ReadAheadSegments int
	CacheEnabled      bool
}

// PartitionLog coordinates buffering, segment serialization, S3 uploads, and caching.
type PartitionLog struct {
	topic        string
	partition    int32
	s3           S3Client
	cache        *cache.SegmentCache
	cfg          PartitionLogConfig
	buffer       *WriteBuffer
	nextOffset   int64
	onFlush      func(context.Context, *SegmentArtifact)
	onS3Op       func(string, time.Duration, error)
	segments     []segmentRange
	indexEntries map[int64][]*IndexEntry
	prefetchMu   sync.Mutex
	mu           sync.Mutex
}

type segmentRange struct {
	baseOffset int64
	lastOffset int64
}

// ErrOffsetOutOfRange is returned when the requested offset is outside persisted data.
var ErrOffsetOutOfRange = errors.New("offset out of range")

// NewPartitionLog constructs a log for a topic partition.
func NewPartitionLog(topic string, partition int32, startOffset int64, s3Client S3Client, cache *cache.SegmentCache, cfg PartitionLogConfig, onFlush func(context.Context, *SegmentArtifact), onS3Op func(string, time.Duration, error)) *PartitionLog {
	return &PartitionLog{
		topic:        topic,
		partition:    partition,
		s3:           s3Client,
		cache:        cache,
		cfg:          cfg,
		buffer:       NewWriteBuffer(cfg.Buffer),
		nextOffset:   startOffset,
		onFlush:      onFlush,
		onS3Op:       onS3Op,
		segments:     make([]segmentRange, 0),
		indexEntries: make(map[int64][]*IndexEntry),
	}
}

// RestoreFromS3 rebuilds segment ranges from objects already stored in S3.
func (l *PartitionLog) RestoreFromS3(ctx context.Context) (int64, error) {
	prefix := path.Join(l.topic, fmt.Sprintf("%d", l.partition)) + "/"
	objects, err := l.s3.ListSegments(ctx, prefix)
	if err != nil {
		return -1, err
	}
	type entry struct {
		base int64
		last int64
	}
	entries := make([]entry, 0, len(objects))
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".kfs") {
			continue
		}
		base, ok := parseSegmentBaseOffset(obj.Key)
		if !ok {
			continue
		}
		if obj.Size < segmentFooterLen {
			continue
		}
		start := obj.Size - segmentFooterLen
		rng := &ByteRange{Start: start, End: obj.Size - 1}
		startTime := time.Now()
		footerBytes, err := l.s3.DownloadSegment(ctx, obj.Key, rng)
		if l.onS3Op != nil {
			l.onS3Op("download_segment_footer", time.Since(startTime), err)
		}
		if err != nil {
			return -1, err
		}
		lastOffset, err := parseSegmentFooter(footerBytes)
		if err != nil {
			return -1, err
		}
		entries = append(entries, entry{base: base, last: lastOffset})
	}
	if len(entries) == 0 {
		return -1, nil
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].base < entries[j].base
	})
	segments := make([]segmentRange, 0, len(entries))
	indexByBase := make(map[int64][]*IndexEntry, len(entries))
	for _, entry := range entries {
		indexKey := l.indexKey(entry.base)
		startTime := time.Now()
		indexBytes, err := l.s3.DownloadIndex(ctx, indexKey)
		if l.onS3Op != nil {
			l.onS3Op("download_index", time.Since(startTime), err)
		}
		if err != nil {
			return -1, err
		}
		parsedEntries, err := ParseIndex(indexBytes)
		if err != nil {
			return -1, fmt.Errorf("parse index %s: %w", indexKey, err)
		}
		segments = append(segments, segmentRange{
			baseOffset: entry.base,
			lastOffset: entry.last,
		})
		indexByBase[entry.base] = parsedEntries
	}
	last := entries[len(entries)-1].last

	l.mu.Lock()
	l.segments = segments
	l.indexEntries = indexByBase
	if last >= l.nextOffset {
		l.nextOffset = last + 1
	}
	l.mu.Unlock()

	return last, nil
}

// AppendBatch writes a record batch to the log, updating offsets and flushing as needed.
func (l *PartitionLog) AppendBatch(ctx context.Context, batch RecordBatch) (*AppendResult, error) {
	var flushed *SegmentArtifact

	l.mu.Lock()
	baseOffset := l.nextOffset
	PatchRecordBatchBaseOffset(&batch, baseOffset)
	l.nextOffset = baseOffset + int64(batch.LastOffsetDelta) + 1

	l.buffer.Append(batch)
	result := &AppendResult{
		BaseOffset: baseOffset,
		LastOffset: l.nextOffset - 1,
	}
	if l.buffer.ShouldFlush(time.Now()) {
		var err error
		flushed, err = l.flushLocked(ctx)
		if err != nil {
			l.mu.Unlock()
			return nil, err
		}
	}
	l.mu.Unlock()

	if flushed != nil && l.onFlush != nil {
		l.onFlush(ctx, flushed)
	}
	return result, nil
}

// EarliestOffset returns the lowest offset available in the log.
func (l *PartitionLog) EarliestOffset() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].baseOffset
}

// Flush forces buffered batches to be written to S3 immediately.
func (l *PartitionLog) Flush(ctx context.Context) error {
	l.mu.Lock()
	artifact, err := l.flushLocked(ctx)
	l.mu.Unlock()
	if err != nil {
		return err
	}
	if l.onFlush != nil {
		target := artifact
		if target == nil {
			current := l.nextOffset - 1
			if current >= 0 {
				target = &SegmentArtifact{LastOffset: current}
			}
		}
		if target != nil {
			l.onFlush(ctx, target)
		}
	}
	return nil
}

func (l *PartitionLog) flushLocked(ctx context.Context) (*SegmentArtifact, error) {
	batches := l.buffer.Drain()
	if len(batches) == 0 {
		return nil, nil
	}
	artifact, err := BuildSegment(l.cfg.Segment, batches, time.Now())
	if err != nil {
		return nil, fmt.Errorf("build segment: %w", err)
	}
	segmentKey := l.segmentKey(artifact.BaseOffset)
	indexKey := l.indexKey(artifact.BaseOffset)

	start := time.Now()
	uploadErr := l.s3.UploadSegment(ctx, segmentKey, artifact.SegmentBytes)
	if l.onS3Op != nil {
		l.onS3Op("upload_segment", time.Since(start), uploadErr)
	}
	if uploadErr != nil {
		return nil, uploadErr
	}
	start = time.Now()
	uploadErr = l.s3.UploadIndex(ctx, indexKey, artifact.IndexBytes)
	if l.onS3Op != nil {
		l.onS3Op("upload_index", time.Since(start), uploadErr)
	}
	if uploadErr != nil {
		return nil, uploadErr
	}
	if l.cache != nil {
		l.cache.SetSegment(l.topic, l.partition, artifact.BaseOffset, artifact.SegmentBytes)
	}
	l.segments = append(l.segments, segmentRange{
		baseOffset: artifact.BaseOffset,
		lastOffset: artifact.LastOffset,
	})
	if artifact.RelativeIndex != nil {
		l.indexEntries[artifact.BaseOffset] = artifact.RelativeIndex
	}
	l.startPrefetch(ctx, len(l.segments)-1)
	return artifact, nil
}

func (l *PartitionLog) segmentKey(baseOffset int64) string {
	return path.Join(l.topic, fmt.Sprintf("%d", l.partition), fmt.Sprintf("segment-%020d.kfs", baseOffset))
}

func (l *PartitionLog) indexKey(baseOffset int64) string {
	return path.Join(l.topic, fmt.Sprintf("%d", l.partition), fmt.Sprintf("segment-%020d.index", baseOffset))
}

func parseSegmentBaseOffset(key string) (int64, bool) {
	name := path.Base(key)
	if !strings.HasPrefix(name, "segment-") || !strings.HasSuffix(name, ".kfs") {
		return 0, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(name, "segment-"), ".kfs")
	if raw == "" {
		return 0, false
	}
	base, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return base, true
}

// AppendResult contains offsets for a flushed batch.
type AppendResult struct {
	BaseOffset int64
	LastOffset int64
}

// Read loads the segment containing the requested offset.
func (l *PartitionLog) Read(ctx context.Context, offset int64, maxBytes int32) ([]byte, error) {
	l.mu.Lock()
	var seg segmentRange
	found := false
	for _, s := range l.segments {
		if offset >= s.baseOffset && offset <= s.lastOffset {
			seg = s
			found = true
			break
		}
	}
	l.mu.Unlock()

	if !found {
		return nil, ErrOffsetOutOfRange
	}

	data, ok := l.cache.GetSegment(l.topic, l.partition, seg.baseOffset)
	if !ok {
		start := time.Now()
		bytes, err := l.s3.DownloadSegment(ctx, l.segmentKey(seg.baseOffset), nil)
		if l.onS3Op != nil {
			l.onS3Op("download_segment", time.Since(start), err)
		}
		if err != nil {
			return nil, err
		}
		data = bytes
		if l.cache != nil && l.cfg.CacheEnabled {
			l.cache.SetSegment(l.topic, l.partition, seg.baseOffset, data)
		}
	}
	l.startPrefetch(ctx, l.segmentIndex(seg.baseOffset)+1)

	const headerLen = 32
	const footerLen = 16
	start := headerLen
	if start > len(data) {
		start = len(data)
	}
	end := len(data)
	if len(data) > footerLen {
		end = len(data) - footerLen
	}
	if end < start {
		end = len(data)
	}
	body := data[start:end]
	result := append([]byte(nil), body...)
	if maxBytes > 0 && len(result) > int(maxBytes) {
		result = result[:maxBytes]
	}
	return result, nil
}

func (l *PartitionLog) segmentIndex(baseOffset int64) int {
	for i, seg := range l.segments {
		if seg.baseOffset == baseOffset {
			return i
		}
	}
	return -1
}

func (l *PartitionLog) startPrefetch(ctx context.Context, nextIndex int) {
	if l.cfg.ReadAheadSegments <= 0 || nextIndex < 0 {
		return
	}
	l.prefetchMu.Lock()
	defer l.prefetchMu.Unlock()
	for i := 0; i < l.cfg.ReadAheadSegments; i++ {
		idx := nextIndex + i
		if idx >= len(l.segments) {
			break
		}
		seg := l.segments[idx]
		if l.cache != nil {
			if _, ok := l.cache.GetSegment(l.topic, l.partition, seg.baseOffset); ok {
				continue
			}
		}
		go func(seg segmentRange) {
			data, err := l.s3.DownloadSegment(ctx, l.segmentKey(seg.baseOffset), nil)
			if err != nil {
				return
			}
			if l.cache != nil && l.cfg.CacheEnabled {
				l.cache.SetSegment(l.topic, l.partition, seg.baseOffset, data)
			}
		}(seg)
	}
}
