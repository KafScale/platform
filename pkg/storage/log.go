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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KafScale/platform/pkg/cache"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// PartitionLogConfig configures per-partition log behavior.
type PartitionLogConfig struct {
	Buffer            WriteBufferConfig
	Segment           SegmentWriterConfig
	ReadAheadSegments int
	CacheEnabled      bool
	Logger            *slog.Logger
}

// PartitionLog coordinates buffering, segment serialization, S3 uploads, and caching.
type PartitionLog struct {
	namespace    string
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
	// Multi-batch appends exclude legacy append/flush operations while staging.
	// Legacy operations retain their existing concurrent flush behavior.
	appendMu  sync.RWMutex
	mu        sync.Mutex
	flushCond *sync.Cond
	s3sem     *semaphore.Weighted
	flushing  bool
	// flushingBatches holds the batches drained by prepareFlush but not yet
	// committed to a segment by uploadFlush. During that window an acknowledged
	// offset is in neither the live buffer (drained) nor l.segments (not yet
	// registered); Read consults this slice so the record stays readable. Set
	// under l.mu by prepareFlush, cleared under l.mu by uploadFlush on commit or
	// on the upload-failure reset.
	flushingBatches []RecordBatch
}

type segmentRange struct {
	baseOffset int64
	lastOffset int64
	size       int64
}

// ErrOffsetOutOfRange is returned when the requested offset is outside persisted data.
var ErrOffsetOutOfRange = errors.New("offset out of range")

// NewPartitionLog constructs a log for a topic partition.
func NewPartitionLog(namespace string, topic string, partition int32, startOffset int64, s3Client S3Client, cache *cache.SegmentCache, cfg PartitionLogConfig, onFlush func(context.Context, *SegmentArtifact), onS3Op func(string, time.Duration, error), sem *semaphore.Weighted) *PartitionLog {
	if namespace == "" {
		namespace = "default"
	}
	pl := &PartitionLog{
		namespace:    namespace,
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
		s3sem:        sem,
	}
	pl.flushCond = sync.NewCond(&pl.mu)
	return pl
}

func (l *PartitionLog) logger() *slog.Logger {
	if l.cfg.Logger != nil {
		return l.cfg.Logger
	}
	return slog.Default()
}

// acquireS3 blocks until a semaphore token is available or ctx is cancelled.
// If no semaphore is configured, it returns immediately.
func (l *PartitionLog) acquireS3(ctx context.Context) error {
	if l.s3sem == nil {
		return nil
	}
	return l.s3sem.Acquire(ctx, 1)
}

func (l *PartitionLog) tryAcquireS3() bool {
	if l.s3sem == nil {
		return true
	}
	return l.s3sem.TryAcquire(1)
}

func (l *PartitionLog) releaseS3() {
	if l.s3sem != nil {
		l.s3sem.Release(1)
	}
}

const (
	restoreConcurrency      = 16
	restoreProgressInterval = 5 * time.Second
)

type restoredSegment struct {
	segment segmentRange
	entries []*IndexEntry
}

// RestoreFromS3 rebuilds segment ranges from objects already stored in S3.
func (l *PartitionLog) RestoreFromS3(ctx context.Context) (lastOffset int64, restoreErr error) {
	started := time.Now()
	logger := l.logger().With("topic", l.topic, "partition", l.partition)
	logger.Info("partition restore started")
	var completed atomic.Int64
	total := 0
	defer func() {
		if restoreErr != nil {
			logger.Error("partition restore failed", "segments", total, "processed", completed.Load(), "duration", time.Since(started), "error", restoreErr)
		}
	}()
	if err := l.acquireS3(ctx); err != nil {
		return -1, err
	}
	objects, err := l.s3.ListSegments(ctx, l.segmentPrefix())
	l.releaseS3()
	if err != nil {
		return -1, err
	}
	candidates := make([]S3Object, 0, len(objects))
	for _, obj := range objects {
		if _, ok := parseSegmentBaseOffset(obj.Key); ok && obj.Size >= segmentFooterLen {
			candidates = append(candidates, obj)
		}
	}
	total = len(candidates)
	logger.Info("partition restore listed segments", "segments", total)
	l.mu.Lock()
	nextOffset := l.nextOffset
	l.mu.Unlock()

	results := make([]*restoredSegment, total)
	group, groupCtx := errgroup.WithContext(ctx)
	workers := min(restoreConcurrency, total)
	for worker := 0; worker < workers; worker++ {
		group.Go(func() error {
			for i := worker; i < total; i += workers {
				if err := groupCtx.Err(); err != nil {
					return err
				}
				result, err := l.restoreSegment(groupCtx, candidates[i], nextOffset)
				if err != nil {
					return fmt.Errorf("restore segment %s: %w", candidates[i].Key, err)
				}
				results[i] = result
				completed.Add(1)
			}
			return nil
		})
	}
	finished := make(chan error, 1)
	go func() { finished <- group.Wait() }()
	ticker := time.NewTicker(restoreProgressInterval)
	defer ticker.Stop()
waiting:
	for {
		select {
		case err := <-finished:
			if err != nil {
				return -1, err
			}
			break waiting
		case <-ticker.C:
			logger.Info("partition restore progress", "segments", total, "processed", completed.Load(), "duration", time.Since(started))
		}
	}
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	segments := make([]segmentRange, 0, total)
	indexByBase := make(map[int64][]*IndexEntry, total)
	for _, result := range results {
		if result != nil {
			segments = append(segments, result.segment)
			indexByBase[result.segment.baseOffset] = result.entries
		}
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].baseOffset < segments[j].baseOffset
	})
	last := int64(-1)
	if len(segments) > 0 {
		last = segments[len(segments)-1].lastOffset
		l.mu.Lock()
		l.segments = segments
		l.indexEntries = indexByBase
		if last >= l.nextOffset {
			l.nextOffset = last + 1
		}
		l.mu.Unlock()
	}
	logger.Info("partition restore completed", "segments", total, "restored", len(segments), "skipped", total-len(segments), "last_offset", last, "duration", time.Since(started))
	return last, nil
}

func (l *PartitionLog) restoreSegment(ctx context.Context, obj S3Object, nextOffset int64) (*restoredSegment, error) {
	base, _ := parseSegmentBaseOffset(obj.Key)
	rng := &ByteRange{Start: obj.Size - segmentFooterLen, End: obj.Size - 1}
	if err := l.acquireS3(ctx); err != nil {
		return nil, err
	}
	started := time.Now()
	footerBytes, err := l.s3.DownloadSegment(ctx, obj.Key, rng)
	l.releaseS3()
	if l.onS3Op != nil {
		l.onS3Op("download_segment_footer", time.Since(started), err)
	}
	if err != nil {
		return nil, err
	}
	lastOffset, err := parseSegmentFooter(footerBytes)
	if err != nil {
		return nil, err
	}
	indexKey := l.indexKey(base)
	if err := l.acquireS3(ctx); err != nil {
		return nil, err
	}
	started = time.Now()
	indexBytes, err := l.s3.DownloadIndex(ctx, indexKey)
	l.releaseS3()
	if l.onS3Op != nil {
		l.onS3Op("download_index", time.Since(started), err)
	}
	if err != nil {
		if errors.Is(err, ErrNotFound) && base >= nextOffset {
			l.logger().Warn("skipping orphaned segment: missing .index",
				"topic", l.topic, "partition", l.partition,
				"segment_base", base, "next_offset", nextOffset,
				"index_key", indexKey, "error", err)
			return nil, nil
		}
		return nil, err
	}
	entries, err := ParseIndex(indexBytes)
	if err != nil {
		if base >= nextOffset {
			l.logger().Warn("skipping orphaned segment: corrupt .index",
				"topic", l.topic, "partition", l.partition,
				"segment_base", base, "next_offset", nextOffset,
				"index_key", indexKey, "error", err)
			return nil, nil
		}
		return nil, fmt.Errorf("parse index %s: %w", indexKey, err)
	}
	return &restoredSegment{segment: segmentRange{baseOffset: base, lastOffset: lastOffset, size: obj.Size}, entries: entries}, nil
}

// AppendBatch writes a record batch to the log, updating offsets and flushing as needed.
func (l *PartitionLog) AppendBatch(ctx context.Context, batch RecordBatch) (*AppendResult, error) {
	l.appendMu.RLock()
	defer l.appendMu.RUnlock()
	l.mu.Lock()
	baseOffset := l.nextOffset
	PatchRecordBatchBaseOffset(&batch, baseOffset)
	l.nextOffset = baseOffset + int64(batch.LastOffsetDelta) + 1

	l.buffer.Append(batch)
	result := &AppendResult{
		BaseOffset: baseOffset,
		LastOffset: l.nextOffset - 1,
	}

	var artifact *SegmentArtifact
	if l.buffer.ShouldFlush(time.Now()) {
		var err error
		artifact, err = l.prepareFlush()
		if err != nil {
			l.mu.Unlock()
			return nil, err
		}
	}
	l.mu.Unlock()

	if artifact != nil {
		if err := l.uploadFlush(ctx, artifact); err != nil {
			return nil, err
		}
		if l.onFlush != nil {
			l.onFlush(ctx, artifact)
		}
	}
	return result, nil
}

// AppendBatches stages a partition's complete Produce record set as one write.
// A failed build or upload leaves the live buffer and offsets unchanged.
// forceFlush makes the whole set durable before returning; otherwise the normal
// buffer thresholds apply. Concurrent readers see only previously accepted data.
func (l *PartitionLog) AppendBatches(ctx context.Context, batches []RecordBatch, forceFlush bool) (*AppendResult, error) {
	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to append")
	}
	l.mu.Lock()
	baseOffset := l.nextOffset
	nextOffset := baseOffset
	staged := l.buffer.clone()
	l.mu.Unlock()
	for _, batch := range batches {
		if len(batch.Bytes) < recordBatchHeaderMinSize || batch.LastOffsetDelta < 0 {
			return nil, fmt.Errorf("invalid record batch")
		}
		batch.Bytes = append([]byte(nil), batch.Bytes...)
		PatchRecordBatchBaseOffset(&batch, nextOffset)
		nextOffset += int64(batch.LastOffsetDelta) + 1
		staged.Append(batch)
	}
	result := &AppendResult{BaseOffset: baseOffset, LastOffset: nextOffset - 1}
	if !forceFlush && !staged.ShouldFlush(time.Now()) {
		l.mu.Lock()
		l.buffer, l.nextOffset = staged, nextOffset
		l.mu.Unlock()
		return result, nil
	}
	artifact, err := BuildSegment(l.cfg.Segment, staged.Drain(), time.Now())
	if err != nil {
		return nil, fmt.Errorf("build segment: %w", err)
	}
	// Publish the index first. The segment object is the recovery commit point;
	// a failed index upload must never leave a discoverable segment behind.
	for _, upload := range []struct {
		op   string
		key  string
		body []byte
		put  func(context.Context, string, []byte) error
	}{
		{"upload_index", l.indexKey(artifact.BaseOffset), artifact.IndexBytes, l.s3.UploadIndex},
		{"upload_segment", l.segmentKey(artifact.BaseOffset), artifact.SegmentBytes, l.s3.UploadSegment},
	} {
		if err := l.acquireS3(ctx); err != nil {
			return nil, err
		}
		start := time.Now()
		err := upload.put(ctx, upload.key, upload.body)
		l.releaseS3()
		if l.onS3Op != nil {
			l.onS3Op(upload.op, time.Since(start), err)
		}
		if err != nil {
			return nil, err
		}
	}
	if l.cache != nil && l.cfg.CacheEnabled {
		l.cache.SetSegment(l.cacheTopicKey(), l.partition, artifact.BaseOffset, artifact.SegmentBytes)
	}
	l.mu.Lock()
	l.buffer, l.nextOffset = staged, nextOffset
	l.segments = append(l.segments, segmentRange{baseOffset: artifact.BaseOffset, lastOffset: artifact.LastOffset, size: int64(len(artifact.SegmentBytes))})
	l.indexEntries[artifact.BaseOffset] = artifact.RelativeIndex
	lastSegIdx := len(l.segments) - 1
	l.mu.Unlock()
	if l.onFlush != nil {
		l.onFlush(ctx, artifact)
	}
	l.startPrefetch(ctx, lastSegIdx)
	return result, nil
}

// BufferedHighWatermark returns the in-memory high-watermark: one past the last
// offset this log has assigned (whether that offset is in a flushed segment or
// still in the write buffer). It is NOT durable. The flushed/durable
// high-watermark is the metadata store's NextOffset, advanced only on flush.
//
// In flush-disabled / acks=0 mode no flush fires for the buffered tail, so the
// durable watermark lags. The fetch handler uses this value, gated on
// flushOnAck=false, to make acknowledged-but-buffered records consumable while
// the process lives. These records are LOST on restart: the buffer is in-memory
// and RestoreFromS3 rebuilds from segments only. This is a read-after-ack
// visibility helper for the non-default config, not a durability guarantee.
func (l *PartitionLog) BufferedHighWatermark() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextOffset
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
// If another flush is already in progress on this partition, Flush waits for
// it to complete and then flushes any data that accumulated in the meantime.
func (l *PartitionLog) Flush(ctx context.Context) error {
	l.appendMu.RLock()
	defer l.appendMu.RUnlock()
	l.mu.Lock()
	for l.flushing {
		if ctx.Err() != nil {
			l.mu.Unlock()
			return ctx.Err()
		}
		l.flushCond.Wait()
	}
	artifact, err := l.prepareFlush()
	l.mu.Unlock()
	if err != nil {
		return err
	}

	if artifact != nil {
		if err := l.uploadFlush(ctx, artifact); err != nil {
			return err
		}
	}
	if l.onFlush != nil {
		target := artifact
		if target == nil {
			l.mu.Lock()
			current := l.nextOffset - 1
			l.mu.Unlock()
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

// prepareFlush drains the buffer and builds a segment artifact under l.mu.
// It sets l.flushing = true to prevent concurrent flushes on the same
// partition. Returns (nil, nil) if the buffer is empty or a flush is already
// in progress (the data stays in the buffer for the next flush).
// Caller must hold l.mu.
func (l *PartitionLog) prepareFlush() (*SegmentArtifact, error) {
	if l.flushing {
		return nil, nil
	}
	batches := l.buffer.Drain()
	if len(batches) == 0 {
		return nil, nil
	}
	artifact, err := BuildSegment(l.cfg.Segment, batches, time.Now())
	if err != nil {
		return nil, fmt.Errorf("build segment: %w", err)
	}
	l.flushing = true
	// Keep the drained batches readable until uploadFlush commits the segment to
	// l.segments. Without this, an acknowledged offset is unreadable for the
	// duration of the S3 upload (in neither the buffer nor a committed segment).
	l.flushingBatches = batches
	return artifact, nil
}

// uploadFlush uploads the segment and index to S3 (with semaphore gating),
// then re-acquires l.mu to commit segment metadata. Called without l.mu held.
func (l *PartitionLog) uploadFlush(ctx context.Context, artifact *SegmentArtifact) error {
	segmentKey := l.segmentKey(artifact.BaseOffset)
	indexKey := l.indexKey(artifact.BaseOffset)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := l.acquireS3(gctx); err != nil {
			return err
		}
		defer l.releaseS3()
		start := time.Now()
		err := l.s3.UploadSegment(gctx, segmentKey, artifact.SegmentBytes)
		if l.onS3Op != nil {
			l.onS3Op("upload_segment", time.Since(start), err)
		}
		return err
	})
	g.Go(func() error {
		if err := l.acquireS3(gctx); err != nil {
			return err
		}
		defer l.releaseS3()
		start := time.Now()
		err := l.s3.UploadIndex(gctx, indexKey, artifact.IndexBytes)
		if l.onS3Op != nil {
			l.onS3Op("upload_index", time.Since(start), err)
		}
		return err
	})
	if err := g.Wait(); err != nil {
		l.mu.Lock()
		l.flushing = false
		l.flushingBatches = nil
		l.flushCond.Broadcast()
		l.mu.Unlock()
		return err
	}

	if l.cache != nil && l.cfg.CacheEnabled {
		l.cache.SetSegment(l.cacheTopicKey(), l.partition, artifact.BaseOffset, artifact.SegmentBytes)
	}

	l.mu.Lock()
	l.segments = append(l.segments, segmentRange{
		baseOffset: artifact.BaseOffset,
		lastOffset: artifact.LastOffset,
		size:       int64(len(artifact.SegmentBytes)),
	})
	if artifact.RelativeIndex != nil {
		l.indexEntries[artifact.BaseOffset] = artifact.RelativeIndex
	}
	l.flushing = false
	// The segment is now in l.segments; the in-flight copy is no longer needed.
	l.flushingBatches = nil
	l.flushCond.Broadcast()
	lastSegIdx := len(l.segments) - 1
	l.mu.Unlock()

	l.startPrefetch(ctx, lastSegIdx)
	return nil
}

func (l *PartitionLog) segmentKey(baseOffset int64) string {
	return path.Join(l.namespace, l.topic, fmt.Sprintf("%d", l.partition), fmt.Sprintf("segment-%020d.kfs", baseOffset))
}

func (l *PartitionLog) indexKey(baseOffset int64) string {
	return path.Join(l.namespace, l.topic, fmt.Sprintf("%d", l.partition), fmt.Sprintf("segment-%020d.index", baseOffset))
}

func (l *PartitionLog) segmentPrefix() string {
	return path.Join(l.namespace, l.topic, fmt.Sprintf("%d", l.partition)) + "/"
}

func (l *PartitionLog) cacheTopicKey() string {
	return path.Join(l.namespace, l.topic)
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
// When the requested offset falls in a gap (e.g. after an orphaned segment was
// skipped during restore), Read snaps forward to the next available segment.
func (l *PartitionLog) Read(ctx context.Context, offset int64, maxBytes int32) ([]byte, error) {
	l.mu.Lock()
	var seg segmentRange
	found := false
	segIdx := -1
	var entries []*IndexEntry
	for i, s := range l.segments {
		if offset >= s.baseOffset && offset <= s.lastOffset {
			seg = s
			found = true
			segIdx = i
			entries = l.indexEntries[s.baseOffset]
			break
		}
		// Offset falls in a gap before this segment — snap forward.
		if s.baseOffset > offset {
			seg = s
			found = true
			segIdx = i
			offset = s.baseOffset
			entries = l.indexEntries[s.baseOffset]
			break
		}
	}
	if !found {
		// Not in any flushed segment. The offset may still be in the in-memory
		// write buffer (acked but not yet flushed: flush is append-triggered, so
		// a partition that goes quiet below the flush threshold keeps its tail
		// buffered). Serve it so acked records are consumable (Kafka
		// read-after-ack) instead of returning ErrOffsetOutOfRange.
		//
		// Hold l.mu across both fallbacks so an in-flight flush cannot move
		// batches from the buffer into flushingBatches (or commit a segment)
		// between the two checks.
		body := l.buffer.RecordsFrom(offset, maxBytes)
		fromFlushWindow := false
		if len(body) == 0 {
			body = recordsFromBatches(l.flushingBatches, offset, maxBytes)
			fromFlushWindow = len(body) > 0
		}
		l.mu.Unlock()
		if len(body) > 0 {
			if fromFlushWindow {
				l.logger().Debug("read served from in-flight flush batches (flush window)",
					"topic", l.topic, "partition", l.partition, "offset", offset, "bytes", len(body))
			} else {
				l.logger().Debug("read served from write buffer (acked-but-unflushed)",
					"topic", l.topic, "partition", l.partition, "offset", offset, "bytes", len(body))
			}
			return body, nil
		}
		return nil, ErrOffsetOutOfRange
	}
	l.mu.Unlock()

	var data []byte
	ok := false
	if l.cache != nil && l.cfg.CacheEnabled {
		data, ok = l.cache.GetSegment(l.cacheTopicKey(), l.partition, seg.baseOffset)
	}
	rangeReadUsed := false
	if !ok {
		if err := l.acquireS3(ctx); err != nil {
			return nil, err
		}
		if rangeRead, rng := l.segmentRangeForOffset(seg, entries, offset, maxBytes); rangeRead {
			start := time.Now()
			bytes, err := l.s3.DownloadSegment(ctx, l.segmentKey(seg.baseOffset), rng)
			l.releaseS3()
			if l.onS3Op != nil {
				l.onS3Op("download_segment_range", time.Since(start), err)
			}
			if err != nil {
				return nil, err
			}
			data = bytes
			rangeReadUsed = true
		} else {
			start := time.Now()
			bytes, err := l.s3.DownloadSegment(ctx, l.segmentKey(seg.baseOffset), nil)
			l.releaseS3()
			if l.onS3Op != nil {
				l.onS3Op("download_segment", time.Since(start), err)
			}
			if err != nil {
				return nil, err
			}
			data = bytes
			if l.cache != nil && l.cfg.CacheEnabled {
				l.cache.SetSegment(l.cacheTopicKey(), l.partition, seg.baseOffset, data)
			}
		}
	}
	l.startPrefetch(ctx, segIdx+1)

	if ok {
		body, err := l.sliceCachedSegment(seg, entries, offset, maxBytes, data)
		if err != nil {
			return nil, err
		}
		return body, nil
	}
	if rangeReadUsed {
		return data, nil
	}
	body, err := l.sliceCachedSegment(seg, entries, offset, maxBytes, data)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (l *PartitionLog) startPrefetch(ctx context.Context, nextIndex int) {
	if l.cfg.ReadAheadSegments <= 0 || nextIndex < 0 || l.cache == nil || !l.cfg.CacheEnabled {
		return
	}
	l.prefetchMu.Lock()
	defer l.prefetchMu.Unlock()

	l.mu.Lock()
	segsLen := len(l.segments)
	// Collect the segments we need to prefetch while holding the lock.
	var toFetch []segmentRange
	for i := 0; i < l.cfg.ReadAheadSegments; i++ {
		idx := nextIndex + i
		if idx >= segsLen {
			break
		}
		seg := l.segments[idx]
		if _, ok := l.cache.GetSegment(l.cacheTopicKey(), l.partition, seg.baseOffset); ok {
			continue
		}
		toFetch = append(toFetch, seg)
	}
	l.mu.Unlock()

	for _, seg := range toFetch {
		go func(seg segmentRange) {
			if !l.tryAcquireS3() {
				return // semaphore full, skip prefetch
			}
			defer l.releaseS3()
			data, err := l.s3.DownloadSegment(ctx, l.segmentKey(seg.baseOffset), nil)
			if err != nil {
				return
			}
			l.cache.SetSegment(l.cacheTopicKey(), l.partition, seg.baseOffset, data)
		}(seg)
	}
}

func (l *PartitionLog) sliceCachedSegment(seg segmentRange, entries []*IndexEntry, offset int64, maxBytes int32, data []byte) ([]byte, error) {
	if len(entries) == 0 {
		return sliceFullSegmentData(data, maxBytes), nil
	}
	start, end := l.computeSegmentRange(seg, entries, offset, maxBytes)
	if start < 0 || end < start {
		return nil, ErrOffsetOutOfRange
	}
	if end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	return append([]byte(nil), data[start:end+1]...), nil
}

func sliceFullSegmentData(data []byte, maxBytes int32) []byte {
	const headerLen = 32
	start := headerLen
	if start > len(data) {
		start = len(data)
	}
	end := len(data)
	if len(data) > segmentFooterLen {
		end = len(data) - segmentFooterLen
	}
	if end < start {
		end = len(data)
	}
	body := append([]byte(nil), data[start:end]...)
	if maxBytes > 0 && len(body) > int(maxBytes) {
		body = body[:maxBytes]
	}
	return body
}

func (l *PartitionLog) segmentRangeForOffset(seg segmentRange, entries []*IndexEntry, offset int64, maxBytes int32) (bool, *ByteRange) {
	if seg.size <= 0 || len(entries) == 0 {
		return false, nil
	}
	start, end := l.computeSegmentRange(seg, entries, offset, maxBytes)
	if start < 0 || end < start {
		return false, nil
	}
	return true, &ByteRange{Start: start, End: end}
}

func (l *PartitionLog) computeSegmentRange(seg segmentRange, entries []*IndexEntry, offset int64, maxBytes int32) (int64, int64) {
	if seg.size <= segmentFooterLen {
		return -1, -1
	}
	entry := findIndexEntry(entries, offset)
	start := int64(entry.Position)
	endLimit := seg.size - segmentFooterLen
	if endLimit <= start {
		return -1, -1
	}
	end := endLimit - 1
	if maxBytes > 0 {
		maxEnd := start + int64(maxBytes) - 1
		if maxEnd < end {
			end = maxEnd
		}
	}
	return start, end
}

func findIndexEntry(entries []*IndexEntry, offset int64) *IndexEntry {
	if len(entries) == 0 {
		return &IndexEntry{Offset: 0, Position: 0}
	}
	lo := 0
	hi := len(entries) - 1
	if offset <= entries[0].Offset {
		return entries[0]
	}
	if offset >= entries[hi].Offset {
		return entries[hi]
	}
	for lo <= hi {
		mid := (lo + hi) / 2
		if entries[mid].Offset == offset {
			return entries[mid]
		}
		if entries[mid].Offset < offset {
			if mid+1 <= hi && entries[mid+1].Offset > offset {
				return entries[mid]
			}
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return entries[0]
}
