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
	"context"
	"encoding/binary"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

const segmentHeaderLen = 32

// TopicRecoveryConfig defines a segment-granular topic restore into a new topic.
type TopicRecoveryConfig struct {
	SourceNamespace string
	SourceTopic     string
	TargetNamespace string
	TargetTopic     string
	RestoreTo       time.Time
	Partitions      []int32
}

// RecoveredSegment describes one copied segment/index pair.
type RecoveredSegment struct {
	Partition   int32
	BaseOffset  int64
	LastOffset  int64
	SizeBytes   int64
	CreatedAt   time.Time
	SourceKey   string
	TargetKey   string
	SourceIndex string
	TargetIndex string
}

// RecoveredPartition summarizes copied data for one partition.
type RecoveredPartition struct {
	Partition      int32
	SegmentsCopied int
	LastOffset     int64
	Segments       []RecoveredSegment
}

// TopicRecoveryResult is the outcome of a restore run.
type TopicRecoveryResult struct {
	SourceNamespace string
	SourceTopic     string
	TargetNamespace string
	TargetTopic     string
	RestoreTo       time.Time
	SegmentsCopied  int
	Partitions      []RecoveredPartition
}

type sourceSegment struct {
	RecoveredSegment
}

type copiedObject struct {
	segmentKey string
	indexKey   string
}

// RecoverTopicToTimestamp copies immutable segment/index pairs into a new topic
// and truncates the final candidate segment at the first record whose timestamp
// exceeds the requested cutoff.
func RecoverTopicToTimestamp(ctx context.Context, s3 S3Client, cfg TopicRecoveryConfig) (*TopicRecoveryResult, error) {
	if s3 == nil {
		return nil, fmt.Errorf("s3 client required")
	}
	if cfg.SourceTopic == "" {
		return nil, fmt.Errorf("source topic required")
	}
	if cfg.TargetTopic == "" {
		return nil, fmt.Errorf("target topic required")
	}
	if cfg.RestoreTo.IsZero() {
		return nil, fmt.Errorf("restore timestamp required")
	}
	if cfg.SourceNamespace == "" {
		cfg.SourceNamespace = "default"
	}
	if cfg.TargetNamespace == "" {
		cfg.TargetNamespace = cfg.SourceNamespace
	}
	if cfg.SourceNamespace == cfg.TargetNamespace && cfg.SourceTopic == cfg.TargetTopic {
		return nil, fmt.Errorf("target topic must differ from source topic")
	}

	targetPrefix := path.Join(cfg.TargetNamespace, cfg.TargetTopic) + "/"
	existing, err := s3.ListSegments(ctx, targetPrefix)
	if err != nil {
		return nil, err
	}
	for _, obj := range existing {
		if strings.HasSuffix(obj.Key, ".kfs") {
			return nil, fmt.Errorf("target topic already has persisted segments under %s", targetPrefix)
		}
	}

	sourcePrefix := path.Join(cfg.SourceNamespace, cfg.SourceTopic) + "/"
	objects, err := s3.ListSegments(ctx, sourcePrefix)
	if err != nil {
		return nil, err
	}

	allowedPartitions := make(map[int32]struct{}, len(cfg.Partitions))
	for _, partition := range cfg.Partitions {
		allowedPartitions[partition] = struct{}{}
	}

	segmentsByPartition := make(map[int32][]sourceSegment)
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".kfs") {
			continue
		}
		segment, err := inspectSourceSegment(ctx, s3, obj, cfg.SourceNamespace, cfg.SourceTopic)
		if err != nil {
			return nil, err
		}
		if len(allowedPartitions) > 0 {
			if _, ok := allowedPartitions[segment.Partition]; !ok {
				continue
			}
		}
		segmentsByPartition[segment.Partition] = append(segmentsByPartition[segment.Partition], segment)
	}

	partitions := make([]int32, 0, len(segmentsByPartition))
	for partition := range segmentsByPartition {
		partitions = append(partitions, partition)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

	result := &TopicRecoveryResult{
		SourceNamespace: cfg.SourceNamespace,
		SourceTopic:     cfg.SourceTopic,
		TargetNamespace: cfg.TargetNamespace,
		TargetTopic:     cfg.TargetTopic,
		RestoreTo:       cfg.RestoreTo.UTC(),
		Partitions:      make([]RecoveredPartition, 0, len(partitions)),
	}
	copiedObjects := make([]copiedObject, 0)
	restoreCommitted := false
	defer func() {
		if restoreCommitted {
			return
		}
		for i := len(copiedObjects) - 1; i >= 0; i-- {
			copied := copiedObjects[i]
			_ = s3.DeleteIndex(context.Background(), copied.indexKey)
			_ = s3.DeleteSegment(context.Background(), copied.segmentKey)
		}
	}()

	for _, partition := range partitions {
		segments := segmentsByPartition[partition]
		sort.Slice(segments, func(i, j int) bool { return segments[i].BaseOffset < segments[j].BaseOffset })
		lastCandidate := -1
		for i, segment := range segments {
			if segment.CreatedAt.After(cfg.RestoreTo) {
				break
			}
			lastCandidate = i
		}

		summary := RecoveredPartition{
			Partition:  partition,
			LastOffset: -1,
			Segments:   make([]RecoveredSegment, 0, len(segments)),
		}
		for i := 0; i <= lastCandidate; i++ {
			segment := segments[i]
			segmentBytes, err := s3.DownloadSegment(ctx, segment.SourceKey, nil)
			if err != nil {
				return nil, err
			}
			indexBytes, err := s3.DownloadIndex(ctx, segment.SourceIndex)
			if err != nil {
				return nil, err
			}

			plan := &segmentRestorePlan{
				segmentBytes: segmentBytes,
				indexBytes:   indexBytes,
				baseOffset:   segment.BaseOffset,
				lastOffset:   segment.LastOffset,
				keep:         true,
			}
			if i == lastCandidate {
				plan, err = buildRestorePlan(segmentBytes, indexBytes, cfg.RestoreTo, segment.CreatedAt)
				if err != nil {
					return nil, err
				}
				if !plan.keep {
					break
				}
			}

			targetSegmentKey := segmentObjectKey(cfg.TargetNamespace, cfg.TargetTopic, partition, plan.baseOffset)
			targetIndexKey := segmentIndexKey(cfg.TargetNamespace, cfg.TargetTopic, partition, plan.baseOffset)
			if err := s3.UploadSegment(ctx, targetSegmentKey, plan.segmentBytes); err != nil {
				return nil, err
			}
			copiedObjects = append(copiedObjects, copiedObject{
				segmentKey: targetSegmentKey,
				indexKey:   targetIndexKey,
			})
			if err := s3.UploadIndex(ctx, targetIndexKey, plan.indexBytes); err != nil {
				return nil, err
			}

			copied := segment.RecoveredSegment
			copied.BaseOffset = plan.baseOffset
			copied.LastOffset = plan.lastOffset
			copied.SizeBytes = int64(len(plan.segmentBytes))
			copied.TargetKey = targetSegmentKey
			copied.TargetIndex = targetIndexKey
			summary.Segments = append(summary.Segments, copied)
			summary.SegmentsCopied++
			summary.LastOffset = copied.LastOffset
			result.SegmentsCopied++
		}
		result.Partitions = append(result.Partitions, summary)
	}

	restoreCommitted = true
	return result, nil
}

func inspectSourceSegment(ctx context.Context, s3 S3Client, obj S3Object, namespace string, topic string) (sourceSegment, error) {
	partition, baseOffset, err := parseSegmentLocation(obj.Key, namespace, topic)
	if err != nil {
		return sourceSegment{}, err
	}
	if obj.Size < segmentFooterLen {
		return sourceSegment{}, fmt.Errorf("segment %s too small", obj.Key)
	}

	headerBytes, err := s3.DownloadSegment(ctx, obj.Key, &ByteRange{Start: 0, End: segmentHeaderLen - 1})
	if err != nil {
		return sourceSegment{}, err
	}
	createdAt, err := parseSegmentHeaderCreatedAt(headerBytes)
	if err != nil {
		return sourceSegment{}, err
	}

	start := obj.Size - segmentFooterLen
	footerBytes, err := s3.DownloadSegment(ctx, obj.Key, &ByteRange{Start: start, End: obj.Size - 1})
	if err != nil {
		return sourceSegment{}, err
	}
	lastOffset, err := parseSegmentFooter(footerBytes)
	if err != nil {
		return sourceSegment{}, err
	}

	return sourceSegment{
		RecoveredSegment: RecoveredSegment{
			Partition:   partition,
			BaseOffset:  baseOffset,
			LastOffset:  lastOffset,
			SizeBytes:   obj.Size,
			CreatedAt:   createdAt,
			SourceKey:   obj.Key,
			SourceIndex: segmentIndexKey(namespace, topic, partition, baseOffset),
		},
	}, nil
}

func parseSegmentLocation(key string, namespace string, topic string) (int32, int64, error) {
	prefix := path.Join(namespace, topic) + "/"
	if !strings.HasPrefix(key, prefix) {
		return 0, 0, fmt.Errorf("segment %s not under %s", key, prefix)
	}
	trimmed := strings.TrimPrefix(key, prefix)
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("segment %s has unexpected layout", key)
	}
	partition, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("parse partition from %s: %w", key, err)
	}
	baseOffset, ok := parseSegmentBaseOffset(parts[1])
	if !ok {
		return 0, 0, fmt.Errorf("parse base offset from %s", key)
	}
	return int32(partition), baseOffset, nil
}

func parseSegmentHeaderCreatedAt(data []byte) (time.Time, error) {
	if len(data) < segmentHeaderLen {
		return time.Time{}, fmt.Errorf("header too small")
	}
	reader := bytes.NewReader(data)
	magic := make([]byte, len(segmentMagic))
	if _, err := reader.Read(magic); err != nil {
		return time.Time{}, err
	}
	if string(magic) != segmentMagic {
		return time.Time{}, fmt.Errorf("invalid segment magic")
	}
	var version uint16
	if err := binary.Read(reader, binary.BigEndian, &version); err != nil {
		return time.Time{}, err
	}
	var flags uint16
	if err := binary.Read(reader, binary.BigEndian, &flags); err != nil {
		return time.Time{}, err
	}
	var baseOffset int64
	if err := binary.Read(reader, binary.BigEndian, &baseOffset); err != nil {
		return time.Time{}, err
	}
	var messageCount int32
	if err := binary.Read(reader, binary.BigEndian, &messageCount); err != nil {
		return time.Time{}, err
	}
	var createdAtMillis int64
	if err := binary.Read(reader, binary.BigEndian, &createdAtMillis); err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(createdAtMillis).UTC(), nil
}

func segmentObjectKey(namespace string, topic string, partition int32, baseOffset int64) string {
	return path.Join(namespace, topic, fmt.Sprintf("%d", partition), fmt.Sprintf("segment-%020d.kfs", baseOffset))
}

func segmentIndexKey(namespace string, topic string, partition int32, baseOffset int64) string {
	return path.Join(namespace, topic, fmt.Sprintf("%d", partition), fmt.Sprintf("segment-%020d.index", baseOffset))
}
