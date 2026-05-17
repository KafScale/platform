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
	"context"
	"testing"
	"time"
)

func TestRecoverTopicToTimestampCopiesEligibleSegments(t *testing.T) {
	s3 := NewMemoryS3Client()
	older := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	newer := older.Add(30 * time.Minute)

	uploadRecoverySegment(t, s3, "default", "orders", 0, 0, older)
	uploadRecoverySegment(t, s3, "default", "orders", 0, 1, newer)

	result, err := RecoverTopicToTimestamp(context.Background(), s3, TopicRecoveryConfig{
		SourceNamespace: "default",
		SourceTopic:     "orders",
		TargetNamespace: "default",
		TargetTopic:     "orders-restore",
		RestoreTo:       older.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("RecoverTopicToTimestamp: %v", err)
	}
	if result.SegmentsCopied != 1 {
		t.Fatalf("expected 1 copied segment, got %d", result.SegmentsCopied)
	}
	if len(result.Partitions) != 1 {
		t.Fatalf("expected 1 partition summary, got %d", len(result.Partitions))
	}
	partition := result.Partitions[0]
	if partition.Partition != 0 {
		t.Fatalf("expected partition 0, got %d", partition.Partition)
	}
	if partition.LastOffset != 0 {
		t.Fatalf("expected last offset 0, got %d", partition.LastOffset)
	}
	if partition.SegmentsCopied != 1 {
		t.Fatalf("expected 1 copied segment for partition, got %d", partition.SegmentsCopied)
	}

	segKey := "default/orders-restore/0/segment-00000000000000000000.kfs"
	if _, err := s3.DownloadSegment(context.Background(), segKey, nil); err != nil {
		t.Fatalf("download restored segment: %v", err)
	}
	if _, err := s3.DownloadIndex(context.Background(), "default/orders-restore/0/segment-00000000000000000000.index"); err != nil {
		t.Fatalf("download restored index: %v", err)
	}
	if _, err := s3.DownloadSegment(context.Background(), "default/orders-restore/0/segment-00000000000000000001.kfs", nil); err == nil {
		t.Fatal("expected newer segment to be excluded")
	}
}

func TestRecoverTopicToTimestampRejectsExistingTarget(t *testing.T) {
	s3 := NewMemoryS3Client()
	created := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	uploadRecoverySegment(t, s3, "default", "orders", 0, 0, created)
	uploadRecoverySegment(t, s3, "default", "orders-restore", 0, 0, created)

	_, err := RecoverTopicToTimestamp(context.Background(), s3, TopicRecoveryConfig{
		SourceNamespace: "default",
		SourceTopic:     "orders",
		TargetNamespace: "default",
		TargetTopic:     "orders-restore",
		RestoreTo:       created.Add(time.Minute),
	})
	if err == nil {
		t.Fatal("expected existing target restore to fail")
	}
}

func TestRecoverTopicToTimestamp_IgnoresSiblingTopicPrefixes(t *testing.T) {
	s3 := NewMemoryS3Client()
	created := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	uploadRecoverySegment(t, s3, "default", "orders", 0, 0, created)
	uploadRecoverySegment(t, s3, "default", "orders-v2", 0, 0, created)

	result, err := RecoverTopicToTimestamp(context.Background(), s3, TopicRecoveryConfig{
		SourceNamespace: "default",
		SourceTopic:     "orders",
		TargetNamespace: "default",
		TargetTopic:     "orders-restore",
		RestoreTo:       created.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("RecoverTopicToTimestamp: %v", err)
	}
	if result.SegmentsCopied != 1 {
		t.Fatalf("expected 1 copied segment, got %d", result.SegmentsCopied)
	}
}

func TestRecoverTopicToTimestamp_IgnoresSiblingTargetPrefixes(t *testing.T) {
	s3 := NewMemoryS3Client()
	created := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	uploadRecoverySegment(t, s3, "default", "orders", 0, 0, created)
	uploadRecoverySegment(t, s3, "default", "orders-restore-old", 0, 0, created)

	result, err := RecoverTopicToTimestamp(context.Background(), s3, TopicRecoveryConfig{
		SourceNamespace: "default",
		SourceTopic:     "orders",
		TargetNamespace: "default",
		TargetTopic:     "orders-restore",
		RestoreTo:       created.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("RecoverTopicToTimestamp: %v", err)
	}
	if result.SegmentsCopied != 1 {
		t.Fatalf("expected 1 copied segment, got %d", result.SegmentsCopied)
	}
}

func uploadRecoverySegment(t *testing.T, s3 *MemoryS3Client, namespace string, topic string, partition int32, baseOffset int64, created time.Time) {
	t.Helper()

	artifact, err := BuildSegment(SegmentWriterConfig{IndexIntervalMessages: 1}, []RecordBatch{
		{
			BaseOffset:      baseOffset,
			LastOffsetDelta: 0,
			MessageCount:    1,
			Bytes:           make([]byte, 70),
		},
	}, created)
	if err != nil {
		t.Fatalf("BuildSegment: %v", err)
	}

	if err := s3.UploadSegment(context.Background(), segmentObjectKey(namespace, topic, partition, baseOffset), artifact.SegmentBytes); err != nil {
		t.Fatalf("UploadSegment: %v", err)
	}
	if err := s3.UploadIndex(context.Background(), segmentIndexKey(namespace, topic, partition, baseOffset), artifact.IndexBytes); err != nil {
		t.Fatalf("UploadIndex: %v", err)
	}
}
