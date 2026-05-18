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
	"hash/crc32"
	"testing"
	"time"
)

func TestRecoverTopicToTimestampTruncatesFinalSegmentByRecordTimestamp(t *testing.T) {
	s3 := NewMemoryS3Client()
	created := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	firstTimestamp := created.UnixMilli()
	batch := buildRecoveryBatch(0, firstTimestamp, []int64{0, 1_000, 2_000})
	uploadRecoverySegmentWithBatches(t, s3, "default", "orders", 0, created, 1, batch)

	result, err := RecoverTopicToTimestamp(context.Background(), s3, TopicRecoveryConfig{
		SourceNamespace: "default",
		SourceTopic:     "orders",
		TargetNamespace: "default",
		TargetTopic:     "orders-restore",
		RestoreTo:       time.UnixMilli(firstTimestamp + 1_500),
	})
	if err != nil {
		t.Fatalf("RecoverTopicToTimestamp: %v", err)
	}
	if result.SegmentsCopied != 1 {
		t.Fatalf("expected 1 copied segment, got %d", result.SegmentsCopied)
	}
	if result.Partitions[0].LastOffset != 1 {
		t.Fatalf("expected last offset 1, got %d", result.Partitions[0].LastOffset)
	}

	segmentBytes, err := s3.DownloadSegment(context.Background(), "default/orders-restore/0/segment-00000000000000000000.kfs", nil)
	if err != nil {
		t.Fatalf("download restored segment: %v", err)
	}
	if got := int32(binary.BigEndian.Uint32(segmentBytes[16:20])); got != 2 {
		t.Fatalf("expected 2 messages in restored segment header, got %d", got)
	}
	if got := CountRecordBatchMessages(segmentBytes[segmentHeaderLen : len(segmentBytes)-segmentFooterLen]); got != 2 {
		t.Fatalf("expected 2 messages in restored segment body, got %d", got)
	}
	lastOffset, err := parseSegmentFooter(segmentBytes[len(segmentBytes)-segmentFooterLen:])
	if err != nil {
		t.Fatalf("parse footer: %v", err)
	}
	if lastOffset != 1 {
		t.Fatalf("expected footer last offset 1, got %d", lastOffset)
	}

	indexBytes, err := s3.DownloadIndex(context.Background(), "default/orders-restore/0/segment-00000000000000000000.index")
	if err != nil {
		t.Fatalf("download restored index: %v", err)
	}
	entries, err := ParseIndex(indexBytes)
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	if len(entries) != 1 || entries[0].Offset != 0 {
		t.Fatalf("unexpected restored index entries: %+v", entries)
	}
}

func TestRecoverTopicToTimestampSkipsFinalSegmentWhenItsFirstRecordIsAfterCutoff(t *testing.T) {
	s3 := NewMemoryS3Client()
	created := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	uploadRecoverySegmentWithBatches(t, s3, "default", "orders", 0, created, 1, buildRecoveryBatch(0, created.UnixMilli(), []int64{0}))
	uploadRecoverySegmentWithBatches(t, s3, "default", "orders", 0, created.Add(time.Minute), 1, buildRecoveryBatch(1, created.Add(2*time.Minute).UnixMilli(), []int64{0}))

	result, err := RecoverTopicToTimestamp(context.Background(), s3, TopicRecoveryConfig{
		SourceNamespace: "default",
		SourceTopic:     "orders",
		TargetNamespace: "default",
		TargetTopic:     "orders-restore",
		RestoreTo:       created.Add(90 * time.Second),
	})
	if err != nil {
		t.Fatalf("RecoverTopicToTimestamp: %v", err)
	}
	if result.SegmentsCopied != 1 {
		t.Fatalf("expected only 1 copied segment, got %d", result.SegmentsCopied)
	}
	if result.Partitions[0].LastOffset != 0 {
		t.Fatalf("expected last offset 0, got %d", result.Partitions[0].LastOffset)
	}
	if _, err := s3.DownloadSegment(context.Background(), "default/orders-restore/0/segment-00000000000000000001.kfs", nil); err == nil {
		t.Fatal("expected second segment to be skipped")
	}
}

func TestRecoverTopicToTimestampRejectsCompressedIntersectingBatch(t *testing.T) {
	s3 := NewMemoryS3Client()
	created := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	batch := buildRecoveryBatch(0, created.UnixMilli(), []int64{0, 1_000})
	binary.BigEndian.PutUint16(batch.Bytes[21:23], 1)
	binary.BigEndian.PutUint32(batch.Bytes[17:21], crc32Checksum(batch.Bytes[21:]))
	uploadRecoverySegmentWithBatches(t, s3, "default", "orders", 0, created, 1, batch)

	_, err := RecoverTopicToTimestamp(context.Background(), s3, TopicRecoveryConfig{
		SourceNamespace: "default",
		SourceTopic:     "orders",
		TargetNamespace: "default",
		TargetTopic:     "orders-restore",
		RestoreTo:       time.UnixMilli(created.UnixMilli() + 500),
	})
	if err == nil {
		t.Fatal("expected compressed intersecting batch to fail exact PITR")
	}
}

func uploadRecoverySegmentWithBatches(t *testing.T, s3 *MemoryS3Client, namespace, topic string, partition int32, created time.Time, indexInterval int32, batches ...RecordBatch) {
	t.Helper()

	artifact, err := BuildSegment(SegmentWriterConfig{IndexIntervalMessages: indexInterval}, batches, created)
	if err != nil {
		t.Fatalf("BuildSegment: %v", err)
	}
	if err := s3.UploadSegment(context.Background(), segmentObjectKey(namespace, topic, partition, artifact.BaseOffset), artifact.SegmentBytes); err != nil {
		t.Fatalf("UploadSegment: %v", err)
	}
	if err := s3.UploadIndex(context.Background(), segmentIndexKey(namespace, topic, partition, artifact.BaseOffset), artifact.IndexBytes); err != nil {
		t.Fatalf("UploadIndex: %v", err)
	}
}

func buildRecoveryBatch(baseOffset, firstTimestamp int64, timestampDeltas []int64) RecordBatch {
	records := make([][]byte, 0, len(timestampDeltas))
	maxTimestamp := firstTimestamp
	for i, delta := range timestampDeltas {
		records = append(records, buildRecoveryRecord(delta, int64(i)))
		if ts := firstTimestamp + delta; ts > maxTimestamp {
			maxTimestamp = ts
		}
	}

	bodyLen := 0
	for _, record := range records {
		bodyLen += len(record)
	}
	batch := make([]byte, recordBatchHeaderLen+bodyLen)
	binary.BigEndian.PutUint64(batch[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(batch[8:12], uint32(len(batch)-batchFrameHeaderLen))
	batch[16] = 2
	binary.BigEndian.PutUint64(batch[27:35], uint64(firstTimestamp))
	binary.BigEndian.PutUint64(batch[35:43], uint64(maxTimestamp))
	binary.BigEndian.PutUint64(batch[43:51], uint64(^uint64(0)))
	binary.BigEndian.PutUint16(batch[51:53], uint16(^uint16(0)))
	binary.BigEndian.PutUint32(batch[53:57], uint32(^uint32(0)))
	binary.BigEndian.PutUint32(batch[57:61], uint32(len(records)))

	offset := recordBatchHeaderLen
	for _, record := range records {
		copy(batch[offset:], record)
		offset += len(record)
	}
	binary.BigEndian.PutUint32(batch[23:27], uint32(len(records)-1))
	binary.BigEndian.PutUint32(batch[17:21], crc32Checksum(batch[21:]))
	return RecordBatch{
		BaseOffset:      baseOffset,
		LastOffsetDelta: int32(len(records) - 1),
		MessageCount:    int32(len(records)),
		Bytes:           batch,
	}
}

func buildRecoveryRecord(timestampDelta, offsetDelta int64) []byte {
	payload := bytes.NewBuffer(nil)
	payload.WriteByte(0)
	payload.Write(encodeRecoveryVarint(timestampDelta))
	payload.Write(encodeRecoveryVarint(offsetDelta))
	payload.Write(encodeRecoveryVarint(-1))
	payload.Write(encodeRecoveryVarint(-1))
	payload.Write(encodeRecoveryVarint(0))

	record := bytes.NewBuffer(nil)
	record.Write(encodeRecoveryVarint(int64(payload.Len())))
	record.Write(payload.Bytes())
	return record.Bytes()
}

func encodeRecoveryVarint(value int64) []byte {
	zigzag := uint64(value<<1) ^ uint64(value>>63)
	out := make([]byte, 0, 10)
	for {
		b := byte(zigzag & 0x7f)
		zigzag >>= 7
		if zigzag != 0 {
			b |= 0x80
		}
		out = append(out, b)
		if zigzag == 0 {
			return out
		}
	}
}

func crc32Checksum(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}
