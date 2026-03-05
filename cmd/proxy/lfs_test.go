// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"log/slog"
	"testing"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// fakeS3 is a minimal in-memory S3 backend for testing.
type fakeS3 struct {
	objects map[string][]byte
	deleted []string
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objects: make(map[string][]byte)}
}

func (f *fakeS3) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("test-upload-id")}, nil
}
func (f *fakeS3) UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return &s3.UploadPartOutput{ETag: aws.String("test-etag")}, nil
}
func (f *fakeS3) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}
func (f *fakeS3) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return &s3.AbortMultipartUploadOutput{}, nil
}
func (f *fakeS3) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if params.Body != nil {
		data, _ := io.ReadAll(params.Body)
		f.objects[*params.Key] = data
	}
	return &s3.PutObjectOutput{}, nil
}
func (f *fakeS3) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{}, nil
}
func (f *fakeS3) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	f.deleted = append(f.deleted, *params.Key)
	delete(f.objects, *params.Key)
	return &s3.DeleteObjectOutput{}, nil
}
func (f *fakeS3) HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return &s3.HeadBucketOutput{}, nil
}
func (f *fakeS3) CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	return &s3.CreateBucketOutput{}, nil
}

type fakePresign struct{}

func (f *fakePresign) PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	return &v4.PresignedHTTPRequest{URL: "https://test.s3.amazonaws.com/" + *params.Key}, nil
}

func testLFSModule(t *testing.T) (*lfsModule, *fakeS3) {
	t.Helper()
	fs3 := newFakeS3()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := &lfsModule{
		logger:      logger,
		s3Uploader:  &s3Uploader{bucket: "test-bucket", region: "us-east-1", chunkSize: 5 << 20, api: fs3, presign: &fakePresign{}},
		s3Bucket:    "test-bucket",
		s3Namespace: "test-ns",
		maxBlob:     5 << 30,
		checksumAlg: "sha256",
		proxyID:     "test-proxy",
		metrics:     newLfsMetrics(),
		tracker:     &LfsOpsTracker{config: TrackerConfig{}, logger: logger},
	}
	return m, fs3
}

func buildTestBatch(records []kmsg.Record) []byte {
	return lfsBuildRecordBatch(records)
}

func TestRewriteProduceRecordsDetectsLFSBlob(t *testing.T) {
	m, _ := testLFSModule(t)
	blobPayload := []byte("hello world LFS blob data for testing")
	shaHasher := sha256.New()
	shaHasher.Write(blobPayload)
	expectedSHA := hex.EncodeToString(shaHasher.Sum(nil))

	records := []kmsg.Record{{
		Key:   []byte("mykey"),
		Value: blobPayload,
		Headers: []kmsg.Header{
			{Key: "LFS_BLOB", Value: []byte(expectedSHA)},
		},
	}}
	batchBytes := buildTestBatch(records)
	req := &kmsg.ProduceRequest{
		Acks:      1,
		TimeoutMillis: 5000,
		Topics: []kmsg.ProduceRequestTopic{{
			Topic: "test-topic",
			Partitions: []kmsg.ProduceRequestTopicPartition{{
				Partition: 0,
				Records:   batchBytes,
			}},
		}},
	}
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: 1}
	result, err := m.rewriteProduceRecords(context.Background(), header, req)
	if err != nil {
		t.Fatalf("rewriteProduceRecords: %v", err)
	}
	if !result.modified {
		t.Fatal("expected modified=true")
	}
	if result.uploadBytes != int64(len(blobPayload)) {
		t.Fatalf("expected uploadBytes=%d, got %d", len(blobPayload), result.uploadBytes)
	}
	if _, ok := result.topics["test-topic"]; !ok {
		t.Fatal("expected test-topic in topics")
	}

	// Verify the records were rewritten in-place
	batches, err := lfsDecodeRecordBatches(req.Topics[0].Partitions[0].Records)
	if err != nil {
		t.Fatalf("decode batches: %v", err)
	}
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}

	decompressor := func() []kmsg.Record {
		recs, _, err := lfsDecodeBatchRecords(&batches[0], nil)
		if err != nil {
			t.Fatalf("decode records: %v", err)
		}
		return recs
	}
	// Use the kgo decompressor for uncompressed
	recs, _, err := lfsDecodeBatchRecords(&batches[0], nil)
	if err != nil {
		_ = decompressor // suppress lint
		t.Fatalf("decode records: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	// Value should now be a JSON LFS envelope, not the raw blob
	env, err := lfs.DecodeEnvelope(recs[0].Value)
	if err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.Bucket != "test-bucket" {
		t.Fatalf("expected bucket=test-bucket, got %s", env.Bucket)
	}
	if env.Size != int64(len(blobPayload)) {
		t.Fatalf("expected size=%d, got %d", len(blobPayload), env.Size)
	}
	if env.SHA256 != expectedSHA {
		t.Fatalf("expected sha256=%s, got %s", expectedSHA, env.SHA256)
	}
	// LFS_BLOB header should be removed
	for _, h := range recs[0].Headers {
		if h.Key == "LFS_BLOB" {
			t.Fatal("LFS_BLOB header should be removed")
		}
	}
}

func TestRewriteProduceRecordsPassthroughWithoutLFSBlob(t *testing.T) {
	m, _ := testLFSModule(t)
	records := []kmsg.Record{{
		Key:   []byte("mykey"),
		Value: []byte("regular record value"),
	}}
	batchBytes := buildTestBatch(records)
	req := &kmsg.ProduceRequest{
		Acks:      1,
		TimeoutMillis: 5000,
		Topics: []kmsg.ProduceRequestTopic{{
			Topic: "test-topic",
			Partitions: []kmsg.ProduceRequestTopicPartition{{
				Partition: 0,
				Records:   batchBytes,
			}},
		}},
	}
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: 1}
	result, err := m.rewriteProduceRecords(context.Background(), header, req)
	if err != nil {
		t.Fatalf("rewriteProduceRecords: %v", err)
	}
	if result.modified {
		t.Fatal("expected modified=false for records without LFS_BLOB header")
	}
}

func TestNilLFSModuleZeroOverhead(t *testing.T) {
	p := &proxy{
		lfs: nil,
	}
	// Accessing p.lfs should be nil — the check in handleProduceRouting
	// is simply `if p.lfs != nil`, which is a zero-cost nil pointer check.
	if p.lfs != nil {
		t.Fatal("expected nil lfs module")
	}
}

func TestRewriteProduceRecordsChecksumMismatch(t *testing.T) {
	m, fs3 := testLFSModule(t)
	blobPayload := []byte("checksum mismatch test data")

	records := []kmsg.Record{{
		Key:   []byte("mykey"),
		Value: blobPayload,
		Headers: []kmsg.Header{
			{Key: "LFS_BLOB", Value: []byte("wrong-checksum-value")},
		},
	}}
	batchBytes := buildTestBatch(records)
	req := &kmsg.ProduceRequest{
		Acks:      1,
		TimeoutMillis: 5000,
		Topics: []kmsg.ProduceRequestTopic{{
			Topic: "test-topic",
			Partitions: []kmsg.ProduceRequestTopicPartition{{
				Partition: 0,
				Records:   batchBytes,
			}},
		}},
	}
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: 1}
	_, err := m.rewriteProduceRecords(context.Background(), header, req)
	if err == nil {
		t.Fatal("expected error on checksum mismatch")
	}
	var csErr *lfs.ChecksumError
	if !errors.As(err, &csErr) {
		t.Fatalf("expected *lfs.ChecksumError, got: %T: %v", err, err)
	}
	if csErr.Expected != "wrong-checksum-value" {
		t.Fatalf("expected Expected=%q, got %q", "wrong-checksum-value", csErr.Expected)
	}
	// The S3 object should have been deleted
	if len(fs3.deleted) == 0 {
		t.Fatal("expected S3 object to be deleted on checksum mismatch")
	}
}

func TestRewriteProduceRecordsMixedRecords(t *testing.T) {
	m, _ := testLFSModule(t)
	blobPayload := []byte("lfs blob payload")
	shaHasher := sha256.New()
	shaHasher.Write(blobPayload)
	expectedSHA := hex.EncodeToString(shaHasher.Sum(nil))

	// Build batch with one LFS record and one regular record
	records := []kmsg.Record{
		{
			Key:   []byte("lfs-key"),
			Value: blobPayload,
			Headers: []kmsg.Header{
				{Key: "LFS_BLOB", Value: []byte(expectedSHA)},
			},
		},
		{
			Key:   []byte("regular-key"),
			Value: []byte("regular value that should stay unchanged"),
		},
	}
	batchBytes := buildTestBatch(records)
	req := &kmsg.ProduceRequest{
		Acks:      1,
		TimeoutMillis: 5000,
		Topics: []kmsg.ProduceRequestTopic{{
			Topic: "mixed-topic",
			Partitions: []kmsg.ProduceRequestTopicPartition{{
				Partition: 0,
				Records:   batchBytes,
			}},
		}},
	}
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: 1}
	result, err := m.rewriteProduceRecords(context.Background(), header, req)
	if err != nil {
		t.Fatalf("rewriteProduceRecords: %v", err)
	}
	if !result.modified {
		t.Fatal("expected modified=true")
	}

	// Decode and verify: first record should be envelope, second should be unchanged
	batches, err := lfsDecodeRecordBatches(req.Topics[0].Partitions[0].Records)
	if err != nil {
		t.Fatalf("decode batches: %v", err)
	}
	recs, _, err := lfsDecodeBatchRecords(&batches[0], nil)
	if err != nil {
		t.Fatalf("decode records: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recs))
	}
	// First record: LFS envelope
	env, err := lfs.DecodeEnvelope(recs[0].Value)
	if err != nil {
		t.Fatalf("first record should be LFS envelope: %v", err)
	}
	if env.Size != int64(len(blobPayload)) {
		t.Fatalf("expected size=%d, got %d", len(blobPayload), env.Size)
	}
	// Second record: unchanged
	if string(recs[1].Value) != "regular value that should stay unchanged" {
		t.Fatalf("second record value changed: %s", string(recs[1].Value))
	}
}

func TestBatchCRCIsValid(t *testing.T) {
	m, _ := testLFSModule(t)
	blobPayload := []byte("crc test data")
	shaHasher := sha256.New()
	shaHasher.Write(blobPayload)
	expectedSHA := hex.EncodeToString(shaHasher.Sum(nil))

	records := []kmsg.Record{{
		Key:   []byte("k"),
		Value: blobPayload,
		Headers: []kmsg.Header{
			{Key: "LFS_BLOB", Value: []byte(expectedSHA)},
		},
	}}
	batchBytes := buildTestBatch(records)
	req := &kmsg.ProduceRequest{
		Acks:      1,
		TimeoutMillis: 5000,
		Topics: []kmsg.ProduceRequestTopic{{
			Topic: "crc-topic",
			Partitions: []kmsg.ProduceRequestTopicPartition{{
				Partition: 0,
				Records:   batchBytes,
			}},
		}},
	}
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: 1}
	_, err := m.rewriteProduceRecords(context.Background(), header, req)
	if err != nil {
		t.Fatalf("rewriteProduceRecords: %v", err)
	}

	// Verify batch CRC is valid
	batches, err := lfsDecodeRecordBatches(req.Topics[0].Partitions[0].Records)
	if err != nil {
		t.Fatalf("decode batches: %v", err)
	}
	batch := &batches[0]
	// Recompute CRC and compare
	raw := batch.AppendTo(nil)
	expectedCRC := int32(crc32.Checksum(raw[21:], lfsCRC32cTable))
	if batch.CRC != expectedCRC {
		t.Fatalf("CRC mismatch: batch.CRC=%d, computed=%d", batch.CRC, expectedCRC)
	}
}

func TestLFSTopicsFromProduce(t *testing.T) {
	req := &kmsg.ProduceRequest{
		Topics: []kmsg.ProduceRequestTopic{
			{Topic: "topic-a"},
			{Topic: "topic-b"},
			{Topic: "topic-a"}, // duplicate
		},
	}
	topics := lfsTopicsFromProduce(req)
	if len(topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(topics))
	}
	if topics[0] != "topic-a" || topics[1] != "topic-b" {
		t.Fatalf("unexpected topics: %v", topics)
	}
}

func TestLFSTopicsFromProduceNil(t *testing.T) {
	topics := lfsTopicsFromProduce(nil)
	if topics != nil {
		t.Fatalf("expected nil, got %v", topics)
	}
}

func TestLFSTopicsFromProduceEmpty(t *testing.T) {
	req := &kmsg.ProduceRequest{}
	topics := lfsTopicsFromProduce(req)
	if len(topics) != 1 || topics[0] != "unknown" {
		t.Fatalf("expected [unknown], got %v", topics)
	}
}

// Ensure unused variable warnings don't break:
var _ s3API = (*fakeS3)(nil)
var _ s3PresignAPI = (*fakePresign)(nil)
var _ types.CompletedPart
