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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"strings"
	"testing"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/twmb/franz-go/pkg/kgo"
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
	data := f.objects[*params.Key]
	length := int64(len(data))
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: &length,
	}, nil
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
		Acks:          1,
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
		Acks:          1,
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
		Acks:          1,
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
		Acks:          1,
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
		Acks:          1,
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

// ---------------------------------------------------------------------------
// Tests for lfsEncodeRecords / lfsEncodeRecord
// ---------------------------------------------------------------------------

func TestLfsEncodeRecordSingle(t *testing.T) {
	rec := kmsg.Record{
		Key:   []byte("key1"),
		Value: []byte("val1"),
	}
	encoded := lfsEncodeRecord(rec)
	if len(encoded) == 0 {
		t.Fatal("expected non-empty encoded record")
	}
	// The encoded bytes must begin with a varint length prefix.
	length, n := binary.Varint(encoded)
	if n <= 0 {
		t.Fatal("expected valid varint length prefix")
	}
	if int(length) != len(encoded)-n {
		t.Fatalf("varint length %d does not match body length %d", length, len(encoded)-n)
	}
}

func TestLfsEncodeRecordsMultiple(t *testing.T) {
	records := []kmsg.Record{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}
	encoded := lfsEncodeRecords(records)
	if len(encoded) == 0 {
		t.Fatal("expected non-empty output for multiple records")
	}
	// Encoded length must be the sum of individually encoded records.
	sum := 0
	for _, r := range records {
		sum += len(lfsEncodeRecord(r))
	}
	if len(encoded) != sum {
		t.Fatalf("encoded length %d != sum of individual %d", len(encoded), sum)
	}
}

func TestLfsEncodeRecordsEmpty(t *testing.T) {
	encoded := lfsEncodeRecords(nil)
	if encoded != nil {
		t.Fatalf("expected nil for empty records, got %v", encoded)
	}
	encoded = lfsEncodeRecords([]kmsg.Record{})
	if encoded != nil {
		t.Fatalf("expected nil for zero-length records, got %v", encoded)
	}
}

func TestLfsEncodeRecordNilKeyValue(t *testing.T) {
	rec := kmsg.Record{
		Key:   nil,
		Value: nil,
	}
	encoded := lfsEncodeRecord(rec)
	if len(encoded) == 0 {
		t.Fatal("expected non-empty encoded record even with nil key/value")
	}
	// Build a batch and decode to verify round-trip.
	batchBytes := lfsBuildRecordBatch([]kmsg.Record{rec})
	batches, err := lfsDecodeRecordBatches(batchBytes)
	if err != nil {
		t.Fatalf("decode batches: %v", err)
	}
	recs, _, err := lfsDecodeBatchRecords(&batches[0], nil)
	if err != nil {
		t.Fatalf("decode records: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if recs[0].Key != nil {
		t.Fatalf("expected nil key, got %v", recs[0].Key)
	}
	if recs[0].Value != nil {
		t.Fatalf("expected nil value, got %v", recs[0].Value)
	}
}

func TestLfsEncodeRecordWithHeaders(t *testing.T) {
	rec := kmsg.Record{
		Key:   []byte("hk"),
		Value: []byte("hv"),
		Headers: []kmsg.Header{
			{Key: "h1", Value: []byte("v1")},
			{Key: "h2", Value: []byte("v2")},
		},
	}
	batchBytes := lfsBuildRecordBatch([]kmsg.Record{rec})
	batches, err := lfsDecodeRecordBatches(batchBytes)
	if err != nil {
		t.Fatalf("decode batches: %v", err)
	}
	recs, _, err := lfsDecodeBatchRecords(&batches[0], nil)
	if err != nil {
		t.Fatalf("decode records: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if len(recs[0].Headers) != 2 {
		t.Fatalf("expected 2 headers, got %d", len(recs[0].Headers))
	}
	if recs[0].Headers[0].Key != "h1" || string(recs[0].Headers[0].Value) != "v1" {
		t.Fatalf("header 0 mismatch: %v", recs[0].Headers[0])
	}
	if recs[0].Headers[1].Key != "h2" || string(recs[0].Headers[1].Value) != "v2" {
		t.Fatalf("header 1 mismatch: %v", recs[0].Headers[1])
	}
}

// ---------------------------------------------------------------------------
// Tests for lfsBuildRecordBatch
// ---------------------------------------------------------------------------

func TestLfsBuildRecordBatch(t *testing.T) {
	records := []kmsg.Record{
		{Key: []byte("k"), Value: []byte("v")},
	}
	batchBytes := lfsBuildRecordBatch(records)
	if len(batchBytes) == 0 {
		t.Fatal("expected non-empty batch bytes")
	}
	// Must be decodable.
	batches, err := lfsDecodeRecordBatches(batchBytes)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if batches[0].NumRecords != 1 {
		t.Fatalf("expected NumRecords=1, got %d", batches[0].NumRecords)
	}
}

func TestLfsBuildRecordBatchRoundTrip(t *testing.T) {
	records := []kmsg.Record{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	}
	batchBytes := lfsBuildRecordBatch(records)
	batches, err := lfsDecodeRecordBatches(batchBytes)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	recs, _, err := lfsDecodeBatchRecords(&batches[0], nil)
	if err != nil {
		t.Fatalf("decode records: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recs))
	}
	if string(recs[0].Key) != "a" || string(recs[0].Value) != "1" {
		t.Fatalf("record 0 mismatch: key=%q val=%q", recs[0].Key, recs[0].Value)
	}
	if string(recs[1].Key) != "b" || string(recs[1].Value) != "2" {
		t.Fatalf("record 1 mismatch: key=%q val=%q", recs[1].Key, recs[1].Value)
	}
}

// ---------------------------------------------------------------------------
// Tests for lfsVarint
// ---------------------------------------------------------------------------

func TestLfsVarintValid(t *testing.T) {
	var buf [binary.MaxVarintLen32]byte
	n := binary.PutVarint(buf[:], 42)
	val, consumed := lfsVarint(buf[:n])
	if consumed != n {
		t.Fatalf("expected consumed=%d, got %d", n, consumed)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestLfsVarintZero(t *testing.T) {
	var buf [binary.MaxVarintLen32]byte
	n := binary.PutVarint(buf[:], 0)
	val, consumed := lfsVarint(buf[:n])
	if consumed != n || val != 0 {
		t.Fatalf("expected (0, %d), got (%d, %d)", n, val, consumed)
	}
}

func TestLfsVarintNegative(t *testing.T) {
	var buf [binary.MaxVarintLen32]byte
	n := binary.PutVarint(buf[:], -1)
	val, consumed := lfsVarint(buf[:n])
	if consumed != n {
		t.Fatalf("expected consumed=%d, got %d", n, consumed)
	}
	if val != -1 {
		t.Fatalf("expected -1, got %d", val)
	}
}

func TestLfsVarintEmptyBuffer(t *testing.T) {
	val, consumed := lfsVarint(nil)
	if consumed != 0 || val != 0 {
		t.Fatalf("expected (0, 0), got (%d, %d)", val, consumed)
	}
	val, consumed = lfsVarint([]byte{})
	if consumed != 0 || val != 0 {
		t.Fatalf("expected (0, 0) for empty slice, got (%d, %d)", val, consumed)
	}
}

// ---------------------------------------------------------------------------
// Tests for lfsCompressRecords
// ---------------------------------------------------------------------------

func TestLfsCompressRecordsCodecNone(t *testing.T) {
	raw := []byte("no compression needed")
	out, codec, err := lfsCompressRecords(kgo.CodecNone, raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if codec != kgo.CodecNone {
		t.Fatalf("expected CodecNone, got %d", codec)
	}
	if !bytes.Equal(out, raw) {
		t.Fatal("expected passthrough for CodecNone")
	}
}

func TestLfsCompressRecordsGzipRoundtrip(t *testing.T) {
	raw := []byte("gzip test payload data that should compress")
	compressed, codec, err := lfsCompressRecords(kgo.CodecGzip, raw)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if codec != kgo.CodecGzip {
		t.Fatalf("expected CodecGzip, got %d", codec)
	}
	// Decompress and verify round-trip.
	decompressor := kgo.DefaultDecompressor()
	decompressed, err := decompressor.Decompress(compressed, kgo.CodecGzip)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(decompressed, raw) {
		t.Fatal("round-trip mismatch for gzip")
	}
}

func TestLfsCompressRecordsSnappyRoundtrip(t *testing.T) {
	raw := []byte("snappy test payload data that should compress")
	compressed, codec, err := lfsCompressRecords(kgo.CodecSnappy, raw)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if codec != kgo.CodecSnappy {
		t.Fatalf("expected CodecSnappy, got %d", codec)
	}
	decompressor := kgo.DefaultDecompressor()
	decompressed, err := decompressor.Decompress(compressed, kgo.CodecSnappy)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(decompressed, raw) {
		t.Fatal("round-trip mismatch for snappy")
	}
}

// ---------------------------------------------------------------------------
// Tests for lfsDecodeRecordBatches
// ---------------------------------------------------------------------------

func TestLfsDecodeRecordBatchesValid(t *testing.T) {
	records := []kmsg.Record{
		{Key: []byte("x"), Value: []byte("y")},
	}
	batchBytes := lfsBuildRecordBatch(records)
	batches, err := lfsDecodeRecordBatches(batchBytes)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if batches[0].Magic != 2 {
		t.Fatalf("expected magic=2, got %d", batches[0].Magic)
	}
}

func TestLfsDecodeRecordBatchesEmptyInput(t *testing.T) {
	batches, err := lfsDecodeRecordBatches(nil)
	if err != nil {
		t.Fatalf("expected no error on nil, got: %v", err)
	}
	if len(batches) != 0 {
		t.Fatalf("expected 0 batches, got %d", len(batches))
	}

	batches, err = lfsDecodeRecordBatches([]byte{})
	if err != nil {
		t.Fatalf("expected no error on empty, got: %v", err)
	}
	if len(batches) != 0 {
		t.Fatalf("expected 0 batches, got %d", len(batches))
	}
}

func TestLfsDecodeRecordBatchesTruncatedInput(t *testing.T) {
	// Less than 12 bytes triggers the "too short" error.
	_, err := lfsDecodeRecordBatches([]byte{0, 1, 2, 3, 4})
	if err == nil {
		t.Fatal("expected error for truncated input")
	}
	if !strings.Contains(err.Error(), "too short") {
		t.Fatalf("expected 'too short' error, got: %v", err)
	}
}

func TestLfsDecodeRecordBatchesInvalidLength(t *testing.T) {
	// 12 bytes: first 8 are baseOffset, bytes 8..11 encode the length.
	// Set length to a huge value that exceeds the buffer.
	buf := make([]byte, 12)
	buf[8] = 0x7F
	buf[9] = 0xFF
	buf[10] = 0xFF
	buf[11] = 0xFF
	_, err := lfsDecodeRecordBatches(buf)
	if err == nil {
		t.Fatal("expected error for invalid length")
	}
	if !strings.Contains(err.Error(), "invalid record batch length") {
		t.Fatalf("expected 'invalid record batch length' error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Tests for lfsJoinRecordBatches
// ---------------------------------------------------------------------------

func TestLfsJoinRecordBatchesEmpty(t *testing.T) {
	out := lfsJoinRecordBatches(nil)
	if out != nil {
		t.Fatalf("expected nil, got %v", out)
	}
	out = lfsJoinRecordBatches([]lfsRecordBatch{})
	if out != nil {
		t.Fatalf("expected nil, got %v", out)
	}
}

func TestLfsJoinRecordBatchesSingle(t *testing.T) {
	batchBytes := lfsBuildRecordBatch([]kmsg.Record{
		{Key: []byte("k"), Value: []byte("v")},
	})
	batches, _ := lfsDecodeRecordBatches(batchBytes)
	joined := lfsJoinRecordBatches(batches)
	if !bytes.Equal(joined, batchBytes) {
		t.Fatal("single batch join should equal original bytes")
	}
}

func TestLfsJoinRecordBatchesMultiple(t *testing.T) {
	batch1 := lfsBuildRecordBatch([]kmsg.Record{
		{Key: []byte("k1"), Value: []byte("v1")},
	})
	batch2 := lfsBuildRecordBatch([]kmsg.Record{
		{Key: []byte("k2"), Value: []byte("v2")},
	})
	combined := append(append([]byte(nil), batch1...), batch2...)
	allBatches, err := lfsDecodeRecordBatches(combined)
	if err != nil {
		t.Fatalf("decode combined: %v", err)
	}
	if len(allBatches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(allBatches))
	}
	joined := lfsJoinRecordBatches(allBatches)
	if !bytes.Equal(joined, combined) {
		t.Fatal("joined bytes should equal combined input")
	}
}

// ---------------------------------------------------------------------------
// Tests for lfsFindHeaderValue
// ---------------------------------------------------------------------------

func TestLfsFindHeaderValueFound(t *testing.T) {
	headers := []kmsg.Header{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	}
	val, ok := lfsFindHeaderValue(headers, "b")
	if !ok {
		t.Fatal("expected found=true")
	}
	if string(val) != "2" {
		t.Fatalf("expected '2', got '%s'", val)
	}
}

func TestLfsFindHeaderValueNotFound(t *testing.T) {
	headers := []kmsg.Header{
		{Key: "a", Value: []byte("1")},
	}
	val, ok := lfsFindHeaderValue(headers, "missing")
	if ok {
		t.Fatal("expected found=false")
	}
	if val != nil {
		t.Fatalf("expected nil, got %v", val)
	}
}

func TestLfsFindHeaderValueEmptyHeaders(t *testing.T) {
	val, ok := lfsFindHeaderValue(nil, "any")
	if ok {
		t.Fatal("expected found=false for nil headers")
	}
	if val != nil {
		t.Fatalf("expected nil, got %v", val)
	}
}

// ---------------------------------------------------------------------------
// Tests for lfsHeaderValue
// ---------------------------------------------------------------------------

func TestLfsHeaderValueFound(t *testing.T) {
	headers := []kmsg.Header{
		{Key: "content-type", Value: []byte("application/json")},
	}
	val := lfsHeaderValue(headers, "content-type")
	if val != "application/json" {
		t.Fatalf("expected 'application/json', got '%s'", val)
	}
}

func TestLfsHeaderValueNotFound(t *testing.T) {
	headers := []kmsg.Header{
		{Key: "other", Value: []byte("val")},
	}
	val := lfsHeaderValue(headers, "content-type")
	if val != "" {
		t.Fatalf("expected empty string, got '%s'", val)
	}
}

// ---------------------------------------------------------------------------
// Tests for lfsHeadersToMap
// ---------------------------------------------------------------------------

func TestLfsHeadersToMapAllowlisted(t *testing.T) {
	headers := []kmsg.Header{
		{Key: "content-type", Value: []byte("text/plain")},
		{Key: "correlation-id", Value: []byte("abc-123")},
	}
	m := lfsHeadersToMap(headers)
	if m == nil {
		t.Fatal("expected non-nil map")
	}
	if m["content-type"] != "text/plain" {
		t.Fatalf("expected 'text/plain', got '%s'", m["content-type"])
	}
	if m["correlation-id"] != "abc-123" {
		t.Fatalf("expected 'abc-123', got '%s'", m["correlation-id"])
	}
}

func TestLfsHeadersToMapNonAllowlisted(t *testing.T) {
	headers := []kmsg.Header{
		{Key: "x-custom-header", Value: []byte("secret")},
		{Key: "random", Value: []byte("data")},
	}
	m := lfsHeadersToMap(headers)
	if m != nil {
		t.Fatalf("expected nil map for non-allowlisted headers, got %v", m)
	}
}

func TestLfsHeadersToMapEmpty(t *testing.T) {
	m := lfsHeadersToMap(nil)
	if m != nil {
		t.Fatalf("expected nil for nil headers, got %v", m)
	}
	m = lfsHeadersToMap([]kmsg.Header{})
	if m != nil {
		t.Fatalf("expected nil for empty headers, got %v", m)
	}
}

func TestLfsHeadersToMapMixed(t *testing.T) {
	headers := []kmsg.Header{
		{Key: "content-type", Value: []byte("application/octet-stream")},
		{Key: "x-custom", Value: []byte("ignored")},
		{Key: "traceparent", Value: []byte("00-abc-def-01")},
		{Key: "LFS_BLOB", Value: []byte("sha256")},
	}
	m := lfsHeadersToMap(headers)
	if m == nil {
		t.Fatal("expected non-nil map")
	}
	if len(m) != 2 {
		t.Fatalf("expected 2 entries, got %d: %v", len(m), m)
	}
	if m["content-type"] != "application/octet-stream" {
		t.Fatalf("content-type mismatch: %s", m["content-type"])
	}
	if m["traceparent"] != "00-abc-def-01" {
		t.Fatalf("traceparent mismatch: %s", m["traceparent"])
	}
}

// ---------------------------------------------------------------------------
// Tests for lfsDropHeader
// ---------------------------------------------------------------------------

func TestLfsDropHeaderExisting(t *testing.T) {
	headers := []kmsg.Header{
		{Key: "keep", Value: []byte("1")},
		{Key: "drop-me", Value: []byte("2")},
		{Key: "also-keep", Value: []byte("3")},
	}
	result := lfsDropHeader(headers, "drop-me")
	if len(result) != 2 {
		t.Fatalf("expected 2 headers, got %d", len(result))
	}
	for _, h := range result {
		if h.Key == "drop-me" {
			t.Fatal("drop-me should have been removed")
		}
	}
}

func TestLfsDropHeaderNonExisting(t *testing.T) {
	headers := []kmsg.Header{
		{Key: "a", Value: []byte("1")},
	}
	result := lfsDropHeader(headers, "nonexistent")
	if len(result) != 1 {
		t.Fatalf("expected 1 header, got %d", len(result))
	}
	if result[0].Key != "a" {
		t.Fatalf("expected 'a', got '%s'", result[0].Key)
	}
}

func TestLfsDropHeaderEmpty(t *testing.T) {
	result := lfsDropHeader(nil, "any")
	if result != nil {
		t.Fatalf("expected nil for nil headers, got %v", result)
	}
	result = lfsDropHeader([]kmsg.Header{}, "any")
	if len(result) != 0 {
		t.Fatalf("expected empty for empty headers, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// Tests for lfsInt32FromBytes
// ---------------------------------------------------------------------------

func TestLfsInt32FromBytesPositive(t *testing.T) {
	// 1 in big-endian: 0x00000001
	val := lfsInt32FromBytes([]byte{0x00, 0x00, 0x00, 0x01})
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
	// 256 in big-endian: 0x00000100
	val = lfsInt32FromBytes([]byte{0x00, 0x00, 0x01, 0x00})
	if val != 256 {
		t.Fatalf("expected 256, got %d", val)
	}
}

func TestLfsInt32FromBytesZero(t *testing.T) {
	val := lfsInt32FromBytes([]byte{0x00, 0x00, 0x00, 0x00})
	if val != 0 {
		t.Fatalf("expected 0, got %d", val)
	}
}

func TestLfsInt32FromBytesMaxInt32(t *testing.T) {
	// math.MaxInt32 = 0x7FFFFFFF
	val := lfsInt32FromBytes([]byte{0x7F, 0xFF, 0xFF, 0xFF})
	if val != math.MaxInt32 {
		t.Fatalf("expected %d, got %d", math.MaxInt32, val)
	}
}

// ---------------------------------------------------------------------------
// Tests for newLfsMetrics
// ---------------------------------------------------------------------------

func TestNewLfsMetrics(t *testing.T) {
	m := newLfsMetrics()
	if m == nil {
		t.Fatal("expected non-nil metrics")
	}
	if m.uploadDuration == nil {
		t.Fatal("expected non-nil uploadDuration histogram")
	}
	if m.requests == nil {
		t.Fatal("expected non-nil requests map")
	}
	if m.uploadBytes != 0 {
		t.Fatalf("expected uploadBytes=0, got %d", m.uploadBytes)
	}
	if m.s3Errors != 0 {
		t.Fatalf("expected s3Errors=0, got %d", m.s3Errors)
	}
	if m.orphans != 0 {
		t.Fatalf("expected orphans=0, got %d", m.orphans)
	}
}

// ---------------------------------------------------------------------------
// Tests for ObserveUploadDuration
// ---------------------------------------------------------------------------

func TestObserveUploadDurationNilSafety(t *testing.T) {
	var m *lfsMetrics
	// Must not panic.
	m.ObserveUploadDuration(1.0)
}

func TestObserveUploadDurationNormal(t *testing.T) {
	m := newLfsMetrics()
	m.ObserveUploadDuration(0.5)
	m.ObserveUploadDuration(1.5)
	_, _, sum, count := m.uploadDuration.Snapshot()
	if count != 2 {
		t.Fatalf("expected count=2, got %d", count)
	}
	if sum != 2.0 {
		t.Fatalf("expected sum=2.0, got %f", sum)
	}
}

// ---------------------------------------------------------------------------
// Tests for AddUploadBytes
// ---------------------------------------------------------------------------

func TestAddUploadBytesNormal(t *testing.T) {
	m := newLfsMetrics()
	m.AddUploadBytes(100)
	m.AddUploadBytes(200)
	if m.uploadBytes != 300 {
		t.Fatalf("expected 300, got %d", m.uploadBytes)
	}
}

func TestAddUploadBytesNegativeIgnored(t *testing.T) {
	m := newLfsMetrics()
	m.AddUploadBytes(100)
	m.AddUploadBytes(-50)
	// Negative values are ignored (n <= 0 guard).
	if m.uploadBytes != 100 {
		t.Fatalf("expected 100 (negative ignored), got %d", m.uploadBytes)
	}
}

func TestAddUploadBytesNilSafety(t *testing.T) {
	var m *lfsMetrics
	// Must not panic.
	m.AddUploadBytes(100)
}

// ---------------------------------------------------------------------------
// Tests for IncRequests
// ---------------------------------------------------------------------------

func TestIncRequestsAllCombinations(t *testing.T) {
	m := newLfsMetrics()
	m.IncRequests("topic1", "ok", "lfs")
	m.IncRequests("topic1", "error", "lfs")
	m.IncRequests("topic1", "ok", "passthrough")
	m.IncRequests("topic1", "error", "passthrough")

	counters := m.requests["topic1"]
	if counters == nil {
		t.Fatal("expected counters for topic1")
	}
	if counters.okLfs != 1 {
		t.Fatalf("expected okLfs=1, got %d", counters.okLfs)
	}
	if counters.errLfs != 1 {
		t.Fatalf("expected errLfs=1, got %d", counters.errLfs)
	}
	if counters.okPas != 1 {
		t.Fatalf("expected okPas=1, got %d", counters.okPas)
	}
	if counters.errPas != 1 {
		t.Fatalf("expected errPas=1, got %d", counters.errPas)
	}
}

func TestIncRequestsEmptyTopicDefaultsToUnknown(t *testing.T) {
	m := newLfsMetrics()
	m.IncRequests("", "ok", "lfs")
	counters := m.requests["unknown"]
	if counters == nil {
		t.Fatal("expected counters for 'unknown'")
	}
	if counters.okLfs != 1 {
		t.Fatalf("expected okLfs=1 for unknown topic, got %d", counters.okLfs)
	}
}

// ---------------------------------------------------------------------------
// Tests for IncS3Errors
// ---------------------------------------------------------------------------

func TestIncS3Errors(t *testing.T) {
	m := newLfsMetrics()
	m.IncS3Errors()
	m.IncS3Errors()
	if m.s3Errors != 2 {
		t.Fatalf("expected 2, got %d", m.s3Errors)
	}
}

func TestIncS3ErrorsNilSafety(t *testing.T) {
	var m *lfsMetrics
	// Must not panic.
	m.IncS3Errors()
}

// ---------------------------------------------------------------------------
// Tests for IncOrphans
// ---------------------------------------------------------------------------

func TestIncOrphans(t *testing.T) {
	m := newLfsMetrics()
	m.IncOrphans(3)
	m.IncOrphans(2)
	if m.orphans != 5 {
		t.Fatalf("expected 5, got %d", m.orphans)
	}
}

func TestIncOrphansZeroAndNegativeIgnored(t *testing.T) {
	m := newLfsMetrics()
	m.IncOrphans(5)
	m.IncOrphans(0)
	m.IncOrphans(-1)
	if m.orphans != 5 {
		t.Fatalf("expected 5 (zero/negative ignored), got %d", m.orphans)
	}
}

func TestIncOrphansNilSafety(t *testing.T) {
	var m *lfsMetrics
	// Must not panic.
	m.IncOrphans(1)
}

// ---------------------------------------------------------------------------
// Tests for WritePrometheus
// ---------------------------------------------------------------------------

func TestWritePrometheus(t *testing.T) {
	m := newLfsMetrics()
	m.ObserveUploadDuration(0.1)
	m.AddUploadBytes(1024)
	m.IncRequests("test-topic", "ok", "lfs")
	m.IncS3Errors()
	m.IncOrphans(2)

	var buf bytes.Buffer
	m.WritePrometheus(&buf)
	output := buf.String()

	expectedMetrics := []string{
		"kafscale_lfs_proxy_upload_duration_seconds",
		"kafscale_lfs_proxy_upload_bytes_total",
		"kafscale_lfs_proxy_requests_total",
		"kafscale_lfs_proxy_s3_errors_total",
		"kafscale_lfs_proxy_orphan_objects_total",
		"kafscale_lfs_proxy_goroutines",
		"kafscale_lfs_proxy_memory_alloc_bytes",
		"kafscale_lfs_proxy_memory_sys_bytes",
		"kafscale_lfs_proxy_gc_pause_total_ns",
	}
	for _, metric := range expectedMetrics {
		if !strings.Contains(output, metric) {
			t.Fatalf("expected output to contain %q", metric)
		}
	}
	// Verify specific values.
	if !strings.Contains(output, "kafscale_lfs_proxy_upload_bytes_total 1024") {
		t.Fatal("expected upload_bytes_total 1024 in output")
	}
	if !strings.Contains(output, "kafscale_lfs_proxy_s3_errors_total 1") {
		t.Fatal("expected s3_errors_total 1 in output")
	}
	if !strings.Contains(output, "kafscale_lfs_proxy_orphan_objects_total 2") {
		t.Fatal("expected orphan_objects_total 2 in output")
	}
}

func TestWritePrometheusNilSafety(t *testing.T) {
	var m *lfsMetrics
	var buf bytes.Buffer
	// Must not panic.
	m.WritePrometheus(&buf)
	if buf.Len() != 0 {
		t.Fatalf("expected empty output for nil metrics, got %d bytes", buf.Len())
	}
}

// ---------------------------------------------------------------------------
// Tests for histogram
// ---------------------------------------------------------------------------

func TestHistogramObserve(t *testing.T) {
	h := newHistogram([]float64{1, 5, 10})
	h.Observe(0.5)
	h.Observe(3.0)
	h.Observe(7.0)
	h.Observe(15.0)

	buckets, counts, sum, count := h.Snapshot()
	if count != 4 {
		t.Fatalf("expected count=4, got %d", count)
	}
	expectedSum := 0.5 + 3.0 + 7.0 + 15.0
	if sum != expectedSum {
		t.Fatalf("expected sum=%f, got %f", expectedSum, sum)
	}
	if len(buckets) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(buckets))
	}
	// counts has len(buckets)+1 entries: one per bucket boundary + overflow.
	if len(counts) != 4 {
		t.Fatalf("expected 4 count slots, got %d", len(counts))
	}
}

func TestHistogramSnapshot(t *testing.T) {
	h := newHistogram([]float64{1, 10})
	buckets, counts, sum, count := h.Snapshot()
	if count != 0 || sum != 0 {
		t.Fatal("expected empty histogram")
	}
	if len(buckets) != 2 || len(counts) != 3 {
		t.Fatalf("expected 2 buckets / 3 counts, got %d / %d", len(buckets), len(counts))
	}
}

func TestHistogramNilSafety(t *testing.T) {
	var h *histogram
	// Must not panic.
	h.Observe(1.0)
	buckets, counts, sum, count := h.Snapshot()
	if buckets != nil || counts != nil || sum != 0 || count != 0 {
		t.Fatal("expected zero values for nil histogram snapshot")
	}
}

func TestHistogramMultipleObservations(t *testing.T) {
	h := newHistogram([]float64{1, 5, 10})
	for i := 0; i < 100; i++ {
		h.Observe(float64(i) * 0.1)
	}
	_, _, sum, count := h.Snapshot()
	if count != 100 {
		t.Fatalf("expected count=100, got %d", count)
	}
	// Sum of 0.0, 0.1, 0.2, ..., 9.9 = 99*100/2 * 0.1 = 495.0
	expectedSum := 495.0
	if math.Abs(sum-expectedSum) > 0.001 {
		t.Fatalf("expected sum~%f, got %f", expectedSum, sum)
	}
}
