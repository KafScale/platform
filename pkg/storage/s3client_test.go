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
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type fakeS3 struct {
	putInputs []*s3.PutObjectInput
	getInput  *s3.GetObjectInput
	getData   []byte
	putErr    error
	getErr    error
	headErr   error
	createErr error
	listErr   error
}

func (f *fakeS3) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.putInputs = append(f.putInputs, params)
	return &s3.PutObjectOutput{}, f.putErr
}

func (f *fakeS3) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.getInput = params
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(f.getData)),
	}, nil
}

func (f *fakeS3) HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return &s3.HeadBucketOutput{}, f.headErr
}

func (f *fakeS3) CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	return &s3.CreateBucketOutput{}, f.createErr
}

func (f *fakeS3) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{}, f.listErr
}

func TestAWSS3Client_Upload(t *testing.T) {
	api := &fakeS3{}
	client := newAWSClientWithAPI("test-bucket", "us-east-1", "arn:kms", api)

	err := client.UploadSegment(context.Background(), "topic/0/segment-0", []byte("payload"))
	if err != nil {
		t.Fatalf("UploadSegment: %v", err)
	}
	if len(api.putInputs) != 1 {
		t.Fatalf("expected 1 put input got %d", len(api.putInputs))
	}
	input := api.putInputs[0]
	if *input.Bucket != "test-bucket" || *input.Key != "topic/0/segment-0" {
		t.Fatalf("bucket/key mismatch: %#v", input)
	}
	if input.ServerSideEncryption == "" || input.SSEKMSKeyId == nil || *input.SSEKMSKeyId != "arn:kms" {
		t.Fatalf("expected kms encryption: %#v", input)
	}
}

func TestAWSS3Client_Download(t *testing.T) {
	api := &fakeS3{getData: []byte("hello")}
	client := newAWSClientWithAPI("test-bucket", "us-east-1", "", api)

	rng := &ByteRange{Start: 0, End: 10}
	data, err := client.DownloadSegment(context.Background(), "topic/segment", rng)
	if err != nil {
		t.Fatalf("DownloadSegment: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("unexpected data: %s", data)
	}
	if api.getInput == nil || api.getInput.Range == nil || *api.getInput.Range != "bytes=0-10" {
		t.Fatalf("range header missing: %#v", api.getInput)
	}
	if *api.getInput.Bucket != "test-bucket" {
		t.Fatalf("bucket mismatch: %s", aws.ToString(api.getInput.Bucket))
	}
}

func TestAWSS3Client_DownloadNoRange(t *testing.T) {
	api := &fakeS3{getData: []byte("fulldata")}
	client := newAWSClientWithAPI("test-bucket", "us-east-1", "", api)

	data, err := client.DownloadSegment(context.Background(), "key", nil)
	if err != nil {
		t.Fatalf("DownloadSegment: %v", err)
	}
	if string(data) != "fulldata" {
		t.Fatalf("unexpected data: %s", data)
	}
	if api.getInput.Range != nil {
		t.Fatal("expected nil range header")
	}
}

func TestAWSS3Client_DownloadError(t *testing.T) {
	api := &fakeS3{getErr: errors.New("access denied")}
	client := newAWSClientWithAPI("test-bucket", "us-east-1", "", api)

	_, err := client.DownloadSegment(context.Background(), "key", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestAWSS3Client_DownloadIndex(t *testing.T) {
	api := &fakeS3{getData: []byte("index-data")}
	client := newAWSClientWithAPI("test-bucket", "us-east-1", "", api)

	data, err := client.DownloadIndex(context.Background(), "topic/index")
	if err != nil {
		t.Fatalf("DownloadIndex: %v", err)
	}
	if string(data) != "index-data" {
		t.Fatalf("unexpected data: %s", data)
	}
}

func TestAWSS3Client_DownloadIndexNotFound(t *testing.T) {
	api := &fakeS3{getErr: &fakeAPIError{code: "NoSuchKey"}}
	client := newAWSClientWithAPI("test-bucket", "us-east-1", "", api)

	_, err := client.DownloadIndex(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestAWSS3Client_UploadIndex(t *testing.T) {
	api := &fakeS3{}
	client := newAWSClientWithAPI("test-bucket", "us-east-1", "", api)

	err := client.UploadIndex(context.Background(), "index/key", []byte("idx"))
	if err != nil {
		t.Fatalf("UploadIndex: %v", err)
	}
	if len(api.putInputs) != 1 {
		t.Fatalf("expected 1 put, got %d", len(api.putInputs))
	}
}

func TestAWSS3Client_UploadNoKMS(t *testing.T) {
	api := &fakeS3{}
	client := newAWSClientWithAPI("test-bucket", "us-east-1", "", api)

	err := client.UploadSegment(context.Background(), "key", []byte("data"))
	if err != nil {
		t.Fatalf("UploadSegment: %v", err)
	}
	if api.putInputs[0].SSEKMSKeyId != nil {
		t.Fatal("expected no KMS key when empty")
	}
}

func TestAWSS3Client_EnsureBucket(t *testing.T) {
	api := &fakeS3{}
	client := newAWSClientWithAPI("test-bucket", "us-east-1", "", api)

	err := client.EnsureBucket(context.Background())
	if err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}
}

func TestAWSS3Client_EnsureBucketAlreadyExists(t *testing.T) {
	api := &fakeS3{
		headErr:   &fakeAPIError{code: "NotFound"},
		createErr: &fakeAPIError{code: "BucketAlreadyOwnedByYou"},
	}
	client := newAWSClientWithAPI("test-bucket", "us-east-1", "", api)

	err := client.EnsureBucket(context.Background())
	if err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}
}

func TestBucketLocationConfig(t *testing.T) {
	c := &awsS3Client{region: "us-east-1"}
	if c.bucketLocationConfig() != nil {
		t.Fatal("us-east-1 should return nil config")
	}
	c2 := &awsS3Client{region: ""}
	if c2.bucketLocationConfig() != nil {
		t.Fatal("empty region should return nil config")
	}
	c3 := &awsS3Client{region: "eu-west-1"}
	cfg := c3.bucketLocationConfig()
	if cfg == nil {
		t.Fatal("non-us-east-1 should return config")
	}
}

func TestIsNotFoundErr(t *testing.T) {
	if isNotFoundErr(nil) {
		t.Fatal("nil should not be not-found")
	}
	if !isNotFoundErr(&fakeAPIError{code: "NoSuchKey"}) {
		t.Fatal("NoSuchKey should be not-found")
	}
	if !isNotFoundErr(&fakeAPIError{code: "NotFound"}) {
		t.Fatal("NotFound should be not-found")
	}
	if isNotFoundErr(errors.New("random error")) {
		t.Fatal("random error should not be not-found")
	}
}

func TestIsBucketMissingErr(t *testing.T) {
	if isBucketMissingErr(nil) {
		t.Fatal("nil should not be bucket-missing")
	}
	if !isBucketMissingErr(&fakeAPIError{code: "NoSuchBucket"}) {
		t.Fatal("NoSuchBucket should be bucket-missing")
	}
	if !isBucketMissingErr(&fakeAPIError{code: "NotFound"}) {
		t.Fatal("NotFound should be bucket-missing")
	}
	if isBucketMissingErr(errors.New("random error")) {
		t.Fatal("random error should not be bucket-missing")
	}
}

func TestByteRangeHeaderValue(t *testing.T) {
	br := &ByteRange{Start: 10, End: 20}
	val := br.headerValue()
	if val == nil || *val != "bytes=10-20" {
		t.Fatalf("expected bytes=10-20, got %v", val)
	}

	var nilBR *ByteRange
	if nilBR.headerValue() != nil {
		t.Fatal("nil ByteRange should return nil header")
	}
}

func TestNewS3ClientValidation(t *testing.T) {
	_, err := NewS3Client(context.Background(), S3Config{Bucket: "", Region: "us-east-1"})
	if err == nil {
		t.Fatal("expected error for empty bucket")
	}
	_, err = NewS3Client(context.Background(), S3Config{Bucket: "b", Region: ""})
	if err == nil {
		t.Fatal("expected error for empty region")
	}
}

// fakeAPIError implements smithy.APIError
type fakeAPIError struct {
	code string
}

func (e *fakeAPIError) Error() string   { return e.code }
func (e *fakeAPIError) ErrorCode() string   { return e.code }
func (e *fakeAPIError) ErrorMessage() string { return e.code }
func (e *fakeAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }

func TestMemoryS3Client_EnsureBucket(t *testing.T) {
	m := NewMemoryS3Client()
	err := m.EnsureBucket(context.Background())
	if err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}
}

func TestMemoryS3Client_UploadAndDownload(t *testing.T) {
	m := NewMemoryS3Client()
	_ = m.UploadIndex(context.Background(), "idx/key", []byte("index"))

	data, err := m.DownloadIndex(context.Background(), "idx/key")
	if err != nil {
		t.Fatalf("DownloadIndex: %v", err)
	}
	if string(data) != "index" {
		t.Fatalf("unexpected index data: %s", data)
	}

	_, err = m.DownloadIndex(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected error for missing index")
	}
}

func TestMemoryS3Client_DownloadSegmentRange(t *testing.T) {
	m := NewMemoryS3Client()
	_ = m.UploadSegment(context.Background(), "seg", []byte("0123456789"))

	// Full download
	data, err := m.DownloadSegment(context.Background(), "seg", nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "0123456789" {
		t.Fatalf("expected full data, got %s", data)
	}

	// Range download
	data, err = m.DownloadSegment(context.Background(), "seg", &ByteRange{Start: 2, End: 5})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "2345" {
		t.Fatalf("expected '2345', got '%s'", data)
	}

	// Not found
	_, err = m.DownloadSegment(context.Background(), "missing", nil)
	if err == nil {
		t.Fatal("expected error for missing segment")
	}
}

func TestMemoryS3Client_ListSegments(t *testing.T) {
	m := NewMemoryS3Client()
	_ = m.UploadSegment(context.Background(), "topic/0/seg-0", []byte("a"))
	_ = m.UploadSegment(context.Background(), "topic/0/seg-1", []byte("bb"))
	_ = m.UploadSegment(context.Background(), "topic/1/seg-0", []byte("ccc"))

	objs, err := m.ListSegments(context.Background(), "topic/0/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(objs))
	}

	// Non-matching prefix
	objs, err = m.ListSegments(context.Background(), "other/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(objs))
	}
}
