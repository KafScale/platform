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
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
