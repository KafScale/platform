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

package lfs

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type mockS3API struct {
	getObjectFunc func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

func (m *mockS3API) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return m.getObjectFunc(ctx, params, optFns...)
}

func newTestS3Client(api s3API) *S3Client {
	return &S3Client{bucket: "test-bucket", api: api}
}

func TestS3ClientFetchSuccess(t *testing.T) {
	mock := &mockS3API{
		getObjectFunc: func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			if *params.Key != "test/key" {
				t.Fatalf("unexpected key: %s", *params.Key)
			}
			if *params.Bucket != "test-bucket" {
				t.Fatalf("unexpected bucket: %s", *params.Bucket)
			}
			return &s3.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader("blob data")),
			}, nil
		},
	}

	client := newTestS3Client(mock)
	data, err := client.Fetch(context.Background(), "test/key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "blob data" {
		t.Fatalf("expected 'blob data', got '%s'", data)
	}
}

func TestS3ClientFetchEmptyKey(t *testing.T) {
	client := newTestS3Client(&mockS3API{})
	_, err := client.Fetch(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestS3ClientFetchError(t *testing.T) {
	mock := &mockS3API{
		getObjectFunc: func(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return nil, errors.New("access denied")
		},
	}
	client := newTestS3Client(mock)
	_, err := client.Fetch(context.Background(), "key")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestS3ClientStreamSuccess(t *testing.T) {
	contentLen := int64(100)
	mock := &mockS3API{
		getObjectFunc: func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{
				Body:          io.NopCloser(strings.NewReader("stream data")),
				ContentLength: aws.Int64(contentLen),
			}, nil
		},
	}

	client := newTestS3Client(mock)
	body, length, err := client.Stream(context.Background(), "key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = body.Close() }()
	if length != contentLen {
		t.Fatalf("expected length %d, got %d", contentLen, length)
	}
	data, _ := io.ReadAll(body)
	if string(data) != "stream data" {
		t.Fatalf("expected 'stream data', got '%s'", data)
	}
}

func TestS3ClientStreamEmptyKey(t *testing.T) {
	client := newTestS3Client(&mockS3API{})
	_, _, err := client.Stream(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestS3ClientStreamError(t *testing.T) {
	mock := &mockS3API{
		getObjectFunc: func(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return nil, errors.New("not found")
		},
	}
	client := newTestS3Client(mock)
	_, _, err := client.Stream(context.Background(), "key")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestS3ClientStreamNilContentLength(t *testing.T) {
	mock := &mockS3API{
		getObjectFunc: func(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{
				Body:          io.NopCloser(strings.NewReader("data")),
				ContentLength: nil,
			}, nil
		},
	}
	client := newTestS3Client(mock)
	_, length, err := client.Stream(context.Background(), "key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if length != 0 {
		t.Fatalf("expected length 0 for nil ContentLength, got %d", length)
	}
}
