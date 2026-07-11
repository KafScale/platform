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

package console

import (
	"context"
	"testing"
)

func TestNewLFSS3ClientNoBucket(t *testing.T) {
	client, err := NewLFSS3Client(context.Background(), LFSS3Config{}, nil)
	if err != nil {
		t.Fatalf("NewLFSS3Client: %v", err)
	}
	if client != nil {
		t.Fatal("expected nil client when no bucket configured")
	}
}

func TestNewLFSS3ClientWithConfig(t *testing.T) {
	client, err := NewLFSS3Client(context.Background(), LFSS3Config{
		Bucket:         "test-bucket",
		Region:         "us-west-2",
		Endpoint:       "http://localhost:9000",
		AccessKey:      "minioadmin",
		SecretKey:      "minioadmin",
		ForcePathStyle: true,
	}, nil)
	if err != nil {
		t.Fatalf("NewLFSS3Client: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.bucket != "test-bucket" {
		t.Fatalf("bucket: %q", client.bucket)
	}
}

func TestNewLFSS3ClientDefaultRegion(t *testing.T) {
	client, err := NewLFSS3Client(context.Background(), LFSS3Config{
		Bucket: "test-bucket",
	}, nil)
	if err != nil {
		t.Fatalf("NewLFSS3Client: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}
