// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package discovery

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestManifestBuilderBuild(t *testing.T) {
	segments := []SegmentRef{
		{
			Topic:      "orders",
			Partition:  0,
			SegmentKey: "prefix/orders/0/segment-10.kfs",
			IndexKey:   "prefix/orders/0/segment-10.index",
			SizeBytes:  100,
			MinOffset:  int64Ptr(10),
			MaxOffset:  int64Ptr(19),
		},
	}
	builder := newManifestBuilder(&fakeS3Putter{}, "bucket", "prefix/", "manifest.json", 0, 0, &fakeLister{segments: segments})
	builder.leaseTTL = time.Millisecond
	if err := builder.Build(context.Background()); err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	putter := builder.client.(*fakeS3Putter)
	if putter.key != "prefix/manifest.json" {
		t.Fatalf("unexpected manifest key: %s", putter.key)
	}
	if !bytes.Contains(putter.body, []byte(`"segment_key":"orders/0/segment-10.kfs"`)) {
		t.Fatalf("expected trimmed segment key in manifest: %s", string(putter.body))
	}
}

func TestManifestBuilderLimits(t *testing.T) {
	segments := []SegmentRef{
		{Topic: "orders", SegmentKey: "a", IndexKey: "b", SizeBytes: 100},
		{Topic: "orders", SegmentKey: "c", IndexKey: "d", SizeBytes: 100},
	}
	builder := newManifestBuilder(&fakeS3Putter{}, "bucket", "", "manifest.json", 1, 0, &fakeLister{segments: segments})
	builder.leaseTTL = time.Millisecond
	if err := builder.Build(context.Background()); err == nil {
		t.Fatalf("expected max segments error")
	}

	builder = newManifestBuilder(&fakeS3Putter{}, "bucket", "", "manifest.json", 0, 50, &fakeLister{segments: segments})
	builder.leaseTTL = time.Millisecond
	if err := builder.Build(context.Background()); err == nil {
		t.Fatalf("expected max bytes error")
	}
}

type fakeS3Putter struct {
	key  string
	body []byte
}

func (f *fakeS3Putter) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.key = ""
	if params.Key != nil {
		f.key = *params.Key
	}
	if params.Body != nil {
		data, _ := io.ReadAll(params.Body)
		f.body = data
	}
	return &s3.PutObjectOutput{}, nil
}
