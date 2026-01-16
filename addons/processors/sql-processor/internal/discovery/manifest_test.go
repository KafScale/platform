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
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestParseManifestArray(t *testing.T) {
	payload := []byte(`[
		{
			"topic": "orders",
			"partition": 0,
			"segment_key": "orders/0/segment-10.kfs",
			"index_key": "orders/0/segment-10.index",
			"size_bytes": 100,
			"min_offset": 10,
			"max_offset": 19,
			"min_ts": 1000,
			"max_ts": 2000
		}
	]`)
	entries, err := parseManifest(payload)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if len(entries) != 1 || entries[0].Topic != "orders" {
		t.Fatalf("unexpected entries: %+v", entries)
	}
}

func TestParseManifestDocument(t *testing.T) {
	payload := []byte(`{"entries":[{"topic":"orders","segment_key":"orders/0/segment-10.kfs","index_key":"orders/0/segment-10.index"}]}`)
	entries, err := parseManifest(payload)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if len(entries) != 1 || entries[0].SegmentKey == "" {
		t.Fatalf("unexpected entries: %+v", entries)
	}
}

func TestManifestEntriesToSegments(t *testing.T) {
	partition := int32(1)
	entries := []manifestEntry{
		{
			Topic:        "orders",
			Partition:    &partition,
			SegmentKey:   "orders/1/segment-100.kfs",
			IndexKey:     "orders/1/segment-100.index",
			SizeBytes:    10,
			MinOffset:    int64Ptr(100),
			MaxOffset:    int64Ptr(199),
			MinTimestamp: int64Ptr(1000),
			MaxTimestamp: int64Ptr(2000),
			LastModified: "2024-01-01T00:00:00Z",
		},
	}
	segments := manifestEntriesToSegments("", entries)
	if len(segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segments))
	}
	if segments[0].BaseOffset != 100 {
		t.Fatalf("expected base offset, got %d", segments[0].BaseOffset)
	}
	if segments[0].LastModified.IsZero() {
		t.Fatalf("expected last modified parsed")
	}
}

func TestManifestListerFallback(t *testing.T) {
	fallback := &fakeLister{segments: []SegmentRef{{Topic: "orders"}}}
	lister := newManifestLister(&fakeS3Getter{err: errors.New("no manifest")}, "bucket", "", "manifest.json", 0, fallback)
	segments, err := lister.ListCompleted(context.Background())
	if err != nil {
		t.Fatalf("list completed: %v", err)
	}
	if len(segments) != 1 || segments[0].Topic != "orders" {
		t.Fatalf("unexpected fallback segments: %+v", segments)
	}
}

func TestManifestListerCache(t *testing.T) {
	payload := []byte(`[{"topic":"orders","partition":0,"segment_key":"orders/0/segment-1.kfs","index_key":"orders/0/segment-1.index"}]`)
	getter := &fakeS3Getter{payload: payload}
	lister := newManifestLister(getter, "bucket", "", "manifest.json", time.Minute, nil)
	segments, err := lister.ListCompleted(context.Background())
	if err != nil {
		t.Fatalf("list completed: %v", err)
	}
	segments, err = lister.ListCompleted(context.Background())
	if err != nil {
		t.Fatalf("list completed cached: %v", err)
	}
	if getter.calls != 1 {
		t.Fatalf("expected cached manifest, got %d calls", getter.calls)
	}
	if len(segments) != 1 || segments[0].Topic != "orders" {
		t.Fatalf("unexpected segments: %+v", segments)
	}
}

type fakeLister struct {
	segments []SegmentRef
}

func (f *fakeLister) ListCompleted(ctx context.Context) ([]SegmentRef, error) {
	return f.segments, nil
}

type fakeS3Getter struct {
	payload []byte
	err     error
	calls   int
}

func (f *fakeS3Getter) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	body := io.NopCloser(bytes.NewReader(f.payload))
	return &s3.GetObjectOutput{
		Body:          body,
		ContentLength: aws.Int64(int64(len(f.payload))),
	}, nil
}
