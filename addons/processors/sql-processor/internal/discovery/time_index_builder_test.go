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
	"context"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/decoder"
)

func TestTimeIndexBuilderBuild(t *testing.T) {
	segments := []SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "orders/0/segment-1.kfs", IndexKey: "orders/0/segment-1.index"},
	}
	dec := &fakeDecoder{
		records: []decoder.Record{
			{Offset: 10, Timestamp: 100},
			{Offset: 12, Timestamp: 200},
		},
	}
	putter := &fakeTimeIndexPutter{}
	builder := newTimeIndexBuilder(putter, "bucket", ".kfst", 0, 0, &fakeLister{segments: segments}, dec)
	builder.leaseTTL = time.Millisecond
	if err := builder.Build(context.Background()); err != nil {
		t.Fatalf("build time index: %v", err)
	}
	if putter.key != "orders/0/segment-1.kfst" {
		t.Fatalf("unexpected key: %s", putter.key)
	}
	footer, err := parseTimeIndexFooter(putter.body)
	if err != nil {
		t.Fatalf("parse footer: %v", err)
	}
	if footer.MinTS != 100 || footer.MaxOffset != 12 {
		t.Fatalf("unexpected footer values: %+v", footer)
	}
}

func TestTimeIndexBuilderLimits(t *testing.T) {
	segments := []SegmentRef{
		{Topic: "orders", SegmentKey: "a.kfs", IndexKey: "a.index", SizeBytes: 100},
		{Topic: "orders", SegmentKey: "b.kfs", IndexKey: "b.index", SizeBytes: 100},
	}
	builder := newTimeIndexBuilder(&fakeTimeIndexPutter{}, "bucket", ".kfst", 1, 0, &fakeLister{segments: segments}, &fakeDecoder{})
	builder.leaseTTL = time.Millisecond
	if err := builder.Build(context.Background()); err == nil {
		t.Fatalf("expected max segments error")
	}

	builder = newTimeIndexBuilder(&fakeTimeIndexPutter{}, "bucket", ".kfst", 0, 50, &fakeLister{segments: segments}, &fakeDecoder{})
	builder.leaseTTL = time.Millisecond
	if err := builder.Build(context.Background()); err == nil {
		t.Fatalf("expected max bytes error")
	}
}

type fakeDecoder struct {
	records []decoder.Record
}

func (f *fakeDecoder) Decode(ctx context.Context, segmentKey, indexKey string, topic string, partition int32) ([]decoder.Record, error) {
	return f.records, nil
}

type fakeTimeIndexPutter struct {
	key  string
	body []byte
}

func (f *fakeTimeIndexPutter) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
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
