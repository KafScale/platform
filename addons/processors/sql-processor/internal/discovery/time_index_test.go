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
	"encoding/binary"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestParseTimeIndexFooter(t *testing.T) {
	footer := make([]byte, timeIndexFooterSize)
	copy(footer[:4], []byte(timeIndexFooterMagic))
	binary.BigEndian.PutUint64(footer[8:16], 10)
	binary.BigEndian.PutUint64(footer[16:24], 20)
	binary.BigEndian.PutUint64(footer[24:32], 100)
	binary.BigEndian.PutUint64(footer[32:40], 200)

	out, err := parseTimeIndexFooter(footer)
	if err != nil {
		t.Fatalf("parse footer: %v", err)
	}
	if out.MinTS != 10 || out.MaxTS != 20 || out.MinOffset != 100 || out.MaxOffset != 200 {
		t.Fatalf("unexpected footer: %+v", out)
	}
}

func TestParseTimeIndexFooterInvalid(t *testing.T) {
	if _, err := parseTimeIndexFooter([]byte("bad")); err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestTimeIndexReaderEnrich(t *testing.T) {
	footer := make([]byte, timeIndexFooterSize)
	copy(footer[:4], []byte(timeIndexFooterMagic))
	binary.BigEndian.PutUint64(footer[8:16], 10)
	binary.BigEndian.PutUint64(footer[16:24], 20)
	binary.BigEndian.PutUint64(footer[24:32], 100)
	binary.BigEndian.PutUint64(footer[32:40], 200)
	getter := &fakeTimeIndexGetter{payload: footer}

	reader := newTimeIndexReader(getter, "bucket", ".kfst")
	segment := SegmentRef{SegmentKey: "orders/0/segment-1.kfs"}
	reader.enrich(context.Background(), &segment)
	if segment.MinTimestamp == nil || *segment.MinTimestamp != 10 {
		t.Fatalf("expected min timestamp")
	}
	if segment.MaxOffset == nil || *segment.MaxOffset != 200 {
		t.Fatalf("expected max offset")
	}
}

type fakeTimeIndexGetter struct {
	payload []byte
}

func (f *fakeTimeIndexGetter) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	body := io.NopCloser(bytes.NewReader(f.payload))
	return &s3.GetObjectOutput{
		Body:          body,
		ContentLength: aws.Int64(int64(len(f.payload))),
	}, nil
}
