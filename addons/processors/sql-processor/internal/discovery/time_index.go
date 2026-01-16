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
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/metrics"
)

const (
	timeIndexFooterVersion = 1
	timeIndexFooterMagic = "KFTF"
	timeIndexFooterSize  = 40
)

type timeIndexReader struct {
	client    s3Getter
	bucket    string
	keySuffix string
}

type timeIndexFooter struct {
	MinTS     int64
	MaxTS     int64
	MinOffset int64
	MaxOffset int64
}

func newTimeIndexReader(client s3Getter, bucket, keySuffix string) *timeIndexReader {
	if keySuffix == "" {
		keySuffix = ".kfst"
	}
	return &timeIndexReader{client: client, bucket: bucket, keySuffix: keySuffix}
}

func (r *timeIndexReader) enrich(ctx context.Context, segment *SegmentRef) {
	if segment == nil || segment.SegmentKey == "" {
		return
	}
	if segment.MinTimestamp != nil && segment.MaxTimestamp != nil && segment.MinOffset != nil && segment.MaxOffset != nil {
		return
	}
	key := r.indexKey(segment.SegmentKey)
	if key == "" {
		return
	}
	footer, err := r.readFooter(ctx, key)
	if err != nil {
		return
	}
	if segment.MinTimestamp == nil {
		segment.MinTimestamp = int64Ptr(footer.MinTS)
	}
	if segment.MaxTimestamp == nil {
		segment.MaxTimestamp = int64Ptr(footer.MaxTS)
	}
	if segment.MinOffset == nil {
		segment.MinOffset = int64Ptr(footer.MinOffset)
	}
	if segment.MaxOffset == nil {
		segment.MaxOffset = int64Ptr(footer.MaxOffset)
	}
}

func (r *timeIndexReader) indexKey(segmentKey string) string {
	if !strings.HasSuffix(segmentKey, ".kfs") {
		return ""
	}
	return strings.TrimSuffix(segmentKey, ".kfs") + r.keySuffix
}

func (r *timeIndexReader) readFooter(ctx context.Context, key string) (timeIndexFooter, error) {
	start := time.Now()
	metrics.S3Requests.WithLabelValues("get_time_index").Inc()
	resp, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=-%d", timeIndexFooterSize)),
	})
	metrics.S3Duration.WithLabelValues("get_time_index").Observe(float64(time.Since(start).Milliseconds()))
	if err != nil {
		metrics.S3Errors.WithLabelValues("get_time_index").Inc()
		return timeIndexFooter{}, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	metrics.S3Bytes.Add(float64(len(data)))
	if err != nil {
		metrics.S3Errors.WithLabelValues("get_time_index").Inc()
		return timeIndexFooter{}, err
	}
	return parseTimeIndexFooter(data)
}

func parseTimeIndexFooter(data []byte) (timeIndexFooter, error) {
	if len(data) < timeIndexFooterSize {
		return timeIndexFooter{}, fmt.Errorf("time index footer too short")
	}
	footer := data[len(data)-timeIndexFooterSize:]
	if string(footer[:4]) != timeIndexFooterMagic {
		return timeIndexFooter{}, fmt.Errorf("invalid time index footer magic")
	}
	minTS := int64(binary.BigEndian.Uint64(footer[8:16]))
	maxTS := int64(binary.BigEndian.Uint64(footer[16:24]))
	minOffset := int64(binary.BigEndian.Uint64(footer[24:32]))
	maxOffset := int64(binary.BigEndian.Uint64(footer[32:40]))
	return timeIndexFooter{
		MinTS:     minTS,
		MaxTS:     maxTS,
		MinOffset: minOffset,
		MaxOffset: maxOffset,
	}, nil
}

func encodeTimeIndexFooter(minTS, maxTS, minOffset, maxOffset int64) []byte {
	footer := make([]byte, timeIndexFooterSize)
	copy(footer[:4], []byte(timeIndexFooterMagic))
	binary.BigEndian.PutUint32(footer[4:8], timeIndexFooterVersion)
	binary.BigEndian.PutUint64(footer[8:16], uint64(minTS))
	binary.BigEndian.PutUint64(footer[16:24], uint64(maxTS))
	binary.BigEndian.PutUint64(footer[24:32], uint64(minOffset))
	binary.BigEndian.PutUint64(footer[32:40], uint64(maxOffset))
	return footer
}
