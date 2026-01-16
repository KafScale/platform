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
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/decoder"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/metrics"
)

type TimeIndexBuilder struct {
	client      s3Putter
	bucket      string
	keySuffix   string
	lister      Lister
	decoder     decoder.Decoder
	maxSegments int
	maxBytes    int64
	lease       *buildLease
	leaseTTL    time.Duration
}

func NewTimeIndexBuilder(cfg config.Config, lister Lister) (*TimeIndexBuilder, error) {
	client, err := newS3Client(cfg)
	if err != nil {
		return nil, err
	}
	dec, err := decoder.New(cfg)
	if err != nil {
		return nil, err
	}
	builder := newTimeIndexBuilder(
		client,
		cfg.S3.Bucket,
		cfg.TimeIndex.KeySuffix,
		cfg.TimeIndex.BuildMaxSegments,
		cfg.TimeIndex.BuildMaxBytes,
		lister,
		dec,
	)
	builder.leaseTTL = time.Duration(cfg.TimeIndex.BuildLeaseTTLSeconds) * time.Second
	return builder, nil
}

func newTimeIndexBuilder(client s3Putter, bucket, keySuffix string, maxSegments int, maxBytes int64, lister Lister, dec decoder.Decoder) *TimeIndexBuilder {
	if keySuffix == "" {
		keySuffix = ".kfst"
	}
	return &TimeIndexBuilder{
		client:      client,
		bucket:      bucket,
		keySuffix:   keySuffix,
		lister:      lister,
		decoder:     dec,
		maxSegments: maxSegments,
		maxBytes:    maxBytes,
		lease:       newBuildLease(),
		leaseTTL:    120 * time.Second,
	}
}

func (b *TimeIndexBuilder) Build(ctx context.Context) error {
	release, err := b.lease.Acquire(ctx, "time-index", b.leaseTTL)
	if err != nil {
		return err
	}
	defer release()

	if b.lister == nil || b.decoder == nil {
		return fmt.Errorf("time index builder requires lister and decoder")
	}
	segments, err := b.lister.ListCompleted(ctx)
	if err != nil {
		return err
	}
	if b.maxSegments > 0 && len(segments) > b.maxSegments {
		return fmt.Errorf("time index build exceeds max segments (%d)", b.maxSegments)
	}
	if b.maxBytes > 0 && segmentsSizeBytes(segments) > b.maxBytes {
		return fmt.Errorf("time index build exceeds max bytes (%d)", b.maxBytes)
	}

	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if segment.SegmentKey == "" || segment.IndexKey == "" {
			continue
		}
		minTS, maxTS, minOffset, maxOffset, ok := b.scanSegment(ctx, segment)
		if !ok {
			continue
		}
		footer := encodeTimeIndexFooter(minTS, maxTS, minOffset, maxOffset)
		key := b.indexKey(segment.SegmentKey)
		if key == "" {
			continue
		}

		start := time.Now()
		metrics.S3Requests.WithLabelValues("put_time_index").Inc()
		_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(b.bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(footer),
			ContentType: aws.String("application/octet-stream"),
		})
		metrics.S3Duration.WithLabelValues("put_time_index").Observe(float64(time.Since(start).Milliseconds()))
		if err != nil {
			metrics.S3Errors.WithLabelValues("put_time_index").Inc()
			return fmt.Errorf("put time index: %w", err)
		}
	}
	return nil
}

func (b *TimeIndexBuilder) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = b.Build(ctx)
		}
	}
}

func (b *TimeIndexBuilder) scanSegment(ctx context.Context, segment SegmentRef) (int64, int64, int64, int64, bool) {
	records, err := b.decoder.Decode(ctx, segment.SegmentKey, segment.IndexKey, segment.Topic, segment.Partition)
	if err != nil || len(records) == 0 {
		return 0, 0, 0, 0, false
	}
	minTS := records[0].Timestamp
	maxTS := records[0].Timestamp
	minOffset := records[0].Offset
	maxOffset := records[0].Offset
	for _, record := range records[1:] {
		if record.Timestamp < minTS {
			minTS = record.Timestamp
		}
		if record.Timestamp > maxTS {
			maxTS = record.Timestamp
		}
		if record.Offset < minOffset {
			minOffset = record.Offset
		}
		if record.Offset > maxOffset {
			maxOffset = record.Offset
		}
	}
	return minTS, maxTS, minOffset, maxOffset, true
}

func (b *TimeIndexBuilder) indexKey(segmentKey string) string {
	if !strings.HasSuffix(segmentKey, ".kfs") {
		return ""
	}
	return strings.TrimSuffix(segmentKey, ".kfs") + b.keySuffix
}

func segmentsSizeBytes(segments []SegmentRef) int64 {
	total := int64(0)
	for _, segment := range segments {
		if segment.SizeBytes > 0 {
			total += segment.SizeBytes
		}
	}
	return total
}
