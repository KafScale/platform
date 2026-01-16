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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/metrics"
)

type ManifestBuilder struct {
	client      s3Putter
	bucket      string
	key         string
	prefix      string
	lister      Lister
	maxSegments int
	maxBytes    int64
	lease       *buildLease
	leaseTTL    time.Duration
}

type s3Putter interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

func NewManifestBuilder(cfg config.Config, lister Lister) (*ManifestBuilder, error) {
	client, err := newS3Client(cfg)
	if err != nil {
		return nil, err
	}
	builder := newManifestBuilder(
		client,
		cfg.S3.Bucket,
		normalizePrefix(cfg.S3.Namespace),
		cfg.Manifest.Key,
		cfg.Manifest.BuildMaxSegments,
		cfg.Manifest.BuildMaxBytes,
		lister,
	)
	builder.leaseTTL = time.Duration(cfg.Manifest.BuildLeaseTTLSeconds) * time.Second
	return builder, nil
}

func newManifestBuilder(client s3Putter, bucket, prefix, key string, maxSegments int, maxBytes int64, lister Lister) *ManifestBuilder {
	if key == "" {
		key = "manifest.json"
	}
	return &ManifestBuilder{
		client:      client,
		bucket:      bucket,
		key:         joinKey(prefix, key),
		prefix:      prefix,
		lister:      lister,
		maxSegments: maxSegments,
		maxBytes:    maxBytes,
		lease:       newBuildLease(),
		leaseTTL:    120 * time.Second,
	}
}

func (b *ManifestBuilder) Build(ctx context.Context) error {
	release, err := b.lease.Acquire(ctx, "manifest", b.leaseTTL)
	if err != nil {
		return err
	}
	defer release()

	if b.lister == nil {
		return fmt.Errorf("manifest builder requires a lister")
	}
	segments, err := b.lister.ListCompleted(ctx)
	if err != nil {
		return err
	}
	if b.maxSegments > 0 && len(segments) > b.maxSegments {
		return fmt.Errorf("manifest build exceeds max segments (%d)", b.maxSegments)
	}
	if b.maxBytes > 0 && manifestEstimatedBytes(segments) > b.maxBytes {
		return fmt.Errorf("manifest build exceeds max bytes (%d)", b.maxBytes)
	}

	entries := make([]manifestEntry, 0, len(segments))
	updatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	for _, segment := range segments {
		if segment.SegmentKey == "" || segment.IndexKey == "" {
			continue
		}
		entries = append(entries, manifestEntry{
			Topic:        segment.Topic,
			Partition:    int32Ptr(segment.Partition),
			SegmentKey:   trimPrefix(b.prefix, segment.SegmentKey),
			IndexKey:     trimPrefix(b.prefix, segment.IndexKey),
			SizeBytes:    segment.SizeBytes,
			MinOffset:    segment.MinOffset,
			MaxOffset:    segment.MaxOffset,
			MinTimestamp: segment.MinTimestamp,
			MaxTimestamp: segment.MaxTimestamp,
			LastModified: formatManifestTime(segment.LastModified),
			UpdatedAt:    updatedAt,
		})
	}

	body, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}

	start := time.Now()
	metrics.S3Requests.WithLabelValues("put_manifest").Inc()
	_, err = b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(b.bucket),
		Key:         aws.String(b.key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
	})
	metrics.S3Duration.WithLabelValues("put_manifest").Observe(float64(time.Since(start).Milliseconds()))
	if err != nil {
		metrics.S3Errors.WithLabelValues("put_manifest").Inc()
		return fmt.Errorf("put manifest: %w", err)
	}
	return nil
}

func (b *ManifestBuilder) Run(ctx context.Context, interval time.Duration) {
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

func trimPrefix(prefix, key string) string {
	if prefix == "" {
		return key
	}
	clean := strings.TrimPrefix(key, prefix)
	if clean == "" {
		return key
	}
	return strings.TrimPrefix(clean, "/")
}

func formatManifestTime(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	return ts.UTC().Format(time.RFC3339Nano)
}

func int32Ptr(value int32) *int32 {
	return &value
}

func manifestEstimatedBytes(segments []SegmentRef) int64 {
	total := int64(0)
	for _, segment := range segments {
		if segment.SizeBytes > 0 {
			total += segment.SizeBytes
		}
	}
	return total
}
