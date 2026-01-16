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
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/metrics"
)

const segmentFooterMagic = "END!"

type SegmentRef struct {
	Topic        string
	Partition    int32
	BaseOffset   int64
	SegmentKey   string
	IndexKey     string
	SizeBytes    int64
	LastModified time.Time
	MinOffset    *int64
	MaxOffset    *int64
	MinTimestamp *int64
	MaxTimestamp *int64
}

type Lister interface {
	ListCompleted(ctx context.Context) ([]SegmentRef, error)
}

func New(cfg config.Config) (Lister, error) {
	client, err := newS3Client(cfg)
	if err != nil {
		return nil, err
	}

	base := &s3Lister{
		client: client,
		bucket: cfg.S3.Bucket,
		prefix: normalizePrefix(cfg.S3.Namespace),
	}
	if cfg.TimeIndex.Enabled {
		base.timeIndex = newTimeIndexReader(client, cfg.S3.Bucket, cfg.TimeIndex.KeySuffix)
	}
	var lister Lister = base
	if cfg.Manifest.Enabled {
		lister = newManifestLister(client, cfg.S3.Bucket, base.prefix, cfg.Manifest.Key, time.Duration(cfg.Manifest.TTLSeconds)*time.Second, base)
	}
	if cfg.DiscoveryCache.TTLSeconds <= 0 {
		return lister, nil
	}
	return newCachedLister(lister, time.Duration(cfg.DiscoveryCache.TTLSeconds)*time.Second, cfg.DiscoveryCache.MaxEntries), nil
}

func newS3Client(cfg config.Config) (*s3.Client, error) {
	loadOptions := []func(*awsconfig.LoadOptions) error{}
	if cfg.S3.Region != "" {
		loadOptions = append(loadOptions, awsconfig.WithRegion(cfg.S3.Region))
	}
	if cfg.S3.Endpoint != "" {
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{URL: cfg.S3.Endpoint, SigningRegion: region}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
		loadOptions = append(loadOptions, awsconfig.WithEndpointResolverWithOptions(resolver))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(opts *s3.Options) {
		if cfg.S3.PathStyle {
			opts.UsePathStyle = true
		}
	})
	return client, nil
}

type s3Lister struct {
	client *s3.Client
	bucket string
	prefix string
	timeIndex *timeIndexReader
}

func (l *s3Lister) ListCompleted(ctx context.Context) ([]SegmentRef, error) {
	entries := make(map[segmentKey]*segmentEntry)
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(l.bucket),
		Prefix: aws.String(l.prefix),
	}
	paginator := s3.NewListObjectsV2Paginator(l.client, input)
	for paginator.HasMorePages() {
		start := time.Now()
		metrics.S3Requests.WithLabelValues("list").Inc()
		page, err := paginator.NextPage(ctx)
		metrics.S3Duration.WithLabelValues("list").Observe(float64(time.Since(start).Milliseconds()))
		if err != nil {
			metrics.S3Errors.WithLabelValues("list").Inc()
			return nil, fmt.Errorf("list objects: %w", err)
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			parsed, kind, ok := parseSegmentKey(l.prefix, key)
			if !ok {
				continue
			}
			entry := entries[parsed]
			if entry == nil {
				entry = &segmentEntry{}
				entries[parsed] = entry
			}
			if kind == "kfs" {
				entry.kfsKey = key
				entry.kfsSize = aws.ToInt64(obj.Size)
				entry.kfsModified = aws.ToTime(obj.LastModified)
			} else {
				entry.indexKey = key
			}
		}
	}

	segments := make([]SegmentRef, 0, len(entries))
	for key, entry := range entries {
		if entry.kfsKey == "" || entry.indexKey == "" {
			continue
		}
		ok, err := l.hasFooterMagic(ctx, entry.kfsKey)
		if err != nil || !ok {
			continue
		}
		segment := SegmentRef{
			Topic:        key.topic,
			Partition:    key.partition,
			BaseOffset:   key.baseOffset,
			SegmentKey:   entry.kfsKey,
			IndexKey:     entry.indexKey,
			SizeBytes:    entry.kfsSize,
			LastModified: entry.kfsModified,
			MinOffset:    int64Ptr(key.baseOffset),
		}
		if l.timeIndex != nil {
			l.timeIndex.enrich(ctx, &segment)
		}
		segments = append(segments, segment)
	}

	sort.Slice(segments, func(i, j int) bool {
		if segments[i].Topic != segments[j].Topic {
			return segments[i].Topic < segments[j].Topic
		}
		if segments[i].Partition != segments[j].Partition {
			return segments[i].Partition < segments[j].Partition
		}
		return segments[i].BaseOffset < segments[j].BaseOffset
	})

	for i := range segments {
		next := findNextSegment(segments, i)
		if next == nil {
			continue
		}
		if next.BaseOffset > 0 {
			max := next.BaseOffset - 1
			segments[i].MaxOffset = &max
		}
	}

	return segments, nil
}

func (l *s3Lister) hasFooterMagic(ctx context.Context, key string) (bool, error) {
	start := time.Now()
	metrics.S3Requests.WithLabelValues("get_range").Inc()
	resp, err := l.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(key),
		Range:  aws.String("bytes=-4"),
	})
	if err != nil {
		metrics.S3Errors.WithLabelValues("get_range").Inc()
		metrics.S3Duration.WithLabelValues("get_range").Observe(float64(time.Since(start).Milliseconds()))
		return false, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	metrics.S3Duration.WithLabelValues("get_range").Observe(float64(time.Since(start).Milliseconds()))
	if err != nil {
		metrics.S3Errors.WithLabelValues("get_range").Inc()
		return false, err
	}
	metrics.S3Bytes.Add(float64(len(data)))
	if len(data) < len(segmentFooterMagic) {
		return false, nil
	}
	return bytes.Equal(data[len(data)-len(segmentFooterMagic):], []byte(segmentFooterMagic)), nil
}

type segmentKey struct {
	topic      string
	partition  int32
	baseOffset int64
}

type segmentEntry struct {
	kfsKey      string
	indexKey    string
	kfsSize     int64
	kfsModified time.Time
}

type cachedLister struct {
	inner      Lister
	ttl        time.Duration
	maxEntries int
	mu         sync.Mutex
	expiresAt  time.Time
	segments   []SegmentRef
}

func newCachedLister(inner Lister, ttl time.Duration, maxEntries int) *cachedLister {
	return &cachedLister{
		inner:      inner,
		ttl:        ttl,
		maxEntries: maxEntries,
	}
}

func (c *cachedLister) ListCompleted(ctx context.Context) ([]SegmentRef, error) {
	now := time.Now()
	c.mu.Lock()
	if len(c.segments) > 0 && now.Before(c.expiresAt) {
		metrics.DiscoveryCacheHits.Inc()
		cached := cloneSegments(c.segments)
		c.mu.Unlock()
		return cached, nil
	}
	c.mu.Unlock()

	metrics.DiscoveryCacheMisses.Inc()
	segments, err := c.inner.ListCompleted(ctx)
	if err != nil {
		return nil, err
	}

	if c.maxEntries > 0 && len(segments) > c.maxEntries {
		return segments, nil
	}

	c.mu.Lock()
	c.segments = cloneSegments(segments)
	c.expiresAt = now.Add(c.ttl)
	c.mu.Unlock()
	return segments, nil
}

func cloneSegments(segments []SegmentRef) []SegmentRef {
	out := make([]SegmentRef, len(segments))
	for i, seg := range segments {
		out[i] = seg
		if seg.MinOffset != nil {
			min := *seg.MinOffset
			out[i].MinOffset = &min
		}
		if seg.MaxOffset != nil {
			max := *seg.MaxOffset
			out[i].MaxOffset = &max
		}
		if seg.MinTimestamp != nil {
			min := *seg.MinTimestamp
			out[i].MinTimestamp = &min
		}
		if seg.MaxTimestamp != nil {
			max := *seg.MaxTimestamp
			out[i].MaxTimestamp = &max
		}
	}
	return out
}

func findNextSegment(segments []SegmentRef, index int) *SegmentRef {
	current := segments[index]
	for i := index + 1; i < len(segments); i++ {
		if segments[i].Topic != current.Topic || segments[i].Partition != current.Partition {
			return nil
		}
		return &segments[i]
	}
	return nil
}

func int64Ptr(value int64) *int64 {
	return &value
}

func parseSegmentKey(prefix, key string) (segmentKey, string, bool) {
	trimmed := strings.TrimPrefix(key, prefix)
	parts := strings.Split(trimmed, "/")
	if len(parts) < 3 {
		return segmentKey{}, "", false
	}
	topic := parts[0]
	partition, err := strconv.Atoi(parts[1])
	if err != nil {
		return segmentKey{}, "", false
	}
	filename := parts[2]

	switch {
	case strings.HasPrefix(filename, "segment-") && strings.HasSuffix(filename, ".kfs"):
		base, ok := parseBaseOffset(filename, "segment-", ".kfs")
		if !ok {
			return segmentKey{}, "", false
		}
		return segmentKey{topic: topic, partition: int32(partition), baseOffset: base}, "kfs", true
	case strings.HasPrefix(filename, "segment-") && strings.HasSuffix(filename, ".index"):
		base, ok := parseBaseOffset(filename, "segment-", ".index")
		if !ok {
			return segmentKey{}, "", false
		}
		return segmentKey{topic: topic, partition: int32(partition), baseOffset: base}, "index", true
	default:
		return segmentKey{}, "", false
	}
}

func parseBaseOffset(filename, prefix, suffix string) (int64, bool) {
	value := strings.TrimSuffix(strings.TrimPrefix(filename, prefix), suffix)
	if value == "" {
		return 0, false
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func normalizePrefix(prefix string) string {
	clean := strings.Trim(prefix, "/")
	if clean == "" {
		return ""
	}
	return clean + "/"
}
