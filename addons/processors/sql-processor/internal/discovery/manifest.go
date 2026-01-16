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
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/metrics"
)

type manifestLister struct {
	client   s3Getter
	bucket   string
	key      string
	prefix   string
	fallback Lister
	ttl      time.Duration

	mu        sync.Mutex
	expiresAt time.Time
	segments  []SegmentRef
}

type s3Getter interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type manifestDocument struct {
	Entries []manifestEntry `json:"entries"`
}

type manifestEntry struct {
	Topic        string `json:"topic"`
	Partition    *int32 `json:"partition"`
	SegmentKey   string `json:"segment_key"`
	IndexKey     string `json:"index_key"`
	SizeBytes    int64  `json:"size_bytes"`
	MinOffset    *int64 `json:"min_offset"`
	MaxOffset    *int64 `json:"max_offset"`
	MinTimestamp *int64 `json:"min_ts"`
	MaxTimestamp *int64 `json:"max_ts"`
	LastModified string `json:"last_modified"`
	UpdatedAt    string `json:"updated_at"`
}

func newManifestLister(client s3Getter, bucket, prefix, key string, ttl time.Duration, fallback Lister) *manifestLister {
	if key == "" {
		key = "manifest.json"
	}
	return &manifestLister{
		client:   client,
		bucket:   bucket,
		key:      joinKey(prefix, key),
		prefix:   prefix,
		fallback: fallback,
		ttl:      ttl,
	}
}

func (m *manifestLister) ListCompleted(ctx context.Context) ([]SegmentRef, error) {
	now := time.Now()
	m.mu.Lock()
	if len(m.segments) > 0 && now.Before(m.expiresAt) {
		cached := cloneSegments(m.segments)
		m.mu.Unlock()
		return cached, nil
	}
	m.mu.Unlock()

	segments, err := m.loadManifest(ctx)
	if err != nil || len(segments) == 0 {
		if m.fallback != nil {
			return m.fallback.ListCompleted(ctx)
		}
		return segments, err
	}

	if m.ttl > 0 {
		m.mu.Lock()
		m.segments = cloneSegments(segments)
		m.expiresAt = now.Add(m.ttl)
		m.mu.Unlock()
	}

	return segments, nil
}

func (m *manifestLister) loadManifest(ctx context.Context) ([]SegmentRef, error) {
	start := time.Now()
	metrics.S3Requests.WithLabelValues("get_manifest").Inc()
	resp, err := m.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(m.key),
	})
	metrics.S3Duration.WithLabelValues("get_manifest").Observe(float64(time.Since(start).Milliseconds()))
	if err != nil {
		metrics.S3Errors.WithLabelValues("get_manifest").Inc()
		return nil, fmt.Errorf("get manifest: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	metrics.S3Bytes.Add(float64(len(body)))
	if err != nil {
		metrics.S3Errors.WithLabelValues("get_manifest").Inc()
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	entries, err := parseManifest(body)
	if err != nil {
		return nil, err
	}
	return manifestEntriesToSegments(m.prefix, entries), nil
}

func parseManifest(data []byte) ([]manifestEntry, error) {
	var entries []manifestEntry
	if err := json.Unmarshal(data, &entries); err == nil {
		return entries, nil
	}
	var doc manifestDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	return doc.Entries, nil
}

func manifestEntriesToSegments(prefix string, entries []manifestEntry) []SegmentRef {
	segments := make([]SegmentRef, 0, len(entries))
	for _, entry := range entries {
		if entry.SegmentKey == "" || entry.IndexKey == "" {
			continue
		}
		segmentKey := joinKey(prefix, entry.SegmentKey)
		indexKey := joinKey(prefix, entry.IndexKey)

		parsed, _, ok := parseSegmentKey(prefix, segmentKey)
		baseOffset, _ := parseBaseOffsetFromKey(segmentKey)
		topic := entry.Topic
		if topic == "" && ok {
			topic = parsed.topic
		}
		partition := int32(0)
		if entry.Partition != nil {
			partition = *entry.Partition
		} else if ok {
			partition = parsed.partition
		}

		if topic == "" {
			continue
		}

		segment := SegmentRef{
			Topic:        topic,
			Partition:    partition,
			BaseOffset:   baseOffset,
			SegmentKey:   segmentKey,
			IndexKey:     indexKey,
			SizeBytes:    entry.SizeBytes,
			MinOffset:    entry.MinOffset,
			MaxOffset:    entry.MaxOffset,
			MinTimestamp: entry.MinTimestamp,
			MaxTimestamp: entry.MaxTimestamp,
		}
		if ts, ok := parseManifestTime(entry.LastModified); ok {
			segment.LastModified = ts
		}
		segments = append(segments, segment)
	}
	return segments
}

func parseBaseOffsetFromKey(key string) (int64, bool) {
	last := key
	if idx := strings.LastIndex(key, "/"); idx >= 0 && idx+1 < len(key) {
		last = key[idx+1:]
	}
	if strings.HasSuffix(last, ".kfs") {
		return parseBaseOffset(last, "segment-", ".kfs")
	}
	return 0, false
}

func parseManifestTime(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts, true
		}
	}
	return time.Time{}, false
}

func joinKey(prefix, key string) string {
	clean := strings.TrimLeft(key, "/")
	if prefix == "" {
		return clean
	}
	if strings.HasPrefix(clean, prefix) {
		return clean
	}
	return prefix + clean
}
