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

package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/metrics"
)

type Resolver interface {
	Topics(ctx context.Context) ([]string, error)
	Partitions(ctx context.Context, topic string) ([]int32, error)
}

type Topic struct {
	Name       string
	Partitions []int32
}

type Snapshot struct {
	Topics []Topic
}

type Snapshotter interface {
	Snapshot(ctx context.Context) (Snapshot, error)
}

type StaticResolver struct {
	topics map[string][]int32
}

func NewStaticResolver(topics []config.TopicConfig) *StaticResolver {
	out := make(map[string][]int32, len(topics))
	for _, t := range topics {
		out[t.Name] = append([]int32(nil), t.Partitions...)
	}
	return &StaticResolver{topics: out}
}

func (r *StaticResolver) Topics(ctx context.Context) ([]string, error) {
	names := make([]string, 0, len(r.topics))
	for name := range r.topics {
		names = append(names, name)
	}
	return names, nil
}

func (r *StaticResolver) Partitions(ctx context.Context, topic string) ([]int32, error) {
	partitions, ok := r.topics[topic]
	if !ok {
		return nil, errors.New("unknown topic")
	}
	return append([]int32(nil), partitions...), nil
}

type SnapshotResolver struct {
	inner Resolver
	ttl   time.Duration

	lastRefresh time.Time
	topics      []string
	partitions  map[string][]int32
}

func NewSnapshotResolver(inner Resolver, ttl time.Duration) *SnapshotResolver {
	return &SnapshotResolver{
		inner:      inner,
		ttl:        ttl,
		partitions: make(map[string][]int32),
	}
}

func (r *SnapshotResolver) Topics(ctx context.Context) ([]string, error) {
	if err := r.refresh(ctx); err != nil {
		return nil, err
	}
	return append([]string(nil), r.topics...), nil
}

func (r *SnapshotResolver) Partitions(ctx context.Context, topic string) ([]int32, error) {
	if err := r.refresh(ctx); err != nil {
		return nil, err
	}
	parts, ok := r.partitions[topic]
	if !ok {
		return nil, errors.New("unknown topic")
	}
	return append([]int32(nil), parts...), nil
}

func (r *SnapshotResolver) refresh(ctx context.Context) error {
	if r.ttl > 0 && time.Since(r.lastRefresh) < r.ttl {
		return nil
	}

	topics, partitions, err := r.loadSnapshot(ctx)
	if err != nil {
		return err
	}

	r.topics = topics
	r.partitions = partitions
	r.lastRefresh = time.Now()
	return nil
}

func NewResolver(cfg config.Config) (Resolver, error) {
	mode := cfg.Metadata.Discovery
	if mode == "" || mode == "static" {
		return NewSnapshotResolver(NewStaticResolver(cfg.Metadata.Topics), ttl(cfg)), nil
	}
	if mode == "etcd" {
		var resolver Resolver
		etcdResolver, err := NewEtcdResolver(cfg)
		if err != nil {
			return nil, err
		}
		resolver = etcdResolver
		fallback, fallbackErr := NewS3Resolver(cfg)
		if fallbackErr == nil {
			resolver = &FallbackResolver{primary: resolver, fallback: fallback}
		}
		return NewSnapshotResolver(resolver, ttl(cfg)), nil
	}
	if mode == "s3" {
		resolver, err := NewS3Resolver(cfg)
		if err != nil {
			return nil, err
		}
		return NewSnapshotResolver(resolver, ttl(cfg)), nil
	}
	if mode == "auto" {
		if len(cfg.Metadata.Etcd.Endpoints) == 0 {
			resolver, err := NewS3Resolver(cfg)
			if err != nil {
				return nil, err
			}
			return NewSnapshotResolver(resolver, ttl(cfg)), nil
		}
		var resolver Resolver
		etcdResolver, err := NewEtcdResolver(cfg)
		if err != nil {
			return nil, err
		}
		resolver = etcdResolver
		fallback, fallbackErr := NewS3Resolver(cfg)
		if fallbackErr == nil {
			resolver = &FallbackResolver{primary: resolver, fallback: fallback}
		}
		return NewSnapshotResolver(resolver, ttl(cfg)), nil
	}
	return nil, errors.New("unknown metadata discovery mode")
}

func ttl(cfg config.Config) time.Duration {
	if cfg.Metadata.Snapshot.TTLSeconds <= 0 {
		return 0
	}
	return time.Duration(cfg.Metadata.Snapshot.TTLSeconds) * time.Second
}

func (r *SnapshotResolver) loadSnapshot(ctx context.Context) ([]string, map[string][]int32, error) {
	if snapper, ok := r.inner.(Snapshotter); ok {
		snap, err := snapper.Snapshot(ctx)
		if err != nil {
			return nil, nil, err
		}
		topics, partitions := snapshotToMaps(snap)
		return topics, partitions, nil
	}

	topics, err := r.inner.Topics(ctx)
	if err != nil {
		return nil, nil, err
	}

	partitions := make(map[string][]int32, len(topics))
	for _, topic := range topics {
		parts, err := r.inner.Partitions(ctx, topic)
		if err != nil {
			return nil, nil, err
		}
		partitions[topic] = parts
	}
	return topics, partitions, nil
}

func snapshotToMaps(snap Snapshot) ([]string, map[string][]int32) {
	topics := make([]string, 0, len(snap.Topics))
	partitions := make(map[string][]int32, len(snap.Topics))
	for _, topic := range snap.Topics {
		if topic.Name == "" {
			continue
		}
		topics = append(topics, topic.Name)
		partitions[topic.Name] = append([]int32(nil), topic.Partitions...)
	}
	return topics, partitions
}

type FallbackResolver struct {
	primary  Resolver
	fallback Resolver
}

func (r *FallbackResolver) Topics(ctx context.Context) ([]string, error) {
	topics, err := r.primary.Topics(ctx)
	if err == nil {
		return topics, nil
	}
	return r.fallback.Topics(ctx)
}

func (r *FallbackResolver) Partitions(ctx context.Context, topic string) ([]int32, error) {
	partitions, err := r.primary.Partitions(ctx, topic)
	if err == nil {
		return partitions, nil
	}
	return r.fallback.Partitions(ctx, topic)
}

type EtcdResolver struct {
	client      *clientv3.Client
	snapshotKey string
}

func NewEtcdResolver(cfg config.Config) (*EtcdResolver, error) {
	if len(cfg.Metadata.Etcd.Endpoints) == 0 {
		return nil, errors.New("metadata.etcd.endpoints is required for discovery=etcd")
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Metadata.Etcd.Endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &EtcdResolver{
		client:      cli,
		snapshotKey: cfg.Metadata.Snapshot.Key,
	}, nil
}

func (r *EtcdResolver) Topics(ctx context.Context) ([]string, error) {
	snap, err := r.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	topics, _ := snapshotToMaps(snap)
	return topics, nil
}

func (r *EtcdResolver) Partitions(ctx context.Context, topic string) ([]int32, error) {
	snap, err := r.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	_, partitions := snapshotToMaps(snap)
	parts, ok := partitions[topic]
	if !ok {
		return nil, errors.New("unknown topic")
	}
	return parts, nil
}

func (r *EtcdResolver) Snapshot(ctx context.Context) (Snapshot, error) {
	resp, err := r.client.Get(ctx, r.snapshotKey)
	if err != nil {
		return Snapshot{}, err
	}
	if len(resp.Kvs) == 0 {
		return Snapshot{}, errors.New("metadata snapshot not found")
	}
	var payload snapshotPayload
	if err := json.Unmarshal(resp.Kvs[0].Value, &payload); err != nil {
		return Snapshot{}, err
	}
	return payload.toSnapshot(), nil
}

type snapshotPayload struct {
	Topics []snapshotTopic
}

type snapshotTopic struct {
	Name       string
	Partitions []snapshotPartition
}

type snapshotPartition struct {
	PartitionIndex int32
}

func (p snapshotPayload) toSnapshot() Snapshot {
	topics := make([]Topic, 0, len(p.Topics))
	for _, topic := range p.Topics {
		parts := make([]int32, 0, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			parts = append(parts, partition.PartitionIndex)
		}
		topics = append(topics, Topic{
			Name:       topic.Name,
			Partitions: parts,
		})
	}
	return Snapshot{Topics: topics}
}

type S3Resolver struct {
	client *s3.Client
	bucket string
	prefix string
}

func NewS3Resolver(cfg config.Config) (*S3Resolver, error) {
	if cfg.S3.Bucket == "" {
		return nil, errors.New("s3.bucket is required for discovery=s3")
	}

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

	return &S3Resolver{
		client: client,
		bucket: cfg.S3.Bucket,
		prefix: normalizePrefix(cfg.S3.Namespace),
	}, nil
}

func (r *S3Resolver) Topics(ctx context.Context) ([]string, error) {
	prefixes, err := r.listPrefixes(ctx, r.prefix)
	if err != nil {
		return nil, err
	}
	topics := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		name := strings.TrimSuffix(strings.TrimPrefix(prefix, r.prefix), "/")
		if name != "" {
			topics = append(topics, name)
		}
	}
	sort.Strings(topics)
	return topics, nil
}

func (r *S3Resolver) Partitions(ctx context.Context, topic string) ([]int32, error) {
	if topic == "" {
		return nil, errors.New("topic is required")
	}
	prefix := r.prefix + topic + "/"
	prefixes, err := r.listPrefixes(ctx, prefix)
	if err != nil {
		return nil, err
	}
	partitions := make([]int32, 0, len(prefixes))
	for _, partitionPrefix := range prefixes {
		raw := strings.TrimSuffix(strings.TrimPrefix(partitionPrefix, prefix), "/")
		if raw == "" {
			continue
		}
		value, err := strconv.Atoi(raw)
		if err != nil {
			continue
		}
		partitions = append(partitions, int32(value))
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	return partitions, nil
}

func (r *S3Resolver) Snapshot(ctx context.Context) (Snapshot, error) {
	topics, err := r.Topics(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	out := make([]Topic, 0, len(topics))
	for _, topic := range topics {
		parts, err := r.Partitions(ctx, topic)
		if err != nil {
			return Snapshot{}, err
		}
		out = append(out, Topic{Name: topic, Partitions: parts})
	}
	return Snapshot{Topics: out}, nil
}

func (r *S3Resolver) listPrefixes(ctx context.Context, prefix string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(r.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}
	paginator := s3.NewListObjectsV2Paginator(r.client, input)
	prefixes := make([]string, 0)
	for paginator.HasMorePages() {
		start := time.Now()
		metrics.S3Requests.WithLabelValues("list").Inc()
		page, err := paginator.NextPage(ctx)
		metrics.S3Duration.WithLabelValues("list").Observe(float64(time.Since(start).Milliseconds()))
		if err != nil {
			metrics.S3Errors.WithLabelValues("list").Inc()
			return nil, err
		}
		for _, p := range page.CommonPrefixes {
			if p.Prefix != nil {
				prefixes = append(prefixes, *p.Prefix)
			}
		}
	}
	return prefixes, nil
}

func normalizePrefix(prefix string) string {
	clean := strings.Trim(prefix, "/")
	if clean == "" {
		return ""
	}
	return clean + "/"
}
