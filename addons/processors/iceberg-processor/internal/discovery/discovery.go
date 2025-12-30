package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	processorconfig "github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/config"
	"github.com/novatechflow/kafscale/pkg/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const segmentFooterMagic = "END!"
const metadataSnapshotKey = "/kafscale/metadata/snapshot"

// SegmentRef identifies a completed segment and its index.
type SegmentRef struct {
	Topic      string
	Partition  int32
	BaseOffset int64
	SegmentKey string
	IndexKey   string
}

// Lister discovers completed segments for topics/partitions.
type Lister interface {
	ListCompleted(ctx context.Context) ([]SegmentRef, error)
}

// New builds an S3-backed lister.
func New(cfg processorconfig.Config) (Lister, error) {
	mode := strings.ToLower(cfg.Discovery.Mode)
	if mode == "" {
		mode = "auto"
	}

	s3Lister, err := newS3Lister(cfg)
	if err != nil {
		return nil, err
	}

	switch mode {
	case "s3":
		return s3Lister, nil
	case "etcd":
		return newEtcdLister(cfg, s3Lister, true)
	case "auto":
		if len(cfg.Etcd.Endpoints) == 0 {
			return s3Lister, nil
		}
		return newEtcdLister(cfg, s3Lister, false)
	default:
		return nil, fmt.Errorf("unsupported discovery.mode %q", cfg.Discovery.Mode)
	}
}

func newS3Lister(cfg processorconfig.Config) (*s3Lister, error) {
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

	return &s3Lister{
		client: client,
		bucket: cfg.S3.Bucket,
		prefix: normalizePrefix(cfg.S3.Namespace),
	}, nil
}

type s3Lister struct {
	client *s3.Client
	bucket string
	prefix string
}

func (l *s3Lister) ListCompleted(ctx context.Context) ([]SegmentRef, error) {
	return l.listCompleted(ctx, nil)
}

func (l *s3Lister) listCompleted(ctx context.Context, filter map[string]map[int32]struct{}) ([]SegmentRef, error) {
	entries := make(map[segmentKey]*segmentEntry)
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(l.bucket),
		Prefix: aws.String(l.prefix),
	}
	paginator := s3.NewListObjectsV2Paginator(l.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			parsed, kind, ok := parseSegmentKey(l.prefix, key)
			if !ok {
				continue
			}
			if filter != nil && !filterAllows(filter, parsed.topic, parsed.partition) {
				continue
			}

			entry := entries[parsed]
			if entry == nil {
				entry = &segmentEntry{}
				entries[parsed] = entry
			}
			if kind == "kfs" {
				entry.kfsKey = key
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

		segments = append(segments, SegmentRef{
			Topic:      key.topic,
			Partition:  key.partition,
			BaseOffset: key.baseOffset,
			SegmentKey: entry.kfsKey,
			IndexKey:   entry.indexKey,
		})
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

	return segments, nil
}

func (l *s3Lister) hasFooterMagic(ctx context.Context, key string) (bool, error) {
	resp, err := l.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.bucket),
		Key:    aws.String(key),
		Range:  aws.String("bytes=-4"),
	})
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	if len(data) < len(segmentFooterMagic) {
		return false, nil
	}
	return bytes.Equal(data[len(data)-len(segmentFooterMagic):], []byte(segmentFooterMagic)), nil
}

type etcdLister struct {
	s3     *s3Lister
	client *clientv3.Client
	strict bool
}

func newEtcdLister(cfg processorconfig.Config, s3Lister *s3Lister, strict bool) (Lister, error) {
	if len(cfg.Etcd.Endpoints) == 0 {
		return nil, fmt.Errorf("etcd.endpoints is required for discovery.mode=etcd")
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Etcd.Endpoints,
		Username:    cfg.Etcd.Username,
		Password:    cfg.Etcd.Password,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &etcdLister{client: client, s3: s3Lister, strict: strict}, nil
}

func (l *etcdLister) ListCompleted(ctx context.Context) ([]SegmentRef, error) {
	filter, ok, err := l.topicPartitions(ctx)
	if err != nil {
		if l.strict {
			return nil, err
		}
		return l.s3.listCompleted(ctx, nil)
	}
	if !ok {
		if l.strict {
			return nil, fmt.Errorf("metadata snapshot not found in etcd")
		}
		return l.s3.listCompleted(ctx, nil)
	}
	return l.s3.listCompleted(ctx, filter)
}

func (l *etcdLister) topicPartitions(ctx context.Context) (map[string]map[int32]struct{}, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := l.client.Get(ctx, metadataSnapshotKey)
	if err != nil {
		return nil, false, err
	}
	if len(resp.Kvs) == 0 {
		return nil, false, nil
	}

	var snapshot metadata.ClusterMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &snapshot); err != nil {
		return nil, false, err
	}

	return buildTopicPartitionFilter(snapshot), true, nil
}

func buildTopicPartitionFilter(snapshot metadata.ClusterMetadata) map[string]map[int32]struct{} {
	filter := make(map[string]map[int32]struct{})
	for _, topic := range snapshot.Topics {
		if topic.ErrorCode != 0 {
			continue
		}
		partitions := make(map[int32]struct{}, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != 0 {
				continue
			}
			partitions[partition.PartitionIndex] = struct{}{}
		}
		if len(partitions) == 0 {
			continue
		}
		filter[topic.Name] = partitions
	}
	return filter
}

func filterAllows(filter map[string]map[int32]struct{}, topic string, partition int32) bool {
	partitions, ok := filter[topic]
	if !ok {
		return false
	}
	_, ok = partitions[partition]
	return ok
}

type segmentKey struct {
	topic      string
	partition  int32
	baseOffset int64
}

type segmentEntry struct {
	kfsKey   string
	indexKey string
}

func normalizePrefix(namespace string) string {
	trimmed := strings.Trim(namespace, "/")
	if trimmed == "" {
		return ""
	}
	return trimmed + "/"
}

func parseSegmentKey(prefix, key string) (segmentKey, string, bool) {
	if prefix != "" {
		if !strings.HasPrefix(key, prefix) {
			return segmentKey{}, "", false
		}
		key = strings.TrimPrefix(key, prefix)
	}

	parts := strings.Split(key, "/")
	if len(parts) != 3 {
		return segmentKey{}, "", false
	}
	topic := parts[0]
	if topic == "" {
		return segmentKey{}, "", false
	}

	partitionID, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return segmentKey{}, "", false
	}

	baseOffset, kind, ok := parseSegmentFile(parts[2])
	if !ok {
		return segmentKey{}, "", false
	}

	return segmentKey{
		topic:      topic,
		partition:  int32(partitionID),
		baseOffset: baseOffset,
	}, kind, true
}

func parseSegmentFile(filename string) (int64, string, bool) {
	if !strings.HasPrefix(filename, "segment-") {
		return 0, "", false
	}

	var baseStr string
	var kind string
	switch {
	case strings.HasSuffix(filename, ".kfs"):
		baseStr = strings.TrimSuffix(strings.TrimPrefix(filename, "segment-"), ".kfs")
		kind = "kfs"
	case strings.HasSuffix(filename, ".index"):
		baseStr = strings.TrimSuffix(strings.TrimPrefix(filename, "segment-"), ".index")
		kind = "index"
	default:
		return 0, "", false
	}

	if baseStr == "" {
		return 0, "", false
	}
	baseOffset, err := strconv.ParseInt(baseStr, 10, 64)
	if err != nil {
		return 0, "", false
	}

	return baseOffset, kind, true
}
