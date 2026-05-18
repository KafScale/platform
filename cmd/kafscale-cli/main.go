// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
)

var (
	newS3Client  = storage.NewS3Client
	newEtcdStore = metadata.NewEtcdStore
	newMemoryS3  = func() storage.S3Client { return storage.NewMemoryS3Client() }
)

type restoreConfig struct {
	SourceTopic     string
	SourceNamespace string
	TargetTopic     string
	TargetNamespace string
	RestoreTo       time.Time
	Partitions      []int32
}

func main() {
	ctx := context.Background()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	if len(args) == 0 {
		writeUsage(stderr)
		return fmt.Errorf("command required")
	}
	switch args[0] {
	case "restore":
		return runRestoreCommand(ctx, args[1:], stdout)
	case "-h", "--help", "help":
		writeUsage(stdout)
		return nil
	default:
		writeUsage(stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func writeUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, "usage: kafscale-cli restore --topic <source> --target-topic <target> --to <RFC3339 timestamp>")
}

func runRestoreCommand(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var (
		sourceTopic     = fs.String("topic", "", "Source topic to recover from")
		sourceNamespace = fs.String("namespace", envOrDefault("KAFSCALE_NAMESPACE", "default"), "Source namespace")
		targetTopic     = fs.String("target-topic", "", "Target topic to create and populate")
		targetNamespace = fs.String("target-namespace", "", "Target namespace (defaults to source namespace)")
		restoreToRaw    = fs.String("to", "", "Restore cutoff in RFC3339 format")
		partitionsRaw   = fs.String("partitions", "", "Optional comma-separated partition list")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *sourceTopic == "" {
		return fmt.Errorf("--topic is required")
	}
	if *targetTopic == "" {
		return fmt.Errorf("--target-topic is required")
	}
	if *restoreToRaw == "" {
		return fmt.Errorf("--to is required")
	}
	restoreTo, err := time.Parse(time.RFC3339, *restoreToRaw)
	if err != nil {
		return fmt.Errorf("parse --to: %w", err)
	}
	partitions, err := parsePartitions(*partitionsRaw)
	if err != nil {
		return err
	}
	if *targetNamespace == "" {
		*targetNamespace = *sourceNamespace
	}

	s3Client, err := s3ClientFromEnv(ctx)
	if err != nil {
		return err
	}

	store, err := metadataStoreFromEnv(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = store.EtcdClient().Close() }()

	return executeRestore(ctx, stdout, restoreConfig{
		SourceTopic:     *sourceTopic,
		SourceNamespace: *sourceNamespace,
		TargetTopic:     *targetTopic,
		TargetNamespace: *targetNamespace,
		RestoreTo:       restoreTo,
		Partitions:      partitions,
	}, s3Client, store)
}

func s3ClientFromEnv(ctx context.Context) (storage.S3Client, error) {
	if envBoolDefault("KAFSCALE_USE_MEMORY_S3", false) {
		return newMemoryS3(), nil
	}
	return newS3Client(ctx, storage.S3Config{
		Bucket:          strings.TrimSpace(os.Getenv("KAFSCALE_S3_BUCKET")),
		Region:          strings.TrimSpace(os.Getenv("KAFSCALE_S3_REGION")),
		Endpoint:        strings.TrimSpace(os.Getenv("KAFSCALE_S3_ENDPOINT")),
		ForcePathStyle:  envBoolDefault("KAFSCALE_S3_PATH_STYLE", strings.TrimSpace(os.Getenv("KAFSCALE_S3_ENDPOINT")) != ""),
		AccessKeyID:     strings.TrimSpace(os.Getenv("KAFSCALE_S3_ACCESS_KEY")),
		SecretAccessKey: strings.TrimSpace(os.Getenv("KAFSCALE_S3_SECRET_KEY")),
		SessionToken:    strings.TrimSpace(os.Getenv("KAFSCALE_S3_SESSION_TOKEN")),
		KMSKeyARN:       strings.TrimSpace(os.Getenv("KAFSCALE_S3_KMS_ARN")),
		MaxConnections:  envIntDefault("KAFSCALE_S3_CONCURRENCY", 16),
	})
}

func metadataStoreFromEnv(ctx context.Context) (*metadata.EtcdStore, error) {
	return newEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{
		Endpoints:   splitCSV(strings.TrimSpace(os.Getenv("KAFSCALE_ETCD_ENDPOINTS"))),
		Username:    strings.TrimSpace(os.Getenv("KAFSCALE_ETCD_USERNAME")),
		Password:    strings.TrimSpace(os.Getenv("KAFSCALE_ETCD_PASSWORD")),
		DialTimeout: 5 * time.Second,
	})
}

func executeRestore(ctx context.Context, stdout io.Writer, cfg restoreConfig, s3Client storage.S3Client, store *metadata.EtcdStore) error {
	if store == nil {
		return fmt.Errorf("metadata store required")
	}
	if s3Client == nil {
		return fmt.Errorf("s3 client required")
	}

	sourceMeta, err := store.Metadata(ctx, []string{cfg.SourceTopic})
	if err != nil {
		return err
	}
	if len(sourceMeta.Topics) == 0 || sourceMeta.Topics[0].ErrorCode != 0 {
		return fmt.Errorf("source topic metadata: %w", metadata.ErrUnknownTopic)
	}

	sourcePartitions := make(map[int32]struct{}, len(sourceMeta.Topics[0].Partitions))
	for _, partition := range sourceMeta.Topics[0].Partitions {
		sourcePartitions[partition.Partition] = struct{}{}
	}
	for _, partition := range cfg.Partitions {
		if _, ok := sourcePartitions[partition]; !ok {
			return fmt.Errorf("partition %d not present in source topic %s", partition, cfg.SourceTopic)
		}
	}

	sourceCfg, err := store.FetchTopicConfig(ctx, cfg.SourceTopic)
	if err != nil {
		return err
	}

	restoreCommitted := false
	targetCreated := false
	defer func() {
		if restoreCommitted || !targetCreated {
			return
		}
		if err := store.DeleteTopic(context.Background(), cfg.TargetTopic); err != nil {
			if !errors.Is(err, metadata.ErrUnknownTopic) {
				fmt.Fprintf(os.Stderr, "warning: failed to roll back target topic %s: %v\n", cfg.TargetTopic, err)
			}
		}
	}()

	if _, err := store.CreateTopic(ctx, metadata.TopicSpec{
		Name:              cfg.TargetTopic,
		NumPartitions:     sourceCfg.Partitions,
		ReplicationFactor: int16(sourceCfg.ReplicationFactor),
	}); err != nil {
		return err
	}
	targetCreated = true

	targetCfg := cloneTopicConfig(sourceCfg)
	targetCfg.Name = cfg.TargetTopic
	targetCfg.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	if err := persistTopicConfig(ctx, store, targetCfg); err != nil {
		return err
	}

	result, err := storage.RecoverTopicToTimestamp(ctx, s3Client, storage.TopicRecoveryConfig{
		SourceNamespace: cfg.SourceNamespace,
		SourceTopic:     cfg.SourceTopic,
		TargetNamespace: cfg.TargetNamespace,
		TargetTopic:     cfg.TargetTopic,
		RestoreTo:       cfg.RestoreTo,
		Partitions:      cfg.Partitions,
	})
	if err != nil {
		return err
	}

	targetMeta, err := store.Metadata(ctx, []string{cfg.TargetTopic})
	if err != nil {
		return err
	}
	if len(targetMeta.Topics) == 0 || targetMeta.Topics[0].ErrorCode != 0 {
		return fmt.Errorf("target topic metadata: %w", metadata.ErrUnknownTopic)
	}

	recoveredByPartition := make(map[int32]storage.RecoveredPartition, len(result.Partitions))
	for _, partition := range result.Partitions {
		recoveredByPartition[partition.Partition] = partition
		if partition.LastOffset >= 0 {
			if err := store.UpdateOffsets(ctx, cfg.TargetTopic, partition.Partition, partition.LastOffset); err != nil {
				return err
			}
		}
	}

	if err := writePartitionStates(ctx, store, cfg.TargetTopic, targetMeta.Topics[0].Partitions, recoveredByPartition); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(stdout, "restored %s to %s up to %s\n", cfg.SourceTopic, cfg.TargetTopic, cfg.RestoreTo.UTC().Format(time.RFC3339))
	for _, partition := range result.Partitions {
		_, _ = fmt.Fprintf(stdout, "partition=%d segments=%d last_offset=%d\n", partition.Partition, partition.SegmentsCopied, partition.LastOffset)
	}
	restoreCommitted = true
	return nil
}

func writePartitionStates(ctx context.Context, store *metadata.EtcdStore, topic string, partitions []protocol.MetadataPartition, recovered map[int32]storage.RecoveredPartition) error {
	for _, partition := range partitions {
		summary, ok := recovered[partition.Partition]
		state := &metadatapb.PartitionState{
			Topic:        topic,
			Partition:    partition.Partition,
			LeaderBroker: fmt.Sprintf("%d", partition.Leader),
			LeaderEpoch:  partition.LeaderEpoch,
		}
		if ok && summary.LastOffset >= 0 {
			state.LogEndOffset = summary.LastOffset + 1
			state.HighWatermark = summary.LastOffset + 1
			if len(summary.Segments) > 0 {
				last := summary.Segments[len(summary.Segments)-1]
				state.ActiveSegment = path.Base(last.TargetKey)
				state.Segments = make([]*metadatapb.SegmentInfo, 0, len(summary.Segments))
				for _, segment := range summary.Segments {
					state.Segments = append(state.Segments, &metadatapb.SegmentInfo{
						BaseOffset: segment.BaseOffset,
						SizeBytes:  segment.SizeBytes,
						CreatedAt:  segment.CreatedAt.UTC().Format(time.RFC3339),
					})
				}
			}
		}
		payload, err := metadata.EncodePartitionState(state)
		if err != nil {
			return err
		}
		putCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err = store.EtcdClient().Put(putCtx, metadata.PartitionStateKey(topic, partition.Partition), string(payload))
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func persistTopicConfig(ctx context.Context, store *metadata.EtcdStore, cfg *metadatapb.TopicConfig) error {
	payload, err := metadata.EncodeTopicConfig(cfg)
	if err != nil {
		return err
	}
	putCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err = store.EtcdClient().Put(putCtx, metadata.TopicConfigKey(cfg.Name), string(payload))
	return err
}

func cloneTopicConfig(cfg *metadatapb.TopicConfig) *metadatapb.TopicConfig {
	if cfg == nil {
		return nil
	}
	cloned := &metadatapb.TopicConfig{
		Name:              cfg.Name,
		Partitions:        cfg.Partitions,
		ReplicationFactor: cfg.ReplicationFactor,
		RetentionMs:       cfg.RetentionMs,
		RetentionBytes:    cfg.RetentionBytes,
		SegmentBytes:      cfg.SegmentBytes,
		CreatedAt:         cfg.CreatedAt,
		Config:            make(map[string]string, len(cfg.Config)),
	}
	for key, value := range cfg.Config {
		cloned.Config[key] = value
	}
	return cloned
}

func parsePartitions(raw string) ([]int32, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parts := splitCSV(raw)
	out := make([]int32, 0, len(parts))
	seen := make(map[int32]struct{}, len(parts))
	for _, part := range parts {
		parsed, err := strconv.ParseInt(part, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parse partition %q: %w", part, err)
		}
		val := int32(parsed)
		if _, ok := seen[val]; ok {
			continue
		}
		seen[val] = struct{}{}
		out = append(out, val)
	}
	slices.Sort(out)
	return out, nil
}

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		val := strings.TrimSpace(part)
		if val != "" {
			out = append(out, val)
		}
	}
	return out
}

func envOrDefault(key string, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return fallback
}

func envBoolDefault(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func envIntDefault(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	val, err := strconv.Atoi(raw)
	if err != nil || val <= 0 {
		return fallback
	}
	return val
}
