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
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/storage"
)

type pruneConfig struct {
	Topic     string
	OlderThan time.Duration
	Force     bool
	Now       time.Time
}

type pruneEntry struct {
	record  metadata.DeletedTopic
	objects []storage.S3Object
	bytes   int64
}

func runPruneCommand(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("prune", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var (
		topic     = fs.String("topic", "", "Only prune deleted incarnations of this topic")
		olderThan = fs.String("older-than", "", "Only prune incarnations deleted longer ago than this, for example 7d or 24h")
		force     = fs.Bool("force", false, "Delete the data instead of listing it")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	age, err := parseAge(*olderThan)
	if err != nil {
		return err
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
	return executePrune(ctx, stdout, pruneConfig{Topic: *topic, OlderThan: age, Force: *force, Now: time.Now()}, s3Client, store)
}

func parseAge(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	if days, ok := strings.CutSuffix(raw, "d"); ok {
		n, err := strconv.Atoi(days)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("parse --older-than %q", raw)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("parse --older-than %q", raw)
	}
	return d, nil
}

func executePrune(ctx context.Context, stdout io.Writer, cfg pruneConfig, s3Client storage.S3Client, store *metadata.EtcdStore) error {
	records, err := listDeletedTopics(ctx, store.EtcdClient(), cfg)
	if err != nil {
		return err
	}
	mode := "dry run, nothing deleted"
	if cfg.Force {
		mode = "deleting"
	}
	_, _ = fmt.Fprintf(stdout, "prune (%s)\n", mode)
	var entries []pruneEntry
	var totalObjects int
	var totalBytes int64
	var failed []string
	for _, record := range records {
		entry, err := inspectDeletedTopic(ctx, s3Client, record)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(stdout, "topic=%s generation=%d prefix=%s objects=%d bytes=%d deleted_at=%s\n",
			record.Name, record.Generation, record.Prefix, len(entry.objects), entry.bytes, record.DeletedAt)
		totalObjects += len(entry.objects)
		totalBytes += entry.bytes
		entries = append(entries, entry)
	}
	if cfg.Force {
		for _, entry := range entries {
			if err := pruneDeletedTopic(ctx, s3Client, store, entry); err != nil {
				failed = append(failed, fmt.Sprintf("%s generation %d: %v", entry.record.Name, entry.record.Generation, err))
				continue
			}
			_, _ = fmt.Fprintf(stdout, "pruned topic=%s generation=%d\n", entry.record.Name, entry.record.Generation)
		}
	}
	_, _ = fmt.Fprintf(stdout, "total incarnations=%d objects=%d bytes=%d\n", len(entries), totalObjects, totalBytes)
	if !cfg.Force {
		_, _ = fmt.Fprintln(stdout, "dry run: rerun with --force to delete")
	}
	if len(failed) > 0 {
		return fmt.Errorf("prune failed, rerun to finish: %s", strings.Join(failed, "; "))
	}
	return nil
}

func listDeletedTopics(ctx context.Context, client *clientv3.Client, cfg pruneConfig) ([]metadata.DeletedTopic, error) {
	prefix := metadata.DeletedTopicPrefix
	if cfg.Topic != "" {
		prefix += cfg.Topic + "/"
	}
	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list deleted topics: %w", err)
	}
	var out []metadata.DeletedTopic
	for _, kv := range resp.Kvs {
		var record metadata.DeletedTopic
		if err := json.Unmarshal(kv.Value, &record); err != nil {
			return nil, fmt.Errorf("decode %s: %w", kv.Key, err)
		}
		if string(kv.Key) != metadata.DeletedTopicKey(record.Name, record.Generation) {
			return nil, fmt.Errorf("record %s does not match its key", kv.Key)
		}
		if cfg.OlderThan > 0 {
			deletedAt, err := time.Parse(time.RFC3339, record.DeletedAt)
			if err != nil {
				return nil, fmt.Errorf("parse deleted_at of %s: %w", kv.Key, err)
			}
			if cfg.Now.Sub(deletedAt) < cfg.OlderThan {
				continue
			}
		}
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Generation < out[j].Generation
	})
	return out, nil
}

func recordNamespace(record metadata.DeletedTopic) (string, error) {
	suffix := "/" + metadata.TopicStorageName(record.Name, record.Generation) + "/"
	namespace, ok := strings.CutSuffix(record.Prefix, suffix)
	if !ok || namespace == "" || strings.Contains(namespace, "/") {
		return "", fmt.Errorf("prefix %q does not match topic %s generation %d", record.Prefix, record.Name, record.Generation)
	}
	return namespace, nil
}

func inspectDeletedTopic(ctx context.Context, s3Client storage.S3Client, record metadata.DeletedTopic) (pruneEntry, error) {
	if _, err := recordNamespace(record); err != nil {
		return pruneEntry{}, err
	}
	objects, err := s3Client.ListSegments(ctx, record.Prefix)
	if err != nil {
		return pruneEntry{}, fmt.Errorf("list %s: %w", record.Prefix, err)
	}
	entry := pruneEntry{record: record}
	for _, obj := range objects {
		if !strings.HasPrefix(obj.Key, record.Prefix) {
			continue
		}
		entry.objects = append(entry.objects, obj)
		entry.bytes += obj.Size
	}
	return entry, nil
}

func checkNotLive(ctx context.Context, store *metadata.EtcdStore, record metadata.DeletedTopic) error {
	namespace, err := recordNamespace(record)
	if err != nil {
		return err
	}
	current, err := store.TopicGeneration(ctx, record.Name)
	if err != nil {
		return err
	}
	if record.Generation >= current {
		return fmt.Errorf("generation %d is the current generation", record.Generation)
	}
	if err := store.RefreshSnapshot(ctx); err != nil {
		return err
	}
	state, err := store.Metadata(ctx, nil)
	if err != nil {
		return err
	}
	for _, topic := range state.Topics {
		if topic.Topic == nil {
			continue
		}
		generation, err := store.TopicGeneration(ctx, *topic.Topic)
		if err != nil {
			return err
		}
		if namespace+"/"+metadata.TopicStorageName(*topic.Topic, generation)+"/" == record.Prefix {
			return fmt.Errorf("prefix %s belongs to live topic %s", record.Prefix, *topic.Topic)
		}
	}
	return nil
}

func pruneDeletedTopic(ctx context.Context, s3Client storage.S3Client, store *metadata.EtcdStore, entry pruneEntry) error {
	if err := checkNotLive(ctx, store, entry.record); err != nil {
		return fmt.Errorf("refused: %w", err)
	}
	fresh, err := inspectDeletedTopic(ctx, s3Client, entry.record)
	if err != nil {
		return err
	}
	for _, obj := range fresh.objects {
		if strings.HasSuffix(obj.Key, ".index") {
			err = s3Client.DeleteIndex(ctx, obj.Key)
		} else {
			err = s3Client.DeleteSegment(ctx, obj.Key)
		}
		if err != nil {
			return fmt.Errorf("delete %s: %w", obj.Key, err)
		}
	}
	key := metadata.DeletedTopicKey(entry.record.Name, entry.record.Generation)
	if _, err := store.EtcdClient().Delete(ctx, key); err != nil {
		return fmt.Errorf("delete record %s: %w", key, err)
	}
	return nil
}
