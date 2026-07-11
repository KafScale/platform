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
	"errors"
	"strings"
	"testing"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestInMemoryStoreMetadata_AllTopics(t *testing.T) {
	clusterID := "cluster-1"
	store := NewInMemoryStore(ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "localhost", Port: 9092},
		},
		ControllerID: 1,
		Topics: []protocol.MetadataTopic{
			{Topic: kmsg.StringPtr("orders")},
			{Topic: kmsg.StringPtr("payments")},
		},
		ClusterID: &clusterID,
	})

	meta, err := store.Metadata(context.Background(), nil)
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}

	if len(meta.Brokers) != 1 || meta.Brokers[0].NodeID != 1 {
		t.Fatalf("unexpected brokers: %#v", meta.Brokers)
	}
	if len(meta.Topics) != 2 {
		t.Fatalf("expected 2 topics got %d", len(meta.Topics))
	}
	if meta.ClusterID == nil || *meta.ClusterID != "cluster-1" {
		t.Fatalf("cluster id mismatch: %#v", meta.ClusterID)
	}
}

func TestInMemoryStoreMetadata_FilterTopics(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{
		Topics: []protocol.MetadataTopic{
			{Topic: kmsg.StringPtr("orders")},
		},
	})

	meta, err := store.Metadata(context.Background(), []string{"orders", "missing"})
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if len(meta.Topics) != 2 {
		t.Fatalf("expected 2 topics got %d", len(meta.Topics))
	}
	if meta.Topics[1].ErrorCode != 3 {
		t.Fatalf("expected missing topic error code 3 got %d", meta.Topics[1].ErrorCode)
	}
}

func TestInMemoryStoreMetadata_ContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Metadata(ctx, nil); err == nil {
		t.Fatalf("expected context error")
	}
}

func TestInMemoryStoreUpdate(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	store.Update(ClusterMetadata{
		ControllerID: 2,
	})
	meta, err := store.Metadata(context.Background(), nil)
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if meta.ControllerID != 2 {
		t.Fatalf("controller id mismatch: %d", meta.ControllerID)
	}
}

func TestCloneMetadataIsolation(t *testing.T) {
	clusterID := "cluster"
	store := NewInMemoryStore(ClusterMetadata{
		Brokers:   []protocol.MetadataBroker{{NodeID: 1}},
		ClusterID: &clusterID,
	})

	meta, err := store.Metadata(context.Background(), nil)
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	meta.Brokers[0].NodeID = 99
	if meta.ClusterID == nil {
		t.Fatalf("expected cluster id copy")
	}
	// fetch again ensure original unaffected
	meta2, _ := store.Metadata(context.Background(), nil)
	if meta2.Brokers[0].NodeID != 1 {
		t.Fatalf("store state mutated via clone")
	}
}

func TestInMemoryStoreOffsets(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 1}},
	})
	ctx := context.Background()

	if _, err := store.NextOffset(ctx, "orders", 0); !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("expected unknown topic, got %v", err)
	}
	if _, err := store.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	offset, err := store.NextOffset(ctx, "orders", 0)
	if err != nil {
		t.Fatalf("NextOffset: %v", err)
	}
	if offset != 0 {
		t.Fatalf("expected initial offset 0 got %d", offset)
	}

	if err := store.UpdateOffsets(ctx, "orders", 0, 9); err != nil {
		t.Fatalf("UpdateOffsets: %v", err)
	}

	offset, err = store.NextOffset(ctx, "orders", 0)
	if err != nil {
		t.Fatalf("NextOffset: %v", err)
	}
	if offset != 10 {
		t.Fatalf("expected offset 10 got %d", offset)
	}
}

func TestInMemoryStoreConsumerGroups(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	group := &metadatapb.ConsumerGroup{
		GroupId:      "group-1",
		State:        "stable",
		ProtocolType: "consumer",
		Protocol:     "range",
		Leader:       "member-1",
		GenerationId: 2,
		Members: map[string]*metadatapb.GroupMember{
			"member-1": {
				Subscriptions: []string{"orders"},
				Assignments: []*metadatapb.Assignment{
					{Topic: "orders", Partitions: []int32{0}},
				},
			},
		},
	}
	if err := store.PutConsumerGroup(context.Background(), group); err != nil {
		t.Fatalf("PutConsumerGroup: %v", err)
	}
	loaded, err := store.FetchConsumerGroup(context.Background(), "group-1")
	if err != nil {
		t.Fatalf("FetchConsumerGroup: %v", err)
	}
	if loaded == nil || loaded.GenerationId != 2 || loaded.Leader != "member-1" {
		t.Fatalf("unexpected group data: %#v", loaded)
	}
	groups, err := store.ListConsumerGroups(context.Background())
	if err != nil {
		t.Fatalf("ListConsumerGroups: %v", err)
	}
	if len(groups) != 1 || groups[0].GetGroupId() != "group-1" {
		t.Fatalf("unexpected list groups: %#v", groups)
	}
	if err := store.DeleteConsumerGroup(context.Background(), "group-1"); err != nil {
		t.Fatalf("DeleteConsumerGroup: %v", err)
	}
	if loaded, err := store.FetchConsumerGroup(context.Background(), "group-1"); err != nil || loaded != nil {
		t.Fatalf("expected group deletion, got %#v err=%v", loaded, err)
	}
}

func TestInMemoryStoreConsumerOffsets(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx := context.Background()
	if err := store.CommitConsumerOffset(ctx, "group-1", "orders", 0, 12, "meta"); err != nil {
		t.Fatalf("CommitConsumerOffset: %v", err)
	}
	if err := store.CommitConsumerOffset(ctx, "group-1", "orders", 1, 42, ""); err != nil {
		t.Fatalf("CommitConsumerOffset: %v", err)
	}
	offsets, err := store.ListConsumerOffsets(ctx)
	if err != nil {
		t.Fatalf("ListConsumerOffsets: %v", err)
	}
	if len(offsets) != 2 {
		t.Fatalf("expected 2 offsets got %d", len(offsets))
	}
}

func TestInMemoryStoreCreateDeleteTopic(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 1}},
	})
	ctx := context.Background()
	if _, err := store.CreateTopic(ctx, TopicSpec{Name: "", NumPartitions: 0}); err == nil {
		t.Fatalf("expected invalid topic error")
	}
	topic, err := store.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 2, ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if topic == nil || *topic.Topic != "orders" {
		t.Fatalf("unexpected topic: %#v", topic)
	}
	if _, err := store.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 1}); err == nil {
		t.Fatalf("expected duplicate topic error")
	}
	if err := store.DeleteTopic(ctx, "missing"); !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("expected unknown topic error, got %v", err)
	}
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	meta, _ := store.Metadata(ctx, nil)
	if len(meta.Topics) != 0 {
		t.Fatalf("expected topic removed")
	}
}

func TestInMemoryStoreTopicConfigAndPartitions(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 1}},
	})
	ctx := context.Background()
	if _, err := store.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	cfg, err := store.FetchTopicConfig(ctx, "orders")
	if err != nil {
		t.Fatalf("FetchTopicConfig: %v", err)
	}
	cfg.RetentionMs = 60000
	if err := store.UpdateTopicConfig(ctx, cfg); err != nil {
		t.Fatalf("UpdateTopicConfig: %v", err)
	}
	updated, err := store.FetchTopicConfig(ctx, "orders")
	if err != nil {
		t.Fatalf("FetchTopicConfig: %v", err)
	}
	if updated.RetentionMs != 60000 {
		t.Fatalf("unexpected retention: %d", updated.RetentionMs)
	}
	if err := store.CreatePartitions(ctx, "orders", 3); err != nil {
		t.Fatalf("CreatePartitions: %v", err)
	}
	meta, err := store.Metadata(ctx, []string{"orders"})
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if len(meta.Topics) != 1 || len(meta.Topics[0].Partitions) != 3 {
		t.Fatalf("unexpected partition count: %#v", meta.Topics)
	}
}

// --- Additional store tests for coverage gaps ---

func TestFetchConsumerOffset(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx := context.Background()

	// Commit then fetch
	if err := store.CommitConsumerOffset(ctx, "g1", "orders", 0, 100, "meta-0"); err != nil {
		t.Fatalf("CommitConsumerOffset: %v", err)
	}
	offset, meta, err := store.FetchConsumerOffset(ctx, "g1", "orders", 0)
	if err != nil {
		t.Fatalf("FetchConsumerOffset: %v", err)
	}
	if offset != 100 {
		t.Fatalf("expected offset 100, got %d", offset)
	}
	if meta != "meta-0" {
		t.Fatalf("expected meta-0, got %q", meta)
	}

	// Fetch non-existent returns zero
	offset, meta, err = store.FetchConsumerOffset(ctx, "g1", "orders", 99)
	if err != nil {
		t.Fatalf("FetchConsumerOffset: %v", err)
	}
	if offset != 0 || meta != "" {
		t.Fatalf("expected 0/empty for missing key, got %d/%q", offset, meta)
	}
}

func TestFetchConsumerOffsetContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := store.FetchConsumerOffset(ctx, "g1", "orders", 0)
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestFetchTopicConfigUnknown(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	_, err := store.FetchTopicConfig(context.Background(), "missing")
	if !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("expected ErrUnknownTopic, got %v", err)
	}
}

func TestFetchTopicConfigContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := store.FetchTopicConfig(ctx, "orders")
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestFetchTopicConfigDefault(t *testing.T) {
	// Topic exists but no explicit config → should return default from topic metadata
	store := NewInMemoryStore(ClusterMetadata{
		Topics: []protocol.MetadataTopic{
			{
				Topic: kmsg.StringPtr("events"),
				Partitions: []protocol.MetadataPartition{
					{Partition: 0, Replicas: []int32{1, 2}},
					{Partition: 1, Replicas: []int32{1, 2}},
				},
			},
		},
	})
	cfg, err := store.FetchTopicConfig(context.Background(), "events")
	if err != nil {
		t.Fatalf("FetchTopicConfig: %v", err)
	}
	if cfg.Name != "events" {
		t.Fatalf("expected name events, got %q", cfg.Name)
	}
	if cfg.Partitions != 2 {
		t.Fatalf("expected 2 partitions, got %d", cfg.Partitions)
	}
	if cfg.ReplicationFactor != 2 {
		t.Fatalf("expected replication factor 2, got %d", cfg.ReplicationFactor)
	}
}

func TestUpdateTopicConfigInvalid(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx := context.Background()
	if err := store.UpdateTopicConfig(ctx, nil); !errors.Is(err, ErrInvalidTopic) {
		t.Fatalf("expected ErrInvalidTopic for nil, got %v", err)
	}
	if err := store.UpdateTopicConfig(ctx, &metadatapb.TopicConfig{Name: ""}); !errors.Is(err, ErrInvalidTopic) {
		t.Fatalf("expected ErrInvalidTopic for empty name, got %v", err)
	}
	if err := store.UpdateTopicConfig(ctx, &metadatapb.TopicConfig{Name: "missing"}); !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("expected ErrUnknownTopic, got %v", err)
	}
}

func TestUpdateTopicConfigContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := store.UpdateTopicConfig(ctx, &metadatapb.TopicConfig{Name: "orders"})
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestCreatePartitionsInvalid(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx := context.Background()
	if err := store.CreatePartitions(ctx, "", 3); !errors.Is(err, ErrInvalidTopic) {
		t.Fatalf("expected ErrInvalidTopic for empty name, got %v", err)
	}
	if err := store.CreatePartitions(ctx, "topic", 0); !errors.Is(err, ErrInvalidTopic) {
		t.Fatalf("expected ErrInvalidTopic for zero count, got %v", err)
	}
	if err := store.CreatePartitions(ctx, "missing", 3); !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("expected ErrUnknownTopic, got %v", err)
	}
}

func TestCreatePartitionsContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := store.CreatePartitions(ctx, "orders", 3)
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestCreatePartitionsShrink(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 1}},
	})
	ctx := context.Background()
	store.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 3, ReplicationFactor: 1})
	err := store.CreatePartitions(ctx, "orders", 2) // shrink
	if err == nil {
		t.Fatal("expected error for shrinking partitions")
	}
}

func TestCommitConsumerOffsetContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := store.CommitConsumerOffset(ctx, "g1", "orders", 0, 10, "")
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestListConsumerOffsetsContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := store.ListConsumerOffsets(ctx)
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestPutConsumerGroupContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "g1"})
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestFetchConsumerGroupContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := store.FetchConsumerGroup(ctx, "g1")
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestListConsumerGroupsContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := store.ListConsumerGroups(ctx)
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestDeleteConsumerGroupContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := store.DeleteConsumerGroup(ctx, "g1")
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestDeleteTopicContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := store.DeleteTopic(ctx, "orders")
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestCreateTopicContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := store.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 1, ReplicationFactor: 1})
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestNextOffsetContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := store.NextOffset(ctx, "orders", 0)
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestUpdateOffsetsContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := store.UpdateOffsets(ctx, "orders", 0, 10)
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestUpdateOffsetsSucceeds(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 1}},
	})
	ctx := context.Background()
	store.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 1, ReplicationFactor: 1})
	err := store.UpdateOffsets(ctx, "orders", 0, 10)
	if err != nil {
		t.Fatalf("UpdateOffsets: %v", err)
	}
	offset, err := store.NextOffset(ctx, "orders", 0)
	if err != nil {
		t.Fatalf("NextOffset: %v", err)
	}
	if offset != 11 {
		t.Fatalf("expected 11 (lastOffset+1), got %d", offset)
	}
}

func TestNextOffsetPartitionMismatch(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 1}},
	})
	ctx := context.Background()
	store.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 1, ReplicationFactor: 1})
	_, err := store.NextOffset(ctx, "orders", 99)
	if err == nil {
		t.Fatal("expected error for non-existent partition")
	}
}

func TestDefaultLeaderID(t *testing.T) {
	// No brokers → uses controller ID
	store := NewInMemoryStore(ClusterMetadata{ControllerID: 5})
	if got := store.defaultLeaderID(); got != 5 {
		t.Fatalf("expected controller ID 5, got %d", got)
	}

	// With brokers → uses first broker
	store = NewInMemoryStore(ClusterMetadata{
		Brokers:      []protocol.MetadataBroker{{NodeID: 7}},
		ControllerID: 5,
	})
	if got := store.defaultLeaderID(); got != 7 {
		t.Fatalf("expected broker 7, got %d", got)
	}
}

func TestCloneTopicConfigNil(t *testing.T) {
	if got := cloneTopicConfig(nil); got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestCloneTopicConfigWithData(t *testing.T) {
	cfg := &metadatapb.TopicConfig{
		Name:              "orders",
		Partitions:        3,
		ReplicationFactor: 2,
		RetentionMs:       86400000,
		Config:            map[string]string{"cleanup.policy": "compact"},
	}
	cloned := cloneTopicConfig(cfg)
	if cloned == cfg {
		t.Fatal("clone should not be same pointer")
	}
	if cloned.Name != "orders" || cloned.Partitions != 3 || cloned.ReplicationFactor != 2 {
		t.Fatalf("unexpected clone: %+v", cloned)
	}
	if cloned.Config["cleanup.policy"] != "compact" {
		t.Fatalf("expected config to be cloned")
	}
	// Mutation isolation
	cloned.Config["new-key"] = "val"
	if _, ok := cfg.Config["new-key"]; ok {
		t.Fatal("mutation should not affect original")
	}
}

func TestDefaultTopicConfigFromTopicNil(t *testing.T) {
	cfg := defaultTopicConfigFromTopic(nil, 1)
	if cfg == nil {
		t.Fatal("expected non-nil config for nil topic")
	}
}

func TestDefaultTopicConfigFromTopicWithReplicas(t *testing.T) {
	topic := &protocol.MetadataTopic{
		Topic: kmsg.StringPtr("events"),
		Partitions: []protocol.MetadataPartition{
			{Partition: 0, Replicas: []int32{1, 2, 3}},
			{Partition: 1, Replicas: []int32{1, 2, 3}},
		},
	}
	cfg := defaultTopicConfigFromTopic(topic, 0) // replicationFactor <= 0 triggers auto-detect
	if cfg.ReplicationFactor != 3 {
		t.Fatalf("expected replication factor 3 from replicas, got %d", cfg.ReplicationFactor)
	}
	if cfg.Partitions != 2 {
		t.Fatalf("expected 2 partitions, got %d", cfg.Partitions)
	}
	if cfg.RetentionMs != -1 {
		t.Fatalf("expected default retention -1, got %d", cfg.RetentionMs)
	}
}

func TestParseConsumerKeyEdgeCases(t *testing.T) {
	// Valid
	group, topic, partition, ok := parseConsumerKey("g1:orders:0")
	if !ok || group != "g1" || topic != "orders" || partition != 0 {
		t.Fatalf("unexpected parse result: %q %q %d %v", group, topic, partition, ok)
	}

	// Too few parts
	_, _, _, ok = parseConsumerKey("g1:orders")
	if ok {
		t.Fatal("expected false for 2-part key")
	}

	// Bad partition
	_, _, _, ok = parseConsumerKey("g1:orders:abc")
	if ok {
		t.Fatal("expected false for non-numeric partition")
	}

	// Too many parts
	_, _, _, ok = parseConsumerKey("g1:orders:0:extra")
	if ok {
		t.Fatal("expected false for 4-part key")
	}
}

func TestDeleteConsumerGroupNotFound(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	// delete of non-existent key is a no-op, should not error
	err := store.DeleteConsumerGroup(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("expected no error for deleting non-existent group, got %v", err)
	}
}

func TestPutConsumerGroupNilAndEmpty(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx := context.Background()
	err := store.PutConsumerGroup(ctx, nil)
	if err == nil {
		t.Fatal("expected error for nil group")
	}
	err = store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: ""})
	if err == nil {
		t.Fatal("expected error for empty group ID")
	}
}

func TestCloneConsumerGroupNil(t *testing.T) {
	if got := cloneConsumerGroup(nil); got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestCloneConsumerGroupWithAssignments(t *testing.T) {
	group := &metadatapb.ConsumerGroup{
		GroupId:      "g1",
		State:        "stable",
		ProtocolType: "consumer",
		Members: map[string]*metadatapb.GroupMember{
			"m1": {
				ClientId:      "client-1",
				Subscriptions: []string{"orders"},
				Assignments: []*metadatapb.Assignment{
					{Topic: "orders", Partitions: []int32{0, 1}},
				},
			},
		},
	}
	cloned := cloneConsumerGroup(group)
	if cloned == group {
		t.Fatal("should not be same pointer")
	}
	if len(cloned.Members) != 1 || cloned.Members["m1"].ClientId != "client-1" {
		t.Fatalf("unexpected clone: %+v", cloned)
	}
	// Mutation isolation
	cloned.Members["m1"].Assignments[0].Partitions[0] = 99
	if group.Members["m1"].Assignments[0].Partitions[0] == 99 {
		t.Fatal("mutation should not affect original")
	}
}

func TestDeleteTopicCleansOffsets(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 1}},
	})
	ctx := context.Background()
	store.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 2, ReplicationFactor: 1})
	store.UpdateOffsets(ctx, "orders", 0, 10)
	store.UpdateOffsets(ctx, "orders", 1, 20)

	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	// Offsets should be cleaned up
	_, err := store.NextOffset(ctx, "orders", 0)
	if !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("expected unknown topic after delete, got %v", err)
	}
}

func TestCreateTopicWithCustomPartitions(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 1}, {NodeID: 2}},
	})
	ctx := context.Background()
	topic, err := store.CreateTopic(ctx, TopicSpec{Name: "events", NumPartitions: 5, ReplicationFactor: 2})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if len(topic.Partitions) != 5 {
		t.Fatalf("expected 5 partitions, got %d", len(topic.Partitions))
	}
}

func TestCodecParseConsumerGroupID(t *testing.T) {
	// Valid
	id, ok := ParseConsumerGroupID("/kafscale/consumers/my-group/metadata")
	if !ok || id != "my-group" {
		t.Fatalf("expected my-group, got %q ok=%v", id, ok)
	}

	// Missing suffix
	_, ok = ParseConsumerGroupID("/kafscale/consumers/my-group/offsets")
	if ok {
		t.Fatal("expected false for missing /metadata suffix")
	}

	// Wrong prefix
	_, ok = ParseConsumerGroupID("/other/my-group/metadata")
	if ok {
		t.Fatal("expected false for wrong prefix")
	}

	// Empty group ID
	_, ok = ParseConsumerGroupID("/kafscale/consumers//metadata")
	if ok {
		t.Fatal("expected false for empty group ID")
	}

	// Nested path
	_, ok = ParseConsumerGroupID("/kafscale/consumers/a/b/metadata")
	if ok {
		t.Fatal("expected false for nested path")
	}
}

func TestCodecParseConsumerOffsetKey(t *testing.T) {
	// Valid
	group, topic, part, ok := ParseConsumerOffsetKey("/kafscale/consumers/g1/offsets/orders/5")
	if !ok || group != "g1" || topic != "orders" || part != 5 {
		t.Fatalf("unexpected: %q %q %d %v", group, topic, part, ok)
	}

	// Wrong prefix
	_, _, _, ok = ParseConsumerOffsetKey("/other/g1/offsets/orders/5")
	if ok {
		t.Fatal("expected false for wrong prefix")
	}

	// Missing offsets segment
	_, _, _, ok = ParseConsumerOffsetKey("/kafscale/consumers/g1/data/orders/5")
	if ok {
		t.Fatal("expected false for missing offsets segment")
	}

	// Non-numeric partition
	_, _, _, ok = ParseConsumerOffsetKey("/kafscale/consumers/g1/offsets/orders/abc")
	if ok {
		t.Fatal("expected false for non-numeric partition")
	}
}

func TestCodecEncodeDecodeRoundTrip(t *testing.T) {
	// TopicConfig round-trip
	cfg := &metadatapb.TopicConfig{
		Name:              "orders",
		Partitions:        3,
		ReplicationFactor: 2,
	}
	data, err := EncodeTopicConfig(cfg)
	if err != nil {
		t.Fatalf("EncodeTopicConfig: %v", err)
	}
	decoded, err := DecodeTopicConfig(data)
	if err != nil {
		t.Fatalf("DecodeTopicConfig: %v", err)
	}
	if decoded.Name != "orders" || decoded.Partitions != 3 {
		t.Fatalf("unexpected decoded: %+v", decoded)
	}

	// PartitionState round-trip
	state := &metadatapb.PartitionState{
		Topic:        "orders",
		Partition:    2,
		LeaderBroker: "broker-1",
		LeaderEpoch:  5,
	}
	stateData, err := EncodePartitionState(state)
	if err != nil {
		t.Fatalf("EncodePartitionState: %v", err)
	}
	decodedState, err := DecodePartitionState(stateData)
	if err != nil {
		t.Fatalf("DecodePartitionState: %v", err)
	}
	if decodedState.Topic != "orders" || decodedState.Partition != 2 || decodedState.LeaderEpoch != 5 {
		t.Fatalf("unexpected decoded state: %+v", decodedState)
	}
}

func TestCodecDecodeErrors(t *testing.T) {
	// Bad data
	_, err := DecodeTopicConfig([]byte{0xff, 0xff})
	if err == nil {
		t.Fatal("expected error for bad topic config data")
	}
	if !strings.Contains(err.Error(), "unmarshal") {
		t.Fatalf("expected unmarshal error, got: %v", err)
	}

	_, err = DecodePartitionState([]byte{0xff, 0xff})
	if err == nil {
		t.Fatal("expected error for bad partition state data")
	}

	_, err = DecodeConsumerGroup([]byte{0xff, 0xff})
	if err == nil {
		t.Fatal("expected error for bad consumer group data")
	}
}

func TestCodecConsumerGroupRoundTrip(t *testing.T) {
	group := &metadatapb.ConsumerGroup{
		GroupId:      "g1",
		State:        "stable",
		ProtocolType: "consumer",
		Protocol:     "range",
	}
	data, err := EncodeConsumerGroup(group)
	if err != nil {
		t.Fatalf("EncodeConsumerGroup: %v", err)
	}
	decoded, err := DecodeConsumerGroup(data)
	if err != nil {
		t.Fatalf("DecodeConsumerGroup: %v", err)
	}
	if decoded.GroupId != "g1" || decoded.State != "stable" {
		t.Fatalf("unexpected: %+v", decoded)
	}
}

func TestCodecKeyFunctions(t *testing.T) {
	if got := TopicConfigKey("orders"); got != "/kafscale/topics/orders/config" {
		t.Fatalf("TopicConfigKey: %q", got)
	}
	if got := PartitionStateKey("orders", 3); got != "/kafscale/topics/orders/partitions/3" {
		t.Fatalf("PartitionStateKey: %q", got)
	}
	if got := ConsumerGroupKey("g1"); got != "/kafscale/consumers/g1/metadata" {
		t.Fatalf("ConsumerGroupKey: %q", got)
	}
	if got := ConsumerGroupPrefix(); got != "/kafscale/consumers" {
		t.Fatalf("ConsumerGroupPrefix: %q", got)
	}
	if got := ConsumerOffsetKey("g1", "orders", 5); got != "/kafscale/consumers/g1/offsets/orders/5" {
		t.Fatalf("ConsumerOffsetKey: %q", got)
	}
	if got := BrokerRegistrationKey("broker-1"); got != "/kafscale/brokers/broker-1" {
		t.Fatalf("BrokerRegistrationKey: %q", got)
	}
	if got := PartitionAssignmentKey("orders", 2); got != "/kafscale/assignments/orders/2" {
		t.Fatalf("PartitionAssignmentKey: %q", got)
	}
}
