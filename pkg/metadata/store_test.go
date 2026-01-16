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
	"testing"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

func TestInMemoryStoreMetadata_AllTopics(t *testing.T) {
	clusterID := "cluster-1"
	store := NewInMemoryStore(ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "localhost", Port: 9092},
		},
		ControllerID: 1,
		Topics: []protocol.MetadataTopic{
			{Name: "orders"},
			{Name: "payments"},
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
			{Name: "orders"},
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
	if topic == nil || topic.Name != "orders" {
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
