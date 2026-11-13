// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/broker"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Exact method set of the published v1.6.3 Store; no modern interface embedding.
type legacyStore struct{ store metadata.Store }

func (s legacyStore) Metadata(ctx context.Context, topics []string) (*metadata.ClusterMetadata, error) {
	return s.store.Metadata(ctx, topics)
}
func (s legacyStore) NextOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	return s.store.NextOffset(ctx, topic, partition)
}
func (s legacyStore) UpdateOffsets(ctx context.Context, topic string, partition int32, lastOffset int64) error {
	return s.store.UpdateOffsets(ctx, topic, partition, lastOffset)
}
func (s legacyStore) CommitConsumerOffset(ctx context.Context, group, topic string, partition int32, offset int64, metadata string) error {
	return s.store.CommitConsumerOffset(ctx, group, topic, partition, offset, metadata)
}
func (s legacyStore) FetchConsumerOffset(ctx context.Context, group, topic string, partition int32) (int64, string, error) {
	return s.store.FetchConsumerOffset(ctx, group, topic, partition)
}
func (s legacyStore) ListConsumerOffsets(ctx context.Context) ([]metadata.ConsumerOffset, error) {
	return s.store.ListConsumerOffsets(ctx)
}
func (s legacyStore) PutConsumerGroup(ctx context.Context, group *metadatapb.ConsumerGroup) error {
	return s.store.PutConsumerGroup(ctx, group)
}
func (s legacyStore) FetchConsumerGroup(ctx context.Context, groupID string) (*metadatapb.ConsumerGroup, error) {
	return s.store.FetchConsumerGroup(ctx, groupID)
}
func (s legacyStore) ListConsumerGroups(ctx context.Context) ([]*metadatapb.ConsumerGroup, error) {
	return s.store.ListConsumerGroups(ctx)
}
func (s legacyStore) DeleteConsumerGroup(ctx context.Context, groupID string) error {
	return s.store.DeleteConsumerGroup(ctx, groupID)
}
func (s legacyStore) FetchTopicConfig(ctx context.Context, topic string) (*metadatapb.TopicConfig, error) {
	return s.store.FetchTopicConfig(ctx, topic)
}
func (s legacyStore) UpdateTopicConfig(ctx context.Context, cfg *metadatapb.TopicConfig) error {
	return s.store.UpdateTopicConfig(ctx, cfg)
}
func (s legacyStore) CreatePartitions(ctx context.Context, topic string, partitionCount int32) error {
	return s.store.CreatePartitions(ctx, topic, partitionCount)
}
func (s legacyStore) CreateTopic(ctx context.Context, spec metadata.TopicSpec) (*protocol.MetadataTopic, error) {
	return s.store.CreateTopic(ctx, spec)
}
func (s legacyStore) DeleteTopic(ctx context.Context, name string) error {
	return s.store.DeleteTopic(ctx, name)
}

var _ metadata.Store = legacyStore{}
var _ = map[broker.CoordinatorConfig]bool{}
var _ = map[metadata.LeaseManagerConfig]bool{}
var _ = map[metadata.PartitionLeaseConfig]bool{}
var _ = map[storage.PartitionLogConfig]bool{}

func TestPublishedLeaseAndLogConfigs(t *testing.T) {
	lease := metadata.LeaseManagerConfig{"broker", "/leases", 10, nil, "partition"}
	partition := metadata.PartitionLeaseConfig{"broker", 10, nil}
	log := storage.PartitionLogConfig{storage.WriteBufferConfig{}, storage.SegmentWriterConfig{}, 2, true, nil}
	if lease != (metadata.LeaseManagerConfig{BrokerID: "broker", Prefix: "/leases", LeaseTTLSeconds: 10, ResourceKind: "partition"}) ||
		partition != (metadata.PartitionLeaseConfig{BrokerID: "broker", LeaseTTLSeconds: 10}) ||
		log != (storage.PartitionLogConfig{ReadAheadSegments: 2, CacheEnabled: true}) {
		t.Fatal("published config equality changed")
	}
}

func TestPublishedCoordinatorConfig(t *testing.T) {
	// The original unkeyed literal must also compile unchanged.
	cfg := broker.CoordinatorConfig{time.Hour}
	if cfg != (broker.CoordinatorConfig{CleanupInterval: time.Hour}) {
		t.Fatal("config equality")
	}
	c := broker.NewGroupCoordinator(metadata.NewInMemoryStore(metadata.ClusterMetadata{}), protocol.MetadataBroker{}, &cfg)
	c.Stop()
}

func TestPublishedStoreOffsetFetchAdapter(t *testing.T) {
	ctx := context.Background()
	base := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	for _, group := range []string{"readers", "readers-other"} {
		if err := base.CommitConsumerOffset(ctx, group, "orders", 0, 42, "checkpoint"); err != nil {
			t.Fatal(err)
		}
	}
	c := broker.NewGroupCoordinator(legacyStore{store: base}, protocol.MetadataBroker{}, &broker.CoordinatorConfig{CleanupInterval: time.Hour})
	defer c.Stop()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Version, req.Group = 5, "readers"
	resp, err := c.OffsetFetch(ctx, req)
	if err != nil || resp.ErrorCode != 0 || len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatal(resp, err)
	}
	part := resp.Topics[0].Partitions[0]
	if part.Offset != 42 || part.Metadata == nil || *part.Metadata != "checkpoint" {
		t.Fatal(part)
	}
}
