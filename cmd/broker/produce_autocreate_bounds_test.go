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

package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Reject unsafe specs before the real store allocates partition metadata, so
// the regression can reproduce the old request safely without exhausting RAM.
type boundedAutoCreateStore struct {
	metadata.Store
	configured int32
	creates    atomic.Int32
	requested  atomic.Int32
}

func (s *boundedAutoCreateStore) CreateTopic(ctx context.Context, spec metadata.TopicSpec) (*protocol.MetadataTopic, error) {
	s.creates.Add(1)
	s.requested.Store(spec.NumPartitions)
	if spec.NumPartitions != s.configured {
		return nil, fmt.Errorf("unsafe automatic partition count %d, configured %d", spec.NumPartitions, s.configured)
	}
	return s.Store.CreateTopic(ctx, spec)
}
func TestAutoCreateRejectsUnconfiguredMissingPartition(t *testing.T) {
	for _, configured := range []int32{1, 3} {
		for _, partition := range []int32{-1, configured, 7, 1<<31 - 2, 1<<31 - 1} {
			for _, acks := range []int16{-1, 0} {
				t.Run(fmt.Sprintf("configured=%d/partition=%d/acks=%d", configured, partition, acks), func(t *testing.T) {
					base := metadata.NewInMemoryStore(defaultMetadata())
					store := &boundedAutoCreateStore{Store: base, configured: configured}
					h := newTestHandler(store)
					t.Cleanup(h.coordinator.Stop)
					h.autoCreateTopics = true
					h.autoCreatePartitions = configured
					raw := producePromptly(t, h, unknownPartitionProduce("missing-boundary", partition, acks, 0))
					if acks == 0 {
						if raw != nil {
							t.Fatal("acks=0 returned a response")
						}
					} else {
						p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
						if p.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
							t.Errorf("partition error=%d, want UNKNOWN_TOPIC_OR_PARTITION", p.ErrorCode)
						}
					}
					if store.creates.Load() != 0 {
						t.Fatalf("invalid ID reached auto-creation: calls=%d requested=%d", store.creates.Load(), store.requested.Load())
					}
					if topicExists(t, context.Background(), base, "missing-boundary") {
						t.Fatal("invalid partition request created a topic")
					}
					if len(h.logs) != 0 {
						t.Fatal("invalid partition initialized a log")
					}
				})
			}
		}
	}
}
func TestAutoCreateUsesConfiguredCount(t *testing.T) {
	for _, partition := range []int32{0, 2} {
		t.Run(fmt.Sprintf("partition=%d", partition), func(t *testing.T) {
			base := metadata.NewInMemoryStore(defaultMetadata())
			store := &boundedAutoCreateStore{Store: base, configured: 3}
			h := newTestHandler(store)
			t.Cleanup(h.coordinator.Stop)
			h.autoCreateTopics = true
			h.autoCreatePartitions = 3
			h.flushOnAck = true
			raw := producePromptly(t, h, unknownPartitionProduce("configured-topic", partition, -1, 0))
			p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
			if p.ErrorCode != protocol.NONE {
				t.Fatalf("valid configured partition rejected: %d", p.ErrorCode)
			}
			meta, err := base.Metadata(context.Background(), []string{"configured-topic"})
			if err != nil {
				t.Fatal(err)
			}
			if len(meta.Topics) != 1 || len(meta.Topics[0].Partitions) != 3 || store.requested.Load() != 3 {
				t.Fatal("automatic topic did not use configured partition count")
			}
		})
	}
}

func (s *boundedAutoCreateStore) RefreshSnapshot(ctx context.Context) error {
	if store, ok := s.Store.(interface{ RefreshSnapshot(context.Context) error }); ok {
		return store.RefreshSnapshot(ctx)
	}
	return ctx.Err()
}

func TestAutoCreateMissingPartitionBoundsWithEtcdLeases(t *testing.T) {
	store := newEtcdTopicStore(t, context.Background())
	h := newTestHandler(store)
	t.Cleanup(h.coordinator.Stop)
	t.Cleanup(h.leaseManager.ReleaseAll)
	guarded := &boundedAutoCreateStore{Store: store, configured: 1}
	h.store = guarded
	h.autoCreateTopics = true
	h.autoCreatePartitions = 1
	for _, partition := range []int32{-1, 1, 1<<31 - 2, 1<<31 - 1} {
		for _, acks := range []int16{-1, 0} {
			raw := producePromptly(t, h, unknownPartitionProduce("leased-missing-boundary", partition, acks, 0))
			if acks != 0 {
				if p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]; p.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
					t.Fatalf("partition %d: %d", partition, p.ErrorCode)
				}
			} else if raw != nil {
				t.Fatal("acks=0 returned a response")
			}
		}
	}
	if guarded.creates.Load() != 0 || topicExists(t, context.Background(), store, "leased-missing-boundary") {
		t.Fatal("out-of-range request reached durable topic creation")
	}
}

type staleAutoCreateStore struct {
	*metadata.EtcdStore
	stale     atomic.Bool
	refreshes atomic.Int32
	creates   atomic.Int32
}

func (s *staleAutoCreateStore) NextOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	if s.stale.Load() {
		return 0, metadata.ErrUnknownTopic
	}
	return s.EtcdStore.NextOffset(ctx, topic, partition)
}
func (s *staleAutoCreateStore) RefreshSnapshot(ctx context.Context) error {
	s.refreshes.Add(1)
	if err := s.EtcdStore.RefreshSnapshot(ctx); err != nil {
		return err
	}
	s.stale.Store(false)
	return nil
}
func (s *staleAutoCreateStore) CreateTopic(ctx context.Context, spec metadata.TopicSpec) (*protocol.MetadataTopic, error) {
	s.creates.Add(1)
	return s.EtcdStore.CreateTopic(ctx, spec)
}
func TestAutoCreateLimitAllowsExistingLargerTopicAfterRefresh(t *testing.T) {
	store := newEtcdTopicStore(t, context.Background())
	if _, err := store.CreateTopic(context.Background(), metadata.TopicSpec{Name: "existing-large", NumPartitions: 3, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	h := newTestHandler(store)
	t.Cleanup(h.coordinator.Stop)
	t.Cleanup(h.leaseManager.ReleaseAll)
	h.autoCreateTopics = true
	h.autoCreatePartitions = 1
	h.flushOnAck = true
	stale := &staleAutoCreateStore{EtcdStore: store}
	stale.stale.Store(true)
	h.store = stale
	raw := producePromptly(t, h, unknownPartitionProduce("existing-large", 2, -1, 0))
	if p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]; p.ErrorCode != protocol.NONE || p.BaseOffset != 0 {
		t.Fatalf("existing partition above auto-create count was rejected: %+v", p)
	}
	if stale.refreshes.Load() != 1 || stale.creates.Load() != 0 {
		t.Fatalf("existing topic was not validated by refresh: refreshes=%d creates=%d", stale.refreshes.Load(), stale.creates.Load())
	}
	if next, err := store.NextOffset(context.Background(), "existing-large", 2); err != nil || next != 1 {
		t.Fatalf("existing partition was not written: %d %v", next, err)
	}
}
