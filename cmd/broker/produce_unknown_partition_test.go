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
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type partitionLookupStore struct {
	metadata.Store
	reads      atomic.Int32
	creates    atomic.Int32
	createRace bool
	stale      bool
}

func (s *partitionLookupStore) NextOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	s.reads.Add(1)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.stale {
		return 0, metadata.ErrUnknownTopic
	}
	return s.Store.NextOffset(ctx, topic, partition)
}
func (s *partitionLookupStore) CreateTopic(ctx context.Context, spec metadata.TopicSpec) (*protocol.MetadataTopic, error) {
	s.creates.Add(1)
	if s.createRace {
		s.createRace = false
		_, err := s.Store.CreateTopic(ctx, metadata.TopicSpec{Name: spec.Name, NumPartitions: 1, ReplicationFactor: 1})
		if err != nil {
			return nil, err
		}
		return nil, metadata.ErrTopicExists
	}
	return s.Store.CreateTopic(ctx, spec)
}

func producePromptly(t *testing.T, h *handler, req *kmsg.ProduceRequest) []byte {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		raw []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		raw, err := h.handleProduce(ctx, &protocol.RequestHeader{APIVersion: 3}, req)
		done <- result{raw, err}
	}()
	select {
	case out := <-done:
		if out.err != nil {
			t.Fatal(out.err)
		}
		return out.raw
	case <-time.After(3 * time.Second):
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("produce did not stop after cancellation")
		}
		t.Fatal("produce to a missing partition waited instead of returning promptly")
		return nil
	}
}
func unknownPartitionProduce(topic string, partition int32, acks int16, timeout int32) *kmsg.ProduceRequest {
	return &kmsg.ProduceRequest{Acks: acks, TimeoutMillis: timeout, Topics: []kmsg.ProduceRequestTopic{{Topic: topic, Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: partition, Records: testBatchBytes(0, 0, 1)}}}}}
}
func TestProduceRejectsUnknownPartitionPromptly(t *testing.T) {
	for _, partition := range []int32{-1, 1, 7, 1<<31 - 1} {
		for _, acks := range []int16{-1, 1, 0} {
			for _, timeout := range []int32{0, 30000} {
				t.Run(fmt.Sprintf("partition=%d/acks=%d/timeout=%d", partition, acks, timeout), func(t *testing.T) {
					base := metadata.NewInMemoryStore(defaultMetadata())
					store := &partitionLookupStore{Store: base}
					h := newTestHandler(store)
					t.Cleanup(h.coordinator.Stop)
					h.autoCreateTopics = true
					raw := producePromptly(t, h, unknownPartitionProduce("orders", partition, acks, timeout))
					if acks == 0 {
						if raw != nil {
							t.Fatal("acks=0 returned a response")
						}
					} else {
						p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
						if p.Partition != partition || p.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
							t.Fatalf("partition error=%d, want UNKNOWN_TOPIC_OR_PARTITION", p.ErrorCode)
						}
					}
					if store.creates.Load() > 1 || store.reads.Load() > 2 {
						t.Fatalf("repeated auto-creation: creates=%d reads=%d", store.creates.Load(), store.reads.Load())
					}
					meta, err := base.Metadata(context.Background(), []string{"orders"})
					if err != nil {
						t.Fatal(err)
					}
					if len(meta.Topics[0].Partitions) != 1 {
						t.Fatal("invalid produce expanded existing topic")
					}
					if next, err := base.NextOffset(context.Background(), "orders", 0); err != nil || next != 0 {
						t.Fatalf("valid partition changed: %d %v", next, err)
					}
					if len(h.logs) != 0 {
						t.Fatal("missing partition initialized a log")
					}
				})
			}
		}
	}
}

func TestProduceUnknownPartitionWithoutAutoCreate(t *testing.T) {
	store := &partitionLookupStore{Store: metadata.NewInMemoryStore(defaultMetadata())}
	h := newTestHandler(store)
	t.Cleanup(h.coordinator.Stop)
	h.autoCreateTopics = false
	raw := producePromptly(t, h, unknownPartitionProduce("orders", 7, -1, 0))
	if p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]; p.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatal(p.ErrorCode)
	}
	if store.creates.Load() != 0 {
		t.Fatal("auto-created with feature disabled")
	}
}
func TestProduceContinuesPastUnknownPartition(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	h := newTestHandler(store)
	t.Cleanup(h.coordinator.Stop)
	h.autoCreateTopics = true
	req := unknownPartitionProduce("orders", 7, -1, 0)
	req.Topics[0].Partitions = append(req.Topics[0].Partitions, kmsg.ProduceRequestTopicPartition{Partition: 0, Records: testBatchBytes(0, 0, 1)})
	raw := producePromptly(t, h, req)
	parts := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions
	if len(parts) != 2 || parts[0].ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION || parts[1].ErrorCode != protocol.NONE || parts[1].BaseOffset != 0 {
		t.Fatalf("unexpected partition results: %+v", parts)
	}
	if next, err := store.NextOffset(context.Background(), "orders", 0); err != nil || next != 1 {
		t.Fatalf("valid partition did not advance: %d %v", next, err)
	}
}
func TestProduceAutoCreateRaceRejectsMissingPartition(t *testing.T) {
	base := metadata.NewInMemoryStore(defaultMetadata())
	store := &partitionLookupStore{Store: base, createRace: true}
	h := newTestHandler(store)
	t.Cleanup(h.coordinator.Stop)
	h.autoCreateTopics = true
	raw := producePromptly(t, h, unknownPartitionProduce("raced", 7, -1, 0))
	if p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]; p.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatal(p.ErrorCode)
	}
	if store.creates.Load() != 1 || store.reads.Load() != 2 {
		t.Fatalf("unbounded retry after concurrent create: creates=%d reads=%d", store.creates.Load(), store.reads.Load())
	}
	meta, err := base.Metadata(context.Background(), []string{"raced"})
	if err != nil || len(meta.Topics) != 1 || len(meta.Topics[0].Partitions) != 1 {
		t.Fatal("changed concurrent creator's topic", err)
	}
}
func TestProduceAutoCreateRetriesOnceWhenMetadataStaysMissing(t *testing.T) {
	store := &partitionLookupStore{Store: metadata.NewInMemoryStore(defaultMetadata()), stale: true}
	h := newTestHandler(store)
	t.Cleanup(h.coordinator.Stop)
	h.autoCreateTopics = true
	raw := producePromptly(t, h, unknownPartitionProduce("new-topic", 0, -1, 0))
	if p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]; p.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatal(p.ErrorCode)
	}
	if store.creates.Load() != 1 || store.reads.Load() != 2 {
		t.Fatalf("metadata retry did not stop: creates=%d reads=%d", store.creates.Load(), store.reads.Load())
	}
}
func TestProduceAutoCreatesMissingTopicWithRequestedPartition(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	h := newTestHandler(store)
	t.Cleanup(h.coordinator.Stop)
	h.autoCreateTopics = true
	raw := producePromptly(t, h, unknownPartitionProduce("new-topic", 2, -1, 0))
	if p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]; p.ErrorCode != protocol.NONE || p.BaseOffset != 0 {
		t.Fatalf("auto-create failed: %+v", p)
	}
	if next, err := store.NextOffset(context.Background(), "new-topic", 2); err != nil || next != 1 {
		t.Fatalf("created partition not written: %d %v", next, err)
	}
}
func TestProduceRejectsUnknownPartitionWithEtcdLeases(t *testing.T) {
	store := newEtcdTopicStore(t, context.Background())
	h := newTestHandler(store)
	t.Cleanup(h.coordinator.Stop)
	t.Cleanup(h.leaseManager.ReleaseAll)
	h.autoCreateTopics = true
	raw := producePromptly(t, h, unknownPartitionProduce("orders", 7, -1, 0))
	if p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]; p.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatal(p.ErrorCode)
	}
	if len(h.logs) != 0 {
		t.Fatal("missing partition initialized a log with leases enabled")
	}
}

type staleLeaseMetadataStore struct {
	*metadata.EtcdStore
	refreshed  atomic.Bool
	refreshes  atomic.Int32
	refreshErr error
}

func (s *staleLeaseMetadataStore) Metadata(ctx context.Context, topics []string) (*metadata.ClusterMetadata, error) {
	if !s.refreshed.Load() {
		return &metadata.ClusterMetadata{}, nil
	}
	return s.EtcdStore.Metadata(ctx, topics)
}
func (s *staleLeaseMetadataStore) RefreshSnapshot(ctx context.Context) error {
	s.refreshes.Add(1)
	if s.refreshErr != nil {
		return s.refreshErr
	}
	if err := s.EtcdStore.RefreshSnapshot(ctx); err != nil {
		return err
	}
	s.refreshed.Store(true)
	return nil
}

func TestProduceUnknownPartitionWithAnotherLeaseOwner(t *testing.T) {
	store := newEtcdTopicStore(t, context.Background())
	a := newTestHandler(store)
	b := newHandler(store, storage.NewMemoryS3Client(), protocol.MetadataBroker{NodeID: 2}, testLogger())
	for _, h := range []*handler{a, b} {
		t.Cleanup(h.coordinator.Stop)
		t.Cleanup(h.leaseManager.ReleaseAll)
		h.autoCreateTopics = true
	}
	for _, h := range []*handler{a, b} {
		raw := producePromptly(t, h, unknownPartitionProduce("orders", 7, -1, 0))
		p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
		if p.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
			t.Fatalf("broker %d returned %d for a nonexistent partition", h.brokerInfo.NodeID, p.ErrorCode)
		}
	}

	// A real partition on the other broker must still produce a leader retry.
	raw := producePromptly(t, a, unknownPartitionProduce("orders", 0, -1, 0))
	if p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]; p.ErrorCode != protocol.NONE {
		t.Fatal(p.ErrorCode)
	}
	stale := &staleLeaseMetadataStore{EtcdStore: store}
	b.store = stale
	req := unknownPartitionProduce("orders", 0, -1, 0)
	req.Topics[0].Partitions = append(req.Topics[0].Partitions, unknownPartitionProduce("orders", 7, -1, 0).Topics[0].Partitions[0])
	raw = producePromptly(t, b, req)
	parts := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions
	if len(parts) != 2 || parts[0].ErrorCode != protocol.NOT_LEADER_OR_FOLLOWER || parts[1].ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatalf("mixed lease errors were misclassified: %+v", parts)
	}
	if !stale.refreshed.Load() || stale.refreshes.Load() != 1 {
		t.Fatal("lease errors did not use one fresh metadata snapshot")
	}
	if next, err := store.NextOffset(context.Background(), "orders", 0); err != nil || next != 1 {
		t.Fatalf("non-owner changed valid offsets: %d %v", next, err)
	}
	// A failed refresh cannot establish that a partition is absent.
	failed := &staleLeaseMetadataStore{EtcdStore: store, refreshErr: fmt.Errorf("snapshot unavailable")}
	b.store = failed
	raw = producePromptly(t, b, unknownPartitionProduce("orders", 7, -1, 0))
	if p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]; p.ErrorCode != protocol.NOT_LEADER_OR_FOLLOWER {
		t.Fatalf("failed metadata refresh changed the known ownership error: %d", p.ErrorCode)
	}
	if failed.refreshes.Load() != 1 {
		t.Fatal("failed metadata refresh was not exercised")
	}
}

func TestProduceConcurrentAutoCreateWithRequestedPartition(t *testing.T) {
	const producers = 32
	store := metadata.NewInMemoryStore(defaultMetadata())
	h := newTestHandler(store)
	t.Cleanup(h.coordinator.Stop)
	h.autoCreateTopics = true
	h.flushOnAck = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type result struct {
		raw []byte
		err error
	}
	done := make(chan result, producers)
	for range producers {
		go func() {
			raw, err := h.handleProduce(ctx, &protocol.RequestHeader{APIVersion: 3}, unknownPartitionProduce("concurrent-topic", 2, -1, 0))
			done <- result{raw, err}
		}()
	}
	offsets := make(map[int64]bool)
	for range producers {
		select {
		case out := <-done:
			if out.err != nil {
				t.Fatal(out.err)
			}
			p := decodeKmsgResponse(t, 3, out.raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
			if p.ErrorCode != protocol.NONE {
				t.Fatalf("concurrent auto-create rejected valid partition: %d", p.ErrorCode)
			}
			if offsets[p.BaseOffset] {
				t.Fatalf("duplicate acknowledged offset %d", p.BaseOffset)
			}
			offsets[p.BaseOffset] = true
		case <-ctx.Done():
			t.Fatal("concurrent auto-creation did not finish", ctx.Err())
		}
	}
	if next, err := store.NextOffset(context.Background(), "concurrent-topic", 2); err != nil || next != producers {
		t.Fatalf("lost acknowledged offsets: %d %v", next, err)
	}
}
