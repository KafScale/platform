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

package processor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/checkpoint"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/config"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/decoder"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/discovery"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/sink"
)

func TestFilterRecords(t *testing.T) {
	records := []sink.Record{
		{Topic: "orders", Partition: 0, Offset: 10},
		{Topic: "orders", Partition: 0, Offset: 11},
		{Topic: "orders", Partition: 0, Offset: 12},
	}

	filtered := filterRecords(records, 10)
	if len(filtered) != 2 {
		t.Fatalf("expected 2 records, got %d", len(filtered))
	}
	if filtered[0].Offset != 11 || filtered[1].Offset != 12 {
		t.Fatalf("unexpected offsets: %d, %d", filtered[0].Offset, filtered[1].Offset)
	}

	filtered = filterRecords(records, 12)
	if len(filtered) != 0 {
		t.Fatalf("expected 0 records, got %d", len(filtered))
	}
}

type testStore struct {
	mu           sync.Mutex
	offsets      map[string]int64
	claimed      []checkpoint.Lease
	renewCalls   int32
	releaseCalls int32
}

func (s *testStore) ClaimLease(ctx context.Context, topic string, partition int32, ownerID string) (checkpoint.Lease, error) {
	lease := checkpoint.Lease{Topic: topic, Partition: partition, OwnerID: ownerID}
	s.mu.Lock()
	s.claimed = append(s.claimed, lease)
	s.mu.Unlock()
	return lease, nil
}

func (s *testStore) RenewLease(ctx context.Context, lease checkpoint.Lease) error {
	atomic.AddInt32(&s.renewCalls, 1)
	return nil
}

func (s *testStore) ReleaseLease(ctx context.Context, lease checkpoint.Lease) error {
	atomic.AddInt32(&s.releaseCalls, 1)
	return nil
}

func (s *testStore) LoadOffset(ctx context.Context, topic string, partition int32) (checkpoint.OffsetState, error) {
	key := offsetKey(topic, partition)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.offsets == nil {
		s.offsets = make(map[string]int64)
	}
	offset, ok := s.offsets[key]
	if !ok {
		offset = -1
	}
	return checkpoint.OffsetState{Topic: topic, Partition: partition, Offset: offset}, nil
}

func (s *testStore) CommitOffset(ctx context.Context, state checkpoint.OffsetState) error {
	key := offsetKey(state.Topic, state.Partition)
	s.mu.Lock()
	if s.offsets == nil {
		s.offsets = make(map[string]int64)
	}
	s.offsets[key] = state.Offset
	s.mu.Unlock()
	return nil
}

type testLister struct {
	segments []discovery.SegmentRef
}

func (l *testLister) ListCompleted(ctx context.Context) ([]discovery.SegmentRef, error) {
	return l.segments, nil
}

type testDecoder struct {
	records map[string][]decoder.Record
}

func (d *testDecoder) Decode(ctx context.Context, segmentKey string, indexKey string, topic string, partition int32) ([]decoder.Record, error) {
	return d.records[segmentKey], nil
}

type testSink struct {
	mu     sync.Mutex
	all    []sink.Record
	writes chan struct{}
}

func (s *testSink) Write(ctx context.Context, records []sink.Record) error {
	s.mu.Lock()
	s.all = append(s.all, records...)
	s.mu.Unlock()
	select {
	case s.writes <- struct{}{}:
	default:
	}
	return nil
}

func (s *testSink) Close(ctx context.Context) error {
	return nil
}

func TestStartLeaseRenewal(t *testing.T) {
	originalInterval := leaseRenewInterval
	leaseRenewInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		leaseRenewInterval = originalInterval
	})

	store := &testStore{}
	p := &Processor{store: store}

	leaseLost := make(chan error, 1)
	stop := p.startLeaseRenewal(context.Background(), checkpoint.Lease{Topic: "orders", Partition: 0}, leaseLost)
	time.Sleep(35 * time.Millisecond)
	stop()

	if atomic.LoadInt32(&store.renewCalls) == 0 {
		t.Fatalf("expected at least one lease renewal call")
	}
}

func TestProcessorPinsLeaseToSinglePartition(t *testing.T) {
	segments := []discovery.SegmentRef{
		{
			Topic:      "orders",
			Partition:  0,
			BaseOffset: 0,
			SegmentKey: "segment-0",
			IndexKey:   "index-0",
		},
		{
			Topic:      "orders",
			Partition:  1,
			BaseOffset: 0,
			SegmentKey: "segment-1",
			IndexKey:   "index-1",
		},
	}
	records := map[string][]decoder.Record{
		"segment-0": {
			{Topic: "orders", Partition: 0, Offset: 10, Timestamp: 1},
		},
		"segment-1": {
			{Topic: "orders", Partition: 1, Offset: 20, Timestamp: 1},
		},
	}

	store := &testStore{}
	sinkWriter := &testSink{writes: make(chan struct{}, 1)}
	p := &Processor{
		cfg: config.Config{
			Processor: config.ProcessorConfig{PollIntervalSeconds: 1},
		},
		discover:  &testLister{segments: segments},
		decode:    &testDecoder{records: records},
		store:     store,
		sink:      sinkWriter,
		validator: nil,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- p.Run(ctx)
	}()

	select {
	case <-sinkWriter.writes:
		cancel()
	case <-ctx.Done():
		t.Fatal("timed out waiting for write")
	}

	if err := <-done; err != nil {
		t.Fatalf("processor run failed: %v", err)
	}

	store.mu.Lock()
	claimed := append([]checkpoint.Lease(nil), store.claimed...)
	store.mu.Unlock()

	if len(claimed) != 1 {
		t.Fatalf("expected 1 lease claim, got %d", len(claimed))
	}
	if claimed[0].Partition != 0 {
		t.Fatalf("expected partition 0 to be claimed, got %d", claimed[0].Partition)
	}
	if atomic.LoadInt32(&store.releaseCalls) != 1 {
		t.Fatalf("expected 1 lease release, got %d", atomic.LoadInt32(&store.releaseCalls))
	}

	sinkWriter.mu.Lock()
	defer sinkWriter.mu.Unlock()
	for _, record := range sinkWriter.all {
		if record.Partition != 0 {
			t.Fatalf("unexpected write for partition %d", record.Partition)
		}
	}
}

func offsetKey(topic string, partition int32) string {
	return fmt.Sprintf("%s:%d", topic, partition)
}
