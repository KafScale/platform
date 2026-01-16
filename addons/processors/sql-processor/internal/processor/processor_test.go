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

package processor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/checkpoint"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/decoder"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/discovery"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/sink"
)

func TestMapRecords(t *testing.T) {
	records := []decoder.Record{
		{Topic: "orders", Partition: 1, Offset: 10, Value: []byte("a")},
		{Topic: "orders", Partition: 1, Offset: 11, Value: []byte("b")},
	}
	out := mapRecords(records)
	if len(out) != 2 {
		t.Fatalf("expected 2 records, got %d", len(out))
	}
	if out[0].Offset != 10 || string(out[0].Payload) != "a" {
		t.Fatalf("unexpected mapped record: %+v", out[0])
	}
}

func TestFilterRecords(t *testing.T) {
	records := []sink.Record{
		{Offset: 1},
		{Offset: 5},
		{Offset: 10},
	}
	out := filterRecords(records, 5)
	if len(out) != 1 || out[0].Offset != 10 {
		t.Fatalf("unexpected filtered records: %+v", out)
	}
}

func TestTopicLocker(t *testing.T) {
	locker := newTopicLocker()
	unlock := locker.Lock("orders")
	done := make(chan struct{})
	go func() {
		unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatalf("lock release timed out")
	}
}

func TestRunContextCancelClosesSink(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mock := &closeSink{}
	p := &Processor{
		sink:  mock,
		locks: newTopicLocker(),
	}
	if err := p.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !mock.closed() {
		t.Fatalf("expected sink to close")
	}
}

func TestRunProcessesSegment(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	segment := discovery.SegmentRef{
		Topic:      "orders",
		Partition:  0,
		SegmentKey: "seg-1",
		IndexKey:   "idx-1",
	}
	records := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Value: []byte("a")},
		{Topic: "orders", Partition: 0, Offset: 2, Value: []byte("b")},
	}
	discover := &mockDiscovery{segments: []discovery.SegmentRef{segment}}
	dec := &mockDecoder{records: map[string][]decoder.Record{"seg-1": records}}
	store := &mockStore{}
	writer := &recordSink{written: make(chan []sink.Record, 1)}

	p := &Processor{
		discover: discover,
		decode:   dec,
		store:    store,
		sink:     writer,
		locks:    newTopicLocker(),
	}

	done := make(chan struct{})
	go func() {
		_ = p.Run(ctx)
		close(done)
	}()

	select {
	case <-writer.written:
		cancel()
	case <-time.After(6 * time.Second):
		t.Fatalf("timed out waiting for write")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("processor did not stop")
	}

	if store.commitCount == 0 {
		t.Fatalf("expected commit calls")
	}
}

type closeSink struct {
	mu         sync.Mutex
	closedFlag bool
}

func (c *closeSink) Write(ctx context.Context, records []sink.Record) error {
	return nil
}

func (c *closeSink) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closedFlag = true
	return nil
}

func (c *closeSink) closed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closedFlag
}

type mockDiscovery struct {
	segments []discovery.SegmentRef
}

func (m *mockDiscovery) ListCompleted(ctx context.Context) ([]discovery.SegmentRef, error) {
	return m.segments, nil
}

type mockDecoder struct {
	records map[string][]decoder.Record
}

func (m *mockDecoder) Decode(ctx context.Context, segmentKey, indexKey string, topic string, partition int32) ([]decoder.Record, error) {
	return m.records[segmentKey], nil
}

type mockStore struct {
	commitCount int
}

func (m *mockStore) ClaimLease(ctx context.Context, topic string, partition int32, ownerID string) (checkpoint.Lease, error) {
	return checkpoint.Lease{Topic: topic, Partition: partition, OwnerID: ownerID}, nil
}

func (m *mockStore) RenewLease(ctx context.Context, lease checkpoint.Lease) error {
	return nil
}

func (m *mockStore) ReleaseLease(ctx context.Context, lease checkpoint.Lease) error {
	return nil
}

func (m *mockStore) LoadOffset(ctx context.Context, topic string, partition int32) (checkpoint.OffsetState, error) {
	return checkpoint.OffsetState{Topic: topic, Partition: partition, Offset: 0}, nil
}

func (m *mockStore) CommitOffset(ctx context.Context, state checkpoint.OffsetState) error {
	m.commitCount++
	return nil
}

type recordSink struct {
	written chan []sink.Record
}

func (r *recordSink) Write(ctx context.Context, records []sink.Record) error {
	select {
	case r.written <- records:
	default:
	}
	return nil
}

func (r *recordSink) Close(ctx context.Context) error {
	return nil
}
