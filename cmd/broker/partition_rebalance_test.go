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
	"log/slog"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
)

type rebalanceMetadataStore struct {
	metadata.Store
	snapshot *metadata.ClusterMetadata
}

type handoverBlockedS3 struct {
	*storage.MemoryS3Client
	persisted chan struct{}
	resume    chan struct{}
	once      sync.Once
}

func (s *handoverBlockedS3) UploadSegment(ctx context.Context, key string, body []byte) error {
	if err := s.MemoryS3Client.UploadSegment(ctx, key, body); err != nil {
		return err
	}
	s.once.Do(func() { close(s.persisted); <-s.resume })
	return nil
}

func TestRebalanceFinishesActiveAppendBeforeRelease(t *testing.T) {
	t.Setenv("KAFSCALE_PRODUCE_SYNC_FLUSH", "true")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	meta := defaultMetadata()
	meta.Brokers = []protocol.MetadataBroker{{NodeID: 0, Host: "broker-0", Port: 9092}, {NodeID: 1, Host: "broker-1", Port: 9092}}
	meta.Topics[0].Partitions[0].Leader = 1
	store, err := metadata.NewEtcdStore(ctx, meta, metadata.EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s3 := &handoverBlockedS3{MemoryS3Client: storage.NewMemoryS3Client(), persisted: make(chan struct{}), resume: make(chan struct{})}
	var resume sync.Once
	defer resume.Do(func() { close(s3.resume) })
	a, b := newHandler(store, s3, meta.Brokers[0], testLogger()), newHandler(store, s3, meta.Brokers[1], testLogger())
	for _, h := range []*handler{a, b} {
		defer h.coordinator.Stop()
		defer h.leaseManager.ReleaseAll()
	}
	request := &kmsg.ProduceRequest{Acks: -1, TimeoutMillis: 1000, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: testBatchBytes(0, 0, 1)}}}}}
	type result struct {
		raw []byte
		err error
	}
	produced := make(chan result, 1)
	go func() {
		raw, err := a.handleProduce(ctx, &protocol.RequestHeader{APIVersion: 3}, request)
		produced <- result{raw, err}
	}()
	select {
	case <-s3.persisted:
	case <-ctx.Done():
		t.Fatal("append did not persist the segment")
	}
	moved := make(chan int, 1)
	go func() {
		moved <- a.rebalancePartitions(ctx, func(context.Context, string) bool { return true }, time.Minute)
	}()
	select {
	case <-moved:
		t.Fatal("handover released ownership while an append was still active")
	case <-time.After(100 * time.Millisecond):
	}
	if !a.leaseManager.Owns("orders", 0) {
		t.Fatal("active append lost its lease before completing")
	}
	// Requests arriving during the drain are rejected before another append,
	// so the proxy can route them to the preferred leader.
	raw, err := a.handleProduce(ctx, &protocol.RequestHeader{APIVersion: 3}, request)
	if err != nil {
		t.Fatal(err)
	}
	redirect := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
	if redirect.ErrorCode != protocol.NOT_LEADER_OR_FOLLOWER {
		t.Fatalf("new request was not redirected during draining: %+v", redirect)
	}
	resume.Do(func() { close(s3.resume) })
	first := <-produced
	if first.err != nil {
		t.Fatal(first.err)
	}
	p := decodeKmsgResponse(t, 3, first.raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
	if p.ErrorCode != 0 || p.BaseOffset != 0 {
		t.Fatalf("planned handover aborted a durable append: %+v", p)
	}
	if n := <-moved; n != 1 {
		t.Fatalf("handover released %d partitions, want 1", n)
	}
	raw, err = b.handleProduce(ctx, &protocol.RequestHeader{APIVersion: 3}, request)
	if err != nil {
		t.Fatal(err)
	}
	p = decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
	if p.ErrorCode != 0 || p.BaseOffset != 1 {
		t.Fatalf("new owner did not continue after the completed append: %+v", p)
	}
}

func (s rebalanceMetadataStore) Metadata(context.Context, []string) (*metadata.ClusterMetadata, error) {
	return s.snapshot, nil
}

func rebalanceTestLeases(t *testing.T, endpoints []string, brokerID string) *metadata.PartitionLeaseManager {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	m := metadata.NewPartitionLeaseManager(cli, metadata.PartitionLeaseConfig{BrokerID: brokerID, LeaseTTLSeconds: 10, Logger: slog.Default()})
	t.Cleanup(m.ReleaseAll)
	return m
}

func rebalanceTestHandler(t *testing.T, endpoints []string, flushOnAck bool) *handler {
	t.Helper()
	meta := metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 0, Host: "broker-0", Port: 9092}, {NodeID: 1, Host: "broker-1", Port: 9092}},
		Topics: []protocol.MetadataTopic{{
			Topic:      kmsg.StringPtr("orders"),
			Partitions: []protocol.MetadataPartition{{Partition: 0, Leader: 0}, {Partition: 1, Leader: 1}, {Partition: 2, Leader: 0}, {Partition: 3, Leader: 1}},
		}},
	}
	h := &handler{
		store:        metadata.NewInMemoryStore(meta),
		leaseManager: rebalanceTestLeases(t, endpoints, "0"),
		brokerInfo:   protocol.MetadataBroker{NodeID: 0, Host: "broker-0", Port: 9092},
		flushOnAck:   flushOnAck,
		logger:       testLogger(),
	}
	for p := int32(0); p < 4; p++ {
		if err := h.leaseManager.Acquire(context.Background(), "orders", p); err != nil {
			t.Fatalf("acquire orders/%d: %v", p, err)
		}
	}
	return h
}

func TestRebalanceHandsPartitionsToListeningPreferredBroker(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	h := rebalanceTestHandler(t, endpoints, true)
	ctx := context.Background()
	var dialed []string
	listening := func(_ context.Context, addr string) bool {
		dialed = append(dialed, addr)
		return addr == "broker-1:9092"
	}
	if got := h.rebalancePartitions(ctx, listening, time.Minute); got != 2 {
		t.Fatalf("released = %d, want 2", got)
	}
	if len(dialed) != 1 {
		t.Fatalf("dialed %v, want one check of broker 1", dialed)
	}
	for _, p := range []int32{0, 2} {
		if !h.leaseManager.Owns("orders", p) {
			t.Fatalf("broker 0 gave up orders/%d it is preferred for", p)
		}
	}
	other := rebalanceTestLeases(t, endpoints, "1")
	for _, p := range []int32{1, 3} {
		if h.leaseManager.Owns("orders", p) {
			t.Fatalf("broker 0 still owns orders/%d", p)
		}
		if err := h.leaseManager.Acquire(ctx, "orders", p); !errors.Is(err, metadata.ErrNotOwner) {
			t.Fatalf("broker 0 took orders/%d back during the hold: %v", p, err)
		}
		if err := other.Acquire(ctx, "orders", p); err != nil {
			t.Fatalf("broker 1 acquire orders/%d: %v", p, err)
		}
	}
	if got := h.rebalancePartitions(ctx, listening, time.Minute); got != 0 {
		t.Fatalf("second pass released = %d, want 0", got)
	}
}

func TestRebalanceKeepsPartitionsWhenPreferredBrokerIsDown(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	h := rebalanceTestHandler(t, endpoints, true)
	if got := h.rebalancePartitions(context.Background(), func(context.Context, string) bool { return false }, time.Minute); got != 0 {
		t.Fatalf("released = %d, want 0", got)
	}
	for p := int32(0); p < 4; p++ {
		if !h.leaseManager.Owns("orders", p) {
			t.Fatalf("broker 0 lost orders/%d", p)
		}
	}
}

func TestRebalanceOffWithoutSyncFlush(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	h := rebalanceTestHandler(t, endpoints, false)
	if got := h.rebalancePartitions(context.Background(), func(context.Context, string) bool { return true }, time.Minute); got != 0 {
		t.Fatalf("released = %d with KAFSCALE_PRODUCE_SYNC_FLUSH=false, want 0", got)
	}
}

func TestPartitionRebalanceTimerAndDial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	addr := listener.Addr().String()
	if !brokerListening(ctx, addr) || brokerListening(ctx, "invalid-address") {
		t.Fatal("broker reachability did not follow TCP dial results")
	}
	h := rebalanceTestHandler(t, testutil.StartEmbeddedEtcd(t), true)
	meta, _ := h.store.Metadata(ctx, nil)
	host, port, _ := net.SplitHostPort(addr)
	p, _ := strconv.Atoi(port)
	meta.Brokers[1].Host, meta.Brokers[1].Port = host, int32(p)
	h.store = metadata.NewInMemoryStore(*meta)
	for _, interval := range []string{"0", "-1"} {
		t.Setenv("KAFSCALE_PARTITION_REBALANCE_INTERVAL_MS", interval)
		h.startPartitionRebalance(ctx)
	}
	t.Setenv("KAFSCALE_PARTITION_REBALANCE_INTERVAL_MS", "2")
	(&handler{}).startPartitionRebalance(ctx)
	h.startPartitionRebalance(ctx)
	deadline := time.After(5 * time.Second)
	for h.leaseManager.Owns("orders", 1) {
		select {
		case <-deadline:
			t.Fatal("periodic rebalance did not hand over the partition")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
}

func TestRebalanceSkipsUnavailableMetadataAndInvalidPlacement(t *testing.T) {
	h := rebalanceTestHandler(t, testutil.StartEmbeddedEtcd(t), true)
	h.store = failingMetadataStore{err: errors.New("metadata unavailable")}
	if got := h.rebalancePartitions(context.Background(), brokerListening, time.Minute); got != 0 {
		t.Fatalf("released %d partitions without metadata", got)
	}
	h.store = failingMetadataStore{} // nil metadata without an error.
	if got := h.rebalancePartitions(context.Background(), brokerListening, time.Minute); got != 0 {
		t.Fatal("released a partition with nil metadata")
	}
	h.store = rebalanceMetadataStore{snapshot: &metadata.ClusterMetadata{Topics: []protocol.MetadataTopic{
		{}, {Topic: kmsg.StringPtr("orders"), ErrorCode: protocol.UNKNOWN_TOPIC_OR_PARTITION},
		{Topic: kmsg.StringPtr("orders"), Partitions: []protocol.MetadataPartition{{Partition: 1, Leader: 99}}},
	}}}
	if got := h.rebalancePartitions(context.Background(), brokerListening, time.Minute); got != 0 {
		t.Fatal("released a partition to a broker missing from metadata")
	}
}

func TestRebalancePreservesAcknowledgedDataAndFencesRetainedLogs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	meta := defaultMetadata()
	meta.Brokers = []protocol.MetadataBroker{{NodeID: 0, Host: "broker-0", Port: 9092}, {NodeID: 1, Host: "broker-1", Port: 9092}}
	meta.Topics[0].Partitions[0].Leader = 1
	store, err := metadata.NewEtcdStore(ctx, meta, metadata.EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s3 := storage.NewMemoryS3Client()
	a := newHandler(store, s3, meta.Brokers[0], testLogger())
	b := newHandler(store, s3, meta.Brokers[1], testLogger())
	for _, h := range []*handler{a, b} {
		defer h.coordinator.Stop()
		defer h.leaseManager.ReleaseAll()
	}
	produce := func(h *handler, want int64) {
		t.Helper()
		raw, err := h.handleProduce(ctx, &protocol.RequestHeader{APIVersion: 3}, &kmsg.ProduceRequest{Acks: -1, TimeoutMillis: 1000, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: testBatchBytes(0, 0, 1)}}}}})
		if err != nil {
			t.Fatal(err)
		}
		part := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
		if part.ErrorCode != 0 || part.BaseOffset != want {
			t.Fatalf("produce after handover: %+v, want offset %d", part, want)
		}
	}
	// An acquired partition with no initialized log can transfer immediately.
	if err := a.leaseManager.Acquire(ctx, "orders", 0); err != nil {
		t.Fatal(err)
	}
	a.leaseManager.Handover("orders", 0, 0)
	if a.leaseManager.Owns("orders", 0) {
		t.Fatal("unused partition retained ownership after handover")
	}
	produce(a, 0)
	old, err := a.getPartitionLog(ctx, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if n := a.rebalancePartitions(ctx, func(context.Context, string) bool { return true }, time.Minute); n != 1 {
		t.Fatalf("released %d partitions, want 1", n)
	}
	produce(b, 1)
	batch, _ := storage.NewRecordBatchFromBytes(testBatchBytes(0, 0, 1))
	if _, err := old.AppendBatches(ctx, []storage.RecordBatch{batch}, true); !errors.Is(err, metadata.ErrNotOwner) {
		t.Fatalf("retained writer was not fenced: %v", err)
	}
	if _, err := old.Read(ctx, 0, 1<<20); !errors.Is(err, metadata.ErrNotOwner) {
		t.Fatalf("retained reader was not fenced: %v", err)
	}
	log, err := b.getPartitionLog(ctx, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	for offset := int64(0); offset < 2; offset++ {
		data, err := log.Read(ctx, offset, 1<<20)
		if err != nil || len(data) == 0 {
			t.Fatalf("acknowledged offset %d missing: %v", offset, err)
		}
	}
	if next, err := store.NextOffset(ctx, "orders", 0); err != nil || next != 2 {
		t.Fatalf("handover changed the append sequence: next=%d, %v", next, err)
	}
}
