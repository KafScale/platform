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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/internal/testutil"
	"github.com/KafScale/platform/v2/pkg/broker"
	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/KafScale/platform/v2/pkg/storage"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func producerBatchBytes(epoch int16, sequence int32) []byte {
	data := testBatchBytes(0, 0, 1)
	binary.BigEndian.PutUint64(data[43:51], 1)
	binary.BigEndian.PutUint16(data[51:53], uint16(epoch))
	binary.BigEndian.PutUint32(data[53:57], uint32(sequence))
	binary.BigEndian.PutUint32(data[17:21], crc32.Checksum(data[21:], produceCRC32CTable))
	return data
}

func TestInitProducerIDAndSequenceErrors(t *testing.T) {
	h := newTestHandler(metadata.NewInMemoryStore(defaultMetadata()))
	t.Cleanup(h.coordinator.Stop)
	for version := int16(0); version <= 4; version++ {
		req := kmsg.NewPtrInitProducerIDRequest()
		req.Version = version
		wire, err := h.Handle(context.Background(), &protocol.RequestHeader{APIKey: 22, APIVersion: version}, req)
		if err != nil {
			t.Fatal(err)
		}
		resp := decodeKmsgResponse(t, version, wire, kmsg.NewPtrInitProducerIDResponse)
		if resp.ErrorCode != 0 || resp.ProducerID != int64(version)+1 || resp.ProducerEpoch != 0 {
			t.Fatalf("v%d: %+v", version, resp)
		}
	}
	for _, tc := range []struct {
		epoch    int16
		sequence int32
		acks     int16
		code     int16
	}{
		{0, 0, -1, 0}, {0, 0, -1, 0}, {0, 2, -1, protocol.OUT_OF_ORDER_SEQUENCE_NUMBER},
		{0, 1, 1, protocol.INVALID_REQUIRED_ACKS}, {0, 1, -1, 0}, {1, 0, -1, 0},
		{0, 1, -1, protocol.INVALID_PRODUCER_EPOCH},
	} {
		req := &kmsg.ProduceRequest{Version: 9, Acks: tc.acks, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Records: producerBatchBytes(tc.epoch, tc.sequence)}}}}}
		wire, err := h.Handle(context.Background(), &protocol.RequestHeader{APIKey: 0, APIVersion: 9}, req)
		if err != nil {
			t.Fatal(err)
		}
		resp := decodeKmsgResponse(t, 9, wire, kmsg.NewPtrProduceResponse)
		if code := resp.Topics[0].Partitions[0].ErrorCode; code != tc.code {
			t.Fatalf("%+v: code %d", tc, code)
		}
	}
}

func TestProduceRejectsTransactionalAndControlBatches(t *testing.T) {
	for _, attributes := range []uint16{0x10, 0x20, 0x30} {
		h := newTestHandler(metadata.NewInMemoryStore(defaultMetadata()))
		t.Cleanup(h.coordinator.Stop)
		records := producerBatchBytes(0, 0)
		binary.BigEndian.PutUint16(records[21:23], attributes)
		binary.BigEndian.PutUint32(records[17:21], crc32.Checksum(records[21:], produceCRC32CTable))
		req := &kmsg.ProduceRequest{Version: 9, Acks: -1, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Records: records}}}}}
		wire, err := h.Handle(context.Background(), &protocol.RequestHeader{APIKey: 0, APIVersion: 9}, req)
		if err != nil {
			t.Fatal(err)
		}
		if resp := decodeKmsgResponse(t, 9, wire, kmsg.NewPtrProduceResponse); resp.Topics[0].Partitions[0].ErrorCode != protocol.UNSUPPORTED_VERSION {
			t.Fatalf("attributes %x: %+v", attributes, resp)
		}
		if len(h.logs) != 0 {
			t.Fatal("unsupported batch opened a partition log")
		}
	}
}

func startIdempotenceBroker(t *testing.T) (string, *handler) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_, portText, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portText)
	listener.Close()
	info := protocol.MetadataBroker{NodeID: 1, Host: "127.0.0.1", Port: int32(port)}
	snapshot := metadataForBroker(info)
	h := newHandler(metadata.NewInMemoryStore(snapshot), storage.NewMemoryS3Client(), info, testLogger())
	t.Cleanup(h.coordinator.Stop)
	ctx, cancel := context.WithCancel(context.Background())
	server := &broker.Server{Addr: addr, Handler: h}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("broker did not stop")
		}
		server.Wait()
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return addr, h
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("broker did not listen")
	return "", nil
}

func TestDefaultIdempotentProducer(t *testing.T) {
	addr, h := startIdempotenceBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := kgo.NewClient(kgo.SeedBrokers(addr)) // idempotence stays enabled
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for i := 0; i < 10; i++ {
		record := &kgo.Record{Topic: "orders", Key: []byte("key"), Value: []byte(fmt.Sprint(i))}
		if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
			t.Fatal(err)
		}
		if record.Offset != int64(i) {
			t.Fatalf("offset %d, want %d", record.Offset, i)
		}
	}
	if next, err := h.store.NextOffset(ctx, "orders", 0); err != nil || next != 10 {
		t.Fatalf("durable offset = %d, %v", next, err)
	}
}

func TestIdempotentRetryAcrossEtcdPartitionOwners(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	endpoints := testutil.StartEmbeddedEtcd(t)
	store, err := metadata.NewEtcdStore(ctx, metadataForBroker(protocol.MetadataBroker{NodeID: 0}), metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s3 := storage.NewMemoryS3Client()
	newOwner := func(id int32) *handler {
		h := newHandler(store, s3, protocol.MetadataBroker{NodeID: id}, testLogger())
		t.Cleanup(h.coordinator.Stop)
		t.Cleanup(h.leaseManager.ReleaseAll)
		h.flushOnAck = false // idempotence must still publish durable offsets
		return h
	}
	a, b := newOwner(0), newOwner(1)
	id, err := store.AllocateProducerID(ctx)
	if err != nil || id != 1 {
		t.Fatalf("allocate ID: %d %v", id, err)
	}
	produce := func(h *handler, sequence int32, wantOffset int64) {
		t.Helper()
		req := &kmsg.ProduceRequest{Version: 9, Acks: -1, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Records: producerBatchBytes(0, sequence)}}}}}
		wire, err := h.Handle(ctx, &protocol.RequestHeader{APIKey: 0, APIVersion: 9}, req)
		if err != nil {
			t.Fatal(err)
		}
		p := decodeKmsgResponse(t, 9, wire, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
		if p.ErrorCode != 0 || p.BaseOffset != wantOffset {
			t.Fatalf("owner %d sequence %d: %+v", h.brokerInfo.NodeID, sequence, p)
		}
	}
	produce(a, 0, 0)
	a.leaseManager.ReleaseAll()
	produce(b, 0, 0) // lost response from the previous owner
	produce(b, 1, 1)
	b.leaseManager.ReleaseAll()
	restarted := newOwner(0)
	produce(restarted, 1, 1)
	produce(restarted, 2, 2)
	if next, err := store.NextOffset(ctx, "orders", 0); err != nil || next != 3 {
		t.Fatalf("durable offset = %d, %v", next, err)
	}
}

func TestDuplicateProduceDoesNotInflateIngestMetric(t *testing.T) {
	h := newTestHandler(metadata.NewInMemoryStore(defaultMetadata()))
	t.Cleanup(h.coordinator.Stop)
	count := func() int64 {
		h.produceRate.mu.Lock()
		defer h.produceRate.mu.Unlock()
		var total int64
		for _, value := range h.produceRate.buckets {
			total += value
		}
		return total
	}
	for _, tc := range []struct {
		sequence  int32
		wantCount int64
	}{{0, 1}, {0, 1}, {0, 1}, {1, 2}, {1, 2}} {
		req := &kmsg.ProduceRequest{Version: 9, Acks: -1, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Records: producerBatchBytes(0, tc.sequence)}}}}}
		wire, err := h.Handle(context.Background(), &protocol.RequestHeader{APIKey: 0, APIVersion: 9}, req)
		if err != nil {
			t.Fatal(err)
		}
		if p := decodeKmsgResponse(t, 9, wire, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]; p.ErrorCode != 0 {
			t.Fatal(p)
		}
		if got := count(); got != tc.wantCount {
			t.Fatalf("sequence %d: ingest tracker counted %d records, want %d", tc.sequence, got, tc.wantCount)
		}
	}
}
