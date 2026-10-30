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
	"encoding/json"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
)

func newEtcdTopicStore(t *testing.T, ctx context.Context) *metadata.EtcdStore {
	t.Helper()
	store, err := metadata.NewEtcdStore(ctx, defaultMetadata(), metadata.EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func produceOrders(t *testing.T, ctx context.Context, h *handler, count int) []int64 {
	t.Helper()
	offsets := make([]int64, 0, count)
	for range count {
		req := &kmsg.ProduceRequest{Acks: -1, TimeoutMillis: 1000, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: testBatchBytes(0, 0, 1)}}}}}
		payload, err := h.handleProduce(ctx, &protocol.RequestHeader{CorrelationID: 1, APIVersion: 3}, req)
		if err != nil {
			t.Fatal(err)
		}
		resp := decodeKmsgResponse(t, 3, payload, kmsg.NewPtrProduceResponse)
		part := resp.Topics[0].Partitions[0]
		if part.ErrorCode != protocol.NONE {
			t.Fatalf("produce error %d", part.ErrorCode)
		}
		offsets = append(offsets, part.BaseOffset)
	}
	return offsets
}

func segmentCount(t *testing.T, ctx context.Context, h *handler, prefix string) int {
	t.Helper()
	objects, err := h.s3.ListSegments(ctx, prefix)
	if err != nil {
		t.Fatal(err)
	}
	return len(objects)
}

func TestDeleteTopicKeepsDataAndRecreatedTopicStartsEmpty(t *testing.T) {
	ctx := context.Background()
	store := newEtcdTopicStore(t, ctx)
	h := newTestHandler(store)
	produceOrders(t, ctx, h, 3)
	legacy := h.s3Namespace + "/orders/0/"
	before := segmentCount(t, ctx, h, legacy)
	if before == 0 {
		t.Fatal("expected segments before delete")
	}
	payload, err := h.handleDeleteTopics(ctx, &protocol.RequestHeader{CorrelationID: 2}, &kmsg.DeleteTopicsRequest{TopicNames: []string{"orders", "missing"}})
	if err != nil {
		t.Fatal(err)
	}
	del := decodeKmsgResponse(t, 0, payload, kmsg.NewPtrDeleteTopicsResponse)
	if del.Topics[0].ErrorCode != protocol.NONE || del.Topics[1].ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatalf("unexpected delete response: %+v", del.Topics)
	}
	if _, ok := h.logs["orders"]; ok {
		t.Fatal("cached partition logs left after delete")
	}
	resp, err := store.EtcdClient().Get(ctx, metadata.DeletedTopicKey("orders", 0))
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("deleted topic record missing: %v", err)
	}
	var record metadata.DeletedTopic
	if err := json.Unmarshal(resp.Kvs[0].Value, &record); err != nil {
		t.Fatal(err)
	}
	if record.Name != "orders" || record.Generation != 0 || record.Prefix != h.s3Namespace+"/orders/" || len(record.TopicID) != 32 {
		t.Fatalf("unexpected record: %+v", record)
	}
	if _, err := time.Parse(time.RFC3339, record.DeletedAt); err != nil {
		t.Fatalf("deleted_at: %v", err)
	}
	create := &kmsg.CreateTopicsRequest{Topics: []kmsg.CreateTopicsRequestTopic{{Topic: "orders", NumPartitions: 1, ReplicationFactor: 1}}}
	payload, err = h.handleCreateTopics(ctx, &protocol.RequestHeader{CorrelationID: 3}, create)
	if err != nil {
		t.Fatal(err)
	}
	if created := decodeKmsgResponse(t, 0, payload, kmsg.NewPtrCreateTopicsResponse); created.Topics[0].ErrorCode != protocol.NONE {
		t.Fatalf("re-create failed: %+v", created.Topics[0])
	}
	if offsets := produceOrders(t, ctx, h, 1); offsets[0] != 0 {
		t.Fatalf("re-created topic starts at offset %d", offsets[0])
	}
	if got := segmentCount(t, ctx, h, legacy); got != before {
		t.Fatalf("old segments changed: %d before, %d after", before, got)
	}
	if segmentCount(t, ctx, h, h.s3Namespace+"/orders@1/0/") == 0 {
		t.Fatal("re-created topic did not write under its own prefix")
	}
}

func TestLegacyTopicLoadsFromNamePrefix(t *testing.T) {
	ctx := context.Background()
	store := newEtcdTopicStore(t, ctx)
	writer := newTestHandler(store)
	produceOrders(t, ctx, writer, 3)
	if _, err := store.EtcdClient().Delete(ctx, "/kafscale/topics/orders/partitions/0/next_offset"); err != nil {
		t.Fatal(err)
	}
	reader := newHandler(store, writer.s3, protocol.MetadataBroker{NodeID: 2, Host: "localhost", Port: 19093}, testLogger())
	if _, err := reader.getPartitionLog(ctx, "orders", 0); err != nil {
		t.Fatal(err)
	}
	next, err := store.NextOffset(ctx, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if next != 3 {
		t.Fatalf("legacy data not restored, next offset %d", next)
	}
}

func TestDeleteTopicFailsWhenGenerationUnreadable(t *testing.T) {
	ctx := context.Background()
	store := newEtcdTopicStore(t, ctx)
	h := newTestHandler(store)
	if _, err := store.EtcdClient().Put(ctx, metadata.TopicGenerationKey("orders"), "bad"); err != nil {
		t.Fatal(err)
	}
	if err := h.deleteTopic(ctx, "orders"); err == nil {
		t.Fatal("expected error for unreadable generation")
	}
	if _, err := h.topicStorageName(ctx, "orders"); err == nil {
		t.Fatal("expected error for unreadable generation")
	}
	if _, err := h.getPartitionLog(ctx, "orders", 0); err == nil {
		t.Fatal("expected partition load to fail")
	}
	meta, err := store.Metadata(ctx, []string{"orders"})
	if err != nil || meta.Topics[0].ErrorCode != protocol.NONE {
		t.Fatalf("topic removed: %v %+v", err, meta)
	}
}

func TestTopicDeleteWatchDropsLogsOnEveryBroker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newEtcdTopicStore(t, ctx)
	handler := newTestHandler(store)
	handler.logs["orders"] = map[int32]*storage.PartitionLog{0: nil}
	handler.logs["payments"] = map[int32]*storage.PartitionLog{0: nil}
	handler.startTopicDeleteWatch(ctx)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := store.EtcdClient().Put(ctx, metadata.TopicGenerationKey("orders"), "1"); err != nil {
			t.Fatal(err)
		}
		handler.logMu.RLock()
		_, cached := handler.logs["orders"]
		handler.logMu.RUnlock()
		if !cached {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cached partition logs not dropped after topic generation changed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	handler.logMu.RLock()
	_, kept := handler.logs["payments"]
	handler.logMu.RUnlock()
	if !kept {
		t.Fatal("other topic dropped")
	}
}

func TestTopicDeleteWatchStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := newEtcdTopicStore(t, ctx)
	cancel()
	newTestHandler(store).watchTopicDeletes(ctx, store.EtcdClient())
	newTestHandler(metadata.NewInMemoryStore(defaultMetadata())).startTopicDeleteWatch(context.Background())
}
