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
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/protocol"
)

func newRetireStore(t *testing.T, ctx context.Context) *EtcdStore {
	t.Helper()
	store, err := NewEtcdStore(ctx, ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 1, Host: "localhost", Port: 9092}},
		Topics: []protocol.MetadataTopic{
			{Topic: kmsg.StringPtr("orders"), TopicID: TopicIDForName("orders"), Partitions: []protocol.MetadataPartition{{Partition: 0, Leader: 1}}},
			{Topic: kmsg.StringPtr("payments"), Partitions: []protocol.MetadataPartition{{Partition: 0, Leader: 1}}},
		},
	}, EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t), StorageNamespace: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestRetireTopicCommitsInOneTransaction(t *testing.T) {
	ctx := context.Background()
	store := newRetireStore(t, ctx)
	if err := store.UpdateOffsets(ctx, "orders", 0, 9); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitConsumerOffset(ctx, "g1", "orders", 0, 5, ""); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitConsumerOffset(ctx, "g1", "payments", 0, 3, ""); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	resp, err := store.client.Get(ctx, "/kafscale/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	revisions := map[int64]bool{}
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		switch key {
		case snapshotKey(), TopicGenerationKey("orders"), DeletedTopicKey("orders", 0):
			revisions[kv.ModRevision] = true
		}
		if key == "/kafscale/topics/orders/partitions/0" || key == consumerOffsetKey("g1", "orders", 0) {
			t.Fatalf("key %s left after delete", key)
		}
	}
	if len(revisions) != 1 {
		t.Fatalf("delete keys written in %d revisions", len(revisions))
	}
	if offset, _, err := store.FetchConsumerOffset(ctx, "g1", "payments", 0); err != nil || offset != 3 {
		t.Fatalf("payments offset %d err %v", offset, err)
	}
	record := readDeletedTopic(t, ctx, store, "orders", 0)
	if record.Prefix != "prod/orders/" || record.TopicID != fmt.Sprintf("%x", TopicIDForName("orders")) || record.DeletedAt == "" {
		t.Fatalf("unexpected record %+v", record)
	}
	if _, err := store.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.RetireTopic(ctx, "orders", "other"); err != nil {
		t.Fatal(err)
	}
	if record := readDeletedTopic(t, ctx, store, "orders", 1); record.Prefix != "other/orders@1/" {
		t.Fatalf("unexpected record %+v", record)
	}
	if gen, err := store.TopicGeneration(ctx, "orders"); err != nil || gen != 2 {
		t.Fatalf("generation %d err %v", gen, err)
	}
	if err := store.DeleteTopic(ctx, "orders"); !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("expected unknown topic, got %v", err)
	}
}

func readDeletedTopic(t *testing.T, ctx context.Context, store *EtcdStore, topic string, generation int64) DeletedTopic {
	t.Helper()
	resp, err := store.client.Get(ctx, DeletedTopicKey(topic, generation))
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("deleted topic record %s/%d: %v", topic, generation, err)
	}
	var record DeletedTopic
	if err := json.Unmarshal(resp.Kvs[0].Value, &record); err != nil {
		t.Fatal(err)
	}
	return record
}

func TestRetireTopicDeletesMoreOffsetsThanOneTransaction(t *testing.T) {
	ctx := context.Background()
	store := newRetireStore(t, ctx)
	for i := range 300 {
		if _, err := store.client.Put(ctx, consumerOffsetKey(fmt.Sprintf("g%d", i), "orders", 0), `{"offset":1}`); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	resp, err := store.client.Get(ctx, "/kafscale/consumers/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		t.Fatal(err)
	}
	if resp.Count != 0 {
		t.Fatalf("%d consumer offsets left", resp.Count)
	}
}

func TestRetireTopicGuardsGeneration(t *testing.T) {
	ctx := context.Background()
	store := newRetireStore(t, ctx)
	if err := store.commitGuarded(ctx, generationIs(TopicGenerationKey("orders"), 1), nil); !errors.Is(err, ErrTopicGenerationChanged) {
		t.Fatalf("expected generation conflict, got %v", err)
	}
	if _, err := store.client.Put(ctx, TopicGenerationKey("orders"), "x"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteTopic(ctx, "orders"); err == nil {
		t.Fatal("expected error for unreadable generation")
	}
	if _, err := store.client.Delete(ctx, TopicGenerationKey("orders")); err != nil {
		t.Fatal(err)
	}
	store.client.Close()
	if _, err := store.TopicGeneration(ctx, "orders"); err == nil {
		t.Fatal("expected error from closed client")
	}
	if err := store.DeleteTopic(ctx, "orders"); err == nil {
		t.Fatal("expected error from closed client")
	}
	if _, err := store.consumerOffsetKeys(ctx, "orders"); err == nil {
		t.Fatal("expected error from closed client")
	}
	if err := store.commitGuarded(ctx, generationIs(TopicGenerationKey("orders"), 0), nil); err == nil {
		t.Fatal("expected error from closed client")
	}
	if TopicStorageName("orders", 0) != "orders" || TopicStorageName("orders", 2) != "orders@2" {
		t.Fatal("unexpected storage name")
	}
}

func TestRetireTopicPreservesOffsetsWhenGroupContainsTopicPath(t *testing.T) {
	ctx := context.Background()
	store := newRetireStore(t, ctx)
	group := "reader/offsets/orders/nested"
	if err := store.CommitConsumerOffset(ctx, group, "payments", 0, 42, ""); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if offset, _, err := store.FetchConsumerOffset(ctx, group, "payments", 0); err != nil || offset != 42 {
		t.Fatalf("deleting orders removed payments offset: %d %v", offset, err)
	}
}

type retireHookKV struct {
	clientv3.KV
	hook func()
}

func (k retireHookKV) Txn(ctx context.Context) clientv3.Txn {
	return &retireHookTxn{Txn: k.KV.Txn(ctx), hook: k.hook}
}

type retireHookTxn struct {
	clientv3.Txn
	hook    func()
	retires bool
}

func (tx *retireHookTxn) If(cmps ...clientv3.Cmp) clientv3.Txn {
	tx.Txn = tx.Txn.If(cmps...)
	return tx
}
func (tx *retireHookTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	for _, op := range ops {
		if op.IsPut() && string(op.KeyBytes()) == TopicGenerationKey("orders") {
			tx.retires = true
		}
	}
	tx.Txn = tx.Txn.Then(ops...)
	return tx
}
func (tx *retireHookTxn) Commit() (*clientv3.TxnResponse, error) {
	resp, err := tx.Txn.Commit()
	if err == nil && resp.Succeeded && tx.retires {
		tx.hook()
	}
	return resp, err
}
func TestRetireTopicLargeCleanupPreservesNewGenerationOffsets(t *testing.T) {
	ctx := context.Background()
	store := newRetireStore(t, ctx)
	for i := range 300 {
		if _, err := store.client.Put(ctx, consumerOffsetKey(fmt.Sprintf("g%03d", i), "orders", 0), `{"offset":1}`); err != nil {
			t.Fatal(err)
		}
	}
	key := consumerOffsetKey("g299", "orders", 0)
	original := store.client.KV
	base := store
	store = &EtcdStore{metadata: base.metadata, storageNamespace: base.storageNamespace,
		client: &clientv3.Client{KV: retireHookKV{KV: original, hook: func() {
			// Model a new-generation commit after the retirement transaction but before
			// later cleanup batches. Its revision must survive the old cleanup.
			if _, err := original.Put(ctx, key, `{"offset":42}`); err != nil {
				t.Error(err)
			}
		}}}}
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	resp, err := store.client.Get(ctx, "/kafscale/consumers/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Key) != key {
		t.Fatalf("unexpected remaining offsets: %v", resp.Kvs)
	}
	if offset, _, err := store.FetchConsumerOffset(ctx, "g299", "orders", 0); err != nil || offset != 42 {
		t.Fatalf("new generation offset removed: %d %v", offset, err)
	}
}
