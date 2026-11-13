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

package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Interleave a competing writer immediately before the first snapshot commit.
// Wrap Put too, so the regression also exercises the old unconditional writer.
type snapshotHookKV struct {
	clientv3.KV
	once sync.Once
	hook func()
}

func (k *snapshotHookKV) Put(ctx context.Context, key, value string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	if key == snapshotKey() {
		k.once.Do(k.hook)
	}
	return k.KV.Put(ctx, key, value, opts...)
}
func (k *snapshotHookKV) Txn(ctx context.Context) clientv3.Txn {
	return &snapshotHookTxn{Txn: k.KV.Txn(ctx), kv: k}
}

type snapshotHookTxn struct {
	clientv3.Txn
	kv     *snapshotHookKV
	writes bool
}

func (t *snapshotHookTxn) If(cmps ...clientv3.Cmp) clientv3.Txn { t.Txn = t.Txn.If(cmps...); return t }
func (t *snapshotHookTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	for _, op := range ops {
		if op.IsPut() && string(op.KeyBytes()) == snapshotKey() {
			t.writes = true
		}
	}
	t.Txn = t.Txn.Then(ops...)
	return t
}
func (t *snapshotHookTxn) Commit() (*clientv3.TxnResponse, error) {
	if t.writes {
		t.kv.once.Do(t.kv.hook)
	}
	return t.Txn.Commit()
}
func writeSnapshotFixture(t *testing.T, kv clientv3.KV, state ClusterMetadata) {
	t.Helper()
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = kv.Put(context.Background(), snapshotKey(), string(data)); err != nil {
		t.Fatal(err)
	}
}
func snapshotMutationStore(t *testing.T, base *EtcdStore, kv clientv3.KV) *EtcdStore {
	t.Helper()
	state, err := base.Metadata(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return &EtcdStore{client: &clientv3.Client{KV: kv}, metadata: NewInMemoryStore(*state), storageNamespace: base.storageNamespace}
}
func snapshotFixture() ClusterMetadata {
	return ClusterMetadata{Brokers: []protocol.MetadataBroker{{NodeID: 0, Host: "broker-0", Port: 9092}, {NodeID: 1, Host: "broker-1", Port: 9092}}, ControllerID: 0,
		Topics: []protocol.MetadataTopic{{Topic: kmsg.StringPtr("existing"), Partitions: []protocol.MetadataPartition{{Partition: 0, Leader: 0}, {Partition: 1, Leader: 1}, {Partition: 2, Leader: 0}, {Partition: 3, Leader: 1}}}}}
}
func TestSnapshotConcurrentCreatesPreserveBothTopics(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
	writeSnapshotFixture(t, a.client, snapshotFixture())
	b := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	stores := []*EtcdStore{a, b}
	for i, store := range stores {
		stores[i] = snapshotMutationStore(t, store, &snapshotHookKV{KV: store.client.KV, hook: func() {
			ready <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
		}})
	}
	errs := make(chan error, 2)
	for i, store := range stores {
		go func() {
			_, err := store.CreateTopic(ctx, TopicSpec{Name: fmt.Sprintf("created-%d", i), NumPartitions: 2})
			errs <- err
		}()
	}
	for range 2 {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("writers did not reach the same snapshot revision")
		}
	}
	close(release)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	saved, err := loadSnapshot(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"existing", "created-0", "created-1"} {
		if !topicExists(saved, name) {
			t.Fatalf("lost %s: %+v", name, saved)
		}
	}
	if len(saved.Brokers) != 2 || len(saved.Topics[0].Partitions) != 4 {
		t.Fatal("creation replaced authoritative broker or partition metadata")
	}
}
func TestSnapshotCreateDoesNotPersistStartupTopics(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	seed := snapshotFixture()
	seed.Topics[0].Topic = kmsg.StringPtr("orders")
	s := newTestEtcdStore(t, context.Background(), seed, endpoints)
	if _, err := s.CreateTopic(context.Background(), TopicSpec{Name: "auto", NumPartitions: 1}); err != nil {
		t.Fatal(err)
	}
	saved, err := loadSnapshot(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved.Topics) != 1 || !topicExists(saved, "auto") {
		t.Fatalf("persisted startup topics: %+v", saved.Topics)
	}
}
func TestSnapshotStaleCreateUsesAuthoritativeCluster(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	seed := ClusterMetadata{Brokers: []protocol.MetadataBroker{{NodeID: 1}}, Topics: []protocol.MetadataTopic{{Topic: kmsg.StringPtr("orders"), Partitions: []protocol.MetadataPartition{{Partition: 0}}}}}
	s := newTestEtcdStore(t, context.Background(), seed, endpoints)
	writeSnapshotFixture(t, s.client, snapshotFixture())
	if _, err := s.CreateTopic(context.Background(), TopicSpec{Name: "auto", NumPartitions: 2}); err != nil {
		t.Fatal(err)
	}
	saved, err := loadSnapshot(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved.Brokers) != 2 || len(saved.Topics) != 2 || len(saved.Topics[0].Partitions) != 4 || topicExists(saved, "orders") {
		t.Fatalf("stale startup view replaced snapshot: %+v", saved)
	}
}
func TestSnapshotMutationsRetryConcurrentCreate(t *testing.T) {
	for _, operation := range []string{"expand", "delete"} {
		t.Run(operation, func(t *testing.T) {
			endpoints := testutil.StartEmbeddedEtcd(t)
			ctx := context.Background()
			a := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
			writeSnapshotFixture(t, a.client, snapshotFixture())
			b := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
			base := a.client
			a = snapshotMutationStore(t, a, &snapshotHookKV{KV: base.KV, hook: func() {
				if _, err := b.CreateTopic(ctx, TopicSpec{Name: "concurrent", NumPartitions: 1}); err != nil {
					t.Error(err)
				}
			}})
			var err error
			if operation == "expand" {
				err = a.CreatePartitions(ctx, "existing", 6)
			} else {
				err = a.DeleteTopic(ctx, "existing")
			}
			if err != nil {
				t.Fatal(err)
			}
			saved, err := loadSnapshot(endpoints)
			if err != nil {
				t.Fatal(err)
			}
			if !topicExists(saved, "concurrent") {
				t.Fatal("snapshot mutation lost concurrent creation")
			}
			if operation == "expand" && len(saved.Topics[0].Partitions) != 6 {
				t.Fatal("expansion lost")
			}
			if operation == "delete" && topicExists(saved, "existing") {
				t.Fatal("deleted topic survived")
			}
		})
	}
}
func TestSnapshotExpansionBatchesAtomically(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()
	s := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
	writeSnapshotFixture(t, s.client, snapshotFixture())
	if err := s.CreatePartitions(ctx, "existing", 260); err != nil {
		t.Fatal(err)
	}
	saved, err := loadSnapshot(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved.Topics[0].Partitions) != 260 {
		t.Fatal("large expansion incomplete")
	}
	for _, part := range []int32{4, 130, 259} {
		resp, err := s.client.Get(ctx, PartitionStateKey("existing", part))
		if err != nil || len(resp.Kvs) != 1 {
			t.Fatalf("missing partition %d: %v", part, err)
		}
	}
}
func TestSnapshotFailedInitialReadRecoversWithoutNewEvent(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	cli := newEtcdClient(t, endpoints)
	defer cli.Close()
	writeSnapshotFixture(t, cli, snapshotFixture())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := newTestEtcdStore(t, ctx, ClusterMetadata{}, endpoints)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		meta, _ := s.Metadata(context.Background(), nil)
		if topicExists(meta, "existing") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("watch never reseeded after the failed initial read")
}

type snapshotFailKV struct {
	clientv3.KV
	failRead  bool
	failWrite bool
}

func (k snapshotFailKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if k.failRead && key == snapshotKey() {
		return nil, errors.New("snapshot read unavailable")
	}
	return k.KV.Get(ctx, key, opts...)
}
func (k snapshotFailKV) Txn(ctx context.Context) clientv3.Txn {
	return &snapshotFailTxn{Txn: k.KV.Txn(ctx), fail: k.failWrite}
}

type snapshotFailTxn struct {
	clientv3.Txn
	fail bool
}

func (t *snapshotFailTxn) If(cmps ...clientv3.Cmp) clientv3.Txn { t.Txn = t.Txn.If(cmps...); return t }
func (t *snapshotFailTxn) Then(ops ...clientv3.Op) clientv3.Txn { t.Txn = t.Txn.Then(ops...); return t }
func (t *snapshotFailTxn) Commit() (*clientv3.TxnResponse, error) {
	if t.fail {
		return nil, errors.New("snapshot write unavailable")
	}
	return t.Txn.Commit()
}
func TestSnapshotFailureDoesNotExposeSpeculativeTopic(t *testing.T) {
	for _, phase := range []string{"read", "write", "corrupt"} {
		t.Run(phase, func(t *testing.T) {
			endpoints := testutil.StartEmbeddedEtcd(t)
			ctx := context.Background()
			s := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
			writeSnapshotFixture(t, s.client, snapshotFixture())
			base := s.client
			s = snapshotMutationStore(t, s, snapshotFailKV{KV: base.KV, failRead: phase == "read", failWrite: phase == "write"})
			if phase == "corrupt" {
				if _, err := base.Put(ctx, snapshotKey(), "broken"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.CreateTopic(ctx, TopicSpec{Name: "uncommitted", NumPartitions: 1}); err == nil {
				t.Fatal("creation accepted failed persistence")
			}
			meta, _ := s.Metadata(ctx, nil)
			if topicExists(meta, "uncommitted") {
				t.Fatal("failed creation leaked into metadata")
			}
			resp, err := base.Get(ctx, snapshotKey())
			if err != nil {
				t.Fatal(err)
			}
			if phase == "corrupt" && string(resp.Kvs[0].Value) != "broken" {
				t.Fatal("corrupt snapshot was overwritten")
			}
		})
	}
}

func TestSnapshotMutationCannotModifyRecreatedTopic(t *testing.T) {
	for _, operation := range []string{"expand", "delete"} {
		t.Run(operation, func(t *testing.T) {
			endpoints := testutil.StartEmbeddedEtcd(t)
			ctx := context.Background()
			base := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
			writeSnapshotFixture(t, base.client, snapshotFixture())
			b := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
			a := snapshotMutationStore(t, base, &snapshotHookKV{KV: base.client.KV, hook: func() {
				if err := b.DeleteTopic(ctx, "existing"); err != nil {
					t.Error(err)
					return
				}
				if _, err := b.CreateTopic(ctx, TopicSpec{Name: "existing", NumPartitions: 2}); err != nil {
					t.Error(err)
				}
			}})
			var err error
			if operation == "expand" {
				err = a.CreatePartitions(ctx, "existing", 6)
			} else {
				err = a.DeleteTopic(ctx, "existing")
			}
			if !errors.Is(err, ErrTopicGenerationChanged) {
				t.Fatalf("stale %s modified new generation: %v", operation, err)
			}
			saved, err := loadSnapshot(endpoints)
			if err != nil {
				t.Fatal(err)
			}
			if len(saved.Topics) != 1 || *saved.Topics[0].Topic != "existing" || len(saved.Topics[0].Partitions) != 2 {
				t.Fatal("new generation changed")
			}
			if gen, err := b.TopicGeneration(ctx, "existing"); err != nil || gen != 1 {
				t.Fatalf("new generation retired again: %d %v", gen, err)
			}
		})
	}
}
func TestSnapshotSameNameCreateHasOneWinner(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()
	base := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
	writeSnapshotFixture(t, base.client, snapshotFixture())
	b := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
	a := snapshotMutationStore(t, base, &snapshotHookKV{KV: base.client.KV, hook: func() {
		if _, err := b.CreateTopic(ctx, TopicSpec{Name: "same", NumPartitions: 3}); err != nil {
			t.Error(err)
		}
	}})
	if _, err := a.CreateTopic(ctx, TopicSpec{Name: "same", NumPartitions: 1}); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("loser must report topic exists: %v", err)
	}
	saved, err := loadSnapshot(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved.Topics) != 2 || len(saved.Topics[1].Partitions) != 3 {
		t.Fatal("loser overwrote winning topic")
	}
}

type snapshotReadHookKV struct {
	clientv3.KV
	once sync.Once
	hook func()
}

func (k *snapshotReadHookKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	resp, err := k.KV.Get(ctx, key, opts...)
	if err == nil && key == snapshotKey() {
		k.once.Do(k.hook)
	}
	return resp, err
}
func TestSnapshotWatchReplaysUpdateBetweenReadAndWatch(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()
	base := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
	writeSnapshotFixture(t, base.client, snapshotFixture())
	updated := snapshotFixture()
	updated.Topics = append(updated.Topics, protocol.MetadataTopic{Topic: kmsg.StringPtr("gap"), Partitions: []protocol.MetadataPartition{{Partition: 0}}})
	kv := &snapshotReadHookKV{KV: base.client.KV, hook: func() { writeSnapshotFixture(t, base.client, updated) }}
	s := snapshotMutationStore(t, base, kv)
	s.client.Watcher = base.client.Watcher
	s.startWatchers()
	defer s.cancel()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		meta, _ := s.Metadata(ctx, nil)
		if topicExists(meta, "gap") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("watch missed the update committed after its full read")
}

func TestSnapshotExistingTopicRefreshesStaleCache(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()
	base := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
	s := snapshotMutationStore(t, base, base.client.KV)
	s.metadata.Update(ClusterMetadata{Brokers: []protocol.MetadataBroker{{NodeID: 1}}})
	writeSnapshotFixture(t, base.client, snapshotFixture())
	if _, err := s.CreateTopic(ctx, TopicSpec{Name: "existing", NumPartitions: 1}); !errors.Is(err, ErrTopicExists) {
		t.Fatal(err)
	}
	meta, err := s.Metadata(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Brokers) != 2 || len(meta.Topics) != 1 || len(meta.Topics[0].Partitions) != 4 {
		t.Fatal("duplicate create left startup metadata cached")
	}
}

func TestSnapshotExpansionPreservesExistingPartitionState(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()
	s := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
	writeSnapshotFixture(t, s.client, snapshotFixture())
	key := PartitionStateKey("existing", 4)
	const state = `{"topic":"existing","partition":4,"high_watermark":99}`
	if _, err := s.client.Put(ctx, key, state); err != nil {
		t.Fatal(err)
	}
	if err := s.CreatePartitions(ctx, "existing", 5); err != nil {
		t.Fatal(err)
	}
	resp, err := s.client.Get(ctx, key)
	if err != nil || len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != state {
		t.Fatalf("expansion reset persisted partition state: %v", err)
	}
}
