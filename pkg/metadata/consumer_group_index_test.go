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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

type rangeCounter struct {
	mu      sync.Mutex
	calls   int
	keys    int
	maxPage int
}

func (c *rangeCounter) reset() {
	c.mu.Lock()
	c.calls, c.keys, c.maxPage = 0, 0, 0
	c.mu.Unlock()
}

func (c *rangeCounter) add(n int, page bool) {
	c.mu.Lock()
	c.calls++
	c.keys += n
	if page {
		c.maxPage = max(c.maxPage, n)
	}
	c.mu.Unlock()
}

func (c *rangeCounter) interceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoke(ctx, method, req, reply, cc, opts...)
	if err != nil {
		return err
	}
	switch resp := reply.(type) {
	case *etcdserverpb.RangeResponse:
		c.add(len(resp.Kvs), true)
	case *etcdserverpb.TxnResponse:
		n := 0
		for _, op := range resp.Responses {
			if r := op.GetResponseRange(); r != nil {
				n += len(r.Kvs)
			}
		}
		c.add(n, false)
	}
	return nil
}

func newCountingEtcdStore(t *testing.T, endpoints []string) (*EtcdStore, *rangeCounter) {
	t.Helper()
	counter := &rangeCounter{}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithUnaryInterceptor(counter.interceptor)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return &EtcdStore{client: client}, counter
}

func putLegacyGroup(t *testing.T, cli *clientv3.Client, id string, group *metadatapb.ConsumerGroup, partitions int, committed time.Time) {
	t.Helper()
	ctx := context.Background()
	if group != nil {
		encoded, err := EncodeConsumerGroup(group)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cli.Put(ctx, ConsumerGroupKey(id), string(encoded)); err != nil {
			t.Fatal(err)
		}
	}
	payload, _ := json.Marshal(consumerOffsetRecord{Offset: 42, CommittedAt: committed.UTC().Format(time.RFC3339Nano)})
	ops := make([]clientv3.Op, 0, partitions)
	for p := 0; p < partitions; p++ {
		ops = append(ops, clientv3.OpPut(ConsumerOffsetKey(id, "orders", int32(p)), string(payload)))
	}
	for start := 0; start < len(ops); start += 100 {
		if _, err := cli.Txn(ctx).Then(ops[start:min(start+100, len(ops))]...).Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

func groupIDs(groups []*metadatapb.ConsumerGroup) string {
	ids := make([]string, 0, len(groups))
	for _, g := range groups {
		ids = append(ids, g.GroupId)
	}
	return strings.Join(ids, ",")
}

func indexIDs(t *testing.T, cli *clientv3.Client) string {
	t.Helper()
	resp, err := cli.Get(context.Background(), consumerGroupIndexPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		ids = append(ids, strings.TrimPrefix(string(kv.Key), consumerGroupIndexPrefix))
	}
	return strings.Join(ids, ",")
}

func TestConsumerGroupIndexWrittenAndRemoved(t *testing.T) {
	ctx := context.Background()
	store := newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t))
	active := &metadatapb.ConsumerGroup{GroupId: "active", State: "stable", Members: map[string]*metadatapb.GroupMember{"m": {}}}
	if err := store.PutConsumerGroup(ctx, active); err != nil {
		t.Fatal(err)
	}
	if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "empty", State: "empty"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitConsumerOffset(ctx, "offsets-only", "orders", 0, 1, ""); err != nil {
		t.Fatal(err)
	}
	if got := indexIDs(t, store.client); got != "active,empty" {
		t.Fatalf("index after writes: %q", got)
	}
	if err := store.DeleteConsumerGroupAndOffsets(ctx, "active"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteConsumerGroup(ctx, "empty"); err != nil {
		t.Fatal(err)
	}
	if got := indexIDs(t, store.client); got != "" {
		t.Fatalf("index after deletes: %q", got)
	}
}

func TestConsumerGroupListingPagesKeys(t *testing.T) {
	ctx := context.Background()
	endpoints := testutil.StartEmbeddedEtcd(t)
	store, counter := newCountingEtcdStore(t, endpoints)
	old := consumerGroupPageSize
	consumerGroupPageSize = 7
	t.Cleanup(func() { consumerGroupPageSize = old })
	var want []string
	for i := 0; i < 25; i++ {
		id := fmt.Sprintf("g%02d", i)
		want = append(want, id)
		if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: id, State: "empty"}); err != nil {
			t.Fatal(err)
		}
		putLegacyGroup(t, store.client, id, nil, 3, time.Now())
	}
	for _, full := range []bool{false, true} {
		counter.reset()
		groups, err := store.ListConsumerGroupsForRetention(ctx, full)
		if err != nil {
			t.Fatal(err)
		}
		if got := groupIDs(groups); got != strings.Join(want, ",") {
			t.Fatalf("full=%v listed %q", full, got)
		}
		if counter.maxPage > 7 {
			t.Fatalf("full=%v read %d keys in one range", full, counter.maxPage)
		}
	}
	counter.reset()
	groups, err := store.ListConsumerGroups(ctx)
	if err != nil || len(groups) != 25 {
		t.Fatalf("ListConsumerGroups: %d, %v", len(groups), err)
	}
	if counter.maxPage > 7 {
		t.Fatalf("ListConsumerGroups read %d keys in one range", counter.maxPage)
	}
}

func TestConsumerGroupIndexMigratesLegacyLayout(t *testing.T) {
	ctx := context.Background()
	store := newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t))
	cli := store.client
	now := time.Now()
	old := now.Add(-48 * time.Hour)
	cutoff := now.Add(-time.Hour)
	putLegacyGroup(t, cli, "expired", &metadatapb.ConsumerGroup{GroupId: "expired", State: "empty", EmptySince: old.UTC().Format(time.RFC3339Nano)}, 3, old)
	putLegacyGroup(t, cli, "live", &metadatapb.ConsumerGroup{GroupId: "live", State: "stable", Members: map[string]*metadatapb.GroupMember{"m": {}}}, 3, now)
	putLegacyGroup(t, cli, "offsets-only", nil, 3, old)
	if _, err := cli.Put(ctx, ConsumerGroupIndexKey("deleted-by-old-broker"), ""); err != nil {
		t.Fatal(err)
	}

	groups, err := store.ListConsumerGroupsForRetention(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := groupIDs(groups); got != "deleted-by-old-broker" {
		t.Fatalf("index before migration: %q", got)
	}
	for range 2 {
		groups, err = store.ListConsumerGroupsForRetention(ctx, true)
		if err != nil {
			t.Fatal(err)
		}
		if got := groupIDs(groups); got != "deleted-by-old-broker,expired,live,offsets-only" {
			t.Fatalf("full listing: %q", got)
		}
		if got := indexIDs(t, cli); got != "deleted-by-old-broker,expired,live" {
			t.Fatalf("index after backfill: %q", got)
		}
	}
	all, err := store.ListConsumerGroups(ctx)
	if err != nil || groupIDs(all) != "expired,live,offsets-only" {
		t.Fatalf("ListConsumerGroups: %q, %v", groupIDs(all), err)
	}
	if offset, _, err := store.FetchConsumerOffset(ctx, "live", "orders", 2); err != nil || offset != 42 {
		t.Fatalf("live offset: %d, %v", offset, err)
	}
	if group, err := store.FetchConsumerGroup(ctx, "live"); err != nil || len(group.Members) != 1 {
		t.Fatalf("live group: %+v, %v", group, err)
	}

	for _, group := range groups {
		if len(group.Members) > 0 {
			continue
		}
		deleted, err := store.ExpireConsumerGroup(ctx, group.GroupId, cutoff, now)
		if err != nil {
			t.Fatalf("expire %s: %v", group.GroupId, err)
		}
		if deleted != (group.GroupId == "expired") {
			t.Fatalf("expire %s deleted=%v", group.GroupId, deleted)
		}
	}
	if got := indexIDs(t, cli); got != "live,offsets-only" {
		t.Fatalf("index after sweep: %q", got)
	}
	groups, err = store.ListConsumerGroupsForRetention(ctx, false)
	if err != nil || groupIDs(groups) != "live,offsets-only" {
		t.Fatalf("index listing after sweep: %q, %v", groupIDs(groups), err)
	}
	if group, err := store.FetchConsumerGroup(ctx, "offsets-only"); err != nil || group == nil || group.EmptySince == "" {
		t.Fatalf("offsets-only group not initialized: %+v, %v", group, err)
	}
	if offsets, err := store.ListGroupConsumerOffsets(ctx, "expired"); err != nil || len(offsets) != 0 {
		t.Fatalf("expired offsets remain: %d, %v", len(offsets), err)
	}
}

func TestConsumerGroupBackfillSkipsDeletedGroup(t *testing.T) {
	ctx := context.Background()
	store := newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t))
	if err := store.backfillConsumerGroupIndex(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	if got := indexIDs(t, store.client); got != "" {
		t.Fatalf("backfill indexed a missing group: %q", got)
	}
}

func TestConsumerGroupListingCost(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 11000 keys")
	}
	ctx := context.Background()
	store, counter := newCountingEtcdStore(t, testutil.StartEmbeddedEtcd(t))
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("group-%04d", i)
		putLegacyGroup(t, store.client, id, &metadatapb.ConsumerGroup{GroupId: id, State: "empty"}, 10, time.Now())
	}
	measure := func(name string, list func() ([]*metadatapb.ConsumerGroup, error)) {
		counter.reset()
		groups, err := list()
		if err != nil || len(groups) != 1000 {
			t.Fatalf("%s: %d groups, %v", name, len(groups), err)
		}
		t.Logf("%s: %d etcd requests, %d keys returned, largest single range response %d keys", name, counter.calls, counter.keys, counter.maxPage)
	}
	measure("ListConsumerGroups", func() ([]*metadatapb.ConsumerGroup, error) { return store.ListConsumerGroups(ctx) })
	measure("retention full scan, first run with backfill", func() ([]*metadatapb.ConsumerGroup, error) { return store.ListConsumerGroupsForRetention(ctx, true) })
	measure("retention full scan", func() ([]*metadatapb.ConsumerGroup, error) { return store.ListConsumerGroupsForRetention(ctx, true) })
	measure("retention index listing", func() ([]*metadatapb.ConsumerGroup, error) { return store.ListConsumerGroupsForRetention(ctx, false) })
	if counter.keys > 2000 {
		t.Fatalf("index listing read %d keys", counter.keys)
	}
}
