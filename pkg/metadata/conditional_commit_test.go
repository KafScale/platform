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
	"errors"
	"sync"
	"testing"

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type legacyCommitStore struct{ Store }

func TestCommitConsumerOffsetIfGroupExists(t *testing.T) {
	ctx := context.Background()
	stores := map[string]Store{
		"memory": NewInMemoryStore(ClusterMetadata{}),
		"etcd":   newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t)),
	}
	for name, store := range stores {
		if err := CommitConsumerOffsetIfGroupExists(ctx, store, "g", 0, "", 0, "orders", 0, 1, ""); !errors.Is(err, ErrConsumerGroupDeleted) {
			t.Fatal(name, err)
		}
		if offsets, _ := store.ListGroupConsumerOffsets(ctx, "g"); len(offsets) != 0 {
			t.Fatal(name, offsets)
		}
		if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "g"}); err != nil {
			t.Fatal(err)
		}
		if err := CommitConsumerOffsetIfGroupExists(ctx, store, "g", 0, "", 0, "orders", 0, 7, "m"); err != nil {
			t.Fatal(name, err)
		}
		if offset, meta, err := store.FetchConsumerOffset(ctx, "g", "orders", 0); err != nil || offset != 7 || meta != "m" {
			t.Fatal(name, offset, meta, err)
		}
	}
	legacy := legacyCommitStore{NewInMemoryStore(ClusterMetadata{})}
	if err := legacy.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "g"}); err != nil {
		t.Fatal(err)
	}
	if err := CommitConsumerOffsetIfGroupExists(ctx, legacy, "g", 0, "m1", 1, "orders", 0, 3, ""); !errors.Is(err, ErrConsumerGroupCommitUnsupported) {
		t.Fatal(err)
	}
	if offsets, _ := legacy.ListGroupConsumerOffsets(ctx, "g"); len(offsets) != 0 {
		t.Fatal(offsets)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := stores["etcd"].(*EtcdStore).CommitConsumerOffsetIfGroupExists(canceled, "g", 0, "", 0, "orders", 0, 1, ""); err == nil {
		t.Fatal("expected canceled")
	}
	if err := stores["memory"].(*InMemoryStore).CommitConsumerOffsetIfGroupExists(canceled, "g", 0, "", 0, "orders", 0, 1, ""); err == nil {
		t.Fatal("expected canceled")
	}
}

type legacyRevisionStore struct{ Store }

func TestCommitConsumerOffsetRejectsRecreatedGroup(t *testing.T) {
	ctx := context.Background()
	store := newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t))
	active := &metadatapb.ConsumerGroup{GroupId: "g", Members: map[string]*metadatapb.GroupMember{"m1": {}}}
	first, err := PutConsumerGroupRevision(ctx, store, active)
	if err != nil || first == 0 {
		t.Fatal(first, err)
	}
	if again, err := PutConsumerGroupRevision(ctx, store, active); err != nil || again != first {
		t.Fatal(again, err)
	}
	if empty, err := PutConsumerGroupRevision(ctx, store, &metadatapb.ConsumerGroup{GroupId: "g"}); err != nil || empty != first {
		t.Fatal(empty, err)
	}
	if _, loaded, err := FetchConsumerGroupRevision(ctx, store, "g"); err != nil || loaded != first {
		t.Fatal(loaded, err)
	}
	if err := CommitConsumerOffsetIfGroupExists(ctx, store, "g", first, "", 0, "orders", 0, 5, ""); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteConsumerGroupAndOffsets(ctx, "g"); err != nil {
		t.Fatal(err)
	}
	if _, missing, err := FetchConsumerGroupRevision(ctx, store, "g"); err != nil || missing != 0 {
		t.Fatal(missing, err)
	}
	second, err := PutConsumerGroupRevision(ctx, store, &metadatapb.ConsumerGroup{GroupId: "g"})
	if err != nil || second <= first {
		t.Fatal(second, err)
	}
	if err := CommitConsumerOffsetIfGroupExists(ctx, store, "g", first, "", 0, "orders", 0, 9, ""); !errors.Is(err, ErrConsumerGroupDeleted) {
		t.Fatal(err)
	}
	if offsets, _ := store.ListGroupConsumerOffsets(ctx, "g"); len(offsets) != 0 {
		t.Fatal(offsets)
	}
	if err := CommitConsumerOffsetIfGroupExists(ctx, store, "g", second, "", 0, "orders", 0, 9, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := PutConsumerGroupRevision(ctx, store, nil); err == nil {
		t.Fatal("expected error")
	}
	legacy := legacyRevisionStore{NewInMemoryStore(ClusterMetadata{})}
	if revision, err := PutConsumerGroupRevision(ctx, legacy, &metadatapb.ConsumerGroup{GroupId: "g"}); err != nil || revision != 0 {
		t.Fatal(revision, err)
	}
	if group, revision, err := FetchConsumerGroupRevision(ctx, legacy, "g"); err != nil || group == nil || revision != 0 {
		t.Fatal(group, revision, err)
	}
}

func TestCommitConsumerOffsetDuringHeartbeats(t *testing.T) {
	ctx := context.Background()
	store := newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t))
	active := &metadatapb.ConsumerGroup{GroupId: "g", Members: map[string]*metadatapb.GroupMember{"m1": {}}}
	revision, err := PutConsumerGroupRevision(ctx, store, active)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					_ = store.PutConsumerGroup(ctx, active)
				}
			}
		}()
	}
	failed := 0
	for i := range 200 {
		if err := CommitConsumerOffsetIfGroupExists(ctx, store, "g", revision, "", 0, "orders", 0, int64(i), ""); err != nil {
			failed++
		}
	}
	close(done)
	wg.Wait()
	if failed > 0 {
		t.Fatalf("%d of 200 commits failed", failed)
	}
}

func TestCommitConsumerOffsetChecksMember(t *testing.T) {
	ctx := context.Background()
	stores := map[string]Store{
		"memory": NewInMemoryStore(ClusterMetadata{}),
		"etcd":   newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t)),
	}
	for name, store := range stores {
		members := map[string]*metadatapb.GroupMember{"m1": {}}
		if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "g", GenerationId: 4, Members: members}); err != nil {
			t.Fatal(err)
		}
		cases := []struct {
			member     string
			generation int32
			want       error
		}{
			{"gone", 4, ErrConsumerGroupMemberGone},
			{"m1", 3, ErrConsumerGroupGenerationChanged},
			{"m1", 4, nil},
		}
		for _, tc := range cases {
			err := CommitConsumerOffsetIfGroupExists(ctx, store, "g", 0, tc.member, tc.generation, "orders", 0, 5, "")
			if !errors.Is(err, tc.want) || (tc.want == nil && err != nil) {
				t.Fatal(name, tc.member, tc.generation, err)
			}
		}
		if offset, _, err := store.FetchConsumerOffset(ctx, "g", "orders", 0); err != nil || offset != 5 {
			t.Fatal(name, offset, err)
		}
	}
}

type racingKV struct {
	clientv3.KV
	before func()
}

func (k racingKV) Txn(ctx context.Context) clientv3.Txn {
	k.before()
	return k.KV.Txn(ctx)
}

func TestCommitConsumerOffsetFencesGroupChangeAfterSnapshot(t *testing.T) {
	ctx := context.Background()
	group := func(members ...string) string {
		g := &metadatapb.ConsumerGroup{GroupId: "g", GenerationId: 2, Members: map[string]*metadatapb.GroupMember{}}
		for _, m := range members {
			g.Members[m] = &metadatapb.GroupMember{}
		}
		encoded, err := EncodeConsumerGroup(g)
		if err != nil {
			t.Fatal(err)
		}
		return string(encoded)
	}
	cases := []struct {
		name   string
		always bool
		after  string
		want   error
	}{
		{"member leaves", false, group("m2"), ErrConsumerGroupMemberGone},
		{"group keeps changing", true, group("m1", "m2"), ErrConsumerGroupChanged},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t))
			inner := store.client.KV
			if _, err := inner.Put(ctx, ConsumerGroupKey("g"), group("m1", "m2")); err != nil {
				t.Fatal(err)
			}
			_, createRevision, err := FetchConsumerGroupRevision(ctx, store, "g")
			if err != nil || createRevision == 0 {
				t.Fatal(createRevision, err)
			}
			fired := false
			store.client.KV = racingKV{KV: inner, before: func() {
				if fired && !tc.always {
					return
				}
				fired = true
				if _, err := inner.Put(ctx, ConsumerGroupKey("g"), tc.after); err != nil {
					t.Error(err)
				}
			}}
			err = CommitConsumerOffsetIfGroupExists(ctx, store, "g", createRevision, "m1", 2, "orders", 0, 5, "")
			store.client.KV = inner
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if offsets, _ := store.ListGroupConsumerOffsets(ctx, "g"); len(offsets) != 0 {
				t.Fatalf("offset written: %v", offsets)
			}
		})
	}
}
