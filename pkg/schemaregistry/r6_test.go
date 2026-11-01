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

package schemaregistry

import (
	"context"
	"sync/atomic"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type hookKV struct {
	clientv3.KV
	key    string
	before func()
}

func (k *hookKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if (k.key == "" || key == k.key) && k.before != nil {
		k.before()
	}
	return k.KV.Get(ctx, key, opts...)
}

func hookGet(r *Registry, key string, before func()) {
	r.cli.KV = &hookKV{KV: r.cli.KV, key: key, before: before}
}

func forceReset(r *Registry) {
	r.mu.Lock()
	r.resetLocked()
	r.mu.Unlock()
}

func raiseRevision(r *Registry) {
	r.mu.Lock()
	r.revision += 1000
	r.mu.Unlock()
}

func counterOf(r *Registry) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.counter
}

func cached(r *Registry, id int64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cache[id] != nil
}

func TestLateReplyAfterRealResetIsRejected(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	r := h.reg
	cluster, term, rev := readState(r)
	pre := r.mark()
	r.observe(markOf(r), &etcdserverpb.ResponseHeader{ClusterId: cluster, RaftTerm: term, Revision: rev - 1})
	if r.observe(&pre, &etcdserverpb.ResponseHeader{ClusterId: cluster, RaftTerm: term, Revision: rev}) {
		t.Fatal("late reply accepted")
	}
	if _, _, got := readState(r); got != rev-1 {
		t.Fatalf("revision %d, want %d", got, rev-1)
	}
}

func TestSchemaReadDuringResetIsNotCached(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	r := h.reg
	forceReset(r)
	r.mu.Lock()
	r.counter = 5
	r.mu.Unlock()
	hookGet(r, idKey(1), func() {
		forceReset(r)
		r.mu.Lock()
		r.counter = 0
		r.mu.Unlock()
	})
	if s, err := r.SchemaByID(context.Background(), 1); err != nil || s == nil {
		t.Fatalf("schema %v err %v", s, err)
	}
	if cached(r, 1) || counterOf(r) != 0 {
		t.Fatalf("stale read kept: cached %v counter %d", cached(r, 1), counterOf(r))
	}
}

func TestCounterReadDuringResetIsIgnored(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	r := h.reg
	hookGet(r, counterKey(), func() {
		forceReset(r)
		r.mu.Lock()
		r.counter = 0
		r.mu.Unlock()
	})
	if err := r.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if counterOf(r) != 0 {
		t.Fatalf("counter %d, want 0", counterOf(r))
	}
}

func TestOutOfOrderCounterDoesNotReset(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	r := h.reg
	base := r.CacheResets()
	hookGet(r, counterKey(), func() {
		r.mu.Lock()
		r.counter = 50
		r.mu.Unlock()
	})
	if err := r.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.CacheResets() != base || counterOf(r) != 50 {
		t.Fatalf("resets %d->%d counter %d", base, r.CacheResets(), counterOf(r))
	}
}

func TestLowerCounterAfterCompletedReadResets(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	r := h.reg
	r.mu.Lock()
	r.counter = 50
	r.mu.Unlock()
	base := r.CacheResets()
	if err := r.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.CacheResets() != base+1 || counterOf(r) != 1 {
		t.Fatalf("resets %d->%d counter %d", base, r.CacheResets(), counterOf(r))
	}
}

func expectReset(t *testing.T, r *Registry, base uint64) {
	t.Helper()
	if r.CacheResets() == base {
		t.Fatal("no reset after etcd went below what the pod saw")
	}
}

func TestRestoreSeenByRead(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	base := h.reg.CacheResets()
	raiseRevision(h.reg)
	h.register("s", schema(userV1))
	expectReset(t, h.reg, base)
}

func TestRestoreSeenBySchemaLookup(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	r := h.reg
	r.mu.Lock()
	clear(r.cache)
	r.counter = 1
	r.mu.Unlock()
	base := r.CacheResets()
	raiseRevision(r)
	if _, err := r.SchemaByID(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	expectReset(t, r, base)
}

func TestRestoreSeenByCounterRead(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	base := h.reg.CacheResets()
	raiseRevision(h.reg)
	if err := h.reg.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	expectReset(t, h.reg, base)
}

func TestRestoreSeenByGUIDLookup(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	r := h.reg
	s, err := r.SchemaByID(context.Background(), 1)
	if err != nil || s == nil {
		t.Fatalf("schema %v err %v", s, err)
	}
	base := r.CacheResets()
	raiseRevision(r)
	if _, err := r.SchemaByGUID(context.Background(), s.GUID()); err != nil {
		t.Fatal(err)
	}
	expectReset(t, r, base)
}

func TestRestoreSeenByCommit(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.ok("PUT", "/config", map[string]string{"compatibility": "NONE"}, nil)
	h.register("s", schema(userV1))
	base := h.reg.CacheResets()
	var atCommit, afterCommit atomic.Int64
	var committed, seen atomic.Bool
	h.reg.beforeCommit = func() {
		raiseRevision(h.reg)
		atCommit.Store(int64(h.reg.CacheResets()))
		committed.Store(true)
	}
	hookGet(h.reg, "", func() {
		if committed.Load() && seen.CompareAndSwap(false, true) {
			afterCommit.Store(int64(h.reg.CacheResets()))
		}
	})
	h.register("s", schema(userV2))
	h.reg.beforeCommit = nil
	if !seen.Load() || atCommit.Load() != int64(base) || afterCommit.Load() != int64(base)+1 {
		t.Fatalf("resets at commit %d, after commit %d, want %d and %d", atCommit.Load(), afterCommit.Load(), base, base+1)
	}
}

func markOf(r *Registry) *readMark {
	m := r.mark()
	return &m
}
