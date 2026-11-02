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
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func simulateRestore(t *testing.T, h *harness) {
	t.Helper()
	ctx := context.Background()
	if _, err := h.cli.Delete(ctx, idKey(1)); err != nil {
		t.Fatal(err)
	}
	var rev int64
	for i := 0; i < 5; i++ {
		resp, err := h.cli.Put(ctx, "/other", "x")
		if err != nil {
			t.Fatal(err)
		}
		rev = resp.Header.Revision
	}
	if _, err := h.cli.Compact(ctx, rev); err != nil {
		t.Fatal(err)
	}
}

func cacheID1(t *testing.T, r *Registry) {
	t.Helper()
	if _, err := r.SchemaByID(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if !cached(r, 1) {
		t.Fatal("id 1 not cached")
	}
}

func runWatchSteps(r *Registry, first func(ctx context.Context) (int64, bool, error), before func(call int)) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	once := func(ctx context.Context, from int64) (int64, bool, error) {
		calls++
		before(calls)
		switch {
		case calls == 1 && first != nil:
			return first(ctx)
		case calls > 2 || (calls > 1 && first == nil):
			cancel()
			return from, false, ctx.Err()
		}
		sctx, scancel := context.WithTimeout(ctx, 3*time.Second)
		defer scancel()
		return r.watchOnce(sctx, from)
	}
	r.watchLoop(ctx, once, func(context.Context, time.Duration) bool { return true })
}

func assertNotStale(t *testing.T, r *Registry) {
	t.Helper()
	if cached(r, 1) {
		t.Fatal("stale id 1 still cached after the restore")
	}
	if _, err := r.SchemaByID(context.Background(), 1); err == nil {
		t.Fatal("registry still serves id 1, which the restore removed")
	}
}

func TestWatchCatchesRestoreBeforeFirstWatch(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	cacheID1(t, h.reg)
	simulateRestore(t, h)
	runWatchSteps(h.reg, nil, func(int) {})
	assertNotStale(t, h.reg)
}

func TestWatchCatchesRestoreAfterEarlierReset(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	r := h.reg
	first := func(ctx context.Context) (int64, bool, error) {
		r.restored(ctx)
		return 0, true, nil
	}
	runWatchSteps(r, first, func(call int) {
		if call == 2 {
			cacheID1(t, r)
			simulateRestore(t, h)
		}
	})
	assertNotStale(t, r)
}

func forgetRevision(r *Registry) {
	r.mu.Lock()
	r.counter, r.revision, r.raftTerm = 0, 0, 0
	r.mu.Unlock()
}

func revisionOf(r *Registry) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.revision
}

func TestWatchRetryAfterPostRestoreRead(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	h.register("t", schema(userV2))
	r := h.reg
	forgetRevision(r)
	first := func(ctx context.Context) (int64, bool, error) {
		return 0, false, errors.New("etcd down")
	}
	runWatchSteps(r, first, func(call int) {
		if call == 2 {
			cacheID1(t, r)
			r.mu.Lock()
			delete(r.cache, 2)
			r.mu.Unlock()
			simulateRestore(t, h)
			if _, err := r.SchemaByID(context.Background(), 2); err != nil {
				t.Fatal(err)
			}
		}
	})
	assertNotStale(t, r)
}

func TestWatchFallbackClearsCacheAfterBaselineRead(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	r := h.reg
	forgetRevision(r)
	cli := newClient(t, h.cli.Endpoints())
	var hooked atomic.Bool
	kv := &hookKV{KV: cli.KV, key: counterKey()}
	kv.before = func() {
		if hooked.CompareAndSwap(false, true) {
			cacheID1(t, r)
		}
	}
	cli.KV = kv
	r.cli = cli
	base := r.CacheResets()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, _, _ = r.watchOnce(ctx, 0)
	if !hooked.Load() {
		t.Fatal("watch did not read a baseline")
	}
	if cached(r, 1) || r.CacheResets() != base+1 {
		t.Fatalf("cached %v resets %d->%d", cached(r, 1), base, r.CacheResets())
	}
}

func TestRestoredStartsFromNoRevision(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	r := h.reg
	resp, err := h.cli.Get(context.Background(), counterKey())
	if err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.revision = 1 << 40
	r.mu.Unlock()
	base := r.CacheResets()
	r.restored(context.Background())
	if got := revisionOf(r); r.CacheResets() != base+1 || got < resp.Header.Revision || got >= 1<<40 {
		t.Fatalf("resets %d->%d revision %d", base, r.CacheResets(), got)
	}
}
