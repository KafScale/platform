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
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestWatchResetsAfterAnyCompaction(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	r := h.reg
	ctx := context.Background()
	if _, err := r.SchemaByID(ctx, 1); err != nil {
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
	base := r.CacheResets()
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	next, _, err := r.watchOnce(wctx, 1)
	if err != nil || next != 0 {
		t.Fatalf("watch returned %d %v", next, err)
	}
	if r.CacheResets() != base+1 || cached(r, 1) || counterOf(r) != 1 {
		t.Fatalf("resets %d->%d cached %v counter %d", base, r.CacheResets(), cached(r, 1), counterOf(r))
	}
}

func TestWatchStopsOnCancel(t *testing.T) {
	h := newHarness(t, nil, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.reg.Watch(ctx)
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not stop after cancel")
	}
}

type fakeWatcher struct {
	clientv3.Watcher
	ch chan clientv3.WatchResponse
}

func (f *fakeWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return f.ch
}

func fakeWatch(t *testing.T, h *harness) chan clientv3.WatchResponse {
	t.Helper()
	cli := newClient(t, h.cli.Endpoints())
	ch := make(chan clientv3.WatchResponse, 4)
	cli.Watcher = &fakeWatcher{Watcher: cli.Watcher, ch: ch}
	h.reg.cli = cli
	return ch
}

func TestWatchResetsOnBackwardsRevision(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	ctx := context.Background()
	if _, err := h.reg.SchemaByID(ctx, 1); err != nil {
		t.Fatal(err)
	}
	ch := fakeWatch(t, h)
	ch <- clientv3.WatchResponse{Header: etcdserverpb.ResponseHeader{Revision: 50}}
	close(ch)
	base := h.reg.CacheResets()
	next, received, err := h.reg.watchOnce(ctx, 100)
	if err != nil || next != 0 || !received {
		t.Fatalf("watch returned %d %v %v", next, received, err)
	}
	if h.reg.CacheResets() != base+1 || cached(h.reg, 1) {
		t.Fatalf("resets %d->%d cached %v", base, h.reg.CacheResets(), cached(h.reg, 1))
	}
}

func TestWatchReportsClosedChannel(t *testing.T) {
	h := newHarness(t, nil, Config{})
	ch := fakeWatch(t, h)
	ch <- clientv3.WatchResponse{Header: etcdserverpb.ResponseHeader{Revision: 120}}
	close(ch)
	next, received, err := h.reg.watchOnce(context.Background(), 100)
	if !errors.Is(err, errWatchClosed) || next != 120 || !received {
		t.Fatalf("watch returned %d %v %v", next, received, err)
	}
}

func TestWatchLoopBacksOffAndClearsFailuresOnResponse(t *testing.T) {
	h := newHarness(t, nil, Config{})
	steps := []bool{false, false, false, false, false, false, false, false, false, true, false}
	calls := 0
	once := func(ctx context.Context, from int64) (int64, bool, error) {
		received := steps[calls]
		calls++
		return from, received, errWatchClosed
	}
	var waits []time.Duration
	wait := func(ctx context.Context, d time.Duration) bool {
		waits = append(waits, d)
		return len(waits) < len(steps)
	}
	h.reg.watchLoop(context.Background(), once, wait)
	want := []time.Duration{
		200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond,
		3200 * time.Millisecond, 6400 * time.Millisecond, 10 * time.Second, 10 * time.Second, 10 * time.Second,
		200 * time.Millisecond, 400 * time.Millisecond,
	}
	if len(waits) != len(want) {
		t.Fatalf("waits %v", waits)
	}
	for i := range want {
		if waits[i] != want[i] {
			t.Fatalf("wait %d is %v, want %v (all %v)", i, waits[i], want[i], waits)
		}
	}
}
