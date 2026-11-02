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
	"testing"
	"time"
)

func TestWatchResetsOnCompaction(t *testing.T) {
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
	next, err := r.watchOnce(wctx, 1)
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
