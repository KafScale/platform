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
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type countKV struct {
	clientv3.KV
	gets atomic.Int64
	txns atomic.Int64
}

func (k *countKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	k.gets.Add(1)
	return k.KV.Get(ctx, key, opts...)
}

func (k *countKV) Txn(ctx context.Context) clientv3.Txn {
	k.txns.Add(1)
	return k.KV.Txn(ctx)
}

func TestRegisterRoundTripsDoNotGrowWithVersions(t *testing.T) {
	h := newHarness(t, nil, Config{})
	endpoints := h.cli.Endpoints()
	measure := func(n int, level string) int64 {
		subject := fmt.Sprintf("s-%s-%d", level, n)
		h.ok("PUT", "/config/"+subject, map[string]string{"compatibility": "NONE"}, nil)
		for i := 0; i < n; i++ {
			fields := make([]string, 0, i+1)
			for j := 0; j <= i; j++ {
				fields = append(fields, fmt.Sprintf(`{"name":"f%d","type":"int","default":0}`, j))
			}
			h.register(subject, schema(`{"type":"record","name":"R","fields":[`+strings.Join(fields, ",")+`]}`))
		}
		h.ok("PUT", "/config/"+subject, map[string]string{"compatibility": level}, nil)
		cold := newHarness(t, endpoints, Config{})
		kv := &countKV{KV: cold.cli.KV}
		cold.reg.cli.KV = kv
		cold.register(subject, schema(`{"type":"record","name":"R","fields":[]}`))
		return kv.gets.Load() + kv.txns.Load()
	}
	for _, level := range []string{CompatNone, CompatBackwardAll} {
		small, large := measure(3, level), measure(60, level)
		if large > small+2 {
			t.Fatalf("%s: %d round trips with 3 versions, %d with 60", level, small, large)
		}
	}
}

func TestPrefetchRereadsCachedIDAboveCounter(t *testing.T) {
	h := newHarness(t, nil, Config{})
	id := h.register("s", schema(userV1))
	h.reg.mu.Lock()
	h.reg.cache[id] = &Schema{ID: id, Type: schemaTypeAvro, Text: "stale"}
	h.reg.counter = id - 1
	h.reg.mu.Unlock()
	if err := h.reg.prefetch(context.Background(), []int64{id}); err != nil {
		t.Fatal(err)
	}
	h.reg.mu.RLock()
	defer h.reg.mu.RUnlock()
	if s := h.reg.cache[id]; s == nil || s.Text == "stale" || h.reg.counter != id {
		t.Fatalf("cache %+v counter %d", s, h.reg.counter)
	}
}
