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
	"sync"
	"testing"

	"github.com/KafScale/platform/v2/internal/testutil"
)

func assertUniqueProducerIDs(t *testing.T, allocators ...ProducerIDAllocator) int64 {
	t.Helper()
	const count = 32
	ids := make(chan int64, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := allocators[i%len(allocators)].AllocateProducerID(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			ids <- id
		}(i)
	}
	wg.Wait()
	close(ids)
	seen := make(map[int64]bool)
	var max int64
	for id := range ids {
		if id < 0 || seen[id] {
			t.Fatalf("invalid or reused producer ID %d", id)
		}
		seen[id] = true
		if id > max {
			max = id
		}
	}
	if len(seen) != count {
		t.Fatalf("allocated %d IDs, want %d", len(seen), count)
	}
	return max
}

func TestProducerIDAllocation(t *testing.T) {
	assertUniqueProducerIDs(t, NewInMemoryStore(ClusterMetadata{}))
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()
	a := newTestEtcdStore(t, ctx, ClusterMetadata{}, endpoints)
	b := newTestEtcdStore(t, ctx, ClusterMetadata{}, endpoints)
	max := assertUniqueProducerIDs(t, a, b)
	// A fresh store must continue the durable cluster counter.
	c := newTestEtcdStore(t, ctx, ClusterMetadata{}, endpoints)
	id, err := c.AllocateProducerID(ctx)
	if err != nil || id <= max {
		t.Fatalf("restart allocation = %d, %v; previous max %d", id, err, max)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := c.AllocateProducerID(canceled); err == nil {
		t.Fatal("canceled allocation succeeded")
	}
}
