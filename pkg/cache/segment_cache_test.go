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

package cache

import (
	"sync"
	"testing"
)

func TestSegmentCacheEviction(t *testing.T) {
	cache := NewSegmentCache(10)
	cache.SetSegment("orders", 0, 0, []byte("12345"))
	if _, ok := cache.GetSegment("orders", 0, 0); !ok {
		t.Fatalf("expected cache hit")
	}
	cache.SetSegment("orders", 0, 1, []byte("67890"))
	if cache.ll.Len() != 2 {
		t.Fatalf("expected two entries")
	}
	cache.SetSegment("orders", 0, 2, []byte("abcde")) // should evict oldest

	if _, ok := cache.GetSegment("orders", 0, 0); ok {
		t.Fatalf("oldest entry should be evicted")
	}
	if _, ok := cache.GetSegment("orders", 0, 2); !ok {
		t.Fatalf("new entry missing")
	}
}

func TestNewSegmentCacheZeroCapacity(t *testing.T) {
	c := NewSegmentCache(0)
	if c.capacity != 1 {
		t.Fatalf("expected capacity 1 for zero input, got %d", c.capacity)
	}
	c2 := NewSegmentCache(-5)
	if c2.capacity != 1 {
		t.Fatalf("expected capacity 1 for negative input, got %d", c2.capacity)
	}
}

func TestGetSegmentCacheMiss(t *testing.T) {
	c := NewSegmentCache(100)
	data, ok := c.GetSegment("missing", 0, 0)
	if ok {
		t.Fatal("expected cache miss")
	}
	if data != nil {
		t.Fatalf("expected nil data on miss, got %v", data)
	}
}

func TestSetSegmentUpdateExisting(t *testing.T) {
	c := NewSegmentCache(100)
	c.SetSegment("topic", 0, 0, []byte("old"))
	c.SetSegment("topic", 0, 0, []byte("new-value"))

	data, ok := c.GetSegment("topic", 0, 0)
	if !ok {
		t.Fatal("expected cache hit after update")
	}
	if string(data) != "new-value" {
		t.Fatalf("expected 'new-value', got '%s'", data)
	}
	if c.ll.Len() != 1 {
		t.Fatalf("expected 1 entry after update, got %d", c.ll.Len())
	}
}

func TestSetSegmentLargerThanCapacity(t *testing.T) {
	c := NewSegmentCache(5)
	c.SetSegment("topic", 0, 0, []byte("ab"))  // 2 bytes, fits
	c.SetSegment("topic", 0, 1, []byte("cde")) // 3 bytes, total=5 at capacity

	// This add exceeds capacity: entry 0 gets evicted to make room
	c.SetSegment("topic", 0, 2, []byte("fghij")) // 5 bytes
	if _, ok := c.GetSegment("topic", 0, 0); ok {
		t.Fatal("expected entry 0 to be evicted")
	}
	if _, ok := c.GetSegment("topic", 0, 2); !ok {
		t.Fatal("expected new entry to be present")
	}
}

func TestLRUOrdering(t *testing.T) {
	c := NewSegmentCache(15) // room for exactly 3 x 5-byte entries

	c.SetSegment("t", 0, 0, []byte("aaaaa")) // 5 bytes
	c.SetSegment("t", 0, 1, []byte("bbbbb")) // 5 bytes
	c.SetSegment("t", 0, 2, []byte("ccccc")) // 5 bytes = 15 total, exactly at capacity

	// Access entry 0 to make it recently used
	c.GetSegment("t", 0, 0)

	// Adding a new entry should evict entry 1 (least recently used), not entry 0
	c.SetSegment("t", 0, 3, []byte("ddddd"))

	if _, ok := c.GetSegment("t", 0, 1); ok {
		t.Fatal("expected entry 1 to be evicted (LRU)")
	}
	if _, ok := c.GetSegment("t", 0, 0); !ok {
		t.Fatal("expected entry 0 to still be present (was accessed recently)")
	}
	if _, ok := c.GetSegment("t", 0, 3); !ok {
		t.Fatal("expected new entry 3 to be present")
	}
}

func TestMultipleTopicsAndPartitions(t *testing.T) {
	c := NewSegmentCache(1000)
	c.SetSegment("orders", 0, 0, []byte("a"))
	c.SetSegment("orders", 1, 0, []byte("b"))
	c.SetSegment("events", 0, 0, []byte("c"))

	if d, ok := c.GetSegment("orders", 0, 0); !ok || string(d) != "a" {
		t.Fatal("orders:0:0 mismatch")
	}
	if d, ok := c.GetSegment("orders", 1, 0); !ok || string(d) != "b" {
		t.Fatal("orders:1:0 mismatch")
	}
	if d, ok := c.GetSegment("events", 0, 0); !ok || string(d) != "c" {
		t.Fatal("events:0:0 mismatch")
	}
}

func TestConcurrentAccess(t *testing.T) {
	c := NewSegmentCache(10000)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int32) {
			defer wg.Done()
			c.SetSegment("topic", n, 0, []byte("data"))
			c.GetSegment("topic", n, 0)
		}(int32(i))
	}
	wg.Wait()
}

func TestMakeKey(t *testing.T) {
	key := makeKey("orders", 3, 100)
	if key != "orders:3:100" {
		t.Fatalf("unexpected key format: %s", key)
	}
}
