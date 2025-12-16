package cache

import "testing"

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
