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
