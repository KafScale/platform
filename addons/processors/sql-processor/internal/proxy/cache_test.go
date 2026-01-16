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

package proxy

import (
	"testing"
	"time"
)

func TestQueryCacheTTL(t *testing.T) {
	cache := newQueryCache(100*time.Millisecond, 2)
	cache.set("a", cacheDecision{created: time.Now().Add(-time.Second), allowed: true})
	if _, ok := cache.get("a"); ok {
		t.Fatalf("expected expired entry")
	}
}

func TestQueryCacheEvictsOldest(t *testing.T) {
	cache := newQueryCache(time.Minute, 1)
	cache.set("a", cacheDecision{created: time.Now(), allowed: true})
	cache.set("b", cacheDecision{created: time.Now(), allowed: false})
	if _, ok := cache.get("a"); ok {
		t.Fatalf("expected oldest entry evicted")
	}
	if _, ok := cache.get("b"); !ok {
		t.Fatalf("expected newest entry present")
	}
}

func TestQueryCacheKey(t *testing.T) {
	key := cacheKey("SELECT  *  FROM Orders")
	if key != "select * from orders" {
		t.Fatalf("unexpected cache key: %q", key)
	}
}
