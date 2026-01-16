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

package server

import (
	"testing"
	"time"

	"github.com/jackc/pgproto3/v2"
)

func TestResultCacheEvictsOldest(t *testing.T) {
	cache := newResultCache(time.Minute, 1)
	cache.Set("a", &cacheEntry{created: time.Now(), fields: []pgproto3.FieldDescription{{Name: []byte("a")}}})
	cache.Set("b", &cacheEntry{created: time.Now(), fields: []pgproto3.FieldDescription{{Name: []byte("b")}}})

	if _, ok := cache.Get("a"); ok {
		t.Fatalf("expected oldest entry evicted")
	}
	if _, ok := cache.Get("b"); !ok {
		t.Fatalf("expected newest entry present")
	}
}

func TestResultCacheTTL(t *testing.T) {
	cache := newResultCache(500*time.Millisecond, 2)
	entry := &cacheEntry{
		created: time.Now().Add(-time.Second),
		fields:  []pgproto3.FieldDescription{{Name: []byte("a")}},
	}
	cache.Set("a", entry)
	if _, ok := cache.Get("a"); ok {
		t.Fatalf("expected entry expired")
	}
}

func TestCacheKeyNormalization(t *testing.T) {
	key := cacheKey("SELECT  *  FROM Orders", "ts=1-2")
	if key != "select * from orders|ts=1-2" {
		t.Fatalf("unexpected cache key: %q", key)
	}
}
