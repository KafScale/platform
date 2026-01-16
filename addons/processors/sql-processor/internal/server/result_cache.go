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
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgproto3/v2"
)

type resultCache struct {
	ttl        time.Duration
	maxEntries int

	mu      sync.Mutex
	entries map[string]*cacheEntry
	order   []string
}

type cacheEntry struct {
	created    time.Time
	fields     []pgproto3.FieldDescription
	rows       [][][]byte
	commandTag []byte
	rowsCount  int
	segments   int
	bytes      int64
}

func newResultCache(ttl time.Duration, maxEntries int) *resultCache {
	if ttl <= 0 || maxEntries <= 0 {
		return nil
	}
	return &resultCache{
		ttl:        ttl,
		maxEntries: maxEntries,
		entries:    make(map[string]*cacheEntry, maxEntries),
		order:      make([]string, 0, maxEntries),
	}
}

func (c *resultCache) Get(key string) (*cacheEntry, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if c.ttl > 0 && time.Since(entry.created) > c.ttl {
		delete(c.entries, key)
		c.removeOrder(key)
		return nil, false
	}
	return entry, true
}

func (c *resultCache) Set(key string, entry *cacheEntry) {
	if c == nil || entry == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.entries[key]; exists {
		c.removeOrder(key)
	}
	c.entries[key] = entry
	c.order = append(c.order, key)

	for len(c.order) > c.maxEntries {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.entries, oldest)
	}
}

func (c *resultCache) removeOrder(key string) {
	for i, entry := range c.order {
		if entry == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

func cacheKey(query string, parsedQueryKey string) string {
	normalized := strings.ToLower(strings.Join(strings.Fields(query), " "))
	if parsedQueryKey == "" {
		return normalized
	}
	return normalized + "|" + parsedQueryKey
}
