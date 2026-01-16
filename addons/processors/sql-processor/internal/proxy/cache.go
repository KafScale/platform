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
	"strings"
	"time"
)

type queryCache struct {
	ttl        time.Duration
	maxEntries int
	entries    map[string]cacheDecision
	order      []string
}

type cacheDecision struct {
	created    time.Time
	allowed    bool
	reason     string
	topics     []string
	showTopics bool
}

func newQueryCache(ttl time.Duration, maxEntries int) *queryCache {
	if ttl <= 0 || maxEntries <= 0 {
		return nil
	}
	return &queryCache{
		ttl:        ttl,
		maxEntries: maxEntries,
		entries:    make(map[string]cacheDecision, maxEntries),
		order:      make([]string, 0, maxEntries),
	}
}

func (c *queryCache) get(key string) (cacheDecision, bool) {
	if c == nil {
		return cacheDecision{}, false
	}
	entry, ok := c.entries[key]
	if !ok {
		return cacheDecision{}, false
	}
	if c.ttl > 0 && time.Since(entry.created) > c.ttl {
		delete(c.entries, key)
		c.removeOrder(key)
		return cacheDecision{}, false
	}
	return entry, true
}

func (c *queryCache) set(key string, entry cacheDecision) {
	if c == nil {
		return
	}
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

func (c *queryCache) removeOrder(key string) {
	for i, entry := range c.order {
		if entry == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

func cacheKey(query string) string {
	return strings.ToLower(strings.Join(strings.Fields(query), " "))
}
