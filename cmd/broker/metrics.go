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

package main

import (
	"sync"
	"time"
)

type throughputTracker struct {
	mu         sync.Mutex
	buckets    map[int64]int64
	window     time.Duration
	resolution time.Duration
}

func newThroughputTracker(window time.Duration) *throughputTracker {
	if window <= 0 {
		window = 60 * time.Second
	}
	resolution := time.Second
	return &throughputTracker{
		buckets:    make(map[int64]int64),
		window:     window,
		resolution: resolution,
	}
}

func (t *throughputTracker) add(count int64) {
	if t == nil || count <= 0 {
		return
	}
	bucket := time.Now().UnixNano() / t.resolution.Nanoseconds()
	t.mu.Lock()
	t.buckets[bucket] += count
	t.pruneLocked(bucket)
	t.mu.Unlock()
}

func (t *throughputTracker) rate() float64 {
	if t == nil {
		return 0
	}
	bucket := time.Now().UnixNano() / t.resolution.Nanoseconds()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pruneLocked(bucket)
	if len(t.buckets) == 0 {
		return 0
	}
	var total int64
	minBucket := bucket
	for b, count := range t.buckets {
		total += count
		if b < minBucket {
			minBucket = b
		}
	}
	windowBuckets := int64(t.window / t.resolution)
	if windowBuckets < 1 {
		windowBuckets = 1
	}
	durationBuckets := bucket - minBucket + 1
	if durationBuckets > windowBuckets {
		durationBuckets = windowBuckets
	}
	if durationBuckets < 1 {
		durationBuckets = 1
	}
	seconds := float64(durationBuckets) * t.resolution.Seconds()
	if seconds <= 0 {
		return 0
	}
	return float64(total) / seconds
}

func (t *throughputTracker) pruneLocked(current int64) {
	windowBuckets := int64(t.window / t.resolution)
	if windowBuckets < 1 {
		windowBuckets = 1
	}
	minBucket := current - windowBuckets
	for bucket := range t.buckets {
		if bucket < minBucket {
			delete(t.buckets, bucket)
		}
	}
}
