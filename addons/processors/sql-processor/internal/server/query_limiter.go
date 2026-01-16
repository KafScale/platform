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
	"errors"
	"time"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/metrics"
)

var errQueueFull = errors.New("query queue full")
var errQueueTimeout = errors.New("query queue timeout")

type queryLimiter struct {
	tokens chan struct{}
	queue  chan struct{}
}

func newQueryLimiter(maxConcurrent int, maxQueue int) *queryLimiter {
	if maxConcurrent <= 0 {
		return nil
	}
	if maxQueue < 0 {
		maxQueue = 0
	}
	return &queryLimiter{
		tokens: make(chan struct{}, maxConcurrent),
		queue:  make(chan struct{}, maxQueue),
	}
}

func (l *queryLimiter) acquire(timeout time.Duration) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	if cap(l.queue) == 0 {
		select {
		case l.tokens <- struct{}{}:
			return func() { <-l.tokens }, nil
		default:
			metrics.QueryQueueRejected.Inc()
			return nil, errQueueFull
		}
	}

	select {
	case l.queue <- struct{}{}:
		metrics.QueryQueueDepth.Inc()
	default:
		metrics.QueryQueueRejected.Inc()
		return nil, errQueueFull
	}
	defer func() {
		<-l.queue
		metrics.QueryQueueDepth.Dec()
	}()

	start := time.Now()
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case l.tokens <- struct{}{}:
		metrics.QueryQueueWait.Observe(float64(time.Since(start).Milliseconds()))
		return func() { <-l.tokens }, nil
	case <-timer.C:
		metrics.QueryQueueTimeout.Inc()
		return nil, errQueueTimeout
	}
}
