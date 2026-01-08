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

package main

import (
	"fmt"
	"io"
	"sync"
	"time"
)

type lagMetrics struct {
	mu        sync.Mutex
	buckets   []float64
	max       float64
	histogram *histogram
	updatedAt time.Time
}

func newLagMetrics(buckets []float64) *lagMetrics {
	return &lagMetrics{
		buckets:   append([]float64(nil), buckets...),
		histogram: newHistogram(buckets),
	}
}

func (m *lagMetrics) Update(values []int64) {
	if m == nil {
		return
	}
	hist := newHistogram(m.buckets)
	max := 0.0
	for _, value := range values {
		if value < 0 {
			continue
		}
		fval := float64(value)
		if fval > max {
			max = fval
		}
		hist.Observe(fval)
	}
	m.mu.Lock()
	m.max = max
	m.histogram = hist
	m.updatedAt = time.Now()
	m.mu.Unlock()
}

func (m *lagMetrics) WritePrometheus(w io.Writer) {
	if m == nil {
		return
	}
	m.mu.Lock()
	hist := m.histogram
	max := m.max
	m.mu.Unlock()
	if hist != nil {
		hist.WritePrometheus(w, "kafscale_consumer_lag", "Consumer lag in records.")
	}
	fmt.Fprintf(w, "# HELP kafscale_consumer_lag_max Maximum consumer lag in records.\n")
	fmt.Fprintf(w, "# TYPE kafscale_consumer_lag_max gauge\n")
	fmt.Fprintf(w, "kafscale_consumer_lag_max %f\n", max)
}
