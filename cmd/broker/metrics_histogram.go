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
	"sort"
	"sync"
)

type histogram struct {
	mu      sync.Mutex
	buckets []float64
	counts  []int64
	sum     float64
	count   int64
}

func newHistogram(buckets []float64) *histogram {
	if len(buckets) == 0 {
		buckets = []float64{1, 2, 5, 10, 25, 50, 100}
	}
	cp := append([]float64(nil), buckets...)
	sort.Float64s(cp)
	return &histogram{
		buckets: cp,
		counts:  make([]int64, len(cp)+1),
	}
}

func (h *histogram) Observe(value float64) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += value
	h.count++
	idx := sort.SearchFloat64s(h.buckets, value)
	h.counts[idx]++
}

func (h *histogram) Snapshot() ([]float64, []int64, float64, int64) {
	if h == nil {
		return nil, nil, 0, 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	buckets := append([]float64(nil), h.buckets...)
	counts := append([]int64(nil), h.counts...)
	return buckets, counts, h.sum, h.count
}

func (h *histogram) WritePrometheus(w io.Writer, name, help string) {
	if h == nil {
		return
	}
	buckets, counts, sum, count := h.Snapshot()
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s histogram\n", name)
	var cumulative int64
	for i, upper := range buckets {
		cumulative += counts[i]
		fmt.Fprintf(w, "%s_bucket{le=%q} %d\n", name, formatFloat(upper), cumulative)
	}
	cumulative += counts[len(counts)-1]
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, cumulative)
	fmt.Fprintf(w, "%s_sum %f\n", name, sum)
	fmt.Fprintf(w, "%s_count %d\n", name, count)
}

func formatFloat(val float64) string {
	return fmt.Sprintf("%g", val)
}
