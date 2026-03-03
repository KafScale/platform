// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
)

type lfsMetrics struct {
	uploadDuration *histogram
	uploadBytes    uint64
	s3Errors       uint64
	orphans        uint64
	mu             sync.Mutex
	requests       map[string]*topicCounters
}

func newLfsMetrics() *lfsMetrics {
	buckets := []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
	return &lfsMetrics{
		uploadDuration: newHistogram(buckets),
		requests:       make(map[string]*topicCounters),
	}
}

func (m *lfsMetrics) ObserveUploadDuration(seconds float64) {
	if m == nil || m.uploadDuration == nil {
		return
	}
	m.uploadDuration.Observe(seconds)
}

func (m *lfsMetrics) AddUploadBytes(n int64) {
	if m == nil || n <= 0 {
		return
	}
	atomic.AddUint64(&m.uploadBytes, uint64(n))
}

func (m *lfsMetrics) IncRequests(topic, status, typ string) {
	if m == nil {
		return
	}
	if topic == "" {
		topic = "unknown"
	}
	m.mu.Lock()
	counters := m.requests[topic]
	if counters == nil {
		counters = &topicCounters{}
		m.requests[topic] = counters
	}
	m.mu.Unlock()
	switch {
	case status == "ok" && typ == "lfs":
		atomic.AddUint64(&counters.okLfs, 1)
	case status == "error" && typ == "lfs":
		atomic.AddUint64(&counters.errLfs, 1)
	case status == "ok" && typ == "passthrough":
		atomic.AddUint64(&counters.okPas, 1)
	case status == "error" && typ == "passthrough":
		atomic.AddUint64(&counters.errPas, 1)
	}
}

func (m *lfsMetrics) IncS3Errors() {
	if m == nil {
		return
	}
	atomic.AddUint64(&m.s3Errors, 1)
}

func (m *lfsMetrics) IncOrphans(count int) {
	if m == nil || count <= 0 {
		return
	}
	atomic.AddUint64(&m.orphans, uint64(count))
}

func (m *lfsMetrics) WritePrometheus(w io.Writer) {
	if m == nil {
		return
	}
	m.uploadDuration.WritePrometheus(w, "kafscale_lfs_proxy_upload_duration_seconds", "LFS proxy upload durations in seconds")
	fmt.Fprintf(w, "# HELP kafscale_lfs_proxy_upload_bytes_total Total bytes uploaded via LFS\n")
	fmt.Fprintf(w, "# TYPE kafscale_lfs_proxy_upload_bytes_total counter\n")
	fmt.Fprintf(w, "kafscale_lfs_proxy_upload_bytes_total %d\n", atomic.LoadUint64(&m.uploadBytes))
	fmt.Fprintf(w, "# HELP kafscale_lfs_proxy_requests_total LFS proxy requests\n")
	fmt.Fprintf(w, "# TYPE kafscale_lfs_proxy_requests_total counter\n")
	topics := m.snapshotTopics()
	for _, topic := range topics {
		counters := m.requests[topic]
		fmt.Fprintf(w, "kafscale_lfs_proxy_requests_total{topic=\"%s\",status=\"ok\",type=\"lfs\"} %d\n", topic, atomic.LoadUint64(&counters.okLfs))
		fmt.Fprintf(w, "kafscale_lfs_proxy_requests_total{topic=\"%s\",status=\"error\",type=\"lfs\"} %d\n", topic, atomic.LoadUint64(&counters.errLfs))
		fmt.Fprintf(w, "kafscale_lfs_proxy_requests_total{topic=\"%s\",status=\"ok\",type=\"passthrough\"} %d\n", topic, atomic.LoadUint64(&counters.okPas))
		fmt.Fprintf(w, "kafscale_lfs_proxy_requests_total{topic=\"%s\",status=\"error\",type=\"passthrough\"} %d\n", topic, atomic.LoadUint64(&counters.errPas))
	}
	fmt.Fprintf(w, "# HELP kafscale_lfs_proxy_s3_errors_total Total S3 errors\n")
	fmt.Fprintf(w, "# TYPE kafscale_lfs_proxy_s3_errors_total counter\n")
	fmt.Fprintf(w, "kafscale_lfs_proxy_s3_errors_total %d\n", atomic.LoadUint64(&m.s3Errors))
	fmt.Fprintf(w, "# HELP kafscale_lfs_proxy_orphan_objects_total LFS objects uploaded but not committed to Kafka\n")
	fmt.Fprintf(w, "# TYPE kafscale_lfs_proxy_orphan_objects_total counter\n")
	fmt.Fprintf(w, "kafscale_lfs_proxy_orphan_objects_total %d\n", atomic.LoadUint64(&m.orphans))

	// Runtime metrics
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	fmt.Fprintf(w, "# HELP kafscale_lfs_proxy_goroutines Number of goroutines\n")
	fmt.Fprintf(w, "# TYPE kafscale_lfs_proxy_goroutines gauge\n")
	fmt.Fprintf(w, "kafscale_lfs_proxy_goroutines %d\n", runtime.NumGoroutine())
	fmt.Fprintf(w, "# HELP kafscale_lfs_proxy_memory_alloc_bytes Bytes allocated and in use\n")
	fmt.Fprintf(w, "# TYPE kafscale_lfs_proxy_memory_alloc_bytes gauge\n")
	fmt.Fprintf(w, "kafscale_lfs_proxy_memory_alloc_bytes %d\n", memStats.Alloc)
	fmt.Fprintf(w, "# HELP kafscale_lfs_proxy_memory_sys_bytes Bytes obtained from system\n")
	fmt.Fprintf(w, "# TYPE kafscale_lfs_proxy_memory_sys_bytes gauge\n")
	fmt.Fprintf(w, "kafscale_lfs_proxy_memory_sys_bytes %d\n", memStats.Sys)
	fmt.Fprintf(w, "# HELP kafscale_lfs_proxy_gc_pause_total_ns Total GC pause time in nanoseconds\n")
	fmt.Fprintf(w, "# TYPE kafscale_lfs_proxy_gc_pause_total_ns counter\n")
	fmt.Fprintf(w, "kafscale_lfs_proxy_gc_pause_total_ns %d\n", memStats.PauseTotalNs)
}

func (m *lfsMetrics) snapshotTopics() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.requests))
	for topic := range m.requests {
		out = append(out, topic)
	}
	sort.Strings(out)
	return out
}

type topicCounters struct {
	okLfs  uint64
	errLfs uint64
	okPas  uint64
	errPas uint64
}

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
