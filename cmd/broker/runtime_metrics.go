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
	"runtime"
	"time"
)

func (h *handler) writeRuntimeMetrics(w io.Writer) {
	if h == nil {
		return
	}
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	uptime := time.Since(h.startTime).Seconds()
	cpuPercent := 0.0
	if h.cpuTracker != nil {
		cpuPercent = h.cpuTracker.Percent()
	}

	fmt.Fprintln(w, "# HELP kafscale_broker_uptime_seconds Seconds since broker start.")
	fmt.Fprintln(w, "# TYPE kafscale_broker_uptime_seconds gauge")
	fmt.Fprintf(w, "kafscale_broker_uptime_seconds %f\n", uptime)

	fmt.Fprintln(w, "# HELP kafscale_broker_mem_alloc_bytes Bytes of allocated heap objects.")
	fmt.Fprintln(w, "# TYPE kafscale_broker_mem_alloc_bytes gauge")
	fmt.Fprintf(w, "kafscale_broker_mem_alloc_bytes %d\n", stats.Alloc)

	fmt.Fprintln(w, "# HELP kafscale_broker_mem_sys_bytes Bytes of memory obtained from the OS.")
	fmt.Fprintln(w, "# TYPE kafscale_broker_mem_sys_bytes gauge")
	fmt.Fprintf(w, "kafscale_broker_mem_sys_bytes %d\n", stats.Sys)

	fmt.Fprintln(w, "# HELP kafscale_broker_heap_inuse_bytes Bytes in in-use spans.")
	fmt.Fprintln(w, "# TYPE kafscale_broker_heap_inuse_bytes gauge")
	fmt.Fprintf(w, "kafscale_broker_heap_inuse_bytes %d\n", stats.HeapInuse)

	fmt.Fprintln(w, "# HELP kafscale_broker_cpu_percent Approximate CPU usage percent since last scrape.")
	fmt.Fprintln(w, "# TYPE kafscale_broker_cpu_percent gauge")
	fmt.Fprintf(w, "kafscale_broker_cpu_percent %f\n", cpuPercent)

	fmt.Fprintln(w, "# HELP kafscale_broker_goroutines Number of goroutines.")
	fmt.Fprintln(w, "# TYPE kafscale_broker_goroutines gauge")
	fmt.Fprintf(w, "kafscale_broker_goroutines %d\n", runtime.NumGoroutine())
}
