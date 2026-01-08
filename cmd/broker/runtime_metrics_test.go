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
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestRuntimeMetricsOutput(t *testing.T) {
	h := &handler{
		startTime:  time.Now().Add(-2 * time.Second),
		cpuTracker: newCPUTracker(),
	}
	var buf bytes.Buffer
	h.writeRuntimeMetrics(&buf)
	out := buf.String()
	if !strings.Contains(out, "kafscale_broker_uptime_seconds") {
		t.Fatalf("expected uptime metric, got:\n%s", out)
	}
	if !strings.Contains(out, "kafscale_broker_mem_alloc_bytes") {
		t.Fatalf("expected mem alloc metric, got:\n%s", out)
	}
	if !strings.Contains(out, "kafscale_broker_cpu_percent") {
		t.Fatalf("expected cpu percent metric, got:\n%s", out)
	}
	if !strings.Contains(out, "kafscale_broker_goroutines") {
		t.Fatalf("expected goroutines metric, got:\n%s", out)
	}
}
