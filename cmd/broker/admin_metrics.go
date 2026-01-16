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
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/KafScale/platform/pkg/protocol"
)

type adminMetrics struct {
	mu   sync.Mutex
	data map[int16]*adminMetric
}

type adminMetric struct {
	count      int64
	errorCount int64
	latencySum time.Duration
}

func newAdminMetrics() *adminMetrics {
	return &adminMetrics{
		data: make(map[int16]*adminMetric),
	}
}

func (m *adminMetrics) Record(apiKey int16, latency time.Duration, err error) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.data[apiKey]
	if entry == nil {
		entry = &adminMetric{}
		m.data[apiKey] = entry
	}
	entry.count++
	if err != nil {
		entry.errorCount++
	}
	entry.latencySum += latency
}

func (m *adminMetrics) writePrometheus(w io.Writer) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Fprintln(w, "# HELP kafscale_admin_requests_total Total admin API requests.")
	fmt.Fprintln(w, "# TYPE kafscale_admin_requests_total counter")
	fmt.Fprintln(w, "# HELP kafscale_admin_request_errors_total Total admin API requests that returned an error.")
	fmt.Fprintln(w, "# TYPE kafscale_admin_request_errors_total counter")
	fmt.Fprintln(w, "# HELP kafscale_admin_request_latency_ms_avg Average admin API request latency in milliseconds.")
	fmt.Fprintln(w, "# TYPE kafscale_admin_request_latency_ms_avg gauge")
	for apiKey, entry := range m.data {
		name := adminAPIName(apiKey)
		avg := 0.0
		if entry.count > 0 {
			avg = float64(entry.latencySum.Milliseconds()) / float64(entry.count)
		}
		fmt.Fprintf(w, "kafscale_admin_requests_total{api=%q} %d\n", name, entry.count)
		fmt.Fprintf(w, "kafscale_admin_request_errors_total{api=%q} %d\n", name, entry.errorCount)
		fmt.Fprintf(w, "kafscale_admin_request_latency_ms_avg{api=%q} %.3f\n", name, avg)
	}
}

func adminAPIName(apiKey int16) string {
	switch apiKey {
	case protocol.APIKeyDescribeGroups:
		return "DescribeGroups"
	case protocol.APIKeyListGroups:
		return "ListGroups"
	case protocol.APIKeyOffsetForLeaderEpoch:
		return "OffsetForLeaderEpoch"
	case protocol.APIKeyDescribeConfigs:
		return "DescribeConfigs"
	case protocol.APIKeyAlterConfigs:
		return "AlterConfigs"
	default:
		return fmt.Sprintf("api_%d", apiKey)
	}
}
