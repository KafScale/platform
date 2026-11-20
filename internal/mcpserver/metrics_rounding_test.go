// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package mcpserver

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/KafScale/platform/internal/console"
)

func TestClusterMetricsRoundsAggregatesWithoutChangingSnapshot(t *testing.T) {
	a, b := 0.1, 0.2
	snapshot := &console.MetricsSnapshot{S3State: "healthy", S3LatencyMS: 12, ProduceRPS: 3.1666659999999998, FetchRPS: a + b, AdminRequestsTotal: 1 << 53, AdminRequestErrorsTotal: 2, AdminRequestLatencyMS: 0.123456789}
	original := *snapshot
	_, output, err := clusterMetricsHandler(Options{Metrics: &mockMetrics{snap: snapshot}})(context.Background(), nil, emptyInput{})
	if err != nil {
		t.Fatal(err)
	}
	if output.ProduceRPS != 3.166666 || output.FetchRPS != 0.3 || output.AdminRequestLatencyMS != 0.123457 || output.AdminRequestsTotal != 1<<53 || output.AdminRequestErrorsTotal != 2 || output.S3LatencyMS != 12 {
		t.Fatalf("metrics=%+v", output)
	}
	if snapshot.ProduceRPS != original.ProduceRPS || snapshot.FetchRPS != original.FetchRPS || snapshot.AdminRequestLatencyMS != original.AdminRequestLatencyMS {
		t.Fatal("rounding mutated the shared metrics snapshot")
	}
	data, err := json.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "3.16666599") || strings.Contains(string(data), "0.30000000000000004") {
		t.Fatalf("float noise in JSON: %s", data)
	}
}

func TestRoundMetricPrecisionAndRange(t *testing.T) {
	for _, tc := range []struct{ input, want float64 }{
		{0, 0}, {-0.00000001, 0}, {0.00000001, 0}, {1.23456789, 1.234568}, {-1.23456789, -1.234568}, {3.1666659999999998, 3.166666}, {math.MaxFloat64, math.MaxFloat64}, {-math.MaxFloat64, -math.MaxFloat64}, {1 << 53, 1 << 53},
	} {
		got := roundMetric(tc.input)
		if got != tc.want || (got == 0 && math.Signbit(got)) {
			t.Fatalf("round(%v)=%v want %v", tc.input, got, tc.want)
		}
	}
}
