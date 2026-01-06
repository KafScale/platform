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
)

func TestLagMetricsPrometheusOutput(t *testing.T) {
	metrics := newLagMetrics([]float64{1, 10, 100})
	metrics.Update([]int64{10, 30, 5})

	var buf bytes.Buffer
	metrics.WritePrometheus(&buf)
	out := buf.String()

	if !strings.Contains(out, "kafscale_consumer_lag_max 30.000000") {
		t.Fatalf("expected max lag output, got:\n%s", out)
	}
	if !strings.Contains(out, `kafscale_consumer_lag_bucket{le="10"}`) {
		t.Fatalf("expected bucket output, got:\n%s", out)
	}
}
