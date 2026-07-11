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

package broker

import (
	"errors"
	"testing"
	"time"
)

func TestS3HealthStateTransitions(t *testing.T) {
	monitor := NewS3HealthMonitor(S3HealthConfig{
		Window:      time.Second,
		LatencyWarn: time.Millisecond,
		LatencyCrit: time.Hour,
		ErrorWarn:   0.5,
		ErrorCrit:   0.8,
		MaxSamples:  64,
	})

	if got := monitor.State(); got != S3StateHealthy {
		t.Fatalf("expected initial state healthy got %s", got)
	}

	monitor.RecordOperation("upload", 2*time.Millisecond, nil)
	if got := monitor.State(); got != S3StateDegraded {
		t.Fatalf("expected degraded after high latency got %s", got)
	}

	for i := 0; i < 10; i++ {
		monitor.RecordOperation("upload", 100*time.Microsecond, errors.New("boom"))
	}
	if got := monitor.State(); got != S3StateUnavailable {
		t.Fatalf("expected unavailable after repeated errors got %s", got)
	}

	// Recover with several healthy uploads.
	for i := 0; i < 20; i++ {
		monitor.RecordUpload(100*time.Microsecond, nil)
	}
	time.Sleep(10 * time.Millisecond)
	monitor.RecordOperation("download", 100*time.Microsecond, nil)
	if got := monitor.State(); got != S3StateHealthy {
		t.Fatalf("expected healthy after recovery got %s", got)
	}
}

func TestS3HealthSnapshot(t *testing.T) {
	monitor := NewS3HealthMonitor(S3HealthConfig{
		Window:      time.Minute,
		LatencyWarn: 100 * time.Millisecond,
		LatencyCrit: time.Second,
		ErrorWarn:   0.3,
		ErrorCrit:   0.7,
		MaxSamples:  64,
	})

	monitor.RecordOperation("upload", 10*time.Millisecond, nil)
	snap := monitor.Snapshot()
	if snap.State != S3StateHealthy {
		t.Fatalf("expected healthy state, got %s", snap.State)
	}
	if snap.Since.IsZero() {
		t.Fatal("expected non-zero Since")
	}
	if snap.AvgLatency == 0 {
		t.Fatal("expected non-zero avg latency")
	}
	if snap.ErrorRate != 0 {
		t.Fatalf("expected 0 error rate, got %f", snap.ErrorRate)
	}
}

func TestS3HealthMonitorDefaults(t *testing.T) {
	// All zero config → should use defaults
	monitor := NewS3HealthMonitor(S3HealthConfig{})
	if monitor.State() != S3StateHealthy {
		t.Fatalf("expected healthy initial state")
	}
	// Record a few operations to ensure it works with defaults
	monitor.RecordOperation("upload", time.Millisecond, nil)
	snap := monitor.Snapshot()
	if snap.State != S3StateHealthy {
		t.Fatalf("expected healthy after normal ops")
	}
}

func TestS3HealthTruncation(t *testing.T) {
	monitor := NewS3HealthMonitor(S3HealthConfig{
		Window:     100 * time.Millisecond,
		MaxSamples: 4,
	})

	// Record several operations
	for i := 0; i < 10; i++ {
		monitor.RecordOperation("upload", time.Millisecond, nil)
	}
	// After truncation, max samples should be honored
	snap := monitor.Snapshot()
	if snap.State != S3StateHealthy {
		t.Fatalf("expected healthy, got %s", snap.State)
	}
}
