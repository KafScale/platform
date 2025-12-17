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
