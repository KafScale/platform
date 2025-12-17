package broker

import (
	"sync"
	"time"
)

// S3HealthState models the broker's view of S3 availability.
type S3HealthState string

const (
	S3StateHealthy     S3HealthState = "healthy"
	S3StateDegraded    S3HealthState = "degraded"
	S3StateUnavailable S3HealthState = "unavailable"
)

// S3HealthConfig defines thresholds for transitioning between states.
type S3HealthConfig struct {
	Window      time.Duration
	LatencyWarn time.Duration
	LatencyCrit time.Duration
	ErrorWarn   float64
	ErrorCrit   float64
	MaxSamples  int
}

// S3HealthMonitor aggregates recent S3 requests to determine a health state.
type S3HealthMonitor struct {
	cfg S3HealthConfig

	mu         sync.Mutex
	samples    []s3Sample
	state      S3HealthState
	stateSince time.Time
	avgLatency time.Duration
	errorRate  float64
}

type s3Sample struct {
	ts      time.Time
	op      string
	latency time.Duration
	err     bool
}

// S3HealthSnapshot captures the monitor's public metrics.
type S3HealthSnapshot struct {
	State      S3HealthState
	Since      time.Time
	AvgLatency time.Duration
	ErrorRate  float64
}

// NewS3HealthMonitor builds a health monitor with sane defaults.
func NewS3HealthMonitor(cfg S3HealthConfig) *S3HealthMonitor {
	if cfg.Window <= 0 {
		cfg.Window = time.Minute
	}
	if cfg.LatencyWarn <= 0 {
		cfg.LatencyWarn = 500 * time.Millisecond
	}
	if cfg.LatencyCrit <= 0 {
		cfg.LatencyCrit = 3 * time.Second
	}
	if cfg.ErrorWarn <= 0 {
		cfg.ErrorWarn = 0.2
	}
	if cfg.ErrorCrit <= 0 {
		cfg.ErrorCrit = 0.6
	}
	if cfg.MaxSamples <= 0 {
		cfg.MaxSamples = 512
	}
	now := time.Now()
	return &S3HealthMonitor{
		cfg:        cfg,
		state:      S3StateHealthy,
		stateSince: now,
	}
}

// RecordUpload remains for backward compatibility.
func (m *S3HealthMonitor) RecordUpload(latency time.Duration, err error) {
	m.RecordOperation("upload", latency, err)
}

// RecordOperation records an arbitrary S3 operation outcome.
func (m *S3HealthMonitor) RecordOperation(op string, latency time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	m.samples = append(m.samples, s3Sample{
		ts:      now,
		op:      op,
		latency: latency,
		err:     err != nil,
	})
	if len(m.samples) > m.cfg.MaxSamples {
		m.samples = m.samples[len(m.samples)-m.cfg.MaxSamples:]
	}
	m.truncateLocked(now)
	m.recomputeLocked(now)
}

// Snapshot returns the current state and key aggregates.
func (m *S3HealthMonitor) Snapshot() S3HealthSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return S3HealthSnapshot{
		State:      m.state,
		Since:      m.stateSince,
		AvgLatency: m.avgLatency,
		ErrorRate:  m.errorRate,
	}
}

// State returns just the current health state.
func (m *S3HealthMonitor) State() S3HealthState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

func (m *S3HealthMonitor) truncateLocked(now time.Time) {
	cutoff := now.Add(-m.cfg.Window)
	idx := 0
	for _, sample := range m.samples {
		if sample.ts.After(cutoff) {
			break
		}
		idx++
	}
	if idx > 0 && idx < len(m.samples) {
		m.samples = append([]s3Sample(nil), m.samples[idx:]...)
	} else if idx >= len(m.samples) {
		m.samples = nil
	}
}

func (m *S3HealthMonitor) recomputeLocked(now time.Time) {
	if len(m.samples) == 0 {
		m.avgLatency = 0
		m.errorRate = 0
		m.setStateLocked(now, S3StateHealthy)
		return
	}
	var (
		totalLatency time.Duration
		errorCount   int
	)
	for _, sample := range m.samples {
		totalLatency += sample.latency
		if sample.err {
			errorCount++
		}
	}
	m.avgLatency = totalLatency / time.Duration(len(m.samples))
	m.errorRate = float64(errorCount) / float64(len(m.samples))

	nextState := S3StateHealthy
	if m.avgLatency >= m.cfg.LatencyCrit || m.errorRate >= m.cfg.ErrorCrit {
		nextState = S3StateUnavailable
	} else if m.avgLatency >= m.cfg.LatencyWarn || m.errorRate >= m.cfg.ErrorWarn {
		nextState = S3StateDegraded
	}
	m.setStateLocked(now, nextState)
}

func (m *S3HealthMonitor) setStateLocked(now time.Time, next S3HealthState) {
	if next == m.state {
		return
	}
	m.state = next
	m.stateSince = now
}
