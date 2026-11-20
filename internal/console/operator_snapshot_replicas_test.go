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

package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const leaderOperatorMetrics = `
kafscale_operator_clusters 1
kafscale_operator_etcd_snapshot_age_seconds{cluster="default/kafscale"} 120
kafscale_operator_etcd_snapshot_last_success_timestamp{cluster="default/kafscale"} 1700000000
kafscale_operator_etcd_snapshot_last_schedule_timestamp{cluster="default/kafscale"} 1700001000
kafscale_operator_etcd_snapshot_stale{cluster="default/kafscale"} 0
kafscale_operator_etcd_snapshot_access_ok{cluster="default/kafscale"} 1
`

const standbyOperatorMetrics = `
# HELP kafscale_operator_clusters Number of managed KafscaleCluster resources.
# TYPE kafscale_operator_clusters gauge
kafscale_operator_clusters 0
`

func TestOperatorSnapshotStandbyScrapeIsNotAccessFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(standbyOperatorMetrics))
	}))
	defer server.Close()

	snap, err := NewCompositeMetricsProvider(nil, server.URL).Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.OperatorMetricsAvailable {
		t.Fatalf("scrape without snapshot gauges must be unknown, got available with access_ok=%v age=%v", snap.OperatorEtcdSnapshotAccessOK, snap.OperatorEtcdSnapshotAgeSeconds)
	}
}

func TestOperatorSnapshotKeepsLastKnownValuesAcrossStandbyScrape(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1)%2 == 1 {
			_, _ = w.Write([]byte(leaderOperatorMetrics))
			return
		}
		_, _ = w.Write([]byte(standbyOperatorMetrics))
	}))
	defer server.Close()

	provider := NewCompositeMetricsProvider(nil, server.URL).(*compositeMetricsProvider)
	fixed := time.Unix(1700001100, 0)
	provider.now = func() time.Time { return fixed }
	for i := 0; i < 4; i++ {
		snap, err := provider.Snapshot(context.Background())
		if err != nil {
			t.Fatalf("Snapshot %d: %v", i, err)
		}
		if !snap.OperatorMetricsAvailable {
			t.Fatalf("scrape %d: expected last known snapshot values", i)
		}
		if snap.OperatorEtcdSnapshotAccessOK != 1 {
			t.Fatalf("scrape %d: access_ok=%v, want 1", i, snap.OperatorEtcdSnapshotAccessOK)
		}
		if snap.OperatorEtcdSnapshotAgeSeconds != 120 || snap.OperatorEtcdSnapshotLastSuccessTS != 1700000000 || snap.OperatorEtcdSnapshotLastScheduleTS != 1700001000 {
			t.Fatalf("scrape %d: got age=%v last_success=%v last_schedule=%v", i, snap.OperatorEtcdSnapshotAgeSeconds, snap.OperatorEtcdSnapshotLastSuccessTS, snap.OperatorEtcdSnapshotLastScheduleTS)
		}
		if snap.OperatorClusters != 1 {
			t.Fatalf("scrape %d: clusters=%v, want 1", i, snap.OperatorClusters)
		}
	}
}

func TestOperatorSnapshotReportsRealAccessFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`
kafscale_operator_clusters 1
kafscale_operator_etcd_snapshot_age_seconds{cluster="default/kafscale"} 0
kafscale_operator_etcd_snapshot_access_ok{cluster="default/kafscale"} 0
`))
	}))
	defer server.Close()

	snap, err := NewCompositeMetricsProvider(nil, server.URL).Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !snap.OperatorMetricsAvailable || snap.OperatorEtcdSnapshotAccessOK != 0 {
		t.Fatalf("expected reported access failure, got available=%v access_ok=%v", snap.OperatorMetricsAvailable, snap.OperatorEtcdSnapshotAccessOK)
	}
}

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type switchingOperator struct {
	mu   sync.Mutex
	body string
}

func (s *switchingOperator) set(body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.body = body
}

func (s *switchingOperator) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	body := s.body
	s.mu.Unlock()
	_, _ = w.Write([]byte(body))
}

const leaderWithThresholdMetrics = `
kafscale_operator_clusters 1
kafscale_operator_etcd_snapshot_age_seconds{cluster="default/kafscale"} 120
kafscale_operator_etcd_snapshot_last_success_timestamp{cluster="default/kafscale"} 1700000000
kafscale_operator_etcd_snapshot_last_schedule_timestamp{cluster="default/kafscale"} 1700001000
kafscale_operator_etcd_snapshot_stale{cluster="default/kafscale"} 0
kafscale_operator_etcd_snapshot_access_ok{cluster="default/kafscale"} 1
kafscale_operator_etcd_snapshot_threshold_seconds 150
`

const standbyWithThresholdMetrics = `
kafscale_operator_clusters 0
kafscale_operator_etcd_snapshot_threshold_seconds 150
`

func newClockedOperatorProvider(t *testing.T, op *switchingOperator) (*compositeMetricsProvider, *testClock) {
	t.Helper()
	server := httptest.NewServer(op)
	t.Cleanup(server.Close)
	provider := NewCompositeMetricsProvider(nil, server.URL).(*compositeMetricsProvider)
	clock := &testClock{now: time.Unix(1700001100, 0)}
	provider.now = clock.Now
	return provider, clock
}

func streamedMetrics(t *testing.T, provider MetricsProvider) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w := &cancelOnFlushRecorder{ResponseRecorder: httptest.NewRecorder(), cancel: cancel}
	h := &consoleHandlers{metrics: provider}
	h.handleMetrics(w, httptest.NewRequest(http.MethodGet, "/ui/api/metrics", nil).WithContext(ctx))
	return w.Body.String()
}

func TestOperatorSnapshotWithoutAccessGaugeShowsAccessUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`
kafscale_operator_clusters 1
kafscale_operator_etcd_snapshot_age_seconds{cluster="default/kafscale"} 120
kafscale_operator_etcd_snapshot_last_success_timestamp{cluster="default/kafscale"} 1700000000
kafscale_operator_etcd_snapshot_stale{cluster="default/kafscale"} 0
`))
	}))
	defer server.Close()

	body := streamedMetrics(t, NewCompositeMetricsProvider(nil, server.URL))
	if !strings.Contains(body, `"etcd_snapshot_age_s":120`) {
		t.Fatalf("expected snapshot age in stream: %s", body)
	}
	if strings.Contains(body, "etcd_snapshot_access_ok") {
		t.Fatalf("access_ok gauge is absent, so access must be unknown: %s", body)
	}
	if strings.Contains(body, "produce_rps") || !strings.Contains(body, `"metrics_available":true`) {
		t.Fatalf("operator-only scrape must not invent broker throughput: %s", body)
	}
}

func TestOperatorSnapshotCachedAgeAdvancesAndTurnsStale(t *testing.T) {
	op := &switchingOperator{body: leaderWithThresholdMetrics}
	provider, clock := newClockedOperatorProvider(t, op)
	if _, err := provider.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	op.set(standbyWithThresholdMetrics)

	clock.Advance(10 * time.Second)
	snap, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snap.OperatorMetricsAvailable || snap.OperatorEtcdSnapshotAgeSeconds != 130 || snap.OperatorEtcdSnapshotStale != 0 {
		t.Fatalf("after 10s: available=%v age=%v stale=%v, want age 130 and not stale", snap.OperatorMetricsAvailable, snap.OperatorEtcdSnapshotAgeSeconds, snap.OperatorEtcdSnapshotStale)
	}

	clock.Advance(50 * time.Second)
	snap, err = provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snap.OperatorMetricsAvailable || snap.OperatorEtcdSnapshotAgeSeconds != 180 || snap.OperatorEtcdSnapshotStale != 1 {
		t.Fatalf("after 60s: available=%v age=%v stale=%v, want age 180 and stale past the 150s threshold", snap.OperatorMetricsAvailable, snap.OperatorEtcdSnapshotAgeSeconds, snap.OperatorEtcdSnapshotStale)
	}
	if snap.OperatorEtcdSnapshotLastSuccessTS != 1700000000 || snap.OperatorEtcdSnapshotAccessOK != 1 {
		t.Fatalf("cached last_success=%v access_ok=%v", snap.OperatorEtcdSnapshotLastSuccessTS, snap.OperatorEtcdSnapshotAccessOK)
	}
}

func TestOperatorSnapshotCacheExpires(t *testing.T) {
	op := &switchingOperator{body: leaderWithThresholdMetrics}
	provider, clock := newClockedOperatorProvider(t, op)
	if _, err := provider.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	op.set(standbyWithThresholdMetrics)

	clock.Advance(3*time.Minute - time.Second)
	snap, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snap.OperatorMetricsAvailable {
		t.Fatal("cached snapshot should still be used just before it expires")
	}

	clock.Advance(2 * time.Second)
	snap, err = provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.OperatorMetricsAvailable {
		t.Fatalf("cached snapshot older than 3 minutes must not be shown, got age=%v", snap.OperatorEtcdSnapshotAgeSeconds)
	}
	if body := streamedMetrics(t, provider); strings.Contains(body, "etcd_snapshot_") {
		t.Fatalf("expired cache must leave the panel at n/a: %s", body)
	}
}
