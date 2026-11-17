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
	"sync/atomic"
	"testing"
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

	provider := NewCompositeMetricsProvider(nil, server.URL)
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
