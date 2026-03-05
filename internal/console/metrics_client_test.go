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
	"net/url"
	"testing"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

func TestFetchPromSnapshotParsesS3ErrorRate(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`
kafscale_s3_error_rate 0.25
kafscale_s3_latency_ms_avg 42
kafscale_produce_rps 100
kafscale_broker_cpu_percent 12.5
kafscale_broker_mem_alloc_bytes 1048576
`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	snap, err := fetchPromSnapshot(context.Background(), server.Client(), server.URL)
	if err != nil {
		t.Fatalf("fetchPromSnapshot: %v", err)
	}
	if snap.S3ErrorRate != 0.25 {
		t.Fatalf("expected error rate 0.25 got %f", snap.S3ErrorRate)
	}
	if snap.BrokerCPUPercent != 12.5 {
		t.Fatalf("expected cpu percent 12.5 got %f", snap.BrokerCPUPercent)
	}
	if snap.BrokerMemBytes != 1048576 {
		t.Fatalf("expected mem bytes 1048576 got %d", snap.BrokerMemBytes)
	}
}

func TestParsePromSample(t *testing.T) {
	val, ok := parsePromSample("kafscale_produce_rps 123.5")
	if !ok || val != 123.5 {
		t.Fatalf("unexpected parse result: %v %v", val, ok)
	}
	if _, ok := parsePromSample("kafscale_produce_rps not-a-number"); ok {
		t.Fatalf("expected parse failure")
	}
}

func TestParseStateLabel(t *testing.T) {
	state, ok := parseStateLabel(`kafscale_s3_health_state{state="degraded"} 1`)
	if !ok || state != "degraded" {
		t.Fatalf("unexpected state parse: %q %v", state, ok)
	}
	if _, ok := parseStateLabel("kafscale_s3_health_state 1"); ok {
		t.Fatalf("expected missing label to fail")
	}
}

func TestAggregatedMetricsFallback(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("kafscale_produce_rps 22\n"))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	client := NewAggregatedPromMetricsClient(store, server.URL)
	snap, err := client.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.ProduceRPS != 22 {
		t.Fatalf("expected produce rps 22 got %f", snap.ProduceRPS)
	}
}

func TestAggregatedMetricsSingleBroker(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`
kafscale_s3_health_state{state="degraded"} 1
kafscale_s3_latency_ms_avg 30
kafscale_s3_error_rate 0.4
kafscale_produce_rps 5
kafscale_fetch_rps 7
`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: parsed.Host},
		},
	})
	client := NewAggregatedPromMetricsClient(store, server.URL+"/metrics")
	snap, err := client.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.S3State != "degraded" {
		t.Fatalf("expected degraded state got %q", snap.S3State)
	}
	if snap.S3ErrorRate != 0.4 {
		t.Fatalf("expected error rate 0.4 got %f", snap.S3ErrorRate)
	}
	if snap.ProduceRPS != 5 || snap.FetchRPS != 7 {
		t.Fatalf("unexpected rps: %f %f", snap.ProduceRPS, snap.FetchRPS)
	}
}

func TestNewPromMetricsClient(t *testing.T) {
	provider := NewPromMetricsClient("http://localhost:9093/metrics")
	if provider == nil {
		t.Fatal("expected non-nil provider")
	}
}

func TestPickMemBytes(t *testing.T) {
	if got := pickMemBytes(1000, 500); got != 1000 {
		t.Fatalf("expected heap bytes 1000, got %d", got)
	}
	if got := pickMemBytes(0, 500); got != 500 {
		t.Fatalf("expected alloc bytes 500, got %d", got)
	}
	if got := pickMemBytes(0, 0); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestNewCompositeMetricsProvider(t *testing.T) {
	provider := NewCompositeMetricsProvider(nil, "")
	if provider == nil {
		t.Fatal("expected non-nil provider")
	}
}

func TestCompositeMetricsProviderSnapshotNoBroker(t *testing.T) {
	provider := NewCompositeMetricsProvider(nil, "")
	snap, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap == nil {
		t.Fatal("expected non-nil snapshot")
	}
}

type mockBrokerMetrics struct {
	snap *MetricsSnapshot
	err  error
}

func (m *mockBrokerMetrics) Snapshot(_ context.Context) (*MetricsSnapshot, error) {
	return m.snap, m.err
}

func TestCompositeMetricsProviderWithBroker(t *testing.T) {
	broker := &mockBrokerMetrics{
		snap: &MetricsSnapshot{
			ProduceRPS: 100,
			FetchRPS:   200,
		},
	}
	provider := NewCompositeMetricsProvider(broker, "")
	snap, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.ProduceRPS != 100 || snap.FetchRPS != 200 {
		t.Fatalf("unexpected rps: %f %f", snap.ProduceRPS, snap.FetchRPS)
	}
}

func TestCompositeMetricsProviderWithOperator(t *testing.T) {
	opHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`
kafscale_operator_clusters 2
kafscale_operator_etcd_snapshot_age_seconds 60
kafscale_operator_etcd_snapshot_access_ok 1
`))
	})
	opServer := httptest.NewServer(opHandler)
	defer opServer.Close()

	provider := NewCompositeMetricsProvider(nil, opServer.URL)
	snap, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !snap.OperatorMetricsAvailable {
		t.Fatal("expected operator metrics available")
	}
	if snap.OperatorClusters != 2 {
		t.Fatalf("expected 2 clusters, got %f", snap.OperatorClusters)
	}
	if snap.OperatorEtcdSnapshotAgeSeconds != 60 {
		t.Fatalf("expected age 60, got %f", snap.OperatorEtcdSnapshotAgeSeconds)
	}
}

func TestFetchPromSnapshotWithAdminMetrics(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`
kafscale_admin_requests_total{handler="metadata"} 10
kafscale_admin_requests_total{handler="produce"} 20
kafscale_admin_request_errors_total{handler="metadata"} 1
kafscale_admin_request_latency_ms_avg{handler="metadata"} 5
kafscale_admin_request_latency_ms_avg{handler="produce"} 15
kafscale_fetch_rps 300
kafscale_broker_heap_inuse_bytes 2097152
kafscale_broker_mem_alloc_bytes 1048576
`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	snap, err := fetchPromSnapshot(context.Background(), server.Client(), server.URL)
	if err != nil {
		t.Fatalf("fetchPromSnapshot: %v", err)
	}
	if snap.AdminRequestsTotal != 30 {
		t.Fatalf("expected admin total 30, got %f", snap.AdminRequestsTotal)
	}
	if snap.AdminRequestErrorsTotal != 1 {
		t.Fatalf("expected admin errors 1, got %f", snap.AdminRequestErrorsTotal)
	}
	if snap.AdminRequestLatencyMS != 10 {
		t.Fatalf("expected admin latency avg 10, got %f", snap.AdminRequestLatencyMS)
	}
	if snap.FetchRPS != 300 {
		t.Fatalf("expected fetch rps 300, got %f", snap.FetchRPS)
	}
	// heapBytes > 0 should be preferred
	if snap.BrokerMemBytes != 2097152 {
		t.Fatalf("expected heap bytes 2097152, got %d", snap.BrokerMemBytes)
	}
}

func TestFetchPromSnapshotHTTPError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	_, err := fetchPromSnapshot(context.Background(), server.Client(), server.URL)
	if err == nil {
		t.Fatal("expected error for non-200 response")
	}
}

func TestFetchOperatorSnapshotHTTPError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	_, err := fetchOperatorSnapshot(context.Background(), server.Client(), server.URL)
	if err == nil {
		t.Fatal("expected error for non-200 response")
	}
}

func TestParsePromSampleEdgeCases(t *testing.T) {
	// Empty line
	_, ok := parsePromSample("")
	if ok {
		t.Fatal("expected false for empty line")
	}
	// Comment
	_, ok = parsePromSample("# HELP some metric")
	if ok {
		t.Fatal("expected false for comment")
	}
}

func TestAggregatedNoBrokersNoFallback(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	client := NewAggregatedPromMetricsClient(store, "")
	_, err := client.Snapshot(context.Background())
	if err == nil {
		t.Fatal("expected error with no brokers and no fallback")
	}
}

func TestAggregatedAllBrokersDown(t *testing.T) {
	// Server that is not reachable (use a closed server)
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	server := httptest.NewServer(handler)
	parsedURL, _ := url.Parse(server.URL)
	server.Close() // close immediately so connections fail

	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 0, Host: parsedURL.Host},
		},
	})
	client := NewAggregatedPromMetricsClient(store, "")
	_, err := client.Snapshot(context.Background())
	if err == nil {
		t.Fatal("expected error when all brokers are down")
	}
}

func TestNewAggregatedPromMetricsClientURLParsing(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	// Custom scheme and port
	client := NewAggregatedPromMetricsClient(store, "https://broker:9999/custom_metrics")
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestFetchOperatorSnapshot(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`
kafscale_operator_clusters 1
kafscale_operator_etcd_snapshot_age_seconds{cluster="default/kafscale"} 120
kafscale_operator_etcd_snapshot_last_success_timestamp{cluster="default/kafscale"} 1700000000
kafscale_operator_etcd_snapshot_last_schedule_timestamp{cluster="default/kafscale"} 1700001000
kafscale_operator_etcd_snapshot_stale{cluster="default/kafscale"} 0
kafscale_operator_etcd_snapshot_access_ok{cluster="default/kafscale"} 1
`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	snap, err := fetchOperatorSnapshot(context.Background(), server.Client(), server.URL)
	if err != nil {
		t.Fatalf("fetchOperatorSnapshot: %v", err)
	}
	if snap.EtcdSnapshotAgeSeconds != 120 {
		t.Fatalf("expected age 120 got %f", snap.EtcdSnapshotAgeSeconds)
	}
	if snap.EtcdSnapshotAccessOK != 1 {
		t.Fatalf("expected access ok 1 got %f", snap.EtcdSnapshotAccessOK)
	}
}
