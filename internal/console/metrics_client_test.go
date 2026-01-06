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

	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/protocol"
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
