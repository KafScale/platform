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
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/mcpserver"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

func TestEnvOrDefault(t *testing.T) {
	t.Setenv("KAFSCALE_MCP_HTTP_ADDR", "127.0.0.1:9999")
	if got := envOrDefault("KAFSCALE_MCP_HTTP_ADDR", ":8090"); got != "127.0.0.1:9999" {
		t.Fatalf("expected env override, got %q", got)
	}
}

func TestParseDurationEnv(t *testing.T) {
	t.Setenv("KAFSCALE_MCP_SESSION_TIMEOUT", "3s")
	dur, err := parseDurationEnv("KAFSCALE_MCP_SESSION_TIMEOUT")
	if err != nil {
		t.Fatalf("parse duration: %v", err)
	}
	if dur != 3*time.Second {
		t.Fatalf("expected 3s, got %s", dur)
	}

	t.Setenv("KAFSCALE_MCP_SESSION_TIMEOUT", "nope")
	if _, err := parseDurationEnv("KAFSCALE_MCP_SESSION_TIMEOUT"); err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestMcpEtcdConfigFromEnv(t *testing.T) {
	t.Setenv("KAFSCALE_MCP_ETCD_ENDPOINTS", "http://a:2379,http://b:2379")
	t.Setenv("KAFSCALE_MCP_ETCD_USERNAME", "user")
	t.Setenv("KAFSCALE_MCP_ETCD_PASSWORD", "pass")

	cfg, ok := mcpEtcdConfigFromEnv()
	if !ok {
		t.Fatalf("expected config to be enabled")
	}
	if len(cfg.Endpoints) != 2 || cfg.Endpoints[0] != "http://a:2379" || cfg.Endpoints[1] != "http://b:2379" {
		t.Fatalf("unexpected endpoints: %v", cfg.Endpoints)
	}
	if cfg.Username != "user" || cfg.Password != "pass" {
		t.Fatalf("unexpected credentials: %+v", cfg)
	}
}

func TestBuildMetricsProvider(t *testing.T) {
	t.Setenv("KAFSCALE_MCP_BROKER_METRICS_URL", "")
	if got := buildMetricsProvider(nil); got != nil {
		t.Fatalf("expected nil metrics provider when env unset")
	}

	t.Setenv("KAFSCALE_MCP_BROKER_METRICS_URL", "http://127.0.0.1:9093/metrics")
	if got := buildMetricsProvider(nil); got == nil {
		t.Fatalf("expected metrics provider when env set")
	}
}

func TestBuildMetricsProviderAggregatesEveryBroker(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("kafscale_produce_rps 5\nkafscale_fetch_rps 7\n"))
		if requests.Add(1) == 1 {
			_, _ = w.Write([]byte("kafscale_s3_latency_ms_avg 0\nkafscale_s3_error_rate 0\nkafscale_admin_request_latency_ms_avg 0\n"))
		} else {
			_, _ = w.Write([]byte("kafscale_s3_latency_ms_avg 40\nkafscale_s3_error_rate 0.4\nkafscale_admin_request_latency_ms_avg 10\n"))
		}
	}))
	defer server.Close()
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 0, Host: parsed.Hostname(), Port: 9092},
			{NodeID: 1, Host: parsed.Hostname() + ":9092", Port: 9092},
		},
	})
	t.Setenv("KAFSCALE_MCP_BROKER_METRICS_URL", "http://unused.invalid:"+parsed.Port()+"/metrics")
	provider := buildMetricsProvider(store)
	if provider == nil {
		t.Fatal("expected metrics provider")
	}
	snap, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.ProduceRPS != 10 || snap.FetchRPS != 14 {
		t.Fatalf("expected the sum over both brokers, got produce %v fetch %v", snap.ProduceRPS, snap.FetchRPS)
	}
	if snap.S3LatencyMS != 20 || snap.S3ErrorRate != 0.2 || snap.AdminRequestLatencyMS != 5 {
		t.Fatalf("zero-valued broker omitted from averages: %+v", snap)
	}
}

func TestBuildMetricsProviderFallsBackToURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("kafscale_produce_rps 3\n"))
	}))
	defer server.Close()
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	t.Setenv("KAFSCALE_MCP_BROKER_METRICS_URL", server.URL+"/metrics")
	snap, err := buildMetricsProvider(store).Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.ProduceRPS != 3 {
		t.Fatalf("expected the URL fallback, got produce %v", snap.ProduceRPS)
	}
}

type readinessTestStore struct {
	metadata.Store
	err error
}

func (s readinessTestStore) CheckReadiness(context.Context) error { return s.err }

func TestMCPProbeRoutes(t *testing.T) {
	handler := mcpserver.RequireBearerToken("test-token", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	for _, failure := range []error{nil, errors.New("etcd unavailable")} {
		mux := newHTTPMux(readinessTestStore{err: failure}, handler)
		for _, path := range []string{"/healthz", "/readyz", "/mcp", "/mcp/"} {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			want := http.StatusOK
			if path == "/readyz" && failure != nil {
				want = http.StatusServiceUnavailable
			}
			if path == "/mcp" || path == "/mcp/" {
				want = http.StatusUnauthorized
			}
			if rec.Code != want {
				t.Fatalf("%s status=%d, want %d", path, rec.Code, want)
			}
		}
		for _, path := range []string{"/mcp", "/mcp/"} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Header.Set("Authorization", "Bearer test-token")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusNoContent {
				t.Fatal(path, rec.Code)
			}
		}
	}
}
