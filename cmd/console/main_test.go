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
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KafScale/platform/v2/internal/testutil"
	"github.com/KafScale/platform/v2/pkg/metadata"
)

func TestBuildMetadataStoreUsesEtcdEnv(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)

	t.Setenv("KAFSCALE_CONSOLE_ETCD_ENDPOINTS", strings.Join(endpoints, ","))
	t.Setenv("KAFSCALE_CONSOLE_ETCD_USERNAME", "user")
	t.Setenv("KAFSCALE_CONSOLE_ETCD_PASSWORD", "pass")

	store, err := buildMetadataStore(context.Background())
	if err != nil {
		t.Fatalf("build metadata store: %v", err)
	}
	if etcdStore, ok := store.(*metadata.EtcdStore); ok {
		defer etcdStore.Close()
	} else {
		t.Fatalf("expected etcd store when KAFSCALE_CONSOLE_ETCD_ENDPOINTS is set")
	}
}

func TestBuildMetricsProvider(t *testing.T) {
	t.Setenv("KAFSCALE_CONSOLE_OPERATOR_METRICS_DISCOVERY_URL", "")
	t.Setenv("KAFSCALE_CONSOLE_BROKER_METRICS_URL", "")
	t.Setenv("KAFSCALE_CONSOLE_OPERATOR_METRICS_URL", "")
	if got := buildMetricsProvider(nil); got != nil {
		t.Fatalf("expected nil metrics provider when env unset")
	}

	t.Setenv("KAFSCALE_CONSOLE_BROKER_METRICS_URL", "http://127.0.0.1:9093/metrics")
	if got := buildMetricsProvider(nil); got == nil {
		t.Fatalf("expected metrics provider when env set")
	}

	t.Setenv("KAFSCALE_CONSOLE_BROKER_METRICS_URL", "")
	t.Setenv("KAFSCALE_CONSOLE_OPERATOR_METRICS_URL", "http://127.0.0.1:8080/metrics")
	if got := buildMetricsProvider(nil); got == nil {
		t.Fatalf("expected metrics provider when operator metrics env set")
	}
}

func TestBuildMetricsProviderWithOperatorDiscovery(t *testing.T) {
	t.Setenv("KAFSCALE_CONSOLE_BROKER_METRICS_URL", "")
	t.Setenv("KAFSCALE_CONSOLE_OPERATOR_METRICS_URL", "")
	t.Setenv("KAFSCALE_CONSOLE_OPERATOR_METRICS_DISCOVERY_URL", "http://operators.test:8080/metrics")
	if got := buildMetricsProvider(nil); got == nil {
		t.Fatal("discovery-only provider must be enabled")
	}
}

func TestBuildMetricsProviderDirectOverridePrecedesDiscovery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "kafscale_operator_etcd_snapshot_age_seconds 42\n")
	}))
	defer server.Close()
	t.Setenv("KAFSCALE_CONSOLE_BROKER_METRICS_URL", "")
	t.Setenv("KAFSCALE_CONSOLE_OPERATOR_METRICS_URL", "")
	t.Setenv("KAFSCALE_CONSOLE_OPERATOR_METRICS_DISCOVERY_URL", server.URL)
	for _, direct := range []bool{false, true} {
		if direct {
			t.Setenv("KAFSCALE_CONSOLE_OPERATOR_METRICS_URL", server.URL)
			t.Setenv("KAFSCALE_CONSOLE_OPERATOR_METRICS_DISCOVERY_URL", "http://invalid.invalid/metrics")
		}
		snap, err := buildMetricsProvider(nil).Snapshot(context.Background())
		if err != nil || !snap.OperatorMetricsAvailable || snap.OperatorEtcdSnapshotAgeSeconds != 42 {
			t.Fatalf("direct=%t snapshot=%+v err=%v", direct, snap, err)
		}
	}
}

func TestConsoleEtcdConfigFromEnv(t *testing.T) {
	t.Setenv("KAFSCALE_CONSOLE_ETCD_ENDPOINTS", "http://a:2379,http://b:2379")
	t.Setenv("KAFSCALE_CONSOLE_ETCD_USERNAME", "user")
	t.Setenv("KAFSCALE_CONSOLE_ETCD_PASSWORD", "pass")
	cfg, ok := consoleEtcdConfigFromEnv()
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

func TestConsoleEtcdConfigStorageNamespace(t *testing.T) {
	t.Setenv("KAFSCALE_CONSOLE_ETCD_ENDPOINTS", "http://a:2379")
	t.Setenv("KAFSCALE_S3_NAMESPACE", " team-a ")
	cfg, ok := consoleEtcdConfigFromEnv()
	if !ok || cfg.StorageNamespace != "team-a" {
		t.Fatalf("StorageNamespace = %q (enabled %v), want team-a", cfg.StorageNamespace, ok)
	}
}

func TestAuthConfigFromEnv(t *testing.T) {
	t.Setenv("KAFSCALE_UI_USERNAME", " user ")
	t.Setenv("KAFSCALE_UI_PASSWORD", " pass ")
	cfg := authConfigFromEnv()
	if cfg.Username != "user" || cfg.Password != "pass" {
		t.Fatalf("unexpected auth config: %+v", cfg)
	}
}

func TestEnvOrDefaultConsoleAddr(t *testing.T) {
	t.Setenv("KAFSCALE_CONSOLE_HTTP_ADDR", "127.0.0.1:9090")
	if got := envOrDefault("KAFSCALE_CONSOLE_HTTP_ADDR", ":8080"); got != "127.0.0.1:9090" {
		t.Fatalf("expected env override, got %q", got)
	}
}
