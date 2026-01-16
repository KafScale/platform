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
	"testing"
	"time"
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
	if got := buildMetricsProvider(); got != nil {
		t.Fatalf("expected nil metrics provider when env unset")
	}

	t.Setenv("KAFSCALE_MCP_BROKER_METRICS_URL", "http://127.0.0.1:9093/metrics")
	if got := buildMetricsProvider(); got == nil {
		t.Fatalf("expected metrics provider when env set")
	}
}
