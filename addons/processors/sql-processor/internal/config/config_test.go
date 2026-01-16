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

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\n")
	path := writeTempConfig(t, data)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Server.Listen == "" || cfg.Server.MetricsListen == "" {
		t.Fatalf("expected server defaults")
	}
	if cfg.Query.DefaultLimit == 0 || cfg.Query.MaxUnbounded == 0 {
		t.Fatalf("expected query defaults")
	}
	if cfg.Query.MaxScanBytes == 0 || cfg.Query.MaxScanSegments == 0 || cfg.Query.MaxRows == 0 || cfg.Query.TimeoutSeconds == 0 {
		t.Fatalf("expected query guardrail defaults")
	}
	if cfg.Query.MaxConcurrent == 0 || cfg.Query.QueueSize == 0 || cfg.Query.QueueTimeoutSec == 0 {
		t.Fatalf("expected query queue defaults")
	}
	if cfg.Metadata.Snapshot.Key == "" {
		t.Fatalf("expected snapshot key default")
	}
	if cfg.DiscoveryCache.TTLSeconds == 0 || cfg.DiscoveryCache.MaxEntries == 0 {
		t.Fatalf("expected discovery cache defaults")
	}
	if cfg.Manifest.Key == "" || cfg.Manifest.TTLSeconds == 0 {
		t.Fatalf("expected manifest defaults")
	}
	if cfg.Manifest.BuildLeaseTTLSeconds == 0 {
		t.Fatalf("expected manifest lease default")
	}
	if cfg.TimeIndex.KeySuffix == "" {
		t.Fatalf("expected time index defaults")
	}
	if cfg.TimeIndex.BuildLeaseTTLSeconds == 0 {
		t.Fatalf("expected time index lease default")
	}
	if cfg.ResultCache.TTLSeconds == 0 || cfg.ResultCache.MaxEntries == 0 || cfg.ResultCache.MaxRows == 0 {
		t.Fatalf("expected result cache defaults")
	}
	if cfg.Proxy.Listen == "" || cfg.Proxy.MaxConnections == 0 {
		t.Fatalf("expected proxy defaults")
	}
	if cfg.Proxy.CacheTTLSeconds == 0 || cfg.Proxy.CacheMaxEntries == 0 {
		t.Fatalf("expected proxy cache defaults")
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\n")
	path := writeTempConfig(t, data)
	t.Setenv("KAFSQL_SERVER_LISTEN", ":5555")
	t.Setenv("KAFSQL_QUERY_DEFAULT_LIMIT", "123")
	t.Setenv("KAFSQL_METADATA_SNAPSHOT_KEY", "/custom/snapshot")
	t.Setenv("KAFSQL_DISCOVERY_CACHE_TTL_SECONDS", "25")
	t.Setenv("KAFSQL_DISCOVERY_CACHE_MAX_ENTRIES", "500")
	t.Setenv("KAFSQL_MANIFEST_ENABLED", "true")
	t.Setenv("KAFSQL_MANIFEST_KEY", "manifests/segments.json")
	t.Setenv("KAFSQL_MANIFEST_TTL_SECONDS", "45")
	t.Setenv("KAFSQL_MANIFEST_BUILD_INTERVAL_SECONDS", "120")
	t.Setenv("KAFSQL_MANIFEST_BUILD_MAX_SEGMENTS", "55")
	t.Setenv("KAFSQL_MANIFEST_BUILD_MAX_BYTES", "999")
	t.Setenv("KAFSQL_MANIFEST_BUILD_LEASE_TTL_SECONDS", "200")
	t.Setenv("KAFSQL_TIME_INDEX_ENABLED", "true")
	t.Setenv("KAFSQL_TIME_INDEX_SUFFIX", ".timeindex")
	t.Setenv("KAFSQL_TIME_INDEX_BUILD_MAX_SEGMENTS", "7")
	t.Setenv("KAFSQL_TIME_INDEX_BUILD_MAX_BYTES", "1234")
	t.Setenv("KAFSQL_TIME_INDEX_BUILD_LEASE_TTL_SECONDS", "180")
	t.Setenv("KAFSQL_QUERY_MAX_SCAN_BYTES", "2048")
	t.Setenv("KAFSQL_QUERY_MAX_SCAN_SEGMENTS", "12")
	t.Setenv("KAFSQL_QUERY_MAX_ROWS", "345")
	t.Setenv("KAFSQL_QUERY_TIMEOUT_SECONDS", "17")
	t.Setenv("KAFSQL_QUERY_MAX_CONCURRENT", "7")
	t.Setenv("KAFSQL_QUERY_QUEUE_SIZE", "9")
	t.Setenv("KAFSQL_QUERY_QUEUE_TIMEOUT_SECONDS", "3")
	t.Setenv("KAFSQL_PROXY_LISTEN", ":6432")
	t.Setenv("KAFSQL_PROXY_UPSTREAMS", "kafsql-0:5432,kafsql-1:5432")
	t.Setenv("KAFSQL_PROXY_MAX_CONNECTIONS", "55")
	t.Setenv("KAFSQL_PROXY_CACHE_TTL_SECONDS", "15")
	t.Setenv("KAFSQL_PROXY_CACHE_MAX_ENTRIES", "500")
	t.Setenv("KAFSQL_PROXY_ACL_ALLOW", "orders,shipments-*")
	t.Setenv("KAFSQL_PROXY_ACL_DENY", "payments")
	t.Setenv("KAFSQL_RESULT_CACHE_TTL_SECONDS", "12")
	t.Setenv("KAFSQL_RESULT_CACHE_MAX_ENTRIES", "9")
	t.Setenv("KAFSQL_RESULT_CACHE_MAX_ROWS", "111")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Server.Listen != ":5555" {
		t.Fatalf("expected server listen override, got %q", cfg.Server.Listen)
	}
	if cfg.Query.DefaultLimit != 123 {
		t.Fatalf("expected default limit override, got %d", cfg.Query.DefaultLimit)
	}
	if cfg.Metadata.Snapshot.Key != "/custom/snapshot" {
		t.Fatalf("expected snapshot key override, got %q", cfg.Metadata.Snapshot.Key)
	}
	if cfg.DiscoveryCache.TTLSeconds != 25 || cfg.DiscoveryCache.MaxEntries != 500 {
		t.Fatalf("expected discovery cache overrides, got %+v", cfg.DiscoveryCache)
	}
	if !cfg.Manifest.Enabled || cfg.Manifest.Key != "manifests/segments.json" || cfg.Manifest.TTLSeconds != 45 {
		t.Fatalf("expected manifest overrides, got %+v", cfg.Manifest)
	}
	if cfg.Manifest.BuildIntervalSeconds != 120 || cfg.Manifest.BuildMaxSegments != 55 || cfg.Manifest.BuildMaxBytes != 999 {
		t.Fatalf("expected manifest build overrides, got %+v", cfg.Manifest)
	}
	if cfg.Manifest.BuildLeaseTTLSeconds != 200 {
		t.Fatalf("expected manifest lease override, got %+v", cfg.Manifest)
	}
	if !cfg.TimeIndex.Enabled || cfg.TimeIndex.KeySuffix != ".timeindex" {
		t.Fatalf("expected time index overrides, got %+v", cfg.TimeIndex)
	}
	if cfg.TimeIndex.BuildMaxSegments != 7 || cfg.TimeIndex.BuildMaxBytes != 1234 {
		t.Fatalf("expected time index build overrides, got %+v", cfg.TimeIndex)
	}
	if cfg.TimeIndex.BuildLeaseTTLSeconds != 180 {
		t.Fatalf("expected time index lease override, got %+v", cfg.TimeIndex)
	}
	if cfg.Query.MaxScanBytes != 2048 || cfg.Query.MaxScanSegments != 12 || cfg.Query.MaxRows != 345 || cfg.Query.TimeoutSeconds != 17 {
		t.Fatalf("expected query guardrail overrides, got %+v", cfg.Query)
	}
	if cfg.Query.MaxConcurrent != 7 || cfg.Query.QueueSize != 9 || cfg.Query.QueueTimeoutSec != 3 {
		t.Fatalf("expected query queue overrides, got %+v", cfg.Query)
	}
	if cfg.Proxy.Listen != ":6432" || cfg.Proxy.MaxConnections != 55 {
		t.Fatalf("expected proxy overrides, got %+v", cfg.Proxy)
	}
	if len(cfg.Proxy.Upstreams) != 2 || cfg.Proxy.Upstreams[0] != "kafsql-0:5432" {
		t.Fatalf("expected proxy upstream overrides, got %+v", cfg.Proxy.Upstreams)
	}
	if len(cfg.Proxy.ACL.Allow) != 2 || cfg.Proxy.ACL.Allow[0] != "orders" || cfg.Proxy.ACL.Deny[0] != "payments" {
		t.Fatalf("expected proxy ACL overrides, got %+v", cfg.Proxy.ACL)
	}
	if cfg.Proxy.CacheTTLSeconds != 15 || cfg.Proxy.CacheMaxEntries != 500 {
		t.Fatalf("expected proxy cache overrides, got %+v", cfg.Proxy)
	}
	if cfg.ResultCache.TTLSeconds != 12 || cfg.ResultCache.MaxEntries != 9 || cfg.ResultCache.MaxRows != 111 {
		t.Fatalf("expected result cache overrides, got %+v", cfg.ResultCache)
	}
}

func TestSchemaValidation(t *testing.T) {
	data := []byte("s3:\n  bucket: test-bucket\nmetadata:\n  topics:\n    - name: orders\n      schema:\n        columns:\n          - name: amount\n            type: decimal\n            path: $.amount\n")
	path := writeTempConfig(t, data)
	if _, err := Load(path); err == nil {
		t.Fatalf("expected schema validation error")
	}
}

func writeTempConfig(t *testing.T, data []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
