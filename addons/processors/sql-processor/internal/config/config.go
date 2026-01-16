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
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config defines the processor configuration schema.
type Config struct {
	S3             S3Config             `yaml:"s3"`
	Server         ServerConfig         `yaml:"server"`
	Metadata       MetaConfig           `yaml:"metadata"`
	Query          QueryConfig          `yaml:"query"`
	DiscoveryCache DiscoveryCacheConfig `yaml:"discovery_cache"`
	Manifest       ManifestConfig       `yaml:"discovery_manifest"`
	TimeIndex      TimeIndexConfig      `yaml:"time_index"`
	Proxy          ProxyConfig          `yaml:"proxy"`
	ResultCache    ResultCacheConfig    `yaml:"result_cache"`

	Mappings []Mapping    `yaml:"mappings"`
	Offsets  OffsetConfig `yaml:"offsets"`
}

type S3Config struct {
	Bucket    string `yaml:"bucket"`
	Namespace string `yaml:"namespace"`
	Endpoint  string `yaml:"endpoint"`
	Region    string `yaml:"region"`
	PathStyle bool   `yaml:"path_style"`
}

type OffsetConfig struct {
	Backend string `yaml:"backend"`
}

type Mapping struct {
	Topic               string `yaml:"topic"`
	Sink                string `yaml:"sink"`
	Mode                string `yaml:"mode"`
	CreateTableIfAbsent bool   `yaml:"create_table_if_missing"`
}

type ServerConfig struct {
	Listen         string `yaml:"listen"`
	MaxConnections int    `yaml:"max_connections"`
	ServerVersion  string `yaml:"server_version"`
	ClientEncoding string `yaml:"client_encoding"`
	MetricsListen  string `yaml:"metrics_listen"`
}

type MetaConfig struct {
	Discovery string         `yaml:"discovery"`
	Etcd      EtcdConfig     `yaml:"etcd"`
	Snapshot  SnapshotConfig `yaml:"snapshot"`
	Topics    []TopicConfig  `yaml:"topics"`
}

type EtcdConfig struct {
	Endpoints []string `yaml:"endpoints"`
}

type SnapshotConfig struct {
	Key        string `yaml:"key"`
	TTLSeconds int    `yaml:"ttl_seconds"`
}

type QueryConfig struct {
	DefaultLimit     int   `yaml:"default_limit"`
	RequireTimeBound bool  `yaml:"require_time_bound"`
	MaxUnbounded     int   `yaml:"max_unbounded_scan"`
	MaxScanBytes     int64 `yaml:"max_scan_bytes"`
	MaxScanSegments  int   `yaml:"max_scan_segments"`
	MaxRows          int   `yaml:"max_rows"`
	TimeoutSeconds   int   `yaml:"timeout_seconds"`
	MaxConcurrent    int   `yaml:"max_concurrent"`
	QueueSize        int   `yaml:"queue_size"`
	QueueTimeoutSec  int   `yaml:"queue_timeout_seconds"`
}

type DiscoveryCacheConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
	MaxEntries int `yaml:"max_entries"`
}

type ManifestConfig struct {
	Enabled              bool   `yaml:"enabled"`
	Key                  string `yaml:"key"`
	TTLSeconds           int    `yaml:"ttl_seconds"`
	BuildIntervalSeconds int    `yaml:"build_interval_seconds"`
	BuildMaxSegments     int    `yaml:"build_max_segments"`
	BuildMaxBytes        int64  `yaml:"build_max_bytes"`
	BuildLeaseTTLSeconds int    `yaml:"build_lease_ttl_seconds"`
}

type TimeIndexConfig struct {
	Enabled         bool  `yaml:"enabled"`
	KeySuffix       string `yaml:"key_suffix"`
	BuildMaxSegments int   `yaml:"build_max_segments"`
	BuildMaxBytes    int64 `yaml:"build_max_bytes"`
	BuildLeaseTTLSeconds int `yaml:"build_lease_ttl_seconds"`
}

type ResultCacheConfig struct {
	TTLSeconds int `yaml:"ttl_seconds"`
	MaxEntries int `yaml:"max_entries"`
	MaxRows    int `yaml:"max_rows"`
}
type ProxyConfig struct {
	Listen          string         `yaml:"listen"`
	Upstreams       []string       `yaml:"upstreams"`
	MaxConnections  int            `yaml:"max_connections"`
	CacheTTLSeconds int            `yaml:"cache_ttl_seconds"`
	CacheMaxEntries int            `yaml:"cache_max_entries"`
	ACL             ProxyACLConfig `yaml:"acl"`
}

type ProxyACLConfig struct {
	Allow []string `yaml:"allow"`
	Deny  []string `yaml:"deny"`
}

type TopicConfig struct {
	Name       string       `yaml:"name"`
	Partitions []int32      `yaml:"partitions"`
	Schema     SchemaConfig `yaml:"schema"`
}

type SchemaConfig struct {
	Columns []SchemaColumn `yaml:"columns"`
}

type SchemaColumn struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
	Path string `yaml:"path"`
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	applyDefaults(&cfg)
	applyEnvOverrides(&cfg)

	if cfg.S3.Bucket == "" {
		return Config{}, fmt.Errorf("s3.bucket is required")
	}
	if err := validateSchema(cfg.Metadata.Topics); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Server.Listen == "" {
		cfg.Server.Listen = ":5432"
	}
	if cfg.Server.MaxConnections == 0 {
		cfg.Server.MaxConnections = 100
	}
	if cfg.Server.ServerVersion == "" {
		cfg.Server.ServerVersion = "15.0"
	}
	if cfg.Server.ClientEncoding == "" {
		cfg.Server.ClientEncoding = "UTF8"
	}
	if cfg.Server.MetricsListen == "" {
		cfg.Server.MetricsListen = ":9090"
	}
	if cfg.Query.DefaultLimit == 0 {
		cfg.Query.DefaultLimit = 1000
	}
	if cfg.Query.MaxUnbounded == 0 {
		cfg.Query.MaxUnbounded = 1000
	}
	if cfg.Query.MaxScanBytes == 0 {
		cfg.Query.MaxScanBytes = 10 * 1024 * 1024 * 1024
	}
	if cfg.Query.MaxScanSegments == 0 {
		cfg.Query.MaxScanSegments = 10000
	}
	if cfg.Query.MaxRows == 0 {
		cfg.Query.MaxRows = 100000
	}
	if cfg.Query.TimeoutSeconds == 0 {
		cfg.Query.TimeoutSeconds = 30
	}
	if cfg.Query.MaxConcurrent == 0 {
		cfg.Query.MaxConcurrent = 20
	}
	if cfg.Query.QueueSize == 0 {
		cfg.Query.QueueSize = 50
	}
	if cfg.Query.QueueTimeoutSec == 0 {
		cfg.Query.QueueTimeoutSec = 10
	}
	if cfg.Metadata.Snapshot.Key == "" {
		cfg.Metadata.Snapshot.Key = "/kafscale/metadata/snapshot"
	}
	if cfg.DiscoveryCache.TTLSeconds == 0 {
		cfg.DiscoveryCache.TTLSeconds = 60
	}
	if cfg.DiscoveryCache.MaxEntries == 0 {
		cfg.DiscoveryCache.MaxEntries = 10000
	}
	if cfg.Manifest.Key == "" {
		cfg.Manifest.Key = "manifest.json"
	}
	if cfg.Manifest.TTLSeconds == 0 {
		cfg.Manifest.TTLSeconds = 60
	}
	if cfg.Manifest.BuildIntervalSeconds < 0 {
		cfg.Manifest.BuildIntervalSeconds = 0
	}
	if cfg.Manifest.BuildLeaseTTLSeconds == 0 {
		cfg.Manifest.BuildLeaseTTLSeconds = 120
	}
	if cfg.TimeIndex.KeySuffix == "" {
		cfg.TimeIndex.KeySuffix = ".kfst"
	}
	if cfg.TimeIndex.BuildLeaseTTLSeconds == 0 {
		cfg.TimeIndex.BuildLeaseTTLSeconds = 120
	}
	if cfg.ResultCache.TTLSeconds == 0 {
		cfg.ResultCache.TTLSeconds = 30
	}
	if cfg.ResultCache.MaxEntries == 0 {
		cfg.ResultCache.MaxEntries = 100
	}
	if cfg.ResultCache.MaxRows == 0 {
		cfg.ResultCache.MaxRows = 10000
	}
	if cfg.Proxy.Listen == "" {
		cfg.Proxy.Listen = ":5432"
	}
	if cfg.Proxy.MaxConnections == 0 {
		cfg.Proxy.MaxConnections = 200
	}
	if cfg.Proxy.CacheTTLSeconds == 0 {
		cfg.Proxy.CacheTTLSeconds = 10
	}
	if cfg.Proxy.CacheMaxEntries == 0 {
		cfg.Proxy.CacheMaxEntries = 1000
	}
}

func applyEnvOverrides(cfg *Config) {
	setString(&cfg.S3.Bucket, "KAFSQL_S3_BUCKET")
	setString(&cfg.S3.Namespace, "KAFSQL_S3_NAMESPACE")
	setString(&cfg.S3.Endpoint, "KAFSQL_S3_ENDPOINT")
	setString(&cfg.S3.Region, "KAFSQL_S3_REGION")
	setBool(&cfg.S3.PathStyle, "KAFSQL_S3_PATH_STYLE")

	setString(&cfg.Server.Listen, "KAFSQL_SERVER_LISTEN")
	setInt(&cfg.Server.MaxConnections, "KAFSQL_SERVER_MAX_CONNECTIONS")
	setString(&cfg.Server.ServerVersion, "KAFSQL_SERVER_VERSION")
	setString(&cfg.Server.ClientEncoding, "KAFSQL_CLIENT_ENCODING")
	setString(&cfg.Server.MetricsListen, "KAFSQL_METRICS_LISTEN")

	setString(&cfg.Metadata.Discovery, "KAFSQL_METADATA_DISCOVERY")
	setCSV(&cfg.Metadata.Etcd.Endpoints, "KAFSQL_METADATA_ETCD_ENDPOINTS")
	setInt(&cfg.Metadata.Snapshot.TTLSeconds, "KAFSQL_METADATA_SNAPSHOT_TTL_SECONDS")
	setString(&cfg.Metadata.Snapshot.Key, "KAFSQL_METADATA_SNAPSHOT_KEY")

	setInt(&cfg.Query.DefaultLimit, "KAFSQL_QUERY_DEFAULT_LIMIT")
	setBool(&cfg.Query.RequireTimeBound, "KAFSQL_QUERY_REQUIRE_TIME_BOUND")
	setInt(&cfg.Query.MaxUnbounded, "KAFSQL_QUERY_MAX_UNBOUNDED")
	setInt64(&cfg.Query.MaxScanBytes, "KAFSQL_QUERY_MAX_SCAN_BYTES")
	setInt(&cfg.Query.MaxScanSegments, "KAFSQL_QUERY_MAX_SCAN_SEGMENTS")
	setInt(&cfg.Query.MaxRows, "KAFSQL_QUERY_MAX_ROWS")
	setInt(&cfg.Query.TimeoutSeconds, "KAFSQL_QUERY_TIMEOUT_SECONDS")
	setInt(&cfg.Query.MaxConcurrent, "KAFSQL_QUERY_MAX_CONCURRENT")
	setInt(&cfg.Query.QueueSize, "KAFSQL_QUERY_QUEUE_SIZE")
	setInt(&cfg.Query.QueueTimeoutSec, "KAFSQL_QUERY_QUEUE_TIMEOUT_SECONDS")

	setInt(&cfg.DiscoveryCache.TTLSeconds, "KAFSQL_DISCOVERY_CACHE_TTL_SECONDS")
	setInt(&cfg.DiscoveryCache.MaxEntries, "KAFSQL_DISCOVERY_CACHE_MAX_ENTRIES")
	setBool(&cfg.Manifest.Enabled, "KAFSQL_MANIFEST_ENABLED")
	setString(&cfg.Manifest.Key, "KAFSQL_MANIFEST_KEY")
	setInt(&cfg.Manifest.TTLSeconds, "KAFSQL_MANIFEST_TTL_SECONDS")
	setInt(&cfg.Manifest.BuildIntervalSeconds, "KAFSQL_MANIFEST_BUILD_INTERVAL_SECONDS")
	setInt(&cfg.Manifest.BuildMaxSegments, "KAFSQL_MANIFEST_BUILD_MAX_SEGMENTS")
	setInt64(&cfg.Manifest.BuildMaxBytes, "KAFSQL_MANIFEST_BUILD_MAX_BYTES")
	setInt(&cfg.Manifest.BuildLeaseTTLSeconds, "KAFSQL_MANIFEST_BUILD_LEASE_TTL_SECONDS")

	setBool(&cfg.TimeIndex.Enabled, "KAFSQL_TIME_INDEX_ENABLED")
	setString(&cfg.TimeIndex.KeySuffix, "KAFSQL_TIME_INDEX_SUFFIX")
	setInt(&cfg.TimeIndex.BuildMaxSegments, "KAFSQL_TIME_INDEX_BUILD_MAX_SEGMENTS")
	setInt64(&cfg.TimeIndex.BuildMaxBytes, "KAFSQL_TIME_INDEX_BUILD_MAX_BYTES")
	setInt(&cfg.TimeIndex.BuildLeaseTTLSeconds, "KAFSQL_TIME_INDEX_BUILD_LEASE_TTL_SECONDS")

	setInt(&cfg.ResultCache.TTLSeconds, "KAFSQL_RESULT_CACHE_TTL_SECONDS")
	setInt(&cfg.ResultCache.MaxEntries, "KAFSQL_RESULT_CACHE_MAX_ENTRIES")
	setInt(&cfg.ResultCache.MaxRows, "KAFSQL_RESULT_CACHE_MAX_ROWS")

	setString(&cfg.Proxy.Listen, "KAFSQL_PROXY_LISTEN")
	setCSV(&cfg.Proxy.Upstreams, "KAFSQL_PROXY_UPSTREAMS")
	setInt(&cfg.Proxy.MaxConnections, "KAFSQL_PROXY_MAX_CONNECTIONS")
	setInt(&cfg.Proxy.CacheTTLSeconds, "KAFSQL_PROXY_CACHE_TTL_SECONDS")
	setInt(&cfg.Proxy.CacheMaxEntries, "KAFSQL_PROXY_CACHE_MAX_ENTRIES")
	setCSV(&cfg.Proxy.ACL.Allow, "KAFSQL_PROXY_ACL_ALLOW")
	setCSV(&cfg.Proxy.ACL.Deny, "KAFSQL_PROXY_ACL_DENY")
}

func validateSchema(topics []TopicConfig) error {
	for _, topic := range topics {
		for _, col := range topic.Schema.Columns {
			if col.Name == "" || col.Path == "" || col.Type == "" {
				return fmt.Errorf("invalid schema column for topic %s", topic.Name)
			}
			if !isSupportedSchemaType(col.Type) {
				return fmt.Errorf("unsupported schema type %q for topic %s", col.Type, topic.Name)
			}
		}
	}
	return nil
}

func isSupportedSchemaType(value string) bool {
	switch strings.ToLower(value) {
	case "string", "int", "long", "double", "boolean", "timestamp":
		return true
	default:
		return false
	}
}

func setString(target *string, envKey string) {
	if val, ok := os.LookupEnv(envKey); ok {
		*target = val
	}
}

func setInt(target *int, envKey string) {
	if val, ok := os.LookupEnv(envKey); ok {
		parsed, err := strconv.Atoi(val)
		if err == nil {
			*target = parsed
		}
	}
}

func setInt64(target *int64, envKey string) {
	if val, ok := os.LookupEnv(envKey); ok {
		parsed, err := strconv.ParseInt(val, 10, 64)
		if err == nil {
			*target = parsed
		}
	}
}

func setBool(target *bool, envKey string) {
	if val, ok := os.LookupEnv(envKey); ok {
		parsed, err := strconv.ParseBool(val)
		if err == nil {
			*target = parsed
		}
	}
}

func setCSV(target *[]string, envKey string) {
	if val, ok := os.LookupEnv(envKey); ok {
		parts := strings.Split(val, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				out = append(out, trimmed)
			}
		}
		if len(out) > 0 {
			*target = out
		}
	}
}
