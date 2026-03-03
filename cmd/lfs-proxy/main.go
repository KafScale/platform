// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

const (
	defaultProxyAddr                 = ":9092"
	defaultMaxBlob                   = int64(5 << 30)
	defaultChunkSize                 = int64(5 << 20)
	defaultDialTimeoutMs             = 5000
	defaultBackendBackoffMs          = 500
	defaultBackendRefreshIntervalSec = 3
	defaultS3HealthIntervalSec       = 30
	defaultHTTPReadTimeoutSec        = 30
	defaultHTTPWriteTimeoutSec       = 300
	defaultHTTPIdleTimeoutSec        = 60
	defaultHTTPHeaderTimeoutSec      = 10
	defaultHTTPMaxHeaderBytes        = 1 << 20
	defaultHTTPShutdownTimeoutSec    = 10
	defaultTopicMaxLength            = 249
	defaultDownloadTTLSec            = 120
	defaultUploadSessionTTLSec       = 3600
)

type lfsProxy struct {
	addr                 string
	advertisedHost       string
	advertisedPort       int32
	store                metadata.Store
	backends             []string
	logger               *slog.Logger
	rr                   uint32
	dialTimeout          time.Duration
	httpReadTimeout      time.Duration
	httpWriteTimeout     time.Duration
	httpIdleTimeout      time.Duration
	httpHeaderTimeout    time.Duration
	httpMaxHeaderBytes   int
	httpShutdownTimeout  time.Duration
	topicMaxLength       int
	downloadTTLMax       time.Duration
	checksumAlg          string
	backendTLSConfig     *tls.Config
	backendSASLMechanism string
	backendSASLUsername  string
	backendSASLPassword  string
	httpTLSConfig        *tls.Config
	httpTLSCertFile      string
	httpTLSKeyFile       string
	ready                uint32
	lastHealthy          int64
	cacheTTL             time.Duration
	cacheMu              sync.RWMutex
	cachedBackends       []string
	apiVersions          []protocol.ApiVersion
	metrics              *lfsMetrics

	s3Uploader  *s3Uploader
	s3Bucket    string
	s3Namespace string
	maxBlob     int64
	chunkSize   int64
	proxyID     string
	s3Healthy   uint32
	corrID      uint32
	httpAPIKey  string

	// LFS Operations Tracker
	tracker *LfsOpsTracker

	uploadSessionTTL time.Duration
	uploadMu         sync.Mutex
	uploadSessions   map[string]*uploadSession
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logLevel := slog.LevelInfo
	if strings.EqualFold(os.Getenv("KAFSCALE_LFS_PROXY_LOG_LEVEL"), "debug") {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))

	addr := envOrDefault("KAFSCALE_LFS_PROXY_ADDR", defaultProxyAddr)
	healthAddr := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_HEALTH_ADDR"))
	metricsAddr := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_METRICS_ADDR"))
	httpAddr := envOrDefault("KAFSCALE_LFS_PROXY_HTTP_ADDR", ":8080")
	httpAPIKey := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_HTTP_API_KEY"))
	advertisedHost := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_ADVERTISED_HOST"))
	advertisedPort := envPort("KAFSCALE_LFS_PROXY_ADVERTISED_PORT", portFromAddr(addr, 9092))
	logger.Info("advertised address configured", "host", advertisedHost, "port", advertisedPort)
	backends := splitCSV(os.Getenv("KAFSCALE_LFS_PROXY_BACKENDS"))
	backendBackoff := time.Duration(envInt("KAFSCALE_LFS_PROXY_BACKEND_BACKOFF_MS", defaultBackendBackoffMs)) * time.Millisecond
	backendRefreshInterval := time.Duration(envInt("KAFSCALE_LFS_PROXY_BACKEND_REFRESH_INTERVAL_SEC", defaultBackendRefreshIntervalSec)) * time.Second
	cacheTTL := time.Duration(envInt("KAFSCALE_LFS_PROXY_BACKEND_CACHE_TTL_SEC", 60)) * time.Second
	if cacheTTL <= 0 {
		cacheTTL = 60 * time.Second
	}

	s3Bucket := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_BUCKET"))
	s3Region := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_REGION"))
	s3Endpoint := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_ENDPOINT"))
	s3PublicURL := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_PUBLIC_ENDPOINT"))
	s3AccessKey := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_ACCESS_KEY"))
	s3SecretKey := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_SECRET_KEY"))
	s3SessionToken := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_SESSION_TOKEN"))
	forcePathStyle := envBoolDefault("KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE", s3Endpoint != "")
	s3EnsureBucket := envBoolDefault("KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET", false)
	maxBlob := envInt64("KAFSCALE_LFS_PROXY_MAX_BLOB_SIZE", defaultMaxBlob)
	chunkSize := envInt64("KAFSCALE_LFS_PROXY_CHUNK_SIZE", defaultChunkSize)
	proxyID := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_ID"))
	s3Namespace := envOrDefault("KAFSCALE_S3_NAMESPACE", "default")
	dialTimeout := time.Duration(envInt("KAFSCALE_LFS_PROXY_DIAL_TIMEOUT_MS", defaultDialTimeoutMs)) * time.Millisecond
	s3HealthInterval := time.Duration(envInt("KAFSCALE_LFS_PROXY_S3_HEALTH_INTERVAL_SEC", defaultS3HealthIntervalSec)) * time.Second
	httpReadTimeout := time.Duration(envInt("KAFSCALE_LFS_PROXY_HTTP_READ_TIMEOUT_SEC", defaultHTTPReadTimeoutSec)) * time.Second
	httpWriteTimeout := time.Duration(envInt("KAFSCALE_LFS_PROXY_HTTP_WRITE_TIMEOUT_SEC", defaultHTTPWriteTimeoutSec)) * time.Second
	httpIdleTimeout := time.Duration(envInt("KAFSCALE_LFS_PROXY_HTTP_IDLE_TIMEOUT_SEC", defaultHTTPIdleTimeoutSec)) * time.Second
	httpHeaderTimeout := time.Duration(envInt("KAFSCALE_LFS_PROXY_HTTP_HEADER_TIMEOUT_SEC", defaultHTTPHeaderTimeoutSec)) * time.Second
	httpMaxHeaderBytes := envInt("KAFSCALE_LFS_PROXY_HTTP_MAX_HEADER_BYTES", defaultHTTPMaxHeaderBytes)
	httpShutdownTimeout := time.Duration(envInt("KAFSCALE_LFS_PROXY_HTTP_SHUTDOWN_TIMEOUT_SEC", defaultHTTPShutdownTimeoutSec)) * time.Second
	uploadSessionTTL := time.Duration(envInt("KAFSCALE_LFS_PROXY_UPLOAD_SESSION_TTL_SEC", defaultUploadSessionTTLSec)) * time.Second
	topicMaxLength := envInt("KAFSCALE_LFS_PROXY_TOPIC_MAX_LENGTH", defaultTopicMaxLength)
	downloadTTLSec := envInt("KAFSCALE_LFS_PROXY_DOWNLOAD_TTL_SEC", defaultDownloadTTLSec)
	if downloadTTLSec <= 0 {
		downloadTTLSec = defaultDownloadTTLSec
	}
	checksumAlg := envOrDefault("KAFSCALE_LFS_PROXY_CHECKSUM_ALGO", "sha256")
	backendTLSConfig, err := buildBackendTLSConfig()
	if err != nil {
		logger.Error("backend tls config failed", "error", err)
		os.Exit(1)
	}
	backendSASLMechanism := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_BACKEND_SASL_MECHANISM"))
	backendSASLUsername := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_BACKEND_SASL_USERNAME"))
	backendSASLPassword := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_BACKEND_SASL_PASSWORD"))
	httpTLSConfig, httpTLSCertFile, httpTLSKeyFile, err := buildHTTPServerTLSConfig()
	if err != nil {
		logger.Error("http tls config failed", "error", err)
		os.Exit(1)
	}

	store, err := buildMetadataStore(ctx)
	if err != nil {
		logger.Error("metadata store init failed", "error", err)
		os.Exit(1)
	}
	if store == nil {
		logger.Error("KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS not set; proxy cannot build metadata responses")
		os.Exit(1)
	}

	if advertisedHost == "" {
		logger.Warn("KAFSCALE_LFS_PROXY_ADVERTISED_HOST not set; clients may not resolve the proxy address")
	}

	s3Uploader, err := newS3Uploader(ctx, s3Config{
		Bucket:          s3Bucket,
		Region:          s3Region,
		Endpoint:        s3Endpoint,
		PublicEndpoint:  s3PublicURL,
		AccessKeyID:     s3AccessKey,
		SecretAccessKey: s3SecretKey,
		SessionToken:    s3SessionToken,
		ForcePathStyle:  forcePathStyle,
		ChunkSize:       chunkSize,
	})
	if err != nil {
		logger.Error("s3 client init failed", "error", err)
		os.Exit(1)
	}
	if s3EnsureBucket {
		if err := s3Uploader.EnsureBucket(ctx); err != nil {
			logger.Error("s3 bucket ensure failed", "error", err)
		}
	}

	metrics := newLfsMetrics()

	// LFS Ops Tracker configuration
	trackerEnabled := envBoolDefault("KAFSCALE_LFS_TRACKER_ENABLED", true)
	trackerTopic := envOrDefault("KAFSCALE_LFS_TRACKER_TOPIC", defaultTrackerTopic)
	trackerBatchSize := envInt("KAFSCALE_LFS_TRACKER_BATCH_SIZE", defaultTrackerBatchSize)
	trackerFlushMs := envInt("KAFSCALE_LFS_TRACKER_FLUSH_MS", defaultTrackerFlushMs)
	trackerEnsureTopic := envBoolDefault("KAFSCALE_LFS_TRACKER_ENSURE_TOPIC", true)
	trackerPartitions := envInt("KAFSCALE_LFS_TRACKER_PARTITIONS", defaultTrackerPartitions)
	trackerReplication := envInt("KAFSCALE_LFS_TRACKER_REPLICATION_FACTOR", defaultTrackerReplication)

	trackerCfg := TrackerConfig{
		Enabled:           trackerEnabled,
		Topic:             trackerTopic,
		Brokers:           backends,
		BatchSize:         trackerBatchSize,
		FlushMs:           trackerFlushMs,
		ProxyID:           proxyID,
		EnsureTopic:       trackerEnsureTopic,
		Partitions:        trackerPartitions,
		ReplicationFactor: trackerReplication,
	}

	tracker, err := NewLfsOpsTracker(ctx, trackerCfg, logger)
	if err != nil {
		logger.Warn("lfs ops tracker init failed, continuing without tracker", "error", err)
		tracker = &LfsOpsTracker{config: trackerCfg, logger: logger}
	}

	p := &lfsProxy{
		addr:                 addr,
		advertisedHost:       advertisedHost,
		advertisedPort:       advertisedPort,
		store:                store,
		backends:             backends,
		logger:               logger,
		dialTimeout:          dialTimeout,
		cacheTTL:             cacheTTL,
		apiVersions:          generateProxyApiVersions(),
		metrics:              metrics,
		s3Uploader:           s3Uploader,
		s3Bucket:             s3Bucket,
		s3Namespace:          s3Namespace,
		maxBlob:              maxBlob,
		chunkSize:            chunkSize,
		proxyID:              proxyID,
		httpAPIKey:           httpAPIKey,
		httpReadTimeout:      httpReadTimeout,
		httpWriteTimeout:     httpWriteTimeout,
		httpIdleTimeout:      httpIdleTimeout,
		httpHeaderTimeout:    httpHeaderTimeout,
		httpMaxHeaderBytes:   httpMaxHeaderBytes,
		httpShutdownTimeout:  httpShutdownTimeout,
		topicMaxLength:       topicMaxLength,
		downloadTTLMax:       time.Duration(downloadTTLSec) * time.Second,
		checksumAlg:          checksumAlg,
		backendTLSConfig:     backendTLSConfig,
		backendSASLMechanism: backendSASLMechanism,
		backendSASLUsername:  backendSASLUsername,
		backendSASLPassword:  backendSASLPassword,
		httpTLSConfig:        httpTLSConfig,
		httpTLSCertFile:      httpTLSCertFile,
		httpTLSKeyFile:       httpTLSKeyFile,
		tracker:              tracker,
		uploadSessionTTL:     uploadSessionTTL,
		uploadSessions:       make(map[string]*uploadSession),
	}
	if len(backends) > 0 {
		p.setCachedBackends(backends)
		p.touchHealthy()
		p.setReady(true)
	}
	p.markS3Healthy(true)
	p.startBackendRefresh(ctx, backendBackoff, backendRefreshInterval)
	p.startS3HealthCheck(ctx, s3HealthInterval)
	if healthAddr != "" {
		p.startHealthServer(ctx, healthAddr)
	}
	if metricsAddr != "" {
		p.startMetricsServer(ctx, metricsAddr)
	}
	if httpAddr != "" {
		p.startHTTPServer(ctx, httpAddr)
	}
	if err := p.listenAndServe(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("lfs proxy server error", "error", err)
		os.Exit(1)
	}

	// Graceful shutdown of tracker
	if p.tracker != nil {
		if err := p.tracker.Close(); err != nil {
			logger.Warn("tracker close error", "error", err)
		}
	}
}

func envOrDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func envPort(key string, fallback int) int32 {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return int32(fallback)
	}
	parsed, err := strconv.ParseInt(val, 10, 32)
	if err != nil || parsed <= 0 {
		return int32(fallback)
	}
	return int32(parsed)
}

func envInt(key string, fallback int) int {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return parsed
}

func envInt64(key string, fallback int64) int64 {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBoolDefault(key string, fallback bool) bool {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback
	}
	switch strings.ToLower(val) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func portFromAddr(addr string, fallback int) int {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fallback
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fallback
	}
	return port
}

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		val := strings.TrimSpace(part)
		if val != "" {
			out = append(out, val)
		}
	}
	return out
}

func buildMetadataStore(ctx context.Context) (metadata.Store, error) {
	cfg, ok := proxyEtcdConfigFromEnv()
	if !ok {
		return nil, nil
	}
	return metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, cfg)
}

func proxyEtcdConfigFromEnv() (metadata.EtcdStoreConfig, bool) {
	endpoints := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS"))
	if endpoints == "" {
		return metadata.EtcdStoreConfig{}, false
	}
	return metadata.EtcdStoreConfig{
		Endpoints: strings.Split(endpoints, ","),
		Username:  os.Getenv("KAFSCALE_LFS_PROXY_ETCD_USERNAME"),
		Password:  os.Getenv("KAFSCALE_LFS_PROXY_ETCD_PASSWORD"),
	}, true
}

func (p *lfsProxy) startMetricsServer(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		p.metrics.WritePrometheus(w)
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       p.httpReadTimeout,
		WriteTimeout:      p.httpWriteTimeout,
		IdleTimeout:       p.httpIdleTimeout,
		ReadHeaderTimeout: p.httpHeaderTimeout,
		MaxHeaderBytes:    p.httpMaxHeaderBytes,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), p.httpShutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		p.logger.Info("lfs proxy metrics listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			p.logger.Warn("lfs proxy metrics server error", "error", err)
		}
	}()
}
