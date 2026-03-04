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
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/google/uuid"
)

const (
	defaultLFSMaxBlob              = int64(5 << 30)
	defaultLFSChunkSize            = int64(5 << 20)
	defaultLFSDialTimeoutMs        = 5000
	defaultLFSBackendBackoffMs     = 500
	defaultLFSS3HealthIntervalSec  = 30
	defaultLFSHTTPReadTimeoutSec   = 30
	defaultLFSHTTPWriteTimeoutSec  = 300
	defaultLFSHTTPIdleTimeoutSec   = 60
	defaultLFSHTTPHeaderTimeoutSec = 10
	defaultLFSHTTPMaxHeaderBytes   = 1 << 20
	defaultLFSHTTPShutdownSec      = 10
	defaultLFSTopicMaxLength       = 249
	defaultLFSDownloadTTLSec       = 120
	defaultLFSUploadSessionTTLSec  = 3600
)

// lfsModule encapsulates LFS functionality as a feature-flagged module
// inside the existing proxy. When enabled, it intercepts produce requests
// to detect LFS_BLOB headers, uploads payloads to S3, and replaces record
// values with JSON envelopes — all before the existing partition-aware fan-out.
type lfsModule struct {
	logger               *slog.Logger
	s3Uploader           *s3Uploader
	s3Bucket             string
	s3Namespace          string
	maxBlob              int64
	chunkSize            int64
	checksumAlg          string
	proxyID              string
	metrics              *lfsMetrics
	tracker              *LfsOpsTracker
	s3Healthy            uint32
	corrID               uint32
	httpAPIKey           string
	httpReadTimeout      time.Duration
	httpWriteTimeout     time.Duration
	httpIdleTimeout      time.Duration
	httpHeaderTimeout    time.Duration
	httpMaxHeaderBytes   int
	httpShutdownTimeout  time.Duration
	topicMaxLength       int
	downloadTTLMax       time.Duration
	dialTimeout          time.Duration
	backendTLSConfig     *tls.Config
	backendSASLMechanism string
	backendSASLUsername  string
	backendSASLPassword  string
	httpTLSConfig        *tls.Config
	httpTLSCertFile      string
	httpTLSKeyFile       string
	uploadSessionTTL     time.Duration
	uploadMu             sync.Mutex
	uploadSessions       map[string]*uploadSession

	// backends and connectivity for the HTTP API path (which needs its own
	// backend connections, independent of the proxy's connection pool)
	backendRetries int
	backendBackoff time.Duration
	backends       []string
	store          interface{ Metadata(ctx context.Context, topics []string) (*protocol.MetadataResponse, error) }
	cacheMu        sync.RWMutex
	cachedBackends []string
	rr             uint32
}

func initLFSModule(ctx context.Context, logger *slog.Logger) (*lfsModule, error) {
	s3Bucket := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_BUCKET"))
	s3Region := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_REGION"))
	s3Endpoint := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_ENDPOINT"))
	s3PublicURL := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_PUBLIC_ENDPOINT"))
	s3AccessKey := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_ACCESS_KEY"))
	s3SecretKey := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_SECRET_KEY"))
	s3SessionToken := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_SESSION_TOKEN"))
	forcePathStyle := lfsEnvBoolDefault("KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE", s3Endpoint != "")
	s3EnsureBucket := lfsEnvBoolDefault("KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET", false)
	maxBlob := lfsEnvInt64("KAFSCALE_LFS_PROXY_MAX_BLOB_SIZE", defaultLFSMaxBlob)
	chunkSize := lfsEnvInt64("KAFSCALE_LFS_PROXY_CHUNK_SIZE", defaultLFSChunkSize)
	proxyID := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_ID"))
	s3Namespace := lfsEnvOrDefault("KAFSCALE_S3_NAMESPACE", "default")
	checksumAlg := lfsEnvOrDefault("KAFSCALE_LFS_PROXY_CHECKSUM_ALGO", "sha256")
	httpAPIKey := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_HTTP_API_KEY"))
	dialTimeout := time.Duration(lfsEnvInt("KAFSCALE_LFS_PROXY_DIAL_TIMEOUT_MS", defaultLFSDialTimeoutMs)) * time.Millisecond
	backendRetries := lfsEnvInt("KAFSCALE_LFS_PROXY_BACKEND_RETRIES", 6)
	if backendRetries < 1 {
		backendRetries = 1
	}
	backendBackoff := time.Duration(lfsEnvInt("KAFSCALE_LFS_PROXY_BACKEND_BACKOFF_MS", defaultLFSBackendBackoffMs)) * time.Millisecond
	if backendBackoff <= 0 {
		backendBackoff = time.Duration(defaultLFSBackendBackoffMs) * time.Millisecond
	}
	httpReadTimeout := time.Duration(lfsEnvInt("KAFSCALE_LFS_PROXY_HTTP_READ_TIMEOUT_SEC", defaultLFSHTTPReadTimeoutSec)) * time.Second
	httpWriteTimeout := time.Duration(lfsEnvInt("KAFSCALE_LFS_PROXY_HTTP_WRITE_TIMEOUT_SEC", defaultLFSHTTPWriteTimeoutSec)) * time.Second
	httpIdleTimeout := time.Duration(lfsEnvInt("KAFSCALE_LFS_PROXY_HTTP_IDLE_TIMEOUT_SEC", defaultLFSHTTPIdleTimeoutSec)) * time.Second
	httpHeaderTimeout := time.Duration(lfsEnvInt("KAFSCALE_LFS_PROXY_HTTP_HEADER_TIMEOUT_SEC", defaultLFSHTTPHeaderTimeoutSec)) * time.Second
	httpMaxHeaderBytes := lfsEnvInt("KAFSCALE_LFS_PROXY_HTTP_MAX_HEADER_BYTES", defaultLFSHTTPMaxHeaderBytes)
	httpShutdownTimeout := time.Duration(lfsEnvInt("KAFSCALE_LFS_PROXY_HTTP_SHUTDOWN_TIMEOUT_SEC", defaultLFSHTTPShutdownSec)) * time.Second
	uploadSessionTTL := time.Duration(lfsEnvInt("KAFSCALE_LFS_PROXY_UPLOAD_SESSION_TTL_SEC", defaultLFSUploadSessionTTLSec)) * time.Second
	topicMaxLength := lfsEnvInt("KAFSCALE_LFS_PROXY_TOPIC_MAX_LENGTH", defaultLFSTopicMaxLength)
	downloadTTLSec := lfsEnvInt("KAFSCALE_LFS_PROXY_DOWNLOAD_TTL_SEC", defaultLFSDownloadTTLSec)
	if downloadTTLSec <= 0 {
		downloadTTLSec = defaultLFSDownloadTTLSec
	}

	backendTLSConfig, err := lfsBuildBackendTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("backend tls config: %w", err)
	}
	backendSASLMechanism := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_BACKEND_SASL_MECHANISM"))
	backendSASLUsername := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_BACKEND_SASL_USERNAME"))
	backendSASLPassword := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_BACKEND_SASL_PASSWORD"))
	httpTLSConfig, httpTLSCertFile, httpTLSKeyFile, err := lfsBuildHTTPServerTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("http tls config: %w", err)
	}

	uploader, err := newS3Uploader(ctx, s3Config{
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
		return nil, fmt.Errorf("s3 client init: %w", err)
	}
	if s3EnsureBucket {
		if err := uploader.EnsureBucket(ctx); err != nil {
			logger.Error("lfs s3 bucket ensure failed", "error", err)
		}
	}

	metrics := newLfsMetrics()

	// Tracker
	backends := splitCSV(os.Getenv("KAFSCALE_LFS_PROXY_BACKENDS"))
	trackerEnabled := lfsEnvBoolDefault("KAFSCALE_LFS_TRACKER_ENABLED", true)
	trackerTopic := lfsEnvOrDefault("KAFSCALE_LFS_TRACKER_TOPIC", defaultTrackerTopic)
	trackerBatchSize := lfsEnvInt("KAFSCALE_LFS_TRACKER_BATCH_SIZE", defaultTrackerBatchSize)
	trackerFlushMs := lfsEnvInt("KAFSCALE_LFS_TRACKER_FLUSH_MS", defaultTrackerFlushMs)
	trackerEnsureTopic := lfsEnvBoolDefault("KAFSCALE_LFS_TRACKER_ENSURE_TOPIC", true)
	trackerPartitions := lfsEnvInt("KAFSCALE_LFS_TRACKER_PARTITIONS", defaultTrackerPartitions)
	trackerReplication := lfsEnvInt("KAFSCALE_LFS_TRACKER_REPLICATION_FACTOR", defaultTrackerReplication)

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

	m := &lfsModule{
		logger:               logger,
		s3Uploader:           uploader,
		s3Bucket:             s3Bucket,
		s3Namespace:          s3Namespace,
		maxBlob:              maxBlob,
		chunkSize:            chunkSize,
		checksumAlg:          checksumAlg,
		proxyID:              proxyID,
		metrics:              metrics,
		tracker:              tracker,
		httpAPIKey:           httpAPIKey,
		httpReadTimeout:      httpReadTimeout,
		httpWriteTimeout:     httpWriteTimeout,
		httpIdleTimeout:      httpIdleTimeout,
		httpHeaderTimeout:    httpHeaderTimeout,
		httpMaxHeaderBytes:   httpMaxHeaderBytes,
		httpShutdownTimeout:  httpShutdownTimeout,
		topicMaxLength:       topicMaxLength,
		downloadTTLMax:       time.Duration(downloadTTLSec) * time.Second,
		dialTimeout:          dialTimeout,
		backendRetries:       backendRetries,
		backendBackoff:       backendBackoff,
		backendTLSConfig:     backendTLSConfig,
		backendSASLMechanism: backendSASLMechanism,
		backendSASLUsername:  backendSASLUsername,
		backendSASLPassword:  backendSASLPassword,
		httpTLSConfig:        httpTLSConfig,
		httpTLSCertFile:      httpTLSCertFile,
		httpTLSKeyFile:       httpTLSKeyFile,
		uploadSessionTTL:     uploadSessionTTL,
		uploadSessions:       make(map[string]*uploadSession),
		backends:             backends,
	}

	// Mark S3 healthy initially and start health check loop
	m.markS3Healthy(true)
	s3HealthInterval := time.Duration(lfsEnvInt("KAFSCALE_LFS_PROXY_S3_HEALTH_INTERVAL_SEC", defaultLFSS3HealthIntervalSec)) * time.Second
	m.startS3HealthCheck(ctx, s3HealthInterval)

	return m, nil
}

// rewriteProduceRequest is the integration point called from handleProduceRouting.
// It scans produce records for LFS_BLOB headers, uploads blobs to S3, and
// replaces record values with LFS envelope JSON — all in-place on the parsed
// ProduceRequest struct. Returns true if any records were rewritten, along with
// orphan candidates (S3 objects that should be tracked if the downstream Kafka
// produce fails).
func (m *lfsModule) rewriteProduceRequest(ctx context.Context, header *protocol.RequestHeader, req *protocol.ProduceRequest) (bool, []orphanInfo, error) {
	result, err := m.rewriteProduceRecords(ctx, header, req)
	if err != nil {
		for _, topic := range lfsTopicsFromProduce(req) {
			m.metrics.IncRequests(topic, "error", "lfs")
		}
		return false, nil, err
	}
	if !result.modified {
		return false, nil, nil
	}
	for topic := range result.topics {
		m.metrics.IncRequests(topic, "ok", "lfs")
	}
	m.metrics.ObserveUploadDuration(result.duration)
	m.metrics.AddUploadBytes(result.uploadBytes)
	return true, result.orphans, nil
}

// Shutdown gracefully shuts down the LFS module.
func (m *lfsModule) Shutdown() {
	if m == nil {
		return
	}
	if m.tracker != nil {
		if err := m.tracker.Close(); err != nil {
			m.logger.Warn("lfs tracker close error", "error", err)
		}
	}
}

func (m *lfsModule) markS3Healthy(ok bool) {
	if ok {
		atomic.StoreUint32(&m.s3Healthy, 1)
		return
	}
	atomic.StoreUint32(&m.s3Healthy, 0)
}

func (m *lfsModule) isS3Healthy() bool {
	return atomic.LoadUint32(&m.s3Healthy) == 1
}

func (m *lfsModule) startS3HealthCheck(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Duration(defaultLFSS3HealthIntervalSec) * time.Second
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := m.s3Uploader.HeadBucket(ctx)
				wasHealthy := m.isS3Healthy()
				m.markS3Healthy(err == nil)
				if err != nil && wasHealthy {
					m.logger.Warn("lfs s3 health check failed", "error", err)
				} else if err == nil && !wasHealthy {
					m.logger.Info("lfs s3 health check recovered")
				}
			}
		}
	}()
}

func (m *lfsModule) buildObjectKey(topic string) string {
	ns := strings.TrimSpace(m.s3Namespace)
	if ns == "" {
		ns = "default"
	}
	now := time.Now().UTC()
	return fmt.Sprintf("%s/%s/lfs/%04d/%02d/%02d/obj-%s", ns, topic, now.Year(), now.Month(), now.Day(), newLFSUUID())
}

func (m *lfsModule) resolveChecksumAlg(raw string) (lfs.ChecksumAlg, error) {
	if strings.TrimSpace(raw) == "" {
		return lfs.NormalizeChecksumAlg(m.checksumAlg)
	}
	return lfs.NormalizeChecksumAlg(raw)
}

func (m *lfsModule) setCachedBackends(backends []string) {
	if len(backends) == 0 {
		return
	}
	copied := make([]string, len(backends))
	copy(copied, backends)
	m.cacheMu.Lock()
	m.cachedBackends = copied
	m.cacheMu.Unlock()
}

func (m *lfsModule) cachedBackendsSnapshot() []string {
	m.cacheMu.RLock()
	if len(m.cachedBackends) == 0 {
		m.cacheMu.RUnlock()
		return nil
	}
	copied := make([]string, len(m.cachedBackends))
	copy(copied, m.cachedBackends)
	m.cacheMu.RUnlock()
	return copied
}

// connectBackend dials a backend broker for the HTTP API path.
func (m *lfsModule) connectBackend(ctx context.Context) (net.Conn, string, error) {
	var lastErr error
	for attempt := 0; attempt < m.backendRetries; attempt++ {
		backends := m.backends
		if len(backends) == 0 {
			if cached := m.cachedBackendsSnapshot(); len(cached) > 0 {
				backends = cached
			}
		}
		if len(backends) == 0 {
			lastErr = fmt.Errorf("no backends available")
			time.Sleep(m.backendBackoff)
			continue
		}
		index := atomic.AddUint32(&m.rr, 1)
		addr := backends[int(index)%len(backends)]
		dialer := net.Dialer{Timeout: m.dialTimeout}
		conn, dialErr := dialer.DialContext(ctx, "tcp", addr)
		if dialErr == nil {
			wrapped, err := m.wrapBackendTLS(ctx, conn, addr)
			if err != nil {
				_ = conn.Close()
				lastErr = err
				time.Sleep(m.backendBackoff)
				continue
			}
			if err := m.performBackendSASL(ctx, wrapped); err != nil {
				_ = wrapped.Close()
				lastErr = err
				time.Sleep(m.backendBackoff)
				continue
			}
			return wrapped, addr, nil
		}
		lastErr = dialErr
		time.Sleep(m.backendBackoff)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no backends available")
	}
	return nil, "", lastErr
}

// forwardToBackend writes a frame and reads the response.
func (m *lfsModule) forwardToBackend(ctx context.Context, conn net.Conn, payload []byte) ([]byte, error) {
	deadline := time.Now().Add(m.dialTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetDeadline(deadline)
	defer func() { _ = conn.SetDeadline(time.Time{}) }()
	if err := protocol.WriteFrame(conn, payload); err != nil {
		return nil, err
	}
	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		return nil, err
	}
	return frame.Payload, nil
}

func (m *lfsModule) trackOrphans(orphans []orphanInfo) {
	if len(orphans) == 0 {
		return
	}
	m.metrics.IncOrphans(len(orphans))
	for _, orphan := range orphans {
		m.logger.Warn("lfs orphaned object", "topic", orphan.Topic, "key", orphan.Key, "reason", orphan.Reason)
		reason := orphan.Reason
		if reason == "" {
			reason = "kafka_produce_failed"
		}
		m.tracker.EmitOrphanDetected(orphan.RequestID, "upload_failure", orphan.Topic, m.s3Bucket, orphan.Key, orphan.RequestID, reason, 0)
	}
}

// Helper functions scoped to the LFS module to avoid collisions with
// identically-named helpers in the existing proxy package.

func newLFSUUID() string {
	return uuid.NewString()
}

func lfsEnvBoolDefault(key string, fallback bool) bool {
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

func lfsEnvOrDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func lfsEnvInt(key string, fallback int) int {
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

func lfsEnvInt64(key string, fallback int64) int64 {
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

func lfsTopicsFromProduce(req *protocol.ProduceRequest) []string {
	if req == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(req.Topics))
	out := make([]string, 0, len(req.Topics))
	for _, topic := range req.Topics {
		if _, ok := seen[topic.Name]; ok {
			continue
		}
		seen[topic.Name] = struct{}{}
		out = append(out, topic.Name)
	}
	if len(out) == 0 {
		return []string{"unknown"}
	}
	return out
}
