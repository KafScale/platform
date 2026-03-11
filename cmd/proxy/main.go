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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/sync/singleflight"
)

const (
	defaultProxyAddr = ":9092"
)

type proxy struct {
	addr           string
	advertisedHost string
	advertisedPort int32
	store          metadata.Store
	backends       []string
	logger         *slog.Logger
	rr             uint32
	dialTimeout    time.Duration
	ready          uint32
	lastHealthy    int64
	cacheTTL       time.Duration
	cacheMu        sync.RWMutex
	cachedBackends []string
	apiVersions    []kmsg.ApiVersionsResponseApiKey
	router         *metadata.PartitionRouter
	groupRouter    *metadata.GroupRouter
	brokerAddrMu   sync.RWMutex
	brokerAddrs    map[string]string // brokerID -> "host:port"
	topicNamesMu   sync.RWMutex
	topicNames     map[[16]byte]string // topicID -> topic name
	metaFlight     singleflight.Group
	backendRetries int
	backendBackoff time.Duration
	lfs            *lfsModule // nil when LFS disabled
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	addr := envOrDefault("KAFSCALE_PROXY_ADDR", defaultProxyAddr)
	healthAddr := strings.TrimSpace(os.Getenv("KAFSCALE_PROXY_HEALTH_ADDR"))
	advertisedHost := strings.TrimSpace(os.Getenv("KAFSCALE_PROXY_ADVERTISED_HOST"))
	advertisedPort := envPort("KAFSCALE_PROXY_ADVERTISED_PORT", portFromAddr(addr, 9092))
	backends := splitCSV(os.Getenv("KAFSCALE_PROXY_BACKENDS"))
	backendBackoff := time.Duration(envInt("KAFSCALE_PROXY_BACKEND_BACKOFF_MS", 500)) * time.Millisecond
	cacheTTL := time.Duration(envInt("KAFSCALE_PROXY_BACKEND_CACHE_TTL_SEC", 60)) * time.Second
	if cacheTTL <= 0 {
		cacheTTL = 60 * time.Second
	}

	store, err := buildMetadataStore(ctx)
	if err != nil {
		logger.Error("metadata store init failed", "error", err)
		os.Exit(1)
	}
	if store == nil {
		logger.Error("KAFSCALE_PROXY_ETCD_ENDPOINTS not set; proxy cannot build metadata responses")
		os.Exit(1)
	}

	if advertisedHost == "" {
		logger.Warn("KAFSCALE_PROXY_ADVERTISED_HOST not set; clients may not resolve the proxy address")
	}

	backendRetries := envInt("KAFSCALE_PROXY_BACKEND_RETRIES", 6)
	if backendRetries < 1 {
		backendRetries = 1
	}
	if backendBackoff <= 0 {
		backendBackoff = 500 * time.Millisecond
	}

	p := &proxy{
		addr:           addr,
		advertisedHost: advertisedHost,
		advertisedPort: advertisedPort,
		store:          store,
		backends:       backends,
		logger:         logger,
		dialTimeout:    5 * time.Second,
		cacheTTL:       cacheTTL,
		apiVersions:    generateProxyApiVersions(),
		brokerAddrs:    make(map[string]string),
		topicNames:     make(map[[16]byte]string),
		backendRetries: backendRetries,
		backendBackoff: backendBackoff,
	}

	if etcdStore, ok := store.(*metadata.EtcdStore); ok {
		router, err := metadata.NewPartitionRouter(ctx, etcdStore.EtcdClient(), logger)
		if err != nil {
			logger.Warn("partition router init failed; using round-robin routing", "error", err)
		} else {
			p.router = router
			logger.Info("partition-aware routing enabled")
		}
		groupRouter, err := metadata.NewGroupRouter(ctx, etcdStore.EtcdClient(), logger)
		if err != nil {
			logger.Warn("group router init failed; using round-robin routing for group ops", "error", err)
		} else {
			p.groupRouter = groupRouter
			logger.Info("group-aware routing enabled")
		}
	}
	if len(backends) > 0 {
		p.setCachedBackends(backends)
		p.touchHealthy()
		p.setReady(true)
	}
	p.initMetadataCache(ctx)

	if lfsEnvBoolDefault("KAFSCALE_PROXY_LFS_ENABLED", false) {
		lfsmod, err := initLFSModule(ctx, logger)
		if err != nil {
			logger.Error("lfs module init failed", "error", err)
			os.Exit(1)
		}
		p.lfs = lfsmod
		// Give the LFS HTTP API access to the proxy's backends for its own connections
		if len(backends) > 0 {
			lfsmod.backends = backends
			lfsmod.setCachedBackends(backends)
		}
		logger.Info("LFS module enabled")

		// Start LFS HTTP API if configured
		lfsHTTPAddr := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_HTTP_ADDR"))
		if lfsHTTPAddr != "" {
			lfsmod.startHTTPServer(ctx, lfsHTTPAddr)
		}
		// Start LFS metrics server if configured
		lfsMetricsAddr := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_METRICS_ADDR"))
		if lfsMetricsAddr != "" {
			lfsmod.startMetricsServer(ctx, lfsMetricsAddr)
		}
	}

	if healthAddr != "" {
		p.startHealthServer(ctx, healthAddr)
	}
	if err := p.listenAndServe(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("proxy server error", "error", err)
		os.Exit(1)
	}
	if p.router != nil {
		p.router.Stop()
	}
	if p.groupRouter != nil {
		p.groupRouter.Stop()
	}
	if p.lfs != nil {
		p.lfs.Shutdown()
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
	endpoints := strings.TrimSpace(os.Getenv("KAFSCALE_PROXY_ETCD_ENDPOINTS"))
	if endpoints == "" {
		return metadata.EtcdStoreConfig{}, false
	}
	return metadata.EtcdStoreConfig{
		Endpoints: strings.Split(endpoints, ","),
		Username:  os.Getenv("KAFSCALE_PROXY_ETCD_USERNAME"),
		Password:  os.Getenv("KAFSCALE_PROXY_ETCD_PASSWORD"),
	}, true
}

func (p *proxy) listenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", p.addr)
	if err != nil {
		return err
	}
	p.logger.Info("proxy listening", "addr", ln.Addr().String())

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				p.logger.Warn("accept temporary error", "error", err)
				continue
			}
			return err
		}
		go p.handleConnection(ctx, conn)
	}
}

func (p *proxy) setReady(ready bool) {
	if ready {
		atomic.StoreUint32(&p.ready, 1)
		return
	}
	atomic.StoreUint32(&p.ready, 0)
}

func (p *proxy) isReady() bool {
	return atomic.LoadUint32(&p.ready) == 1
}

func (p *proxy) setCachedBackends(backends []string) {
	if len(backends) == 0 {
		return
	}
	copied := make([]string, len(backends))
	copy(copied, backends)
	p.cacheMu.Lock()
	p.cachedBackends = copied
	p.cacheMu.Unlock()
}

func (p *proxy) cachedBackendsSnapshot() []string {
	p.cacheMu.RLock()
	if len(p.cachedBackends) == 0 {
		p.cacheMu.RUnlock()
		return nil
	}
	copied := make([]string, len(p.cachedBackends))
	copy(copied, p.cachedBackends)
	p.cacheMu.RUnlock()
	return copied
}

func (p *proxy) touchHealthy() {
	atomic.StoreInt64(&p.lastHealthy, time.Now().UnixNano())
}

func (p *proxy) cacheFresh() bool {
	last := atomic.LoadInt64(&p.lastHealthy)
	if last == 0 {
		return false
	}
	return time.Since(time.Unix(0, last)) <= p.cacheTTL
}

// checkReady uses cached state when fresh, falling back to a live metadata
// fetch only when the cache TTL has expired (e.g. no traffic for >60s).
// The fallback uses a short timeout to prevent health probes from blocking.
func (p *proxy) checkReady(ctx context.Context) bool {
	if len(p.backends) > 0 {
		return true
	}
	if p.cacheFresh() {
		return true
	}
	if p.store == nil {
		return false
	}
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	backends, err := p.currentBackends(checkCtx)
	return err == nil && len(backends) > 0
}

func (p *proxy) initMetadataCache(ctx context.Context) {
	if p.store == nil {
		return
	}
	p.refreshMetadataCache(ctx)
	// Periodic refresh so topology changes are picked up without a cache miss.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.refreshMetadataCache(ctx)
			}
		}
	}()
}

func (p *proxy) startHealthServer(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if p.checkReady(r.Context()) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready\n"))
			return
		}
		http.Error(w, "no backends available", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	go func() {
		p.logger.Info("proxy health listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			p.logger.Warn("proxy health server error", "error", err)
		}
	}()
}

func (p *proxy) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	var backendConn net.Conn
	var backendAddr string
	defer func() {
		if backendConn != nil {
			backendConn.Close()
		}
	}()
	pool := newConnPool(p.dialTimeout)
	defer pool.Close()

	for {
		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			return
		}
		header, body, err := protocol.ParseRequestHeader(frame.Payload)
		if err != nil {
			p.logger.Warn("parse request header failed", "error", err)
			return
		}

		if header.APIKey == protocol.APIKeyApiVersion {
			resp, err := p.handleApiVersions(header)
			if err != nil {
				p.logger.Warn("api versions handling failed", "error", err)
				return
			}
			if err := protocol.WriteFrame(conn, resp); err != nil {
				p.logger.Warn("write api versions response failed", "error", err)
				return
			}
			continue
		}

		if !p.isReady() {
			resp, ok, err := p.buildNotReadyResponse(header, body)
			if err != nil {
				p.logger.Warn("not-ready response build failed", "error", err)
				return
			}
			if ok {
				if err := protocol.WriteFrame(conn, resp); err != nil {
					p.logger.Warn("write not-ready response failed", "error", err)
				}
			}
			return
		}

		switch header.APIKey {
		case protocol.APIKeyMetadata:
			resp, err := p.handleMetadata(ctx, header, frame.Payload)
			if err != nil {
				p.logger.Warn("metadata handling failed", "error", err)
				return
			}
			if err := protocol.WriteFrame(conn, resp); err != nil {
				p.logger.Warn("write metadata response failed", "error", err)
				return
			}
			continue
		case protocol.APIKeyFindCoordinator:
			resp, err := p.handleFindCoordinator(header)
			if err != nil {
				p.logger.Warn("find coordinator handling failed", "error", err)
				return
			}
			if err := protocol.WriteFrame(conn, resp); err != nil {
				p.logger.Warn("write coordinator response failed", "error", err)
				return
			}
			continue
		case protocol.APIKeyProduce:
			resp, err := p.handleProduceRouting(ctx, header, frame.Payload, pool)
			if err != nil {
				p.logger.Warn("produce routing failed", "error", err)
				p.respondBackendError(conn, header, body)
				return
			}
			if resp == nil {
				// acks=0: no response expected by the client.
				continue
			}
			if err := protocol.WriteFrame(conn, resp); err != nil {
				p.logger.Warn("write produce response failed", "error", err)
				return
			}
			continue
		case protocol.APIKeyFetch:
			resp, err := p.handleFetchRouting(ctx, header, frame.Payload, pool)
			if err != nil {
				p.logger.Warn("fetch routing failed", "error", err)
				p.respondBackendError(conn, header, body)
				return
			}
			if err := protocol.WriteFrame(conn, resp); err != nil {
				p.logger.Warn("write fetch response failed", "error", err)
				return
			}
			continue
		case protocol.APIKeyJoinGroup,
			protocol.APIKeySyncGroup,
			protocol.APIKeyHeartbeat,
			protocol.APIKeyLeaveGroup,
			protocol.APIKeyOffsetCommit,
			protocol.APIKeyOffsetFetch,
			protocol.APIKeyDescribeGroups:
			resp, err := p.handleGroupRouting(ctx, header, frame.Payload, pool)
			if err != nil {
				p.logger.Warn("group routing failed", "error", err)
				p.respondBackendError(conn, header, frame.Payload)
				return
			}
			if err := protocol.WriteFrame(conn, resp); err != nil {
				p.logger.Warn("write group response failed", "error", err)
				return
			}
			continue
		default:
		}

		if backendConn == nil {
			backendConn, backendAddr, err = p.connectBackend(ctx)
			if err != nil {
				p.logger.Error("backend connect failed", "error", err)
				p.respondBackendError(conn, header, frame.Payload)
				return
			}
		}

		resp, err := p.forwardToBackend(ctx, backendConn, backendAddr, frame.Payload)
		if err != nil {
			backendConn.Close()
			backendConn, backendAddr, err = p.connectBackend(ctx)
			if err != nil {
				p.logger.Warn("backend reconnect failed", "error", err)
				p.respondBackendError(conn, header, frame.Payload)
				return
			}
			resp, err = p.forwardToBackend(ctx, backendConn, backendAddr, frame.Payload)
			if err != nil {
				p.logger.Warn("backend forward failed", "error", err)
				p.respondBackendError(conn, header, frame.Payload)
				return
			}
		}
		if err := protocol.WriteFrame(conn, resp); err != nil {
			p.logger.Warn("write response failed", "error", err)
			return
		}
	}
}

// connPool is a per-client-connection pool of backend TCP connections, keyed by
// broker address. Connections are reused across produces from the same client
// and cleaned up when the client disconnects.
type connPool struct {
	conns       map[string]net.Conn
	dialTimeout time.Duration
}

func newConnPool(dialTimeout time.Duration) *connPool {
	return &connPool{
		conns:       make(map[string]net.Conn),
		dialTimeout: dialTimeout,
	}
}

// Borrow returns a pooled connection for addr (removing it from the pool), or
// dials a new one. The caller must either Return or close the connection.
func (cp *connPool) Borrow(ctx context.Context, addr string) (net.Conn, error) {
	if c, ok := cp.conns[addr]; ok {
		delete(cp.conns, addr)
		return c, nil
	}
	dialer := net.Dialer{Timeout: cp.dialTimeout}
	return dialer.DialContext(ctx, "tcp", addr)
}

// Return puts a connection back into the pool, closing any existing connection
// for the same address.
func (cp *connPool) Return(addr string, conn net.Conn) {
	if old, ok := cp.conns[addr]; ok {
		old.Close()
	}
	cp.conns[addr] = conn
}

// Close closes all pooled connections.
func (cp *connPool) Close() {
	for addr, c := range cp.conns {
		c.Close()
		delete(cp.conns, addr)
	}
}

// handleProduceRouting routes produce requests to the partition-owning broker(s).
// The request is split by owning broker, forwarded concurrently, and responses
// are merged. On NOT_LEADER_OR_FOLLOWER, failed partitions are retried on a
// different broker.
//
// Returns (nil, nil) for acks=0 produces (fire-and-forget, no client response).
func (p *proxy) handleProduceRouting(ctx context.Context, header *protocol.RequestHeader, payload []byte, pool *connPool) ([]byte, error) {
	_, req, err := protocol.ParseRequest(payload)
	if err != nil {
		return p.forwardProduceRaw(ctx, payload, pool)
	}
	produceReq, ok := req.(*kmsg.ProduceRequest)
	if !ok || len(produceReq.Topics) == 0 {
		return p.forwardProduceRaw(ctx, payload, pool)
	}

	// LFS rewrite: detect LFS_BLOB headers, upload to S3, replace values
	var lfsOrphans []orphanInfo
	if p.lfs != nil {
		rewritten, orphans, err := p.lfs.rewriteProduceRequest(ctx, header, produceReq)
		if err != nil {
			return nil, err
		}
		if rewritten {
			payload = nil // force re-encode in fanOut
			lfsOrphans = orphans
		}
	}

	if produceReq.Acks == 0 {
		p.fireAndForgetProduce(ctx, header, produceReq, payload, pool)
		return nil, nil
	}

	groups := p.groupPartitionsByBroker(ctx, produceReq, nil)
	resp, err := p.forwardProduce(ctx, header, produceReq, payload, groups, pool)
	if err != nil && p.lfs != nil && len(lfsOrphans) > 0 {
		p.lfs.trackOrphans(lfsOrphans)
	}
	return resp, err
}

// forwardProduceRaw forwards an unparseable produce payload to any backend.
// Used when the request can't be parsed (e.g. unsupported version).
func (p *proxy) forwardProduceRaw(ctx context.Context, payload []byte, pool *connPool) ([]byte, error) {
	conn, addr, err := p.connectForAddr(ctx, "", nil, pool)
	if err != nil {
		return nil, err
	}
	resp, err := p.forwardToBackend(ctx, conn, addr, payload)
	if err != nil {
		conn.Close()
		return nil, err
	}
	pool.Return(addr, conn)
	return resp, nil
}

// fireAndForgetProduce writes a produce request to backends without reading a
// response. Used for acks=0 produces where the Kafka protocol specifies no
// server response.
func (p *proxy) fireAndForgetProduce(ctx context.Context, header *protocol.RequestHeader, req *kmsg.ProduceRequest, originalPayload []byte, pool *connPool) {
	groups := p.groupPartitionsByBroker(ctx, req, nil)

	for addr, subReq := range groups {
		var payload []byte
		if len(groups) == 1 {
			payload = originalPayload
		} else {
			payload = encodeProduceRequest(header, subReq)
		}

		conn, targetAddr, err := p.connectForAddr(ctx, addr, nil, pool)
		if err != nil {
			p.logger.Debug("fire-and-forget connect failed", "target", addr, "error", err)
			continue
		}
		if writeErr := protocol.WriteFrame(conn, payload); writeErr != nil {
			conn.Close()
			p.logger.Debug("fire-and-forget write failed", "target", targetAddr, "error", writeErr)
			continue
		}
		pool.Return(targetAddr, conn)
	}
}

// groupPartitionsByBroker groups topic-partitions by the owning broker's address.
// If include is non-nil, only partitions present in the include map are grouped.
// Partitions with no known owner are grouped under "" for round-robin fallback.
func (p *proxy) groupPartitionsByBroker(ctx context.Context, req *kmsg.ProduceRequest, include map[string]map[int32]bool) map[string]*kmsg.ProduceRequest {
	groups := make(map[string]*kmsg.ProduceRequest)
	topicIndices := make(map[string]map[string]int) // addr -> topic name -> index in subReq.Topics

	for _, topic := range req.Topics {
		var includeParts map[int32]bool
		if include != nil {
			includeParts = include[topic.Topic]
			if len(includeParts) == 0 {
				continue
			}
		}
		for _, part := range topic.Partitions {
			if includeParts != nil && !includeParts[part.Partition] {
				continue
			}
			addr := ""
			if p.router != nil {
				if ownerID := p.router.LookupOwner(topic.Topic, part.Partition); ownerID != "" {
					addr = p.brokerIDToAddr(ctx, ownerID)
				}
			}
			subReq, ok := groups[addr]
			if !ok {
				subReq = &kmsg.ProduceRequest{
					Version:       req.Version,
					Acks:          req.Acks,
					TimeoutMillis: req.TimeoutMillis,
					TransactionID: req.TransactionID,
				}
				groups[addr] = subReq
				topicIndices[addr] = make(map[string]int)
			}
			idx, ok := topicIndices[addr][topic.Topic]
			if !ok {
				idx = len(subReq.Topics)
				subReq.Topics = append(subReq.Topics, kmsg.ProduceRequestTopic{Topic: topic.Topic})
				topicIndices[addr][topic.Topic] = idx
			}
			subReq.Topics[idx].Partitions = append(subReq.Topics[idx].Partitions, part)
		}
	}
	return groups
}

// forwardProduce splits a produce request by broker, forwards each sub-request
// concurrently, and merges the responses. If any partitions are rejected with
// NOT_LEADER_OR_FOLLOWER, those partitions are retried on a different broker
// (up to maxRetries total attempts).
func (p *proxy) forwardProduce(ctx context.Context, header *protocol.RequestHeader, fullReq *kmsg.ProduceRequest, originalPayload []byte, groups map[string]*kmsg.ProduceRequest, pool *connPool) ([]byte, error) {
	const maxRetries = 3

	merged := &kmsg.ProduceResponse{Version: header.APIVersion}

	var failedPartitions map[string]map[int32]bool
	for attempt := 0; attempt < maxRetries; attempt++ {
		failedPartitions = nil
		triedBackends := make(map[string]bool)
		subResults := p.fanOutProduce(ctx, header, groups, originalPayload, triedBackends, pool)

		for _, r := range subResults {
			if r.err != nil {
				p.logger.Warn("produce forward failed", "target", r.target, "error", r.err)
				addErrorForAllPartitions(merged, r.subReq, protocol.REQUEST_TIMED_OUT)
				continue
			}
			if r.conn != nil {
				pool.Return(r.target, r.conn)
			}
			for _, topic := range r.subResp.Topics {
				for _, part := range topic.Partitions {
					if part.ErrorCode == protocol.NOT_LEADER_OR_FOLLOWER {
						if failedPartitions == nil {
							failedPartitions = make(map[string]map[int32]bool)
						}
						if failedPartitions[topic.Topic] == nil {
							failedPartitions[topic.Topic] = make(map[int32]bool)
						}
						failedPartitions[topic.Topic][part.Partition] = true
						if p.router != nil {
							p.router.Invalidate(topic.Topic, part.Partition)
						}
					} else {
						tr := findOrAddTopicResponse(merged, topic.Topic)
						tr.Partitions = append(tr.Partitions, part)
					}
				}
			}
			if r.subResp.ThrottleMillis > merged.ThrottleMillis {
				merged.ThrottleMillis = r.subResp.ThrottleMillis
			}
		}

		if len(failedPartitions) == 0 {
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, merged), nil
		}

		groups = p.groupPartitionsByBroker(ctx, fullReq, failedPartitions)
		originalPayload = nil
		if len(groups) == 0 {
			break
		}
		p.logger.Debug("retrying NOT_LEADER partitions", "attempt", attempt+1, "partitions", len(failedPartitions))
	}

	for _, topic := range fullReq.Topics {
		failedParts, ok := failedPartitions[topic.Topic]
		if !ok {
			continue
		}
		tr := findOrAddTopicResponse(merged, topic.Topic)
		for _, part := range topic.Partitions {
			if failedParts[part.Partition] {
				tr.Partitions = append(tr.Partitions, kmsg.ProduceResponseTopicPartition{
					Partition:  part.Partition,
					ErrorCode:  protocol.NOT_LEADER_OR_FOLLOWER,
					BaseOffset: -1,
				})
			}
		}
	}
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, merged), nil
}

type fanOutResult struct {
	subReq  *kmsg.ProduceRequest
	subResp *kmsg.ProduceResponse
	conn    net.Conn // non-nil on success; caller must Return or Close
	target  string
	err     error
}

// fanOutProduce borrows connections and forwards sub-requests concurrently.
// When there's only one group and originalPayload is non-nil, the original
// payload is forwarded as-is (avoiding re-encoding).
func (p *proxy) fanOutProduce(ctx context.Context, header *protocol.RequestHeader, groups map[string]*kmsg.ProduceRequest, originalPayload []byte, triedBackends map[string]bool, pool *connPool) []fanOutResult {
	type workItem struct {
		subReq  *kmsg.ProduceRequest
		conn    net.Conn
		target  string
		payload []byte
	}
	work := make([]workItem, 0, len(groups))
	var connectErrors []fanOutResult

	canUseOriginal := originalPayload != nil && len(groups) == 1
	for addr, subReq := range groups {
		conn, targetAddr, err := p.connectForAddr(ctx, addr, triedBackends, pool)
		if err != nil {
			connectErrors = append(connectErrors, fanOutResult{subReq: subReq, target: addr, err: err})
			continue
		}
		triedBackends[targetAddr] = true

		var payload []byte
		if canUseOriginal {
			payload = originalPayload
		} else {
			payload = encodeProduceRequest(header, subReq)
		}
		work = append(work, workItem{subReq: subReq, conn: conn, target: targetAddr, payload: payload})
	}

	results := make([]fanOutResult, len(work))
	var wg sync.WaitGroup
	for i := range work {
		i := i
		w := work[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			respBytes, err := p.forwardToBackend(ctx, w.conn, w.target, w.payload)
			if err != nil {
				w.conn.Close()
				results[i] = fanOutResult{subReq: w.subReq, target: w.target, err: err}
				return
			}
			subResp, parseErr := parseProduceResponse(respBytes, header.APIVersion)
			if parseErr != nil {
				w.conn.Close()
				results[i] = fanOutResult{subReq: w.subReq, target: w.target, err: parseErr}
				return
			}
			results[i] = fanOutResult{subReq: w.subReq, subResp: subResp, conn: w.conn, target: w.target}
		}()
	}
	wg.Wait()

	return append(connectErrors, results...)
}

// connectForAddr borrows or dials a connection for the given addr. If addr is
// empty, a round-robin backend is selected (excluding triedBackends).
func (p *proxy) connectForAddr(ctx context.Context, addr string, exclude map[string]bool, pool *connPool) (net.Conn, string, error) {
	if addr != "" && !exclude[addr] {
		conn, err := pool.Borrow(ctx, addr)
		if err == nil {
			return conn, addr, nil
		}
	}
	return p.connectBackendExcluding(ctx, exclude)
}

func findOrAddTopicResponse(resp *kmsg.ProduceResponse, name string) *kmsg.ProduceResponseTopic {
	for i := range resp.Topics {
		if resp.Topics[i].Topic == name {
			return &resp.Topics[i]
		}
	}
	resp.Topics = append(resp.Topics, kmsg.ProduceResponseTopic{Topic: name})
	return &resp.Topics[len(resp.Topics)-1]
}

// addErrorForAllPartitions fills the response with errorCode for every partition
// in the sub-request (used when a broker is unreachable).
func addErrorForAllPartitions(resp *kmsg.ProduceResponse, req *kmsg.ProduceRequest, errorCode int16) {
	for _, topic := range req.Topics {
		topicResp := findOrAddTopicResponse(resp, topic.Topic)
		for _, part := range topic.Partitions {
			topicResp.Partitions = append(topicResp.Partitions, kmsg.ProduceResponseTopicPartition{
				Partition:  part.Partition,
				ErrorCode:  errorCode,
				BaseOffset: -1,
			})
		}
	}
}

// brokerIDToAddr resolves broker ID to address. Triggers a metadata fetch on
// cache miss.
func (p *proxy) brokerIDToAddr(ctx context.Context, brokerID string) string {
	p.brokerAddrMu.RLock()
	addr := p.brokerAddrs[brokerID]
	p.brokerAddrMu.RUnlock()
	if addr != "" {
		return addr
	}
	p.refreshMetadataCache(ctx)
	p.brokerAddrMu.RLock()
	addr = p.brokerAddrs[brokerID]
	p.brokerAddrMu.RUnlock()
	return addr
}

// connectBackendExcluding connects to a backend not in the exclude set.
// Uses the retry/backoff configured at startup.
func (p *proxy) connectBackendExcluding(ctx context.Context, exclude map[string]bool) (net.Conn, string, error) {
	var lastErr error
	for attempt := 0; attempt < p.backendRetries; attempt++ {
		backends, err := p.currentBackends(ctx)
		if err != nil || len(backends) == 0 {
			if cached := p.cachedBackendsSnapshot(); len(cached) > 0 && p.cacheFresh() {
				backends = cached
			} else {
				lastErr = err
				time.Sleep(p.backendBackoff)
				continue
			}
		}

		startIndex := int(atomic.AddUint32(&p.rr, 1))
		for i := 0; i < len(backends); i++ {
			addr := backends[(startIndex+i)%len(backends)]
			if exclude[addr] {
				continue
			}
			dialer := net.Dialer{Timeout: p.dialTimeout}
			conn, dialErr := dialer.DialContext(ctx, "tcp", addr)
			if dialErr == nil {
				return conn, addr, nil
			}
			lastErr = dialErr
		}
		time.Sleep(p.backendBackoff)
	}
	if lastErr == nil {
		lastErr = errors.New("no backends available")
	}
	return nil, "", lastErr
}

func (p *proxy) handleApiVersions(header *protocol.RequestHeader) ([]byte, error) {
	resp := kmsg.NewPtrApiVersionsResponse()
	resp.ErrorCode = protocol.NONE
	resp.ApiKeys = p.apiVersions
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (p *proxy) respondBackendError(conn net.Conn, header *protocol.RequestHeader, body []byte) {
	resp, ok, err := p.buildNotReadyResponse(header, body)
	if err != nil || !ok {
		return
	}
	_ = protocol.WriteFrame(conn, resp)
}

func (p *proxy) handleMetadata(ctx context.Context, header *protocol.RequestHeader, payload []byte) ([]byte, error) {
	_, req, err := protocol.ParseRequest(payload)
	if err != nil {
		return nil, err
	}
	metaReq, ok := req.(*kmsg.MetadataRequest)
	if !ok {
		return nil, fmt.Errorf("unexpected metadata request type %T", req)
	}

	meta, err := p.loadMetadata(ctx, metaReq)
	if err != nil {
		return nil, err
	}
	resp := buildProxyMetadataResponse(meta, header.CorrelationID, header.APIVersion, p.advertisedHost, p.advertisedPort)
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (p *proxy) handleFindCoordinator(header *protocol.RequestHeader) ([]byte, error) {
	resp := kmsg.NewPtrFindCoordinatorResponse()
	resp.ErrorCode = protocol.NONE
	resp.NodeID = 0
	resp.Host = p.advertisedHost
	resp.Port = p.advertisedPort
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (p *proxy) loadMetadata(ctx context.Context, req *kmsg.MetadataRequest) (*metadata.ClusterMetadata, error) {
	var zeroID [16]byte
	useIDs := false
	var topicNames []string
	if req.Topics != nil {
		for _, t := range req.Topics {
			if t.TopicID != zeroID {
				useIDs = true
				break
			}
			if t.Topic != nil {
				topicNames = append(topicNames, *t.Topic)
			}
		}
	}
	if !useIDs {
		return p.store.Metadata(ctx, topicNames)
	}
	all, err := p.store.Metadata(ctx, nil)
	if err != nil {
		return nil, err
	}
	index := make(map[[16]byte]protocol.MetadataTopic, len(all.Topics))
	for _, topic := range all.Topics {
		index[topic.TopicID] = topic
	}
	filtered := make([]protocol.MetadataTopic, 0, len(req.Topics))
	for _, t := range req.Topics {
		if t.TopicID == zeroID {
			continue
		}
		if topic, ok := index[t.TopicID]; ok {
			filtered = append(filtered, topic)
		} else {
			filtered = append(filtered, protocol.MetadataTopic{
				ErrorCode: protocol.UNKNOWN_TOPIC_ID,
				TopicID:   t.TopicID,
			})
		}
	}
	return &metadata.ClusterMetadata{
		Brokers:      all.Brokers,
		ClusterID:    all.ClusterID,
		ControllerID: all.ControllerID,
		Topics:       filtered,
	}, nil
}

func (p *proxy) buildNotReadyResponse(header *protocol.RequestHeader, body []byte) ([]byte, bool, error) {
	_, req, err := protocol.ParseRequestBody(header, body)
	if err != nil {
		return nil, false, err
	}
	encode := func(resp kmsg.Response) ([]byte, bool, error) {
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), true, nil
	}
	switch header.APIKey {
	case protocol.APIKeyMetadata:
		metaReq := req.(*kmsg.MetadataRequest)
		resp := kmsg.NewPtrMetadataResponse()
		resp.ControllerID = -1
		for _, t := range metaReq.Topics {
			mt := kmsg.NewMetadataResponseTopic()
			mt.ErrorCode = protocol.REQUEST_TIMED_OUT
			mt.Topic = t.Topic
			mt.TopicID = t.TopicID
			resp.Topics = append(resp.Topics, mt)
		}
		return encode(resp)
	case protocol.APIKeyFindCoordinator:
		resp := kmsg.NewPtrFindCoordinatorResponse()
		resp.ErrorCode = protocol.REQUEST_TIMED_OUT
		resp.NodeID = -1
		return encode(resp)
	case protocol.APIKeyProduce:
		prodReq := req.(*kmsg.ProduceRequest)
		resp := kmsg.NewPtrProduceResponse()
		for _, topic := range prodReq.Topics {
			rt := kmsg.NewProduceResponseTopic()
			rt.Topic = topic.Topic
			for _, part := range topic.Partitions {
				rp := kmsg.NewProduceResponseTopicPartition()
				rp.Partition = part.Partition
				rp.ErrorCode = protocol.REQUEST_TIMED_OUT
				rp.BaseOffset = -1
				rp.LogAppendTime = -1
				rp.LogStartOffset = -1
				rt.Partitions = append(rt.Partitions, rp)
			}
			resp.Topics = append(resp.Topics, rt)
		}
		return encode(resp)
	case protocol.APIKeyFetch:
		fetchReq := req.(*kmsg.FetchRequest)
		resp := kmsg.NewPtrFetchResponse()
		resp.ErrorCode = protocol.REQUEST_TIMED_OUT
		resp.SessionID = fetchReq.SessionID
		for _, topic := range fetchReq.Topics {
			rt := kmsg.NewFetchResponseTopic()
			rt.Topic = topic.Topic
			rt.TopicID = topic.TopicID
			for _, part := range topic.Partitions {
				rp := kmsg.NewFetchResponseTopicPartition()
				rp.Partition = part.Partition
				rp.ErrorCode = protocol.REQUEST_TIMED_OUT
				rt.Partitions = append(rt.Partitions, rp)
			}
			resp.Topics = append(resp.Topics, rt)
		}
		return encode(resp)
	case protocol.APIKeyListOffsets:
		offsetReq := req.(*kmsg.ListOffsetsRequest)
		resp := kmsg.NewPtrListOffsetsResponse()
		for _, topic := range offsetReq.Topics {
			rt := kmsg.NewListOffsetsResponseTopic()
			rt.Topic = topic.Topic
			for _, part := range topic.Partitions {
				rp := kmsg.NewListOffsetsResponseTopicPartition()
				rp.Partition = part.Partition
				rp.ErrorCode = protocol.REQUEST_TIMED_OUT
				rp.Timestamp = -1
				rp.Offset = -1
				rp.LeaderEpoch = -1
				rt.Partitions = append(rt.Partitions, rp)
			}
			resp.Topics = append(resp.Topics, rt)
		}
		return encode(resp)
	case protocol.APIKeyJoinGroup:
		resp := kmsg.NewPtrJoinGroupResponse()
		resp.ErrorCode = protocol.REQUEST_TIMED_OUT
		resp.Generation = -1
		return encode(resp)
	case protocol.APIKeySyncGroup:
		resp := kmsg.NewPtrSyncGroupResponse()
		resp.ErrorCode = protocol.REQUEST_TIMED_OUT
		return encode(resp)
	case protocol.APIKeyHeartbeat:
		resp := kmsg.NewPtrHeartbeatResponse()
		resp.ErrorCode = protocol.REQUEST_TIMED_OUT
		return encode(resp)
	case protocol.APIKeyLeaveGroup:
		resp := kmsg.NewPtrLeaveGroupResponse()
		resp.ErrorCode = protocol.REQUEST_TIMED_OUT
		return encode(resp)
	case protocol.APIKeyOffsetCommit:
		commitReq := req.(*kmsg.OffsetCommitRequest)
		resp := kmsg.NewPtrOffsetCommitResponse()
		for _, topic := range commitReq.Topics {
			rt := kmsg.NewOffsetCommitResponseTopic()
			rt.Topic = topic.Topic
			for _, part := range topic.Partitions {
				rp := kmsg.NewOffsetCommitResponseTopicPartition()
				rp.Partition = part.Partition
				rp.ErrorCode = protocol.REQUEST_TIMED_OUT
				rt.Partitions = append(rt.Partitions, rp)
			}
			resp.Topics = append(resp.Topics, rt)
		}
		return encode(resp)
	case protocol.APIKeyOffsetFetch:
		ofReq := req.(*kmsg.OffsetFetchRequest)
		resp := kmsg.NewPtrOffsetFetchResponse()
		resp.ErrorCode = protocol.REQUEST_TIMED_OUT
		for _, topic := range ofReq.Topics {
			rt := kmsg.NewOffsetFetchResponseTopic()
			rt.Topic = topic.Topic
			for _, part := range topic.Partitions {
				rp := kmsg.NewOffsetFetchResponseTopicPartition()
				rp.Partition = part
				rp.Offset = -1
				rp.LeaderEpoch = -1
				rp.ErrorCode = protocol.REQUEST_TIMED_OUT
				rt.Partitions = append(rt.Partitions, rp)
			}
			resp.Topics = append(resp.Topics, rt)
		}
		return encode(resp)
	case protocol.APIKeyOffsetForLeaderEpoch:
		epochReq := req.(*kmsg.OffsetForLeaderEpochRequest)
		resp := kmsg.NewPtrOffsetForLeaderEpochResponse()
		for _, topic := range epochReq.Topics {
			rt := kmsg.NewOffsetForLeaderEpochResponseTopic()
			rt.Topic = topic.Topic
			for _, part := range topic.Partitions {
				rp := kmsg.NewOffsetForLeaderEpochResponseTopicPartition()
				rp.Partition = part.Partition
				rp.ErrorCode = protocol.REQUEST_TIMED_OUT
				rp.LeaderEpoch = -1
				rp.EndOffset = -1
				rt.Partitions = append(rt.Partitions, rp)
			}
			resp.Topics = append(resp.Topics, rt)
		}
		return encode(resp)
	case protocol.APIKeyDescribeGroups:
		descReq := req.(*kmsg.DescribeGroupsRequest)
		resp := kmsg.NewPtrDescribeGroupsResponse()
		for _, group := range descReq.Groups {
			rg := kmsg.NewDescribeGroupsResponseGroup()
			rg.ErrorCode = protocol.REQUEST_TIMED_OUT
			rg.Group = group
			resp.Groups = append(resp.Groups, rg)
		}
		return encode(resp)
	case protocol.APIKeyListGroups:
		resp := kmsg.NewPtrListGroupsResponse()
		resp.ErrorCode = protocol.REQUEST_TIMED_OUT
		return encode(resp)
	case protocol.APIKeyDescribeConfigs:
		descReq := req.(*kmsg.DescribeConfigsRequest)
		resp := kmsg.NewPtrDescribeConfigsResponse()
		for _, res := range descReq.Resources {
			rr := kmsg.NewDescribeConfigsResponseResource()
			rr.ErrorCode = protocol.REQUEST_TIMED_OUT
			rr.ResourceType = res.ResourceType
			rr.ResourceName = res.ResourceName
			resp.Resources = append(resp.Resources, rr)
		}
		return encode(resp)
	case protocol.APIKeyAlterConfigs:
		alterReq := req.(*kmsg.AlterConfigsRequest)
		resp := kmsg.NewPtrAlterConfigsResponse()
		for _, res := range alterReq.Resources {
			rr := kmsg.NewAlterConfigsResponseResource()
			rr.ErrorCode = protocol.REQUEST_TIMED_OUT
			rr.ResourceType = res.ResourceType
			rr.ResourceName = res.ResourceName
			resp.Resources = append(resp.Resources, rr)
		}
		return encode(resp)
	case protocol.APIKeyCreatePartitions:
		createReq := req.(*kmsg.CreatePartitionsRequest)
		resp := kmsg.NewPtrCreatePartitionsResponse()
		for _, topic := range createReq.Topics {
			rt := kmsg.NewCreatePartitionsResponseTopic()
			rt.Topic = topic.Topic
			rt.ErrorCode = protocol.REQUEST_TIMED_OUT
			resp.Topics = append(resp.Topics, rt)
		}
		return encode(resp)
	case protocol.APIKeyCreateTopics:
		createReq := req.(*kmsg.CreateTopicsRequest)
		resp := kmsg.NewPtrCreateTopicsResponse()
		for _, topic := range createReq.Topics {
			rt := kmsg.NewCreateTopicsResponseTopic()
			rt.Topic = topic.Topic
			rt.ErrorCode = protocol.REQUEST_TIMED_OUT
			resp.Topics = append(resp.Topics, rt)
		}
		return encode(resp)
	case protocol.APIKeyDeleteTopics:
		delReq := req.(*kmsg.DeleteTopicsRequest)
		resp := kmsg.NewPtrDeleteTopicsResponse()
		for _, t := range delReq.Topics {
			rt := kmsg.NewDeleteTopicsResponseTopic()
			rt.Topic = t.Topic
			rt.TopicID = t.TopicID
			rt.ErrorCode = protocol.REQUEST_TIMED_OUT
			resp.Topics = append(resp.Topics, rt)
		}
		return encode(resp)
	case protocol.APIKeyDeleteGroups:
		delReq := req.(*kmsg.DeleteGroupsRequest)
		resp := kmsg.NewPtrDeleteGroupsResponse()
		for _, group := range delReq.Groups {
			rg := kmsg.NewDeleteGroupsResponseGroup()
			rg.Group = group
			rg.ErrorCode = protocol.REQUEST_TIMED_OUT
			resp.Groups = append(resp.Groups, rg)
		}
		return encode(resp)
	default:
		return nil, false, nil
	}
}

func generateProxyApiVersions() []kmsg.ApiVersionsResponseApiKey {
	supported := []struct {
		key      int16
		min, max int16
	}{
		{key: protocol.APIKeyApiVersion, min: 0, max: 4},
		{key: protocol.APIKeyMetadata, min: 0, max: 12},
		{key: protocol.APIKeyProduce, min: 0, max: 9},
		{key: protocol.APIKeyFetch, min: 11, max: 13},
		{key: protocol.APIKeyFindCoordinator, min: 3, max: 3},
		{key: protocol.APIKeyListOffsets, min: 0, max: 4},
		{key: protocol.APIKeyJoinGroup, min: 4, max: 4},
		{key: protocol.APIKeySyncGroup, min: 4, max: 4},
		{key: protocol.APIKeyHeartbeat, min: 4, max: 4},
		{key: protocol.APIKeyLeaveGroup, min: 4, max: 4},
		{key: protocol.APIKeyOffsetCommit, min: 3, max: 3},
		{key: protocol.APIKeyOffsetFetch, min: 5, max: 5},
		{key: protocol.APIKeyDescribeGroups, min: 5, max: 5},
		{key: protocol.APIKeyListGroups, min: 5, max: 5},
		{key: protocol.APIKeyOffsetForLeaderEpoch, min: 3, max: 3},
		{key: protocol.APIKeyDescribeConfigs, min: 4, max: 4},
		{key: protocol.APIKeyAlterConfigs, min: 1, max: 1},
		{key: protocol.APIKeyCreatePartitions, min: 0, max: 3},
		{key: protocol.APIKeyCreateTopics, min: 0, max: 2},
		{key: protocol.APIKeyDeleteTopics, min: 0, max: 2},
		{key: protocol.APIKeyDeleteGroups, min: 0, max: 2},
	}
	unsupported := []int16{4, 5, 6, 7, 21, 22, 24, 25, 26}
	entries := make([]kmsg.ApiVersionsResponseApiKey, 0, len(supported)+len(unsupported))
	for _, entry := range supported {
		entries = append(entries, kmsg.ApiVersionsResponseApiKey{
			ApiKey:     entry.key,
			MinVersion: entry.min,
			MaxVersion: entry.max,
		})
	}
	for _, key := range unsupported {
		entries = append(entries, kmsg.ApiVersionsResponseApiKey{
			ApiKey:     key,
			MinVersion: -1,
			MaxVersion: -1,
		})
	}
	return entries
}

func buildProxyMetadataResponse(meta *metadata.ClusterMetadata, correlationID int32, version int16, host string, port int32) *kmsg.MetadataResponse {
	brokers := []protocol.MetadataBroker{{
		NodeID: 0,
		Host:   host,
		Port:   port,
	}}
	topics := make([]protocol.MetadataTopic, 0, len(meta.Topics))
	for _, topic := range meta.Topics {
		if topic.ErrorCode != protocol.NONE {
			topics = append(topics, topic)
			continue
		}
		partitions := make([]protocol.MetadataPartition, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			partitions = append(partitions, protocol.MetadataPartition{
				ErrorCode:   part.ErrorCode,
				Partition:   part.Partition,
				Leader:      0,
				LeaderEpoch: part.LeaderEpoch,
				Replicas:    []int32{0},
				ISR:         []int32{0},
			})
		}
		topics = append(topics, protocol.MetadataTopic{
			ErrorCode:  topic.ErrorCode,
			Topic:      topic.Topic,
			TopicID:    topic.TopicID,
			IsInternal: topic.IsInternal,
			Partitions: partitions,
		})
	}
	resp := kmsg.NewPtrMetadataResponse()
	resp.Brokers = brokers
	resp.ClusterID = meta.ClusterID
	resp.ControllerID = 0
	resp.Topics = topics
	return resp
}

func (p *proxy) connectBackend(ctx context.Context) (net.Conn, string, error) {
	return p.connectBackendExcluding(ctx, nil)
}

func (p *proxy) currentBackends(ctx context.Context) ([]string, error) {
	if len(p.backends) > 0 {
		return p.backends, nil
	}
	meta, err := p.store.Metadata(ctx, nil)
	if err != nil {
		return nil, err
	}
	addrs := make([]string, 0, len(meta.Brokers))
	for _, broker := range meta.Brokers {
		if broker.Host == "" || broker.Port == 0 {
			continue
		}
		addrs = append(addrs, fmt.Sprintf("%s:%d", broker.Host, broker.Port))
	}
	if len(addrs) > 0 {
		p.setCachedBackends(addrs)
		p.touchHealthy()
		p.setReady(true)
	}
	p.updateBrokerAddrs(meta.Brokers)
	p.updateTopicNames(meta.Topics)
	return addrs, nil
}

// updateBrokerAddrs rebuilds the broker ID -> address mapping from metadata.
func (p *proxy) updateBrokerAddrs(brokers []protocol.MetadataBroker) {
	brokerAddrs := make(map[string]string, len(brokers))
	for _, broker := range brokers {
		if broker.Host == "" || broker.Port == 0 {
			continue
		}
		brokerAddrs[fmt.Sprintf("%d", broker.NodeID)] = fmt.Sprintf("%s:%d", broker.Host, broker.Port)
	}
	p.brokerAddrMu.Lock()
	p.brokerAddrs = brokerAddrs
	p.brokerAddrMu.Unlock()
}

// refreshMetadataCache updates broker address and topic name caches from
// metadata. Concurrent calls are coalesced via singleflight. Uses a detached
// context so that a single caller's cancellation does not abort the shared fetch.
func (p *proxy) refreshMetadataCache(ctx context.Context) {
	if p.store == nil {
		return
	}
	fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err, _ := p.metaFlight.Do("refresh", func() (interface{}, error) {
		meta, err := p.store.Metadata(fetchCtx, nil)
		if err != nil {
			return nil, err
		}
		p.updateBrokerAddrs(meta.Brokers)
		p.updateTopicNames(meta.Topics)
		p.touchHealthy()
		return nil, nil
	})
	if err != nil {
		p.logger.Warn("metadata cache refresh failed", "error", err)
	}
}

func (p *proxy) updateTopicNames(topics []protocol.MetadataTopic) {
	names := make(map[[16]byte]string, len(topics))
	var zeroID [16]byte
	for _, topic := range topics {
		name := *topic.Topic
		if topic.TopicID != zeroID && name != "" {
			names[topic.TopicID] = name
		}
	}
	p.topicNamesMu.Lock()
	p.topicNames = names
	p.topicNamesMu.Unlock()
}

// resolveTopicID maps topic UUID to name. Triggers a metadata fetch on cache miss.
func (p *proxy) resolveTopicID(ctx context.Context, id [16]byte) string {
	p.topicNamesMu.RLock()
	name := p.topicNames[id]
	p.topicNamesMu.RUnlock()
	if name != "" {
		return name
	}
	p.refreshMetadataCache(ctx)
	p.topicNamesMu.RLock()
	name = p.topicNames[id]
	p.topicNamesMu.RUnlock()
	return name
}

func (p *proxy) forwardToBackend(ctx context.Context, conn net.Conn, backendAddr string, payload []byte) ([]byte, error) {
	if err := protocol.WriteFrame(conn, payload); err != nil {
		return nil, err
	}
	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		return nil, err
	}
	return frame.Payload, nil
}

// handleGroupRouting forwards group requests to the coordination lease owner,
// retrying on NOT_COORDINATOR. DescribeGroups is forwarded once without retry
// since it may span multiple groups on different brokers.
func (p *proxy) handleGroupRouting(ctx context.Context, header *protocol.RequestHeader, payload []byte, pool *connPool) ([]byte, error) {
	groupID := p.extractGroupID(header.APIKey, payload)

	maxAttempts := 3
	if header.APIKey == protocol.APIKeyDescribeGroups {
		maxAttempts = 1
	}

	triedBackends := make(map[string]bool)

	for attempt := 0; attempt < maxAttempts; attempt++ {
		targetAddr := ""
		if p.groupRouter != nil && groupID != "" {
			if ownerID := p.groupRouter.LookupOwner(groupID); ownerID != "" {
				targetAddr = p.brokerIDToAddr(ctx, ownerID)
			}
		}

		conn, actualAddr, err := p.connectForAddr(ctx, targetAddr, triedBackends, pool)
		if err != nil {
			continue
		}
		triedBackends[actualAddr] = true

		resp, err := p.forwardToBackend(ctx, conn, actualAddr, payload)
		if err != nil {
			conn.Close()
			continue
		}

		if p.groupRouter != nil && groupID != "" {
			if ec, ok := protocol.GroupResponseErrorCode(header.APIKey, header.APIVersion, resp); ok && ec == protocol.NOT_COORDINATOR {
				pool.Return(actualAddr, conn)
				p.groupRouter.Invalidate(groupID)
				p.logger.Debug("NOT_COORDINATOR, retrying group request",
					"group", groupID, "attempt", attempt+1, "broker", actualAddr)
				continue
			}
		}

		pool.Return(actualAddr, conn)
		return resp, nil
	}

	return nil, fmt.Errorf("group request for %q failed after %d attempts", groupID, maxAttempts)
}

func (p *proxy) extractGroupID(apiKey int16, payload []byte) string {
	_, req, err := protocol.ParseRequest(payload)
	if err != nil {
		return ""
	}
	switch r := req.(type) {
	case *kmsg.JoinGroupRequest:
		return r.Group
	case *kmsg.SyncGroupRequest:
		return r.Group
	case *kmsg.HeartbeatRequest:
		return r.Group
	case *kmsg.LeaveGroupRequest:
		return r.Group
	case *kmsg.OffsetCommitRequest:
		return r.Group
	case *kmsg.OffsetFetchRequest:
		return r.Group
	case *kmsg.DescribeGroupsRequest:
		if len(r.Groups) > 0 {
			return r.Groups[0]
		}
		return ""
	default:
		return ""
	}
}

// handleFetchRouting splits fetch requests by partition owner, fans out, and
// merges responses. Retries NOT_LEADER_OR_FOLLOWER on a different broker.
func (p *proxy) handleFetchRouting(ctx context.Context, header *protocol.RequestHeader, payload []byte, pool *connPool) ([]byte, error) {
	_, req, err := protocol.ParseRequest(payload)
	if err != nil {
		return p.forwardFetchRaw(ctx, payload, pool)
	}
	fetchReq, ok := req.(*kmsg.FetchRequest)
	if !ok || len(fetchReq.Topics) == 0 {
		return p.forwardFetchRaw(ctx, payload, pool)
	}

	p.resolveFetchTopicNames(ctx, fetchReq)

	groups := p.groupFetchPartitionsByBroker(ctx, fetchReq, nil)
	return p.forwardFetch(ctx, header, fetchReq, payload, groups, pool)
}

// forwardFetchRaw forwards an unparseable fetch payload to any backend.
func (p *proxy) forwardFetchRaw(ctx context.Context, payload []byte, pool *connPool) ([]byte, error) {
	conn, addr, err := p.connectForAddr(ctx, "", nil, pool)
	if err != nil {
		return nil, err
	}
	resp, err := p.forwardToBackend(ctx, conn, addr, payload)
	if err != nil {
		conn.Close()
		return nil, err
	}
	pool.Return(addr, conn)
	return resp, nil
}

// resolveFetchTopicNames resolves topic IDs to names so the partition router
// (which is keyed by name) can look up owners for v12+ requests.
func (p *proxy) resolveFetchTopicNames(ctx context.Context, req *kmsg.FetchRequest) {
	var zeroID [16]byte
	for i := range req.Topics {
		if req.Topics[i].Topic == "" && req.Topics[i].TopicID != zeroID {
			req.Topics[i].Topic = p.resolveTopicID(ctx, req.Topics[i].TopicID)
		}
	}
}

// fetchTopicKey returns name when available, or hex topic ID as fallback.
// Prevents unresolved v12+ topics (all name="") from colliding in maps.
func fetchTopicKey(name string, id [16]byte) string {
	if name != "" {
		return name
	}
	return fmt.Sprintf("id:%x", id)
}

// groupFetchPartitionsByBroker groups partitions by owning broker. If include
// is non-nil, only listed partitions are grouped. Unknown owners go under ""
// for round-robin.
func (p *proxy) groupFetchPartitionsByBroker(ctx context.Context, req *kmsg.FetchRequest, include map[string]map[int32]bool) map[string]*kmsg.FetchRequest {
	groups := make(map[string]*kmsg.FetchRequest)
	topicIndices := make(map[string]map[string]int) // addr -> topicKey -> index in subReq.Topics

	for _, topic := range req.Topics {
		topicName := topic.Topic
		key := fetchTopicKey(topicName, topic.TopicID)
		var includeParts map[int32]bool
		if include != nil {
			includeParts = include[key]
			if len(includeParts) == 0 {
				continue
			}
		}
		for _, part := range topic.Partitions {
			if includeParts != nil && !includeParts[part.Partition] {
				continue
			}
			addr := ""
			if p.router != nil && topicName != "" {
				if ownerID := p.router.LookupOwner(topicName, part.Partition); ownerID != "" {
					addr = p.brokerIDToAddr(ctx, ownerID)
				}
			}
			subReq, ok := groups[addr]
			if !ok {
				subReq = &kmsg.FetchRequest{
					Version:        req.Version,
					ReplicaID:      req.ReplicaID,
					MaxWaitMillis:  req.MaxWaitMillis,
					MinBytes:       req.MinBytes,
					MaxBytes:       req.MaxBytes,
					IsolationLevel: req.IsolationLevel,
					SessionID:      req.SessionID,
					SessionEpoch:   req.SessionEpoch,
				}
				groups[addr] = subReq
				topicIndices[addr] = make(map[string]int)
			}
			idx, ok := topicIndices[addr][key]
			if !ok {
				idx = len(subReq.Topics)
				subReq.Topics = append(subReq.Topics, kmsg.FetchRequestTopic{
					Topic:   topic.Topic,
					TopicID: topic.TopicID,
				})
				topicIndices[addr][key] = idx
			}
			subReq.Topics[idx].Partitions = append(subReq.Topics[idx].Partitions, part)
		}
	}
	return groups
}

type fetchFanOutResult struct {
	subReq  *kmsg.FetchRequest
	subResp *kmsg.FetchResponse
	conn    net.Conn
	target  string
	err     error
}

// forwardFetch fans out sub-requests, merges responses, and retries
// NOT_LEADER_OR_FOLLOWER partitions on a different broker.
func (p *proxy) forwardFetch(ctx context.Context, header *protocol.RequestHeader, fullReq *kmsg.FetchRequest, originalPayload []byte, groups map[string]*kmsg.FetchRequest, pool *connPool) ([]byte, error) {
	const maxRetries = 3

	merged := &kmsg.FetchResponse{
		Version:   header.APIVersion,
		SessionID: fullReq.SessionID,
	}

	var failedPartitions map[string]map[int32]bool
	for attempt := 0; attempt < maxRetries; attempt++ {
		failedPartitions = nil
		triedBackends := make(map[string]bool)
		subResults := p.fanOutFetch(ctx, header, groups, originalPayload, triedBackends, pool)

		for _, r := range subResults {
			if r.err != nil {
				p.logger.Warn("fetch forward failed", "target", r.target, "error", r.err)
				addFetchErrorForAllPartitions(merged, r.subReq, protocol.REQUEST_TIMED_OUT)
				continue
			}
			if r.conn != nil {
				pool.Return(r.target, r.conn)
			}
			if r.subResp.ErrorCode != 0 {
				merged.ErrorCode = r.subResp.ErrorCode
			}
			for _, topic := range r.subResp.Topics {
				for _, part := range topic.Partitions {
					if part.ErrorCode == protocol.NOT_LEADER_OR_FOLLOWER {
						topicName := topic.Topic
						if topicName == "" {
							topicName = p.resolveTopicID(ctx, topic.TopicID)
						}
						key := fetchTopicKey(topicName, topic.TopicID)
						if failedPartitions == nil {
							failedPartitions = make(map[string]map[int32]bool)
						}
						if failedPartitions[key] == nil {
							failedPartitions[key] = make(map[int32]bool)
						}
						failedPartitions[key][part.Partition] = true
						if p.router != nil && topicName != "" {
							p.router.Invalidate(topicName, part.Partition)
						}
					} else {
						tr := findOrAddFetchTopicResponse(merged, topic.Topic, topic.TopicID)
						tr.Partitions = append(tr.Partitions, part)
					}
				}
			}
			if r.subResp.ThrottleMillis > merged.ThrottleMillis {
				merged.ThrottleMillis = r.subResp.ThrottleMillis
			}
		}

		if len(failedPartitions) == 0 {
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, merged), nil
		}

		groups = p.groupFetchPartitionsByBroker(ctx, fullReq, failedPartitions)
		originalPayload = nil
		if len(groups) == 0 {
			break
		}
		p.logger.Debug("retrying NOT_LEADER fetch partitions", "attempt", attempt+1, "partitions", len(failedPartitions))
	}

	for _, topic := range fullReq.Topics {
		key := fetchTopicKey(topic.Topic, topic.TopicID)
		failedParts, ok := failedPartitions[key]
		if !ok {
			continue
		}
		tr := findOrAddFetchTopicResponse(merged, topic.Topic, topic.TopicID)
		for _, part := range topic.Partitions {
			if failedParts[part.Partition] {
				tr.Partitions = append(tr.Partitions, kmsg.FetchResponseTopicPartition{
					Partition: part.Partition,
					ErrorCode: protocol.NOT_LEADER_OR_FOLLOWER,
				})
			}
		}
	}
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, merged), nil
}

// fanOutFetch borrows connections and forwards fetch sub-requests concurrently.
func (p *proxy) fanOutFetch(ctx context.Context, header *protocol.RequestHeader, groups map[string]*kmsg.FetchRequest, originalPayload []byte, triedBackends map[string]bool, pool *connPool) []fetchFanOutResult {
	type workItem struct {
		subReq  *kmsg.FetchRequest
		conn    net.Conn
		target  string
		payload []byte
	}
	work := make([]workItem, 0, len(groups))
	var connectErrors []fetchFanOutResult

	canUseOriginal := originalPayload != nil && len(groups) == 1
	for addr, subReq := range groups {
		conn, targetAddr, err := p.connectForAddr(ctx, addr, triedBackends, pool)
		if err != nil {
			connectErrors = append(connectErrors, fetchFanOutResult{subReq: subReq, target: addr, err: err})
			continue
		}
		triedBackends[targetAddr] = true

		var payload []byte
		if canUseOriginal {
			payload = originalPayload
		} else {
			payload = encodeFetchRequest(header, subReq)
		}
		work = append(work, workItem{subReq: subReq, conn: conn, target: targetAddr, payload: payload})
	}

	results := make([]fetchFanOutResult, len(work))
	var wg sync.WaitGroup
	for i := range work {
		i := i
		w := work[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			respBytes, err := p.forwardToBackend(ctx, w.conn, w.target, w.payload)
			if err != nil {
				w.conn.Close()
				results[i] = fetchFanOutResult{subReq: w.subReq, target: w.target, err: err}
				return
			}
			subResp, parseErr := parseFetchResponse(respBytes, header.APIVersion)
			if parseErr != nil {
				w.conn.Close()
				results[i] = fetchFanOutResult{subReq: w.subReq, target: w.target, err: parseErr}
				return
			}
			results[i] = fetchFanOutResult{subReq: w.subReq, subResp: subResp, conn: w.conn, target: w.target}
		}()
	}
	wg.Wait()

	return append(connectErrors, results...)
}

func findOrAddFetchTopicResponse(resp *kmsg.FetchResponse, name string, topicID [16]byte) *kmsg.FetchResponseTopic {
	var zeroID [16]byte
	for i := range resp.Topics {
		if topicID != zeroID {
			if resp.Topics[i].TopicID == topicID {
				return &resp.Topics[i]
			}
		} else {
			if resp.Topics[i].Topic == name {
				return &resp.Topics[i]
			}
		}
	}
	resp.Topics = append(resp.Topics, kmsg.FetchResponseTopic{Topic: name, TopicID: topicID})
	return &resp.Topics[len(resp.Topics)-1]
}

func addFetchErrorForAllPartitions(resp *kmsg.FetchResponse, req *kmsg.FetchRequest, errorCode int16) {
	for _, topic := range req.Topics {
		tr := findOrAddFetchTopicResponse(resp, topic.Topic, topic.TopicID)
		for _, part := range topic.Partitions {
			tr.Partitions = append(tr.Partitions, kmsg.FetchResponseTopicPartition{
				Partition: part.Partition,
				ErrorCode: errorCode,
			})
		}
	}
}

// encodeProduceRequest serializes a produce request with header into a wire frame.
func encodeProduceRequest(header *protocol.RequestHeader, req *kmsg.ProduceRequest) []byte {
	formatter := kmsg.NewRequestFormatter(kmsg.FormatterClientID(clientIDStr(header.ClientID)))
	return formatter.AppendRequest(nil, req, header.CorrelationID)
}

// parseProduceResponse deserializes a produce response from wire bytes.
func parseProduceResponse(data []byte, version int16) (*kmsg.ProduceResponse, error) {
	body, ok := protocol.SkipResponseHeader(protocol.APIKeyProduce, version, data)
	if !ok {
		return nil, fmt.Errorf("produce response too short or malformed header")
	}
	resp := kmsg.NewPtrProduceResponse()
	resp.SetVersion(version)
	if err := resp.ReadFrom(body); err != nil {
		return nil, fmt.Errorf("decode produce response v%d: %w", version, err)
	}
	return resp, nil
}

// encodeFetchRequest serializes a fetch request with header into a wire frame.
func encodeFetchRequest(header *protocol.RequestHeader, req *kmsg.FetchRequest) []byte {
	formatter := kmsg.NewRequestFormatter(kmsg.FormatterClientID(clientIDStr(header.ClientID)))
	return formatter.AppendRequest(nil, req, header.CorrelationID)
}

// parseFetchResponse deserializes a fetch response from wire bytes.
func parseFetchResponse(data []byte, version int16) (*kmsg.FetchResponse, error) {
	body, ok := protocol.SkipResponseHeader(protocol.APIKeyFetch, version, data)
	if !ok {
		return nil, fmt.Errorf("fetch response too short or malformed header")
	}
	resp := kmsg.NewPtrFetchResponse()
	resp.SetVersion(version)
	if err := resp.ReadFrom(body); err != nil {
		return nil, fmt.Errorf("decode fetch response v%d: %w", version, err)
	}
	return resp, nil
}

func clientIDStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
