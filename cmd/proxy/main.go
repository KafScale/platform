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

	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/protocol"
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
	apiVersions    []protocol.ApiVersion
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
	}
	if len(backends) > 0 {
		p.setCachedBackends(backends)
		p.touchHealthy()
		p.setReady(true)
	}
	p.startBackendRefresh(ctx, backendBackoff)
	if healthAddr != "" {
		p.startHealthServer(ctx, healthAddr)
	}
	if err := p.listenAndServe(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("proxy server error", "error", err)
		os.Exit(1)
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
	return atomic.LoadUint32(&p.ready) == 1 && p.cacheFresh()
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

func (p *proxy) startBackendRefresh(ctx context.Context, backoff time.Duration) {
	if p.store == nil || len(p.backends) > 0 {
		return
	}
	if backoff <= 0 {
		backoff = 500 * time.Millisecond
	}
	ticker := time.NewTicker(3 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, err := p.refreshBackends(ctx)
				if err != nil {
					if !p.cacheFresh() {
						p.setReady(false)
					}
					time.Sleep(backoff)
				}
			}
		}
	}()
}

func (p *proxy) refreshBackends(ctx context.Context) ([]string, error) {
	backends, err := p.currentBackends(ctx)
	if err != nil {
		return nil, err
	}
	if len(backends) > 0 {
		p.touchHealthy()
		p.setReady(true)
	}
	return backends, nil
}

func (p *proxy) startHealthServer(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if p.isReady() || (len(p.cachedBackendsSnapshot()) > 0 && p.cacheFresh()) {
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

	for {
		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			return
		}
		header, _, err := protocol.ParseRequestHeader(frame.Payload)
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
			resp, ok, err := p.buildNotReadyResponse(header, frame.Payload)
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

func (p *proxy) handleApiVersions(header *protocol.RequestHeader) ([]byte, error) {
	resp := &protocol.ApiVersionsResponse{
		CorrelationID: header.CorrelationID,
		ErrorCode:     protocol.NONE,
		ThrottleMs:    0,
		Versions:      p.apiVersions,
	}
	return protocol.EncodeApiVersionsResponse(resp, header.APIVersion)
}

func (p *proxy) respondBackendError(conn net.Conn, header *protocol.RequestHeader, payload []byte) {
	resp, ok, err := p.buildNotReadyResponse(header, payload)
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
	metaReq, ok := req.(*protocol.MetadataRequest)
	if !ok {
		return nil, fmt.Errorf("unexpected metadata request type %T", req)
	}

	meta, err := p.loadMetadata(ctx, metaReq)
	if err != nil {
		return nil, err
	}
	resp := buildProxyMetadataResponse(meta, header.CorrelationID, header.APIVersion, p.advertisedHost, p.advertisedPort)
	return protocol.EncodeMetadataResponse(resp, header.APIVersion)
}

func (p *proxy) handleFindCoordinator(header *protocol.RequestHeader) ([]byte, error) {
	resp := &protocol.FindCoordinatorResponse{
		CorrelationID: header.CorrelationID,
		ThrottleMs:    0,
		ErrorCode:     protocol.NONE,
		NodeID:        0,
		Host:          p.advertisedHost,
		Port:          p.advertisedPort,
		ErrorMessage:  nil,
	}
	return protocol.EncodeFindCoordinatorResponse(resp, header.APIVersion)
}

func (p *proxy) loadMetadata(ctx context.Context, req *protocol.MetadataRequest) (*metadata.ClusterMetadata, error) {
	useIDs := false
	zeroID := [16]byte{}
	for _, id := range req.TopicIDs {
		if id != zeroID {
			useIDs = true
			break
		}
	}
	if !useIDs {
		return p.store.Metadata(ctx, req.Topics)
	}
	all, err := p.store.Metadata(ctx, nil)
	if err != nil {
		return nil, err
	}
	index := make(map[[16]byte]protocol.MetadataTopic, len(all.Topics))
	for _, topic := range all.Topics {
		index[topic.TopicID] = topic
	}
	filtered := make([]protocol.MetadataTopic, 0, len(req.TopicIDs))
	for _, id := range req.TopicIDs {
		if id == zeroID {
			continue
		}
		if topic, ok := index[id]; ok {
			filtered = append(filtered, topic)
		} else {
			filtered = append(filtered, protocol.MetadataTopic{
				ErrorCode: protocol.UNKNOWN_TOPIC_ID,
				TopicID:   id,
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

func (p *proxy) buildNotReadyResponse(header *protocol.RequestHeader, payload []byte) ([]byte, bool, error) {
	_, req, err := protocol.ParseRequest(payload)
	if err != nil {
		return nil, false, err
	}
	wrapEncode := func(payload []byte, err error) ([]byte, bool, error) {
		return payload, true, err
	}
	switch header.APIKey {
	case protocol.APIKeyMetadata:
		metaReq := req.(*protocol.MetadataRequest)
		topics := make([]protocol.MetadataTopic, 0, len(metaReq.Topics)+len(metaReq.TopicIDs))
		for _, name := range metaReq.Topics {
			topics = append(topics, protocol.MetadataTopic{
				ErrorCode: protocol.REQUEST_TIMED_OUT,
				Name:      name,
			})
		}
		for _, id := range metaReq.TopicIDs {
			topics = append(topics, protocol.MetadataTopic{
				ErrorCode: protocol.REQUEST_TIMED_OUT,
				TopicID:   id,
			})
		}
		resp := &protocol.MetadataResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			Brokers:       nil,
			ClusterID:     nil,
			ControllerID:  -1,
			Topics:        topics,
		}
		return wrapEncode(protocol.EncodeMetadataResponse(resp, header.APIVersion))
	case protocol.APIKeyFindCoordinator:
		resp := &protocol.FindCoordinatorResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			ErrorCode:     protocol.REQUEST_TIMED_OUT,
			ErrorMessage:  nil,
			NodeID:        -1,
			Host:          "",
			Port:          0,
		}
		return wrapEncode(protocol.EncodeFindCoordinatorResponse(resp, header.APIVersion))
	case protocol.APIKeyProduce:
		prodReq := req.(*protocol.ProduceRequest)
		topics := make([]protocol.ProduceTopicResponse, 0, len(prodReq.Topics))
		for _, topic := range prodReq.Topics {
			partitions := make([]protocol.ProducePartitionResponse, 0, len(topic.Partitions))
			for _, part := range topic.Partitions {
				partitions = append(partitions, protocol.ProducePartitionResponse{
					Partition:       part.Partition,
					ErrorCode:       protocol.REQUEST_TIMED_OUT,
					BaseOffset:      -1,
					LogAppendTimeMs: -1,
					LogStartOffset:  -1,
				})
			}
			topics = append(topics, protocol.ProduceTopicResponse{
				Name:       topic.Name,
				Partitions: partitions,
			})
		}
		resp := &protocol.ProduceResponse{
			CorrelationID: header.CorrelationID,
			Topics:        topics,
			ThrottleMs:    0,
		}
		return wrapEncode(protocol.EncodeProduceResponse(resp, header.APIVersion))
	case protocol.APIKeyFetch:
		fetchReq := req.(*protocol.FetchRequest)
		topics := make([]protocol.FetchTopicResponse, 0, len(fetchReq.Topics))
		for _, topic := range fetchReq.Topics {
			partitions := make([]protocol.FetchPartitionResponse, 0, len(topic.Partitions))
			for _, part := range topic.Partitions {
				partitions = append(partitions, protocol.FetchPartitionResponse{
					Partition:     part.Partition,
					ErrorCode:     protocol.REQUEST_TIMED_OUT,
					HighWatermark: 0,
				})
			}
			topics = append(topics, protocol.FetchTopicResponse{
				Name:       topic.Name,
				TopicID:    topic.TopicID,
				Partitions: partitions,
			})
		}
		resp := &protocol.FetchResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			ErrorCode:     protocol.REQUEST_TIMED_OUT,
			SessionID:     fetchReq.SessionID,
			Topics:        topics,
		}
		return wrapEncode(protocol.EncodeFetchResponse(resp, header.APIVersion))
	case protocol.APIKeyListOffsets:
		offsetReq := req.(*protocol.ListOffsetsRequest)
		topics := make([]protocol.ListOffsetsTopicResponse, 0, len(offsetReq.Topics))
		for _, topic := range offsetReq.Topics {
			partitions := make([]protocol.ListOffsetsPartitionResponse, 0, len(topic.Partitions))
			for _, part := range topic.Partitions {
				partitions = append(partitions, protocol.ListOffsetsPartitionResponse{
					Partition:       part.Partition,
					ErrorCode:       protocol.REQUEST_TIMED_OUT,
					Timestamp:       -1,
					Offset:          -1,
					LeaderEpoch:     -1,
					OldStyleOffsets: nil,
				})
			}
			topics = append(topics, protocol.ListOffsetsTopicResponse{
				Name:       topic.Name,
				Partitions: partitions,
			})
		}
		resp := &protocol.ListOffsetsResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			Topics:        topics,
		}
		return wrapEncode(protocol.EncodeListOffsetsResponse(header.APIVersion, resp))
	case protocol.APIKeyJoinGroup:
		resp := &protocol.JoinGroupResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			ErrorCode:     protocol.REQUEST_TIMED_OUT,
			GenerationID:  -1,
			ProtocolName:  "",
			LeaderID:      "",
			MemberID:      "",
			Members:       nil,
		}
		return wrapEncode(protocol.EncodeJoinGroupResponse(resp, header.APIVersion))
	case protocol.APIKeySyncGroup:
		resp := &protocol.SyncGroupResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			ErrorCode:     protocol.REQUEST_TIMED_OUT,
			ProtocolType:  nil,
			ProtocolName:  nil,
			Assignment:    nil,
		}
		return wrapEncode(protocol.EncodeSyncGroupResponse(resp, header.APIVersion))
	case protocol.APIKeyHeartbeat:
		resp := &protocol.HeartbeatResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			ErrorCode:     protocol.REQUEST_TIMED_OUT,
		}
		return wrapEncode(protocol.EncodeHeartbeatResponse(resp, header.APIVersion))
	case protocol.APIKeyLeaveGroup:
		resp := &protocol.LeaveGroupResponse{
			CorrelationID: header.CorrelationID,
			ErrorCode:     protocol.REQUEST_TIMED_OUT,
		}
		return wrapEncode(protocol.EncodeLeaveGroupResponse(resp))
	case protocol.APIKeyOffsetCommit:
		commitReq := req.(*protocol.OffsetCommitRequest)
		topics := make([]protocol.OffsetCommitTopicResponse, 0, len(commitReq.Topics))
		for _, topic := range commitReq.Topics {
			partitions := make([]protocol.OffsetCommitPartitionResponse, 0, len(topic.Partitions))
			for _, part := range topic.Partitions {
				partitions = append(partitions, protocol.OffsetCommitPartitionResponse{
					Partition: part.Partition,
					ErrorCode: protocol.REQUEST_TIMED_OUT,
				})
			}
			topics = append(topics, protocol.OffsetCommitTopicResponse{
				Name:       topic.Name,
				Partitions: partitions,
			})
		}
		resp := &protocol.OffsetCommitResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			Topics:        topics,
		}
		return wrapEncode(protocol.EncodeOffsetCommitResponse(resp))
	case protocol.APIKeyOffsetFetch:
		fetchReq := req.(*protocol.OffsetFetchRequest)
		topics := make([]protocol.OffsetFetchTopicResponse, 0, len(fetchReq.Topics))
		for _, topic := range fetchReq.Topics {
			partitions := make([]protocol.OffsetFetchPartitionResponse, 0, len(topic.Partitions))
			for _, part := range topic.Partitions {
				partitions = append(partitions, protocol.OffsetFetchPartitionResponse{
					Partition:   part.Partition,
					Offset:      -1,
					LeaderEpoch: -1,
					Metadata:    nil,
					ErrorCode:   protocol.REQUEST_TIMED_OUT,
				})
			}
			topics = append(topics, protocol.OffsetFetchTopicResponse{
				Name:       topic.Name,
				Partitions: partitions,
			})
		}
		resp := &protocol.OffsetFetchResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			Topics:        topics,
			ErrorCode:     protocol.REQUEST_TIMED_OUT,
		}
		return wrapEncode(protocol.EncodeOffsetFetchResponse(resp, header.APIVersion))
	case protocol.APIKeyOffsetForLeaderEpoch:
		epochReq := req.(*protocol.OffsetForLeaderEpochRequest)
		topics := make([]protocol.OffsetForLeaderEpochTopicResponse, 0, len(epochReq.Topics))
		for _, topic := range epochReq.Topics {
			partitions := make([]protocol.OffsetForLeaderEpochPartitionResponse, 0, len(topic.Partitions))
			for _, part := range topic.Partitions {
				partitions = append(partitions, protocol.OffsetForLeaderEpochPartitionResponse{
					Partition:   part.Partition,
					ErrorCode:   protocol.REQUEST_TIMED_OUT,
					LeaderEpoch: -1,
					EndOffset:   -1,
				})
			}
			topics = append(topics, protocol.OffsetForLeaderEpochTopicResponse{
				Name:       topic.Name,
				Partitions: partitions,
			})
		}
		resp := &protocol.OffsetForLeaderEpochResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			Topics:        topics,
		}
		return wrapEncode(protocol.EncodeOffsetForLeaderEpochResponse(resp, header.APIVersion))
	case protocol.APIKeyDescribeGroups:
		descReq := req.(*protocol.DescribeGroupsRequest)
		groups := make([]protocol.DescribeGroupsResponseGroup, 0, len(descReq.Groups))
		for _, group := range descReq.Groups {
			groups = append(groups, protocol.DescribeGroupsResponseGroup{
				ErrorCode:            protocol.REQUEST_TIMED_OUT,
				GroupID:              group,
				State:                "",
				ProtocolType:         "",
				Protocol:             "",
				Members:              nil,
				AuthorizedOperations: 0,
			})
		}
		resp := &protocol.DescribeGroupsResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			Groups:        groups,
		}
		return wrapEncode(protocol.EncodeDescribeGroupsResponse(resp, header.APIVersion))
	case protocol.APIKeyListGroups:
		resp := &protocol.ListGroupsResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			ErrorCode:     protocol.REQUEST_TIMED_OUT,
			Groups:        nil,
		}
		return wrapEncode(protocol.EncodeListGroupsResponse(resp, header.APIVersion))
	case protocol.APIKeyDescribeConfigs:
		descReq := req.(*protocol.DescribeConfigsRequest)
		resources := make([]protocol.DescribeConfigsResponseResource, 0, len(descReq.Resources))
		for _, res := range descReq.Resources {
			resources = append(resources, protocol.DescribeConfigsResponseResource{
				ErrorCode:    protocol.REQUEST_TIMED_OUT,
				ErrorMessage: nil,
				ResourceType: res.ResourceType,
				ResourceName: res.ResourceName,
				Configs:      nil,
			})
		}
		resp := &protocol.DescribeConfigsResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			Resources:     resources,
		}
		return wrapEncode(protocol.EncodeDescribeConfigsResponse(resp, header.APIVersion))
	case protocol.APIKeyAlterConfigs:
		alterReq := req.(*protocol.AlterConfigsRequest)
		resources := make([]protocol.AlterConfigsResponseResource, 0, len(alterReq.Resources))
		for _, res := range alterReq.Resources {
			resources = append(resources, protocol.AlterConfigsResponseResource{
				ErrorCode:    protocol.REQUEST_TIMED_OUT,
				ErrorMessage: nil,
				ResourceType: res.ResourceType,
				ResourceName: res.ResourceName,
			})
		}
		resp := &protocol.AlterConfigsResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			Resources:     resources,
		}
		return wrapEncode(protocol.EncodeAlterConfigsResponse(resp, header.APIVersion))
	case protocol.APIKeyCreatePartitions:
		createReq := req.(*protocol.CreatePartitionsRequest)
		topics := make([]protocol.CreatePartitionsResponseTopic, 0, len(createReq.Topics))
		for _, topic := range createReq.Topics {
			topics = append(topics, protocol.CreatePartitionsResponseTopic{
				Name:         topic.Name,
				ErrorCode:    protocol.REQUEST_TIMED_OUT,
				ErrorMessage: nil,
			})
		}
		resp := &protocol.CreatePartitionsResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			Topics:        topics,
		}
		return wrapEncode(protocol.EncodeCreatePartitionsResponse(resp, header.APIVersion))
	case protocol.APIKeyCreateTopics:
		createReq := req.(*protocol.CreateTopicsRequest)
		topics := make([]protocol.CreateTopicResult, 0, len(createReq.Topics))
		for _, topic := range createReq.Topics {
			topics = append(topics, protocol.CreateTopicResult{
				Name:         topic.Name,
				ErrorCode:    protocol.REQUEST_TIMED_OUT,
				ErrorMessage: "",
			})
		}
		resp := &protocol.CreateTopicsResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			Topics:        topics,
		}
		return wrapEncode(protocol.EncodeCreateTopicsResponse(resp, header.APIVersion))
	case protocol.APIKeyDeleteTopics:
		delReq := req.(*protocol.DeleteTopicsRequest)
		topics := make([]protocol.DeleteTopicResult, 0, len(delReq.TopicNames))
		for _, name := range delReq.TopicNames {
			topics = append(topics, protocol.DeleteTopicResult{
				Name:         name,
				ErrorCode:    protocol.REQUEST_TIMED_OUT,
				ErrorMessage: "",
			})
		}
		resp := &protocol.DeleteTopicsResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			Topics:        topics,
		}
		return wrapEncode(protocol.EncodeDeleteTopicsResponse(resp, header.APIVersion))
	case protocol.APIKeyDeleteGroups:
		delReq := req.(*protocol.DeleteGroupsRequest)
		groups := make([]protocol.DeleteGroupsResponseGroup, 0, len(delReq.Groups))
		for _, group := range delReq.Groups {
			groups = append(groups, protocol.DeleteGroupsResponseGroup{
				Group:     group,
				ErrorCode: protocol.REQUEST_TIMED_OUT,
			})
		}
		resp := &protocol.DeleteGroupsResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			Groups:        groups,
		}
		return wrapEncode(protocol.EncodeDeleteGroupsResponse(resp, header.APIVersion))
	default:
		return nil, false, nil
	}
}

func generateProxyApiVersions() []protocol.ApiVersion {
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
	entries := make([]protocol.ApiVersion, 0, len(supported)+len(unsupported))
	for _, entry := range supported {
		entries = append(entries, protocol.ApiVersion{
			APIKey:     entry.key,
			MinVersion: entry.min,
			MaxVersion: entry.max,
		})
	}
	for _, key := range unsupported {
		entries = append(entries, protocol.ApiVersion{
			APIKey:     key,
			MinVersion: -1,
			MaxVersion: -1,
		})
	}
	return entries
}

func buildProxyMetadataResponse(meta *metadata.ClusterMetadata, correlationID int32, version int16, host string, port int32) *protocol.MetadataResponse {
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
				ErrorCode:      part.ErrorCode,
				PartitionIndex: part.PartitionIndex,
				LeaderID:       0,
				LeaderEpoch:    part.LeaderEpoch,
				ReplicaNodes:   []int32{0},
				ISRNodes:       []int32{0},
			})
		}
		topics = append(topics, protocol.MetadataTopic{
			ErrorCode:  topic.ErrorCode,
			Name:       topic.Name,
			TopicID:    topic.TopicID,
			IsInternal: topic.IsInternal,
			Partitions: partitions,
		})
	}
	return &protocol.MetadataResponse{
		CorrelationID: correlationID,
		ThrottleMs:    0,
		Brokers:       brokers,
		ClusterID:     meta.ClusterID,
		ControllerID:  0,
		Topics:        topics,
	}
}

func (p *proxy) connectBackend(ctx context.Context) (net.Conn, string, error) {
	retries := envInt("KAFSCALE_PROXY_BACKEND_RETRIES", 6)
	if retries < 1 {
		retries = 1
	}
	backoff := time.Duration(envInt("KAFSCALE_PROXY_BACKEND_BACKOFF_MS", 500)) * time.Millisecond
	if backoff <= 0 {
		backoff = 500 * time.Millisecond
	}
	var lastErr error
	for attempt := 0; attempt < retries; attempt++ {
		backends, err := p.currentBackends(ctx)
		if err != nil || len(backends) == 0 {
			if cached := p.cachedBackendsSnapshot(); len(cached) > 0 && p.cacheFresh() {
				backends = cached
				err = nil
			}
		}
		if err != nil || len(backends) == 0 {
			lastErr = err
			time.Sleep(backoff)
			continue
		}
		index := atomic.AddUint32(&p.rr, 1)
		addr := backends[int(index)%len(backends)]
		dialer := net.Dialer{Timeout: p.dialTimeout}
		conn, dialErr := dialer.DialContext(ctx, "tcp", addr)
		if dialErr == nil {
			return conn, addr, nil
		}
		lastErr = dialErr
		time.Sleep(backoff)
	}
	if lastErr == nil {
		lastErr = errors.New("no backends available")
	}
	return nil, "", lastErr
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
	return addrs, nil
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
