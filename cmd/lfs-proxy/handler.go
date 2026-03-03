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
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (p *lfsProxy) listenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", p.addr)
	if err != nil {
		return err
	}
	p.logger.Info("lfs proxy listening", "addr", ln.Addr().String())

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
		p.logger.Debug("connection accepted", "remote", conn.RemoteAddr().String())
		go p.handleConnection(ctx, conn)
	}
}

func (p *lfsProxy) setReady(ready bool) {
	prev := atomic.LoadUint32(&p.ready)
	if ready {
		atomic.StoreUint32(&p.ready, 1)
		if prev == 0 {
			p.logger.Info("proxy ready state changed", "ready", true)
		}
		return
	}
	atomic.StoreUint32(&p.ready, 0)
	if prev == 1 {
		p.logger.Warn("proxy ready state changed", "ready", false)
	}
}

func (p *lfsProxy) isReady() bool {
	readyFlag := atomic.LoadUint32(&p.ready) == 1
	cacheFresh := p.cacheFresh()
	s3Healthy := p.isS3Healthy()
	ready := readyFlag && cacheFresh && s3Healthy
	if !ready {
		p.logger.Debug("ready check failed", "readyFlag", readyFlag, "cacheFresh", cacheFresh, "s3Healthy", s3Healthy)
	}
	return ready
}

func (p *lfsProxy) markS3Healthy(ok bool) {
	if ok {
		atomic.StoreUint32(&p.s3Healthy, 1)
		return
	}
	atomic.StoreUint32(&p.s3Healthy, 0)
}

func (p *lfsProxy) isS3Healthy() bool {
	return atomic.LoadUint32(&p.s3Healthy) == 1
}

func (p *lfsProxy) startS3HealthCheck(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Duration(defaultS3HealthIntervalSec) * time.Second
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := p.s3Uploader.HeadBucket(ctx)
				wasHealthy := p.isS3Healthy()
				p.markS3Healthy(err == nil)
				if err != nil && wasHealthy {
					p.logger.Warn("s3 health check failed", "error", err)
				} else if err == nil && !wasHealthy {
					p.logger.Info("s3 health check recovered")
				}
			}
		}
	}()
}

func (p *lfsProxy) setCachedBackends(backends []string) {
	if len(backends) == 0 {
		return
	}
	copied := make([]string, len(backends))
	copy(copied, backends)
	p.cacheMu.Lock()
	p.cachedBackends = copied
	p.cacheMu.Unlock()
}

func (p *lfsProxy) cachedBackendsSnapshot() []string {
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

func (p *lfsProxy) touchHealthy() {
	atomic.StoreInt64(&p.lastHealthy, time.Now().UnixNano())
}

func (p *lfsProxy) cacheFresh() bool {
	// Static backends are always fresh (no TTL expiry)
	if len(p.backends) > 0 {
		return true
	}
	last := atomic.LoadInt64(&p.lastHealthy)
	if last == 0 {
		return false
	}
	return time.Since(time.Unix(0, last)) <= p.cacheTTL
}

func (p *lfsProxy) startBackendRefresh(ctx context.Context, backoff time.Duration, interval time.Duration) {
	if p.store == nil || len(p.backends) > 0 {
		p.logger.Debug("backend refresh disabled", "hasStore", p.store != nil, "staticBackends", len(p.backends))
		return
	}
	if backoff <= 0 {
		backoff = time.Duration(defaultBackendBackoffMs) * time.Millisecond
	}
	if interval <= 0 {
		interval = time.Duration(defaultBackendRefreshIntervalSec) * time.Second
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				backends, err := p.refreshBackends(ctx)
				if err != nil {
					p.logger.Warn("backend refresh failed", "error", err)
					if !p.cacheFresh() {
						p.setReady(false)
					}
					time.Sleep(backoff)
				} else {
					p.logger.Debug("backend refresh succeeded", "count", len(backends))
				}
			}
		}
	}()
}

func (p *lfsProxy) refreshBackends(ctx context.Context) ([]string, error) {
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

func (p *lfsProxy) startHealthServer(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if p.isReady() || (len(p.cachedBackendsSnapshot()) > 0 && p.cacheFresh() && p.isS3Healthy()) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready\n"))
			return
		}
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
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
		p.logger.Info("lfs proxy health listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			p.logger.Warn("lfs proxy health server error", "error", err)
		}
	}()
}

func (p *lfsProxy) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	var backendConn net.Conn
	var backendAddr string

	for {
		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			p.logger.Debug("connection read ended", "remote", conn.RemoteAddr().String(), "error", err)
			return
		}
		header, _, err := protocol.ParseRequestHeader(frame.Payload)
		if err != nil {
			p.logger.Warn("parse request header failed", "error", err)
			return
		}
		p.logger.Debug("request received", "apiKey", header.APIKey, "correlationId", header.CorrelationID, "remote", conn.RemoteAddr().String())

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
			p.logger.Warn("rejecting request: proxy not ready", "apiKey", header.APIKey, "remote", conn.RemoteAddr().String())
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
		case protocol.APIKeyProduce:
			resp, handled, err := p.handleProduce(ctx, header, frame.Payload)
			if err != nil {
				p.logger.Warn("produce handling failed", "error", err)
				if resp != nil {
					_ = protocol.WriteFrame(conn, resp)
				}
				return
			}
			if handled {
				if err := protocol.WriteFrame(conn, resp); err != nil {
					p.logger.Warn("write produce response failed", "error", err)
				}
				continue
			}
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

func (p *lfsProxy) handleApiVersions(header *protocol.RequestHeader) ([]byte, error) {
	resp := &protocol.ApiVersionsResponse{
		CorrelationID: header.CorrelationID,
		ErrorCode:     protocol.NONE,
		ThrottleMs:    0,
		Versions:      p.apiVersions,
	}
	return protocol.EncodeApiVersionsResponse(resp, header.APIVersion)
}

func (p *lfsProxy) respondBackendError(conn net.Conn, header *protocol.RequestHeader, payload []byte) {
	resp, ok, err := p.buildNotReadyResponse(header, payload)
	if err != nil || !ok {
		return
	}
	_ = protocol.WriteFrame(conn, resp)
}

func (p *lfsProxy) handleMetadata(ctx context.Context, header *protocol.RequestHeader, payload []byte) ([]byte, error) {
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
	p.logger.Debug("metadata response", "advertisedHost", p.advertisedHost, "advertisedPort", p.advertisedPort, "topics", len(meta.Topics))
	resp := buildProxyMetadataResponse(meta, header.CorrelationID, header.APIVersion, p.advertisedHost, p.advertisedPort)
	return protocol.EncodeMetadataResponse(resp, header.APIVersion)
}

func (p *lfsProxy) handleFindCoordinator(header *protocol.RequestHeader) ([]byte, error) {
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

func (p *lfsProxy) loadMetadata(ctx context.Context, req *protocol.MetadataRequest) (*metadata.ClusterMetadata, error) {
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

func (p *lfsProxy) handleProduce(ctx context.Context, header *protocol.RequestHeader, payload []byte) ([]byte, bool, error) {
	start := time.Now()
	_, req, err := protocol.ParseRequest(payload)
	if err != nil {
		return nil, false, err
	}
	prodReq, ok := req.(*protocol.ProduceRequest)
	if !ok {
		return nil, false, fmt.Errorf("unexpected produce request type %T", req)
	}

	p.logger.Debug("handling produce request", "topics", topicsFromProduce(prodReq))
	lfsResult, err := p.rewriteProduceRecords(ctx, header, prodReq)
	if err != nil {
		for _, topic := range topicsFromProduce(prodReq) {
			p.metrics.IncRequests(topic, "error", "lfs")
		}
		resp, errResp := buildProduceErrorResponse(prodReq, header.CorrelationID, header.APIVersion, protocol.UNKNOWN_SERVER_ERROR)
		if errResp != nil {
			return nil, true, err
		}
		return resp, true, err
	}
	if !lfsResult.modified {
		for _, topic := range topicsFromProduce(prodReq) {
			p.metrics.IncRequests(topic, "ok", "passthrough")
		}
		return nil, false, nil
	}
	for topic := range lfsResult.topics {
		p.metrics.IncRequests(topic, "ok", "lfs")
	}
	p.metrics.ObserveUploadDuration(time.Since(start).Seconds())
	p.metrics.AddUploadBytes(lfsResult.uploadBytes)

	backendConn, backendAddr, err := p.connectBackend(ctx)
	if err != nil {
		p.trackOrphans(lfsResult.orphans)
		return nil, true, err
	}
	defer backendConn.Close()

	resp, err := p.forwardToBackend(ctx, backendConn, backendAddr, lfsResult.payload)
	if err != nil {
		p.trackOrphans(lfsResult.orphans)
	}
	return resp, true, err
}

func (p *lfsProxy) rewriteProduceRecords(ctx context.Context, header *protocol.RequestHeader, req *protocol.ProduceRequest) (rewriteResult, error) {
	if p.logger == nil {
		p.logger = slog.Default()
	}

	if req == nil {
		return rewriteResult{}, errors.New("nil produce request")
	}

	modified := false
	uploadBytes := int64(0)
	decompressor := kgo.DefaultDecompressor()
	topics := make(map[string]struct{})
	orphans := make([]orphanInfo, 0, 4)

	for ti := range req.Topics {
		topic := &req.Topics[ti]
		for pi := range topic.Partitions {
			partition := &topic.Partitions[pi]
			if len(partition.Records) == 0 {
				continue
			}
			batches, err := decodeRecordBatches(partition.Records)
			if err != nil {
				return rewriteResult{}, err
			}
			batchModified := false
			for bi := range batches {
				batch := &batches[bi]
				records, codec, err := decodeBatchRecords(batch, decompressor)
				if err != nil {
					return rewriteResult{}, err
				}
				if len(records) == 0 {
					continue
				}
				recordChanged := false
				for ri := range records {
					rec := &records[ri]
					headers := rec.Headers
					lfsValue, ok := findHeaderValue(headers, "LFS_BLOB")
					if !ok {
						continue
					}
					recordChanged = true
					modified = true
					topics[topic.Name] = struct{}{}
					checksumHeader := strings.TrimSpace(string(lfsValue))
					algHeader, _ := findHeaderValue(headers, "LFS_BLOB_ALG")
					alg, err := p.resolveChecksumAlg(string(algHeader))
					if err != nil {
						return rewriteResult{}, err
					}
					if checksumHeader != "" && alg == lfs.ChecksumNone {
						return rewriteResult{}, errors.New("checksum provided but checksum algorithm is none")
					}
					payload := rec.Value
					p.logger.Info("LFS blob detected", "topic", topic.Name, "size", len(payload))
					if int64(len(payload)) > p.maxBlob {
						p.logger.Error("blob exceeds max size", "size", len(payload), "max", p.maxBlob)
						return rewriteResult{}, fmt.Errorf("blob size %d exceeds max %d", len(payload), p.maxBlob)
					}
					key := p.buildObjectKey(topic.Name)
					sha256Hex, checksum, checksumAlg, err := p.s3Uploader.Upload(ctx, key, payload, alg)
					if err != nil {
						p.metrics.IncS3Errors()
						return rewriteResult{}, err
					}
					if checksumHeader != "" && checksum != "" && !strings.EqualFold(checksumHeader, checksum) {
						if err := p.s3Uploader.DeleteObject(ctx, key); err != nil {
							p.trackOrphans([]orphanInfo{{Topic: topic.Name, Key: key, RequestID: "", Reason: "checksum_mismatch_delete_failed"}})
							return rewriteResult{}, fmt.Errorf("checksum mismatch; delete failed: %w", err)
						}
						return rewriteResult{}, &lfs.ChecksumError{Expected: checksumHeader, Actual: checksum}
					}
					env := lfs.Envelope{
						Version:         1,
						Bucket:          p.s3Bucket,
						Key:             key,
						Size:            int64(len(payload)),
						SHA256:          sha256Hex,
						Checksum:        checksum,
						ChecksumAlg:     checksumAlg,
						ContentType:     headerValue(headers, "content-type"),
						OriginalHeaders: headersToMap(headers),
						CreatedAt:       time.Now().UTC().Format(time.RFC3339),
						ProxyID:         p.proxyID,
					}
					encoded, err := lfs.EncodeEnvelope(env)
					if err != nil {
						return rewriteResult{}, err
					}
					rec.Value = encoded
					rec.Headers = dropHeader(headers, "LFS_BLOB")
					uploadBytes += int64(len(payload))
					orphans = append(orphans, orphanInfo{Topic: topic.Name, Key: key, RequestID: "", Reason: "kafka_produce_failed"})
				}
				if !recordChanged {
					continue
				}
				newRecords := encodeRecords(records)
				compressedRecords, usedCodec, err := compressRecords(codec, newRecords)
				if err != nil {
					return rewriteResult{}, err
				}
				batch.Records = compressedRecords
				batch.NumRecords = int32(len(records))
				batch.Attributes = (batch.Attributes &^ 0x0007) | int16(usedCodec)
				batch.Length = 0
				batch.CRC = 0
				batchBytes := batch.AppendTo(nil)
				batch.Length = int32(len(batchBytes) - 12)
				batchBytes = batch.AppendTo(nil)
				batch.CRC = int32(crc32.Checksum(batchBytes[21:], crc32cTable))
				batchBytes = batch.AppendTo(nil)
				batch.Raw = batchBytes
				batchModified = true
			}
			if !batchModified {
				continue
			}
			partition.Records = joinRecordBatches(batches)
		}
	}
	if !modified {
		return rewriteResult{modified: false}, nil
	}

	payloadBytes, err := encodeProduceRequest(header, req)
	if err != nil {
		return rewriteResult{}, err
	}
	return rewriteResult{modified: true, payload: payloadBytes, uploadBytes: uploadBytes, topics: topics, orphans: orphans}, nil
}

func (p *lfsProxy) buildObjectKey(topic string) string {
	ns := strings.TrimSpace(p.s3Namespace)
	if ns == "" {
		ns = "default"
	}
	now := time.Now().UTC()
	return fmt.Sprintf("%s/%s/lfs/%04d/%02d/%02d/obj-%s", ns, topic, now.Year(), now.Month(), now.Day(), newUUID())
}

func (p *lfsProxy) connectBackend(ctx context.Context) (net.Conn, string, error) {
	retries := envInt("KAFSCALE_LFS_PROXY_BACKEND_RETRIES", 6)
	if retries < 1 {
		retries = 1
	}
	backoff := time.Duration(envInt("KAFSCALE_LFS_PROXY_BACKEND_BACKOFF_MS", 500)) * time.Millisecond
	if backoff <= 0 {
		backoff = time.Duration(defaultBackendBackoffMs) * time.Millisecond
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
			wrapped, err := p.wrapBackendTLS(ctx, conn, addr)
			if err != nil {
				_ = conn.Close()
				lastErr = err
				time.Sleep(backoff)
				continue
			}
			if err := p.performBackendSASL(ctx, wrapped); err != nil {
				_ = wrapped.Close()
				lastErr = err
				time.Sleep(backoff)
				continue
			}
			return wrapped, addr, nil
		}
		lastErr = dialErr
		time.Sleep(backoff)
	}
	if lastErr == nil {
		lastErr = errors.New("no backends available")
	}
	return nil, "", lastErr
}

func (p *lfsProxy) currentBackends(ctx context.Context) ([]string, error) {
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

func (p *lfsProxy) forwardToBackend(ctx context.Context, conn net.Conn, backendAddr string, payload []byte) ([]byte, error) {
	if err := protocol.WriteFrame(conn, payload); err != nil {
		return nil, err
	}
	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		return nil, err
	}
	return frame.Payload, nil
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

func (p *lfsProxy) buildNotReadyResponse(header *protocol.RequestHeader, payload []byte) ([]byte, bool, error) {
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
		resp, err := buildProduceErrorResponse(prodReq, header.CorrelationID, header.APIVersion, protocol.REQUEST_TIMED_OUT)
		return wrapEncode(resp, err)
	default:
		return nil, false, nil
	}
}

func buildProduceErrorResponse(req *protocol.ProduceRequest, correlationID int32, version int16, code int16) ([]byte, error) {
	topics := make([]protocol.ProduceTopicResponse, 0, len(req.Topics))
	for _, topic := range req.Topics {
		partitions := make([]protocol.ProducePartitionResponse, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			partitions = append(partitions, protocol.ProducePartitionResponse{
				Partition:       part.Partition,
				ErrorCode:       code,
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
		CorrelationID: correlationID,
		Topics:        topics,
		ThrottleMs:    0,
	}
	return protocol.EncodeProduceResponse(resp, version)
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

func topicsFromProduce(req *protocol.ProduceRequest) []string {
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

type recordBatch struct {
	kmsg.RecordBatch
	Raw []byte
}

type rewriteResult struct {
	modified    bool
	payload     []byte
	uploadBytes int64
	topics      map[string]struct{}
	orphans     []orphanInfo
}

type orphanInfo struct {
	Topic     string
	Key       string
	RequestID string
	Reason    string
}

func (p *lfsProxy) trackOrphans(orphans []orphanInfo) {
	if len(orphans) == 0 {
		return
	}
	p.metrics.IncOrphans(len(orphans))
	for _, orphan := range orphans {
		p.logger.Warn("lfs orphaned object", "topic", orphan.Topic, "key", orphan.Key, "reason", orphan.Reason)
		// Emit orphan_detected event
		reason := orphan.Reason
		if reason == "" {
			reason = "kafka_produce_failed"
		}
		p.tracker.EmitOrphanDetected(orphan.RequestID, "upload_failure", orphan.Topic, p.s3Bucket, orphan.Key, orphan.RequestID, reason, 0)
	}
}

func decodeRecordBatches(records []byte) ([]recordBatch, error) {
	out := make([]recordBatch, 0, 4)
	buf := records
	for len(buf) > 0 {
		if len(buf) < 12 {
			return nil, fmt.Errorf("record batch too short: %d", len(buf))
		}
		length := int(int32FromBytes(buf[8:12]))
		total := 12 + length
		if length < 0 || len(buf) < total {
			return nil, fmt.Errorf("invalid record batch length %d", length)
		}
		batchBytes := buf[:total]
		var batch kmsg.RecordBatch
		if err := batch.ReadFrom(batchBytes); err != nil {
			return nil, err
		}
		out = append(out, recordBatch{RecordBatch: batch, Raw: batchBytes})
		buf = buf[total:]
	}
	return out, nil
}

func joinRecordBatches(batches []recordBatch) []byte {
	if len(batches) == 0 {
		return nil
	}
	size := 0
	for _, batch := range batches {
		size += len(batch.Raw)
	}
	out := make([]byte, 0, size)
	for _, batch := range batches {
		out = append(out, batch.Raw...)
	}
	return out
}

func decodeBatchRecords(batch *recordBatch, decompressor kgo.Decompressor) ([]kmsg.Record, kgo.CompressionCodecType, error) {
	codec := kgo.CompressionCodecType(batch.Attributes & 0x0007)
	rawRecords := batch.Records
	if codec != kgo.CodecNone {
		var err error
		rawRecords, err = decompressor.Decompress(rawRecords, codec)
		if err != nil {
			return nil, codec, err
		}
	}
	numRecords := int(batch.NumRecords)
	records := make([]kmsg.Record, numRecords)
	records = readRawRecordsInto(records, rawRecords)
	return records, codec, nil
}

func readRawRecordsInto(rs []kmsg.Record, in []byte) []kmsg.Record {
	for i := range rs {
		length, used := varint(in)
		total := used + int(length)
		if used == 0 || length < 0 || len(in) < total {
			return rs[:i]
		}
		if err := (&rs[i]).ReadFrom(in[:total]); err != nil {
			rs[i] = kmsg.Record{}
			return rs[:i]
		}
		in = in[total:]
	}
	return rs
}

func compressRecords(codec kgo.CompressionCodecType, raw []byte) ([]byte, kgo.CompressionCodecType, error) {
	if codec == kgo.CodecNone {
		return raw, kgo.CodecNone, nil
	}
	var comp kgo.Compressor
	var err error
	switch codec {
	case kgo.CodecGzip:
		comp, err = kgo.DefaultCompressor(kgo.GzipCompression())
	case kgo.CodecSnappy:
		comp, err = kgo.DefaultCompressor(kgo.SnappyCompression())
	case kgo.CodecLz4:
		comp, err = kgo.DefaultCompressor(kgo.Lz4Compression())
	case kgo.CodecZstd:
		comp, err = kgo.DefaultCompressor(kgo.ZstdCompression())
	default:
		return raw, kgo.CodecNone, nil
	}
	if err != nil || comp == nil {
		return raw, kgo.CodecNone, err
	}
	out, usedCodec := comp.Compress(bytes.NewBuffer(nil), raw)
	return out, usedCodec, nil
}

func findHeaderValue(headers []kmsg.Header, key string) ([]byte, bool) {
	for _, header := range headers {
		if header.Key == key {
			return header.Value, true
		}
	}
	return nil, false
}

func headerValue(headers []kmsg.Header, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

// safeHeaderAllowlist defines headers that are safe to include in the LFS envelope.
// Headers not in this list are redacted to prevent leaking sensitive information.
var safeHeaderAllowlist = map[string]bool{
	"content-type":     true,
	"content-encoding": true,
	"correlation-id":   true,
	"message-id":       true,
	"x-correlation-id": true,
	"x-request-id":     true,
	"traceparent":      true, // W3C trace context
	"tracestate":       true, // W3C trace context
}

func headersToMap(headers []kmsg.Header) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	out := make(map[string]string)
	for _, header := range headers {
		key := strings.ToLower(header.Key)
		// Only include safe headers in the envelope
		if safeHeaderAllowlist[key] {
			out[header.Key] = string(header.Value)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func dropHeader(headers []kmsg.Header, key string) []kmsg.Header {
	if len(headers) == 0 {
		return headers
	}
	out := headers[:0]
	for _, header := range headers {
		if header.Key == key {
			continue
		}
		out = append(out, header)
	}
	return out
}

func int32FromBytes(b []byte) int32 {
	return int32(uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]))
}

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func (p *lfsProxy) resolveChecksumAlg(raw string) (lfs.ChecksumAlg, error) {
	if strings.TrimSpace(raw) == "" {
		return lfs.NormalizeChecksumAlg(p.checksumAlg)
	}
	return lfs.NormalizeChecksumAlg(raw)
}
