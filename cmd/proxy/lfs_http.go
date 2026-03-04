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
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	lfsHeaderTopic       = "X-Kafka-Topic"
	lfsHeaderKey         = "X-Kafka-Key"
	lfsHeaderPartition   = "X-Kafka-Partition"
	lfsHeaderChecksum    = "X-LFS-Checksum"
	lfsHeaderChecksumAlg = "X-LFS-Checksum-Alg"
	lfsHeaderRequestID   = "X-Request-ID"
)

var lfsValidTopicPattern = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

type lfsErrorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

type lfsDownloadRequest struct {
	Bucket         string `json:"bucket"`
	Key            string `json:"key"`
	Mode           string `json:"mode"`
	ExpiresSeconds int    `json:"expires_seconds"`
}

type lfsDownloadResponse struct {
	Mode      string `json:"mode"`
	URL       string `json:"url"`
	ExpiresAt string `json:"expires_at"`
}

type lfsUploadInitRequest struct {
	Topic       string `json:"topic"`
	Key         string `json:"key"`
	Partition   *int32 `json:"partition,omitempty"`
	ContentType string `json:"content_type"`
	SizeBytes   int64  `json:"size_bytes"`
	Checksum    string `json:"checksum,omitempty"`
	ChecksumAlg string `json:"checksum_alg,omitempty"`
}

type lfsUploadInitResponse struct {
	UploadID  string `json:"upload_id"`
	S3Key     string `json:"s3_key"`
	PartSize  int64  `json:"part_size"`
	ExpiresAt string `json:"expires_at"`
}

type lfsUploadPartResponse struct {
	UploadID   string `json:"upload_id"`
	PartNumber int32  `json:"part_number"`
	ETag       string `json:"etag"`
}

type lfsUploadCompleteRequest struct {
	Parts []struct {
		PartNumber int32  `json:"part_number"`
		ETag       string `json:"etag"`
	} `json:"parts"`
}

type uploadSession struct {
	mu             sync.Mutex
	ID             string
	Topic          string
	S3Key          string
	UploadID       string
	ContentType    string
	SizeBytes      int64
	KeyBytes       []byte
	Partition      int32
	Checksum       string
	ChecksumAlg    lfs.ChecksumAlg
	CreatedAt      time.Time
	ExpiresAt      time.Time
	PartSize       int64
	NextPart       int32
	TotalUploaded  int64
	Parts          map[int32]string
	PartSizes      map[int32]int64
	sha256Hasher   lfsHashWriter
	checksumHasher lfsHashWriter
}

type lfsHashWriter interface {
	Write([]byte) (int, error)
	Sum([]byte) []byte
}

func (m *lfsModule) startHTTPServer(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/lfs/produce", m.lfsCORSMiddleware(m.handleHTTPProduce))
	mux.HandleFunc("/lfs/download", m.lfsCORSMiddleware(m.handleHTTPDownload))
	mux.HandleFunc("/lfs/uploads", m.lfsCORSMiddleware(m.handleHTTPUploadInit))
	mux.HandleFunc("/lfs/uploads/", m.lfsCORSMiddleware(m.handleHTTPUploadSession))
	mux.HandleFunc("/swagger", m.lfsHandleSwaggerUI)
	mux.HandleFunc("/swagger/", m.lfsHandleSwaggerUI)
	mux.HandleFunc("/api/openapi.yaml", m.lfsHandleOpenAPISpec)
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       m.httpReadTimeout,
		WriteTimeout:      m.httpWriteTimeout,
		IdleTimeout:       m.httpIdleTimeout,
		ReadHeaderTimeout: m.httpHeaderTimeout,
		MaxHeaderBytes:    m.httpMaxHeaderBytes,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), m.httpShutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		m.logger.Info("lfs http listening", "addr", addr, "tls", m.httpTLSConfig != nil)
		var err error
		if m.httpTLSConfig != nil {
			srv.TLSConfig = m.httpTLSConfig
			err = srv.ListenAndServeTLS(m.httpTLSCertFile, m.httpTLSKeyFile)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			m.logger.Warn("lfs http server error", "error", err)
		}
	}()
}

func (m *lfsModule) startMetricsServer(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		m.metrics.WritePrometheus(w)
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       m.httpReadTimeout,
		WriteTimeout:      m.httpWriteTimeout,
		IdleTimeout:       m.httpIdleTimeout,
		ReadHeaderTimeout: m.httpHeaderTimeout,
		MaxHeaderBytes:    m.httpMaxHeaderBytes,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), m.httpShutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		m.logger.Info("lfs metrics listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			m.logger.Warn("lfs metrics server error", "error", err)
		}
	}()
}

func (m *lfsModule) lfsCORSMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Range, X-Kafka-Topic, X-Kafka-Key, X-Kafka-Partition, X-LFS-Checksum, X-LFS-Checksum-Alg, X-LFS-Size, X-LFS-Mode, X-Request-ID, X-API-Key, Authorization")
		w.Header().Set("Access-Control-Expose-Headers", "X-Request-ID")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func (m *lfsModule) handleHTTPProduce(w http.ResponseWriter, r *http.Request) {
	requestID := strings.TrimSpace(r.Header.Get(lfsHeaderRequestID))
	if requestID == "" {
		requestID = newLFSUUID()
	}
	w.Header().Set(lfsHeaderRequestID, requestID)
	if r.Method != http.MethodPost {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if m.httpAPIKey != "" && !m.lfsValidateHTTPAPIKey(r) {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusUnauthorized, "unauthorized", "unauthorized")
		return
	}
	if !m.isS3Healthy() {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusServiceUnavailable, "proxy_not_ready", "proxy not ready")
		return
	}
	topic := strings.TrimSpace(r.Header.Get(lfsHeaderTopic))
	if topic == "" {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusBadRequest, "missing_topic", "missing topic")
		return
	}
	if !m.lfsIsValidTopicName(topic) {
		m.lfsWriteHTTPError(w, requestID, topic, http.StatusBadRequest, "invalid_topic", "invalid topic name")
		return
	}

	var keyBytes []byte
	if keyHeader := strings.TrimSpace(r.Header.Get(lfsHeaderKey)); keyHeader != "" {
		decoded, err := base64.StdEncoding.DecodeString(keyHeader)
		if err != nil {
			m.lfsWriteHTTPError(w, requestID, topic, http.StatusBadRequest, "invalid_key", "invalid key")
			return
		}
		keyBytes = decoded
	}

	partition := int32(0)
	if partitionHeader := strings.TrimSpace(r.Header.Get(lfsHeaderPartition)); partitionHeader != "" {
		parsed, err := strconv.ParseInt(partitionHeader, 10, 32)
		if err != nil {
			m.lfsWriteHTTPError(w, requestID, topic, http.StatusBadRequest, "invalid_partition", "invalid partition")
			return
		}
		partition = int32(parsed)
	}

	checksumHeader := strings.TrimSpace(r.Header.Get(lfsHeaderChecksum))
	checksumAlgHeader := strings.TrimSpace(r.Header.Get(lfsHeaderChecksumAlg))
	alg, err := m.resolveChecksumAlg(checksumAlgHeader)
	if err != nil {
		m.lfsWriteHTTPError(w, requestID, topic, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if checksumHeader != "" && alg == lfs.ChecksumNone {
		m.lfsWriteHTTPError(w, requestID, topic, http.StatusBadRequest, "invalid_checksum", "checksum provided but checksum algorithm is none")
		return
	}
	objectKey := m.buildObjectKey(topic)
	clientIP := lfsGetClientIP(r)
	contentType := r.Header.Get("Content-Type")

	start := time.Now()
	m.tracker.EmitUploadStarted(requestID, topic, partition, objectKey, contentType, clientIP, "http", r.ContentLength)

	sha256Hex, checksum, checksumAlg, size, err := m.s3Uploader.UploadStream(r.Context(), objectKey, r.Body, m.maxBlob, alg)
	if err != nil {
		m.metrics.IncRequests(topic, "error", "lfs")
		m.metrics.IncS3Errors()
		status, code := lfsStatusForUploadError(err)
		m.tracker.EmitUploadFailed(requestID, topic, objectKey, code, err.Error(), "s3_upload", 0, time.Since(start))
		m.lfsWriteHTTPError(w, requestID, topic, status, code, err.Error())
		return
	}
	if checksumHeader != "" && checksum != "" && !strings.EqualFold(checksumHeader, checksum) {
		if err := m.s3Uploader.DeleteObject(r.Context(), objectKey); err != nil {
			m.trackOrphans([]orphanInfo{{Topic: topic, Key: objectKey, RequestID: requestID, Reason: "kafka_produce_failed"}})
			m.metrics.IncRequests(topic, "error", "lfs")
			m.tracker.EmitUploadFailed(requestID, topic, objectKey, "checksum_mismatch", "checksum mismatch; delete failed", "validation", size, time.Since(start))
			m.lfsWriteHTTPError(w, requestID, topic, http.StatusBadRequest, "checksum_mismatch", "checksum mismatch; delete failed")
			return
		}
		m.metrics.IncRequests(topic, "error", "lfs")
		m.tracker.EmitUploadFailed(requestID, topic, objectKey, "checksum_mismatch", (&lfs.ChecksumError{Expected: checksumHeader, Actual: checksum}).Error(), "validation", size, time.Since(start))
		m.lfsWriteHTTPError(w, requestID, topic, http.StatusBadRequest, "checksum_mismatch", (&lfs.ChecksumError{Expected: checksumHeader, Actual: checksum}).Error())
		return
	}

	env := lfs.Envelope{
		Version:     1,
		Bucket:      m.s3Bucket,
		Key:         objectKey,
		Size:        size,
		SHA256:      sha256Hex,
		Checksum:    checksum,
		ChecksumAlg: checksumAlg,
		ContentType: r.Header.Get("Content-Type"),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		ProxyID:     m.proxyID,
	}
	encoded, err := lfs.EncodeEnvelope(env)
	if err != nil {
		m.metrics.IncRequests(topic, "error", "lfs")
		m.lfsWriteHTTPError(w, requestID, topic, http.StatusInternalServerError, "encode_failed", err.Error())
		return
	}

	record := kmsg.Record{
		TimestampDelta64: 0,
		OffsetDelta:      0,
		Key:              keyBytes,
		Value:            encoded,
	}
	batchBytes := lfsBuildRecordBatch([]kmsg.Record{record})

	produceReq := &protocol.ProduceRequest{
		Acks:      1,
		TimeoutMs: 15000,
		Topics: []protocol.ProduceTopic{{
			Name: topic,
			Partitions: []protocol.ProducePartition{{
				Partition: partition,
				Records:   batchBytes,
			}},
		}},
	}

	correlationID := int32(atomic.AddUint32(&m.corrID, 1))
	reqHeader := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: correlationID}
	payload, err := lfsEncodeProduceRequest(reqHeader, produceReq)
	if err != nil {
		m.metrics.IncRequests(topic, "error", "lfs")
		m.lfsWriteHTTPError(w, requestID, topic, http.StatusInternalServerError, "encode_failed", err.Error())
		return
	}

	backendConn, _, err := m.connectBackend(r.Context())
	if err != nil {
		m.metrics.IncRequests(topic, "error", "lfs")
		m.trackOrphans([]orphanInfo{{Topic: topic, Key: objectKey, RequestID: requestID, Reason: "kafka_produce_failed"}})
		m.tracker.EmitUploadFailed(requestID, topic, objectKey, "backend_unavailable", err.Error(), "kafka_produce", size, time.Since(start))
		m.lfsWriteHTTPError(w, requestID, topic, http.StatusServiceUnavailable, "backend_unavailable", err.Error())
		return
	}
	defer func() { _ = backendConn.Close() }()

	_, err = m.forwardToBackend(r.Context(), backendConn, payload)
	if err != nil {
		m.metrics.IncRequests(topic, "error", "lfs")
		m.trackOrphans([]orphanInfo{{Topic: topic, Key: objectKey, RequestID: requestID, Reason: "kafka_produce_failed"}})
		m.tracker.EmitUploadFailed(requestID, topic, objectKey, "backend_error", err.Error(), "kafka_produce", size, time.Since(start))
		m.lfsWriteHTTPError(w, requestID, topic, http.StatusBadGateway, "backend_error", err.Error())
		return
	}

	m.metrics.IncRequests(topic, "ok", "lfs")
	m.metrics.AddUploadBytes(size)
	m.metrics.ObserveUploadDuration(time.Since(start).Seconds())
	m.tracker.EmitUploadCompleted(requestID, topic, partition, 0, m.s3Bucket, objectKey, size, sha256Hex, checksum, checksumAlg, contentType, time.Since(start))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(env)
}

func (m *lfsModule) handleHTTPDownload(w http.ResponseWriter, r *http.Request) {
	requestID := strings.TrimSpace(r.Header.Get(lfsHeaderRequestID))
	if requestID == "" {
		requestID = newLFSUUID()
	}
	w.Header().Set(lfsHeaderRequestID, requestID)
	if r.Method != http.MethodPost {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if m.httpAPIKey != "" && !m.lfsValidateHTTPAPIKey(r) {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusUnauthorized, "unauthorized", "unauthorized")
		return
	}
	if !m.isS3Healthy() {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusServiceUnavailable, "proxy_not_ready", "proxy not ready")
		return
	}

	var req lfsDownloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return
	}
	req.Bucket = strings.TrimSpace(req.Bucket)
	req.Key = strings.TrimSpace(req.Key)
	if req.Bucket == "" || req.Key == "" {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusBadRequest, "invalid_request", "bucket and key required")
		return
	}
	if req.Bucket != m.s3Bucket {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusBadRequest, "invalid_bucket", "bucket not allowed")
		return
	}
	if err := m.lfsValidateObjectKey(req.Key); err != nil {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusBadRequest, "invalid_key", err.Error())
		return
	}

	mode := strings.ToLower(strings.TrimSpace(req.Mode))
	if mode == "" {
		mode = "presign"
	}
	if mode != "presign" && mode != "stream" {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusBadRequest, "invalid_mode", "mode must be presign or stream")
		return
	}

	clientIP := lfsGetClientIP(r)
	start := time.Now()
	ttlSeconds := 0
	if mode == "presign" {
		ttlSeconds = req.ExpiresSeconds
		if ttlSeconds <= 0 {
			ttlSeconds = int(m.downloadTTLMax.Seconds())
		}
	}
	m.tracker.EmitDownloadRequested(requestID, req.Bucket, req.Key, mode, clientIP, ttlSeconds)

	switch mode {
	case "presign":
		ttl := m.downloadTTLMax
		if req.ExpiresSeconds > 0 {
			requested := time.Duration(req.ExpiresSeconds) * time.Second
			if requested < ttl {
				ttl = requested
			}
		}
		url, err := m.s3Uploader.PresignGetObject(r.Context(), req.Key, ttl)
		if err != nil {
			m.metrics.IncS3Errors()
			m.lfsWriteHTTPError(w, requestID, "", http.StatusBadGateway, "s3_presign_failed", err.Error())
			return
		}
		m.tracker.EmitDownloadCompleted(requestID, req.Key, mode, time.Since(start), 0)

		resp := lfsDownloadResponse{
			Mode:      "presign",
			URL:       url,
			ExpiresAt: time.Now().UTC().Add(ttl).Format(time.RFC3339),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	case "stream":
		obj, err := m.s3Uploader.GetObject(r.Context(), req.Key)
		if err != nil {
			m.metrics.IncS3Errors()
			m.lfsWriteHTTPError(w, requestID, "", http.StatusBadGateway, "s3_get_failed", err.Error())
			return
		}
		defer func() { _ = obj.Body.Close() }()
		contentType := "application/octet-stream"
		if obj.ContentType != nil && *obj.ContentType != "" {
			contentType = *obj.ContentType
		}
		w.Header().Set("Content-Type", contentType)
		var size int64
		if obj.ContentLength != nil {
			size = *obj.ContentLength
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		}
		if _, err := io.Copy(w, obj.Body); err != nil {
			m.logger.Warn("download stream failed", "error", err)
		}
		m.tracker.EmitDownloadCompleted(requestID, req.Key, mode, time.Since(start), size)
	}
}

func (m *lfsModule) handleHTTPUploadInit(w http.ResponseWriter, r *http.Request) {
	requestID := strings.TrimSpace(r.Header.Get(lfsHeaderRequestID))
	if requestID == "" {
		requestID = newLFSUUID()
	}
	w.Header().Set(lfsHeaderRequestID, requestID)
	if r.Method != http.MethodPost {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if m.httpAPIKey != "" && !m.lfsValidateHTTPAPIKey(r) {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusUnauthorized, "unauthorized", "unauthorized")
		return
	}
	if !m.isS3Healthy() {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusServiceUnavailable, "proxy_not_ready", "proxy not ready")
		return
	}

	var req lfsUploadInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return
	}

	req.Topic = strings.TrimSpace(req.Topic)
	req.ContentType = strings.TrimSpace(req.ContentType)
	req.Checksum = strings.TrimSpace(req.Checksum)
	req.ChecksumAlg = strings.TrimSpace(req.ChecksumAlg)
	if req.Topic == "" {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusBadRequest, "missing_topic", "missing topic")
		return
	}
	if !m.lfsIsValidTopicName(req.Topic) {
		m.lfsWriteHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "invalid_topic", "invalid topic name")
		return
	}
	if req.ContentType == "" {
		m.lfsWriteHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "missing_content_type", "content_type required")
		return
	}
	if req.SizeBytes <= 0 {
		m.lfsWriteHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "invalid_size", "size_bytes must be > 0")
		return
	}
	if m.maxBlob > 0 && req.SizeBytes > m.maxBlob {
		m.lfsWriteHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "payload_too_large", "payload exceeds max size")
		return
	}

	keyBytes := []byte(nil)
	if req.Key != "" {
		decoded, err := base64.StdEncoding.DecodeString(req.Key)
		if err != nil {
			m.lfsWriteHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "invalid_key", "invalid key")
			return
		}
		keyBytes = decoded
	}

	partition := int32(0)
	if req.Partition != nil {
		partition = *req.Partition
		if partition < 0 {
			m.lfsWriteHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "invalid_partition", "invalid partition")
			return
		}
	}

	alg, err := m.resolveChecksumAlg(req.ChecksumAlg)
	if err != nil {
		m.lfsWriteHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if req.Checksum != "" && alg == lfs.ChecksumNone {
		m.lfsWriteHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "invalid_checksum", "checksum provided but checksum algorithm is none")
		return
	}

	objectKey := m.buildObjectKey(req.Topic)
	uploadID, err := m.s3Uploader.StartMultipartUpload(r.Context(), objectKey, req.ContentType)
	if err != nil {
		m.metrics.IncS3Errors()
		m.lfsWriteHTTPError(w, requestID, req.Topic, http.StatusBadGateway, "s3_upload_failed", err.Error())
		return
	}
	m.logger.Info("http chunked upload init", "requestId", requestID, "topic", req.Topic, "s3Key", objectKey, "uploadId", uploadID, "sizeBytes", req.SizeBytes, "partSize", m.chunkSize)

	partSize := normalizeChunkSize(m.chunkSize)
	session := &uploadSession{
		ID:           newLFSUUID(),
		Topic:        req.Topic,
		S3Key:        objectKey,
		UploadID:     uploadID,
		ContentType:  req.ContentType,
		SizeBytes:    req.SizeBytes,
		KeyBytes:     keyBytes,
		Partition:    partition,
		Checksum:     req.Checksum,
		ChecksumAlg:  alg,
		CreatedAt:    time.Now().UTC(),
		ExpiresAt:    time.Now().UTC().Add(m.uploadSessionTTL),
		PartSize:     partSize,
		NextPart:     1,
		Parts:        make(map[int32]string),
		PartSizes:    make(map[int32]int64),
		sha256Hasher: sha256.New(),
	}
	if alg != lfs.ChecksumNone {
		if alg == lfs.ChecksumSHA256 {
			session.checksumHasher = session.sha256Hasher
		} else if h, err := lfs.NewChecksumHasher(alg); err == nil {
			session.checksumHasher = h
		} else if err != nil {
			m.lfsWriteHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "invalid_checksum", err.Error())
			return
		}
	}

	m.lfsStoreUploadSession(session)
	m.tracker.EmitUploadStarted(requestID, req.Topic, partition, objectKey, req.ContentType, lfsGetClientIP(r), "http-chunked", req.SizeBytes)

	resp := lfsUploadInitResponse{
		UploadID:  session.ID,
		S3Key:     session.S3Key,
		PartSize:  session.PartSize,
		ExpiresAt: session.ExpiresAt.Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (m *lfsModule) handleHTTPUploadSession(w http.ResponseWriter, r *http.Request) {
	requestID := strings.TrimSpace(r.Header.Get(lfsHeaderRequestID))
	if requestID == "" {
		requestID = newLFSUUID()
	}
	w.Header().Set(lfsHeaderRequestID, requestID)
	if m.httpAPIKey != "" && !m.lfsValidateHTTPAPIKey(r) {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusUnauthorized, "unauthorized", "unauthorized")
		return
	}
	if !m.isS3Healthy() {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusServiceUnavailable, "proxy_not_ready", "proxy not ready")
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/lfs/uploads/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusNotFound, "not_found", "not found")
		return
	}
	uploadID := parts[0]

	switch {
	case len(parts) == 1 && r.Method == http.MethodDelete:
		m.handleHTTPUploadAbort(w, r, requestID, uploadID)
		return
	case len(parts) == 2 && parts[1] == "complete" && r.Method == http.MethodPost:
		m.handleHTTPUploadComplete(w, r, requestID, uploadID)
		return
	case len(parts) == 3 && parts[1] == "parts" && r.Method == http.MethodPut:
		partNum, err := strconv.ParseInt(parts[2], 10, 32)
		if err != nil || partNum <= 0 || partNum > math.MaxInt32 {
			m.lfsWriteHTTPError(w, requestID, "", http.StatusBadRequest, "invalid_part", "invalid part number")
			return
		}
		m.handleHTTPUploadPart(w, r, requestID, uploadID, int32(partNum))
		return
	default:
		m.lfsWriteHTTPError(w, requestID, "", http.StatusNotFound, "not_found", "not found")
		return
	}
}

func (m *lfsModule) handleHTTPUploadPart(w http.ResponseWriter, r *http.Request, requestID, sessionID string, partNumber int32) {
	session, ok := m.lfsGetUploadSession(sessionID)
	if !ok {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusNotFound, "upload_not_found", "upload session not found")
		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if time.Now().UTC().After(session.ExpiresAt) {
		m.lfsDeleteUploadSession(sessionID)
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusGone, "upload_expired", "upload session expired")
		return
	}

	if etag, exists := session.Parts[partNumber]; exists {
		_, _ = io.Copy(io.Discard, r.Body)
		m.logger.Info("http chunked upload part already received", "requestId", requestID, "uploadId", sessionID, "part", partNumber, "etag", etag)
		resp := lfsUploadPartResponse{UploadID: sessionID, PartNumber: partNumber, ETag: etag}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	if partNumber != session.NextPart {
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusConflict, "out_of_order", "part out of order")
		return
	}

	limit := session.PartSize + 1
	body, err := io.ReadAll(io.LimitReader(r.Body, limit))
	if err != nil {
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_part", err.Error())
		return
	}
	if int64(len(body)) == 0 {
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_part", "empty part")
		return
	}
	if int64(len(body)) > session.PartSize {
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_part", "part too large")
		return
	}
	if session.TotalUploaded+int64(len(body)) > session.SizeBytes {
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_part", "part exceeds declared size")
		return
	}
	if session.TotalUploaded+int64(len(body)) < session.SizeBytes && int64(len(body)) < minMultipartChunkSize {
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_part", "part too small")
		return
	}

	if _, err := session.sha256Hasher.Write(body); err != nil {
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "hash_error", err.Error())
		return
	}
	if session.checksumHasher != nil && session.checksumHasher != session.sha256Hasher {
		if _, err := session.checksumHasher.Write(body); err != nil {
			m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "hash_error", err.Error())
			return
		}
	}

	etag, err := m.s3Uploader.UploadPart(r.Context(), session.S3Key, session.UploadID, partNumber, body)
	if err != nil {
		m.metrics.IncS3Errors()
		m.tracker.EmitUploadFailed(requestID, session.Topic, session.S3Key, "s3_upload_failed", err.Error(), "upload_part", session.TotalUploaded, 0)
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadGateway, "s3_upload_failed", err.Error())
		return
	}
	m.logger.Info("http chunked upload part stored", "requestId", requestID, "uploadId", sessionID, "part", partNumber, "etag", etag, "bytes", len(body))

	session.Parts[partNumber] = etag
	session.PartSizes[partNumber] = int64(len(body))
	session.TotalUploaded += int64(len(body))
	session.NextPart++

	resp := lfsUploadPartResponse{UploadID: sessionID, PartNumber: partNumber, ETag: etag}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (m *lfsModule) handleHTTPUploadComplete(w http.ResponseWriter, r *http.Request, requestID, sessionID string) {
	session, ok := m.lfsGetUploadSession(sessionID)
	if !ok {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusNotFound, "upload_not_found", "upload session not found")
		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if time.Now().UTC().After(session.ExpiresAt) {
		m.lfsDeleteUploadSession(sessionID)
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusGone, "upload_expired", "upload session expired")
		return
	}
	if session.TotalUploaded != session.SizeBytes {
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "incomplete_upload", "not all bytes uploaded")
		return
	}

	var req lfsUploadCompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return
	}
	if len(req.Parts) == 0 {
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_request", "parts required")
		return
	}

	completed := make([]types.CompletedPart, 0, len(req.Parts))
	for _, part := range req.Parts {
		etag, ok := session.Parts[part.PartNumber]
		if !ok || etag == "" || part.ETag == "" || etag != part.ETag {
			m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_part", "part etag mismatch")
			return
		}
		completed = append(completed, types.CompletedPart{
			ETag:       aws.String(part.ETag),
			PartNumber: aws.Int32(part.PartNumber),
		})
	}

	if err := m.s3Uploader.CompleteMultipartUpload(r.Context(), session.S3Key, session.UploadID, completed); err != nil {
		m.metrics.IncS3Errors()
		m.tracker.EmitUploadFailed(requestID, session.Topic, session.S3Key, "s3_upload_failed", err.Error(), "upload_complete", session.TotalUploaded, 0)
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadGateway, "s3_upload_failed", err.Error())
		return
	}
	m.logger.Info("http chunked upload completed", "requestId", requestID, "uploadId", sessionID, "parts", len(completed), "bytes", session.TotalUploaded)

	shaHex := hex.EncodeToString(session.sha256Hasher.Sum(nil))
	checksum := ""
	if session.ChecksumAlg != lfs.ChecksumNone {
		if session.ChecksumAlg == lfs.ChecksumSHA256 {
			checksum = shaHex
		} else if session.checksumHasher != nil {
			checksum = hex.EncodeToString(session.checksumHasher.Sum(nil))
		}
	}
	if session.Checksum != "" && checksum != "" && !strings.EqualFold(session.Checksum, checksum) {
		_ = m.s3Uploader.AbortMultipartUpload(r.Context(), session.S3Key, session.UploadID)
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "checksum_mismatch", "checksum mismatch")
		return
	}

	env := lfs.Envelope{
		Version:     1,
		Bucket:      m.s3Bucket,
		Key:         session.S3Key,
		Size:        session.TotalUploaded,
		SHA256:      shaHex,
		Checksum:    checksum,
		ChecksumAlg: string(session.ChecksumAlg),
		ContentType: session.ContentType,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		ProxyID:     m.proxyID,
	}
	encoded, err := lfs.EncodeEnvelope(env)
	if err != nil {
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusInternalServerError, "encode_failed", err.Error())
		return
	}

	record := kmsg.Record{
		TimestampDelta64: 0,
		OffsetDelta:      0,
		Key:              session.KeyBytes,
		Value:            encoded,
	}
	batchBytes := lfsBuildRecordBatch([]kmsg.Record{record})

	produceReq := &protocol.ProduceRequest{
		Acks:      1,
		TimeoutMs: 15000,
		Topics: []protocol.ProduceTopic{{
			Name: session.Topic,
			Partitions: []protocol.ProducePartition{{
				Partition: session.Partition,
				Records:   batchBytes,
			}},
		}},
	}

	correlationID := int32(atomic.AddUint32(&m.corrID, 1))
	reqHeader := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: correlationID}
	payload, err := lfsEncodeProduceRequest(reqHeader, produceReq)
	if err != nil {
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusInternalServerError, "encode_failed", err.Error())
		return
	}

	backendConn, _, err := m.connectBackend(r.Context())
	if err != nil {
		m.trackOrphans([]orphanInfo{{Topic: session.Topic, Key: session.S3Key, RequestID: requestID, Reason: "kafka_produce_failed"}})
		m.tracker.EmitUploadFailed(requestID, session.Topic, session.S3Key, "backend_unavailable", err.Error(), "kafka_produce", session.TotalUploaded, 0)
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusServiceUnavailable, "backend_unavailable", err.Error())
		return
	}
	defer func() { _ = backendConn.Close() }()

	if _, err := m.forwardToBackend(r.Context(), backendConn, payload); err != nil {
		m.trackOrphans([]orphanInfo{{Topic: session.Topic, Key: session.S3Key, RequestID: requestID, Reason: "kafka_produce_failed"}})
		m.tracker.EmitUploadFailed(requestID, session.Topic, session.S3Key, "backend_error", err.Error(), "kafka_produce", session.TotalUploaded, 0)
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadGateway, "backend_error", err.Error())
		return
	}

	m.metrics.IncRequests(session.Topic, "ok", "lfs")
	m.metrics.AddUploadBytes(session.TotalUploaded)
	m.tracker.EmitUploadCompleted(requestID, session.Topic, session.Partition, 0, m.s3Bucket, session.S3Key, session.TotalUploaded, shaHex, checksum, string(session.ChecksumAlg), session.ContentType, 0)

	m.lfsDeleteUploadSession(sessionID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(env)
}

func (m *lfsModule) handleHTTPUploadAbort(w http.ResponseWriter, r *http.Request, requestID, sessionID string) {
	session, ok := m.lfsGetUploadSession(sessionID)
	if !ok {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusNotFound, "upload_not_found", "upload session not found")
		return
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	_ = m.s3Uploader.AbortMultipartUpload(r.Context(), session.S3Key, session.UploadID)
	m.lfsDeleteUploadSession(sessionID)
	w.WriteHeader(http.StatusNoContent)
}

func (m *lfsModule) lfsStoreUploadSession(session *uploadSession) {
	if session == nil {
		return
	}
	m.uploadMu.Lock()
	defer m.uploadMu.Unlock()
	m.lfsCleanupUploadSessionsLocked()
	m.uploadSessions[session.ID] = session
}

func (m *lfsModule) lfsGetUploadSession(id string) (*uploadSession, bool) {
	m.uploadMu.Lock()
	defer m.uploadMu.Unlock()
	m.lfsCleanupUploadSessionsLocked()
	session, ok := m.uploadSessions[id]
	return session, ok
}

func (m *lfsModule) lfsDeleteUploadSession(id string) {
	m.uploadMu.Lock()
	defer m.uploadMu.Unlock()
	delete(m.uploadSessions, id)
}

func (m *lfsModule) lfsCleanupUploadSessionsLocked() {
	now := time.Now().UTC()
	for id, session := range m.uploadSessions {
		if session.ExpiresAt.Before(now) {
			delete(m.uploadSessions, id)
		}
	}
}

func lfsStatusForUploadError(err error) (int, string) {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "exceeds max"):
		return http.StatusBadRequest, "payload_too_large"
	case strings.Contains(msg, "empty upload"):
		return http.StatusBadRequest, "empty_upload"
	case strings.Contains(msg, "s3 key required"):
		return http.StatusBadRequest, "invalid_key"
	case strings.Contains(msg, "reader required"):
		return http.StatusBadRequest, "invalid_reader"
	default:
		return http.StatusBadGateway, "s3_upload_failed"
	}
}

func (m *lfsModule) lfsWriteHTTPError(w http.ResponseWriter, requestID, topic string, status int, code, message string) {
	if topic != "" {
		m.logger.Warn("lfs http failed", "status", status, "code", code, "requestId", requestID, "topic", topic, "error", message)
	} else {
		m.logger.Warn("lfs http failed", "status", status, "code", code, "requestId", requestID, "error", message)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(lfsErrorResponse{
		Code:      code,
		Message:   message,
		RequestID: requestID,
	})
}

func (m *lfsModule) lfsValidateHTTPAPIKey(r *http.Request) bool {
	if r == nil {
		return false
	}
	key := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if key == "" {
		auth := strings.TrimSpace(r.Header.Get("Authorization"))
		if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			key = strings.TrimSpace(auth[len("bearer "):])
		}
	}
	if key == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(key), []byte(m.httpAPIKey)) == 1
}

func (m *lfsModule) lfsValidateObjectKey(key string) error {
	if strings.HasPrefix(key, "/") {
		return errors.New("key must be relative")
	}
	if strings.Contains(key, "..") {
		return errors.New("key must not contain '..'")
	}
	ns := strings.TrimSpace(m.s3Namespace)
	if ns != "" && !strings.HasPrefix(key, ns+"/") {
		return errors.New("key outside namespace")
	}
	if !strings.Contains(key, "/lfs/") {
		return errors.New("key must include /lfs/ segment")
	}
	return nil
}

func (m *lfsModule) lfsIsValidTopicName(topic string) bool {
	if len(topic) == 0 || len(topic) > m.topicMaxLength {
		return false
	}
	return lfsValidTopicPattern.MatchString(topic)
}

func lfsGetClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if idx := strings.Index(xff, ","); idx > 0 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	host, _, found := strings.Cut(r.RemoteAddr, ":")
	if found {
		return host
	}
	return r.RemoteAddr
}
