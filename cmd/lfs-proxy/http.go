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
	headerTopic       = "X-Kafka-Topic"
	headerKey         = "X-Kafka-Key"
	headerPartition   = "X-Kafka-Partition"
	headerChecksum    = "X-LFS-Checksum"
	headerChecksumAlg = "X-LFS-Checksum-Alg"
	headerRequestID   = "X-Request-ID"
)

// validTopicPattern matches valid Kafka topic names (alphanumeric, dots, underscores, hyphens)
var validTopicPattern = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

type errorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

type downloadRequest struct {
	Bucket         string `json:"bucket"`
	Key            string `json:"key"`
	Mode           string `json:"mode"`
	ExpiresSeconds int    `json:"expires_seconds"`
}

type downloadResponse struct {
	Mode      string `json:"mode"`
	URL       string `json:"url"`
	ExpiresAt string `json:"expires_at"`
}

type uploadInitRequest struct {
	Topic       string `json:"topic"`
	Key         string `json:"key"`
	Partition   *int32 `json:"partition,omitempty"`
	ContentType string `json:"content_type"`
	SizeBytes   int64  `json:"size_bytes"`
	Checksum    string `json:"checksum,omitempty"`
	ChecksumAlg string `json:"checksum_alg,omitempty"`
}

type uploadInitResponse struct {
	UploadID  string `json:"upload_id"`
	S3Key     string `json:"s3_key"`
	PartSize  int64  `json:"part_size"`
	ExpiresAt string `json:"expires_at"`
}

type uploadPartResponse struct {
	UploadID   string `json:"upload_id"`
	PartNumber int32  `json:"part_number"`
	ETag       string `json:"etag"`
}

type uploadCompleteRequest struct {
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
	sha256Hasher   hashWriter
	checksumHasher hashWriter
}

type hashWriter interface {
	Write([]byte) (int, error)
	Sum([]byte) []byte
}

func (p *lfsProxy) startHTTPServer(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/lfs/produce", p.corsMiddleware(p.handleHTTPProduce))
	mux.HandleFunc("/lfs/download", p.corsMiddleware(p.handleHTTPDownload))
	mux.HandleFunc("/lfs/uploads", p.corsMiddleware(p.handleHTTPUploadInit))
	mux.HandleFunc("/lfs/uploads/", p.corsMiddleware(p.handleHTTPUploadSession))
	// Swagger UI and OpenAPI spec endpoints
	mux.HandleFunc("/swagger", p.handleSwaggerUI)
	mux.HandleFunc("/swagger/", p.handleSwaggerUI)
	mux.HandleFunc("/api/openapi.yaml", p.handleOpenAPISpec)
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
		p.logger.Info("lfs proxy http listening", "addr", addr, "tls", p.httpTLSConfig != nil)
		var err error
		if p.httpTLSConfig != nil {
			srv.TLSConfig = p.httpTLSConfig
			err = srv.ListenAndServeTLS(p.httpTLSCertFile, p.httpTLSKeyFile)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			p.logger.Warn("lfs proxy http server error", "error", err)
		}
	}()
}

// corsMiddleware adds CORS headers to allow browser-based clients.
func (p *lfsProxy) corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers for all responses
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Range, X-Kafka-Topic, X-Kafka-Key, X-Kafka-Partition, X-LFS-Checksum, X-LFS-Checksum-Alg, X-LFS-Size, X-LFS-Mode, X-Request-ID, X-API-Key, Authorization")
		w.Header().Set("Access-Control-Expose-Headers", "X-Request-ID")

		// Handle preflight OPTIONS request
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next(w, r)
	}
}

func (p *lfsProxy) handleHTTPProduce(w http.ResponseWriter, r *http.Request) {
	requestID := strings.TrimSpace(r.Header.Get(headerRequestID))
	if requestID == "" {
		requestID = newUUID()
	}
	w.Header().Set(headerRequestID, requestID)
	if r.Method != http.MethodPost {
		p.writeHTTPError(w, requestID, "", http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if p.httpAPIKey != "" && !p.validateHTTPAPIKey(r) {
		p.writeHTTPError(w, requestID, "", http.StatusUnauthorized, "unauthorized", "unauthorized")
		return
	}
	if !p.isReady() {
		p.writeHTTPError(w, requestID, "", http.StatusServiceUnavailable, "proxy_not_ready", "proxy not ready")
		return
	}
	topic := strings.TrimSpace(r.Header.Get(headerTopic))
	if topic == "" {
		p.writeHTTPError(w, requestID, "", http.StatusBadRequest, "missing_topic", "missing topic")
		return
	}
	if !p.isValidTopicName(topic) {
		p.writeHTTPError(w, requestID, topic, http.StatusBadRequest, "invalid_topic", "invalid topic name")
		return
	}

	var keyBytes []byte
	if keyHeader := strings.TrimSpace(r.Header.Get(headerKey)); keyHeader != "" {
		decoded, err := base64.StdEncoding.DecodeString(keyHeader)
		if err != nil {
			p.writeHTTPError(w, requestID, topic, http.StatusBadRequest, "invalid_key", "invalid key")
			return
		}
		keyBytes = decoded
	}

	partition := int32(0)
	if partitionHeader := strings.TrimSpace(r.Header.Get(headerPartition)); partitionHeader != "" {
		parsed, err := strconv.ParseInt(partitionHeader, 10, 32)
		if err != nil {
			p.writeHTTPError(w, requestID, topic, http.StatusBadRequest, "invalid_partition", "invalid partition")
			return
		}
		partition = int32(parsed)
	}

	checksumHeader := strings.TrimSpace(r.Header.Get(headerChecksum))
	checksumAlgHeader := strings.TrimSpace(r.Header.Get(headerChecksumAlg))
	alg, err := p.resolveChecksumAlg(checksumAlgHeader)
	if err != nil {
		p.writeHTTPError(w, requestID, topic, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if checksumHeader != "" && alg == lfs.ChecksumNone {
		p.writeHTTPError(w, requestID, topic, http.StatusBadRequest, "invalid_checksum", "checksum provided but checksum algorithm is none")
		return
	}
	objectKey := p.buildObjectKey(topic)
	clientIP := getClientIP(r)
	contentType := r.Header.Get("Content-Type")

	start := time.Now()

	// Emit upload_started event
	p.tracker.EmitUploadStarted(requestID, topic, partition, objectKey, contentType, clientIP, "http", r.ContentLength)

	sha256Hex, checksum, checksumAlg, size, err := p.s3Uploader.UploadStream(r.Context(), objectKey, r.Body, p.maxBlob, alg)
	if err != nil {
		p.metrics.IncRequests(topic, "error", "lfs")
		p.metrics.IncS3Errors()
		status, code := statusForUploadError(err)
		p.tracker.EmitUploadFailed(requestID, topic, objectKey, code, err.Error(), "s3_upload", 0, time.Since(start))
		p.writeHTTPError(w, requestID, topic, status, code, err.Error())
		return
	}
	if checksumHeader != "" && checksum != "" && !strings.EqualFold(checksumHeader, checksum) {
		if err := p.s3Uploader.DeleteObject(r.Context(), objectKey); err != nil {
			p.trackOrphans([]orphanInfo{{Topic: topic, Key: objectKey, RequestID: requestID, Reason: "kafka_produce_failed"}})
			p.metrics.IncRequests(topic, "error", "lfs")
			p.tracker.EmitUploadFailed(requestID, topic, objectKey, "checksum_mismatch", "checksum mismatch; delete failed", "validation", size, time.Since(start))
			p.writeHTTPError(w, requestID, topic, http.StatusBadRequest, "checksum_mismatch", "checksum mismatch; delete failed")
			return
		}
		p.metrics.IncRequests(topic, "error", "lfs")
		p.tracker.EmitUploadFailed(requestID, topic, objectKey, "checksum_mismatch", (&lfs.ChecksumError{Expected: checksumHeader, Actual: checksum}).Error(), "validation", size, time.Since(start))
		p.writeHTTPError(w, requestID, topic, http.StatusBadRequest, "checksum_mismatch", (&lfs.ChecksumError{Expected: checksumHeader, Actual: checksum}).Error())
		return
	}

	env := lfs.Envelope{
		Version:     1,
		Bucket:      p.s3Bucket,
		Key:         objectKey,
		Size:        size,
		SHA256:      sha256Hex,
		Checksum:    checksum,
		ChecksumAlg: checksumAlg,
		ContentType: r.Header.Get("Content-Type"),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		ProxyID:     p.proxyID,
	}
	encoded, err := lfs.EncodeEnvelope(env)
	if err != nil {
		p.metrics.IncRequests(topic, "error", "lfs")
		p.writeHTTPError(w, requestID, topic, http.StatusInternalServerError, "encode_failed", err.Error())
		return
	}

	record := kmsg.Record{
		TimestampDelta64: 0,
		OffsetDelta:      0,
		Key:              keyBytes,
		Value:            encoded,
	}
	batchBytes := buildRecordBatch([]kmsg.Record{record})

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

	correlationID := int32(atomic.AddUint32(&p.corrID, 1))
	reqHeader := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: correlationID}
	payload, err := encodeProduceRequest(reqHeader, produceReq)
	if err != nil {
		p.metrics.IncRequests(topic, "error", "lfs")
		p.writeHTTPError(w, requestID, topic, http.StatusInternalServerError, "encode_failed", err.Error())
		return
	}

	backendConn, backendAddr, err := p.connectBackend(r.Context())
	if err != nil {
		p.metrics.IncRequests(topic, "error", "lfs")
		p.trackOrphans([]orphanInfo{{Topic: topic, Key: objectKey, RequestID: requestID, Reason: "kafka_produce_failed"}})
		p.tracker.EmitUploadFailed(requestID, topic, objectKey, "backend_unavailable", err.Error(), "kafka_produce", size, time.Since(start))
		p.writeHTTPError(w, requestID, topic, http.StatusServiceUnavailable, "backend_unavailable", err.Error())
		return
	}
	defer backendConn.Close()

	_, err = p.forwardToBackend(r.Context(), backendConn, backendAddr, payload)
	if err != nil {
		p.metrics.IncRequests(topic, "error", "lfs")
		p.trackOrphans([]orphanInfo{{Topic: topic, Key: objectKey, RequestID: requestID, Reason: "kafka_produce_failed"}})
		p.tracker.EmitUploadFailed(requestID, topic, objectKey, "backend_error", err.Error(), "kafka_produce", size, time.Since(start))
		p.writeHTTPError(w, requestID, topic, http.StatusBadGateway, "backend_error", err.Error())
		return
	}

	p.metrics.IncRequests(topic, "ok", "lfs")
	p.metrics.AddUploadBytes(size)
	p.metrics.ObserveUploadDuration(time.Since(start).Seconds())

	// Emit upload_completed event
	p.tracker.EmitUploadCompleted(requestID, topic, partition, 0, p.s3Bucket, objectKey, size, sha256Hex, checksum, checksumAlg, contentType, time.Since(start))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(env)
}

func (p *lfsProxy) handleHTTPDownload(w http.ResponseWriter, r *http.Request) {
	requestID := strings.TrimSpace(r.Header.Get(headerRequestID))
	if requestID == "" {
		requestID = newUUID()
	}
	w.Header().Set(headerRequestID, requestID)
	if r.Method != http.MethodPost {
		p.writeHTTPError(w, requestID, "", http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if p.httpAPIKey != "" && !p.validateHTTPAPIKey(r) {
		p.writeHTTPError(w, requestID, "", http.StatusUnauthorized, "unauthorized", "unauthorized")
		return
	}
	if !p.isReady() {
		p.writeHTTPError(w, requestID, "", http.StatusServiceUnavailable, "proxy_not_ready", "proxy not ready")
		return
	}

	var req downloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		p.writeHTTPError(w, requestID, "", http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return
	}
	req.Bucket = strings.TrimSpace(req.Bucket)
	req.Key = strings.TrimSpace(req.Key)
	if req.Bucket == "" || req.Key == "" {
		p.writeHTTPError(w, requestID, "", http.StatusBadRequest, "invalid_request", "bucket and key required")
		return
	}
	if req.Bucket != p.s3Bucket {
		p.writeHTTPError(w, requestID, "", http.StatusBadRequest, "invalid_bucket", "bucket not allowed")
		return
	}
	if err := p.validateObjectKey(req.Key); err != nil {
		p.writeHTTPError(w, requestID, "", http.StatusBadRequest, "invalid_key", err.Error())
		return
	}

	mode := strings.ToLower(strings.TrimSpace(req.Mode))
	if mode == "" {
		mode = "presign"
	}
	if mode != "presign" && mode != "stream" {
		p.writeHTTPError(w, requestID, "", http.StatusBadRequest, "invalid_mode", "mode must be presign or stream")
		return
	}

	clientIP := getClientIP(r)
	start := time.Now()

	// Emit download_requested event
	ttlSeconds := 0
	if mode == "presign" {
		ttlSeconds = req.ExpiresSeconds
		if ttlSeconds <= 0 {
			ttlSeconds = int(p.downloadTTLMax.Seconds())
		}
	}
	p.tracker.EmitDownloadRequested(requestID, req.Bucket, req.Key, mode, clientIP, ttlSeconds)

	switch mode {
	case "presign":
		ttl := p.downloadTTLMax
		if req.ExpiresSeconds > 0 {
			requested := time.Duration(req.ExpiresSeconds) * time.Second
			if requested < ttl {
				ttl = requested
			}
		}
		url, err := p.s3Uploader.PresignGetObject(r.Context(), req.Key, ttl)
		if err != nil {
			p.metrics.IncS3Errors()
			p.writeHTTPError(w, requestID, "", http.StatusBadGateway, "s3_presign_failed", err.Error())
			return
		}
		// Emit download_completed for presign (URL generated)
		p.tracker.EmitDownloadCompleted(requestID, req.Key, mode, time.Since(start), 0)

		resp := downloadResponse{
			Mode:      "presign",
			URL:       url,
			ExpiresAt: time.Now().UTC().Add(ttl).Format(time.RFC3339),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	case "stream":
		obj, err := p.s3Uploader.GetObject(r.Context(), req.Key)
		if err != nil {
			p.metrics.IncS3Errors()
			p.writeHTTPError(w, requestID, "", http.StatusBadGateway, "s3_get_failed", err.Error())
			return
		}
		defer obj.Body.Close()
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
			p.logger.Warn("download stream failed", "error", err)
		}
		// Emit download_completed for stream
		p.tracker.EmitDownloadCompleted(requestID, req.Key, mode, time.Since(start), size)
	}
}

func (p *lfsProxy) handleHTTPUploadInit(w http.ResponseWriter, r *http.Request) {
	requestID := strings.TrimSpace(r.Header.Get(headerRequestID))
	if requestID == "" {
		requestID = newUUID()
	}
	w.Header().Set(headerRequestID, requestID)
	if r.Method != http.MethodPost {
		p.writeHTTPError(w, requestID, "", http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if p.httpAPIKey != "" && !p.validateHTTPAPIKey(r) {
		p.writeHTTPError(w, requestID, "", http.StatusUnauthorized, "unauthorized", "unauthorized")
		return
	}
	if !p.isReady() {
		p.writeHTTPError(w, requestID, "", http.StatusServiceUnavailable, "proxy_not_ready", "proxy not ready")
		return
	}

	var req uploadInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		p.writeHTTPError(w, requestID, "", http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return
	}

	req.Topic = strings.TrimSpace(req.Topic)
	req.ContentType = strings.TrimSpace(req.ContentType)
	req.Checksum = strings.TrimSpace(req.Checksum)
	req.ChecksumAlg = strings.TrimSpace(req.ChecksumAlg)
	if req.Topic == "" {
		p.writeHTTPError(w, requestID, "", http.StatusBadRequest, "missing_topic", "missing topic")
		return
	}
	if !p.isValidTopicName(req.Topic) {
		p.writeHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "invalid_topic", "invalid topic name")
		return
	}
	if req.ContentType == "" {
		p.writeHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "missing_content_type", "content_type required")
		return
	}
	if req.SizeBytes <= 0 {
		p.writeHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "invalid_size", "size_bytes must be > 0")
		return
	}
	if p.maxBlob > 0 && req.SizeBytes > p.maxBlob {
		p.writeHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "payload_too_large", "payload exceeds max size")
		return
	}

	keyBytes := []byte(nil)
	if req.Key != "" {
		decoded, err := base64.StdEncoding.DecodeString(req.Key)
		if err != nil {
			p.writeHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "invalid_key", "invalid key")
			return
		}
		keyBytes = decoded
	}

	partition := int32(0)
	if req.Partition != nil {
		partition = *req.Partition
		if partition < 0 {
			p.writeHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "invalid_partition", "invalid partition")
			return
		}
	}

	alg, err := p.resolveChecksumAlg(req.ChecksumAlg)
	if err != nil {
		p.writeHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if req.Checksum != "" && alg == lfs.ChecksumNone {
		p.writeHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "invalid_checksum", "checksum provided but checksum algorithm is none")
		return
	}

	objectKey := p.buildObjectKey(req.Topic)
	uploadID, err := p.s3Uploader.StartMultipartUpload(r.Context(), objectKey, req.ContentType)
	if err != nil {
		p.metrics.IncS3Errors()
		p.writeHTTPError(w, requestID, req.Topic, http.StatusBadGateway, "s3_upload_failed", err.Error())
		return
	}
	p.logger.Info("http chunked upload init", "requestId", requestID, "topic", req.Topic, "s3Key", objectKey, "uploadId", uploadID, "sizeBytes", req.SizeBytes, "partSize", p.chunkSize)

	partSize := normalizeChunkSize(p.chunkSize)
	session := &uploadSession{
		ID:           newUUID(),
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
		ExpiresAt:    time.Now().UTC().Add(p.uploadSessionTTL),
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
			p.writeHTTPError(w, requestID, req.Topic, http.StatusBadRequest, "invalid_checksum", err.Error())
			return
		}
	}

	p.storeUploadSession(session)
	p.tracker.EmitUploadStarted(requestID, req.Topic, partition, objectKey, req.ContentType, getClientIP(r), "http-chunked", req.SizeBytes)

	resp := uploadInitResponse{
		UploadID:  session.ID,
		S3Key:     session.S3Key,
		PartSize:  session.PartSize,
		ExpiresAt: session.ExpiresAt.Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (p *lfsProxy) handleHTTPUploadSession(w http.ResponseWriter, r *http.Request) {
	requestID := strings.TrimSpace(r.Header.Get(headerRequestID))
	if requestID == "" {
		requestID = newUUID()
	}
	w.Header().Set(headerRequestID, requestID)
	if p.httpAPIKey != "" && !p.validateHTTPAPIKey(r) {
		p.writeHTTPError(w, requestID, "", http.StatusUnauthorized, "unauthorized", "unauthorized")
		return
	}
	if !p.isReady() {
		p.writeHTTPError(w, requestID, "", http.StatusServiceUnavailable, "proxy_not_ready", "proxy not ready")
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/lfs/uploads/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		p.writeHTTPError(w, requestID, "", http.StatusNotFound, "not_found", "not found")
		return
	}
	uploadID := parts[0]

	switch {
	case len(parts) == 1 && r.Method == http.MethodDelete:
		p.handleHTTPUploadAbort(w, r, requestID, uploadID)
		return
	case len(parts) == 2 && parts[1] == "complete" && r.Method == http.MethodPost:
		p.handleHTTPUploadComplete(w, r, requestID, uploadID)
		return
	case len(parts) == 3 && parts[1] == "parts" && r.Method == http.MethodPut:
		partNum, err := strconv.ParseInt(parts[2], 10, 32)
		if err != nil || partNum <= 0 || partNum > math.MaxInt32 {
			p.writeHTTPError(w, requestID, "", http.StatusBadRequest, "invalid_part", "invalid part number")
			return
		}
		p.handleHTTPUploadPart(w, r, requestID, uploadID, int32(partNum))
		return
	default:
		p.writeHTTPError(w, requestID, "", http.StatusNotFound, "not_found", "not found")
		return
	}
}

func (p *lfsProxy) handleHTTPUploadPart(w http.ResponseWriter, r *http.Request, requestID, sessionID string, partNumber int32) {
	session, ok := p.getUploadSession(sessionID)
	if !ok {
		p.writeHTTPError(w, requestID, "", http.StatusNotFound, "upload_not_found", "upload session not found")
		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if time.Now().UTC().After(session.ExpiresAt) {
		p.deleteUploadSession(sessionID)
		p.writeHTTPError(w, requestID, session.Topic, http.StatusGone, "upload_expired", "upload session expired")
		return
	}

	if etag, exists := session.Parts[partNumber]; exists {
		_, _ = io.Copy(io.Discard, r.Body)
		p.logger.Info("http chunked upload part already received", "requestId", requestID, "uploadId", sessionID, "part", partNumber, "etag", etag)
		resp := uploadPartResponse{UploadID: sessionID, PartNumber: partNumber, ETag: etag}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	if partNumber != session.NextPart {
		p.writeHTTPError(w, requestID, session.Topic, http.StatusConflict, "out_of_order", "part out of order")
		return
	}

	limit := session.PartSize + 1
	body, err := io.ReadAll(io.LimitReader(r.Body, limit))
	if err != nil {
		p.writeHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_part", err.Error())
		return
	}
	if int64(len(body)) == 0 {
		p.writeHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_part", "empty part")
		return
	}
	if int64(len(body)) > session.PartSize {
		p.writeHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_part", "part too large")
		return
	}
	if session.TotalUploaded+int64(len(body)) > session.SizeBytes {
		p.writeHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_part", "part exceeds declared size")
		return
	}
	if session.TotalUploaded+int64(len(body)) < session.SizeBytes && int64(len(body)) < minMultipartChunkSize {
		p.writeHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_part", "part too small")
		return
	}

	if _, err := session.sha256Hasher.Write(body); err != nil {
		p.writeHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "hash_error", err.Error())
		return
	}
	if session.checksumHasher != nil && session.checksumHasher != session.sha256Hasher {
		if _, err := session.checksumHasher.Write(body); err != nil {
			p.writeHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "hash_error", err.Error())
			return
		}
	}

	etag, err := p.s3Uploader.UploadPart(r.Context(), session.S3Key, session.UploadID, partNumber, body)
	if err != nil {
		p.metrics.IncS3Errors()
		p.tracker.EmitUploadFailed(requestID, session.Topic, session.S3Key, "s3_upload_failed", err.Error(), "upload_part", session.TotalUploaded, 0)
		p.writeHTTPError(w, requestID, session.Topic, http.StatusBadGateway, "s3_upload_failed", err.Error())
		return
	}
	p.logger.Info("http chunked upload part stored", "requestId", requestID, "uploadId", sessionID, "part", partNumber, "etag", etag, "bytes", len(body))

	session.Parts[partNumber] = etag
	session.PartSizes[partNumber] = int64(len(body))
	session.TotalUploaded += int64(len(body))
	session.NextPart++

	resp := uploadPartResponse{UploadID: sessionID, PartNumber: partNumber, ETag: etag}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (p *lfsProxy) handleHTTPUploadComplete(w http.ResponseWriter, r *http.Request, requestID, sessionID string) {
	session, ok := p.getUploadSession(sessionID)
	if !ok {
		p.writeHTTPError(w, requestID, "", http.StatusNotFound, "upload_not_found", "upload session not found")
		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if time.Now().UTC().After(session.ExpiresAt) {
		p.deleteUploadSession(sessionID)
		p.writeHTTPError(w, requestID, session.Topic, http.StatusGone, "upload_expired", "upload session expired")
		return
	}
	if session.TotalUploaded != session.SizeBytes {
		p.writeHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "incomplete_upload", "not all bytes uploaded")
		return
	}

	var req uploadCompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		p.writeHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return
	}
	if len(req.Parts) == 0 {
		p.writeHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_request", "parts required")
		return
	}

	completed := make([]types.CompletedPart, 0, len(req.Parts))
	for _, part := range req.Parts {
		etag, ok := session.Parts[part.PartNumber]
		if !ok || etag == "" || part.ETag == "" || etag != part.ETag {
			p.writeHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "invalid_part", "part etag mismatch")
			return
		}
		completed = append(completed, types.CompletedPart{
			ETag:       aws.String(part.ETag),
			PartNumber: aws.Int32(part.PartNumber),
		})
	}

	if err := p.s3Uploader.CompleteMultipartUpload(r.Context(), session.S3Key, session.UploadID, completed); err != nil {
		p.metrics.IncS3Errors()
		p.tracker.EmitUploadFailed(requestID, session.Topic, session.S3Key, "s3_upload_failed", err.Error(), "upload_complete", session.TotalUploaded, 0)
		p.writeHTTPError(w, requestID, session.Topic, http.StatusBadGateway, "s3_upload_failed", err.Error())
		return
	}
	p.logger.Info("http chunked upload completed", "requestId", requestID, "uploadId", sessionID, "parts", len(completed), "bytes", session.TotalUploaded)

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
		_ = p.s3Uploader.AbortMultipartUpload(r.Context(), session.S3Key, session.UploadID)
		p.writeHTTPError(w, requestID, session.Topic, http.StatusBadRequest, "checksum_mismatch", "checksum mismatch")
		return
	}

	env := lfs.Envelope{
		Version:     1,
		Bucket:      p.s3Bucket,
		Key:         session.S3Key,
		Size:        session.TotalUploaded,
		SHA256:      shaHex,
		Checksum:    checksum,
		ChecksumAlg: string(session.ChecksumAlg),
		ContentType: session.ContentType,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		ProxyID:     p.proxyID,
	}
	encoded, err := lfs.EncodeEnvelope(env)
	if err != nil {
		p.writeHTTPError(w, requestID, session.Topic, http.StatusInternalServerError, "encode_failed", err.Error())
		return
	}

	record := kmsg.Record{
		TimestampDelta64: 0,
		OffsetDelta:      0,
		Key:              session.KeyBytes,
		Value:            encoded,
	}
	batchBytes := buildRecordBatch([]kmsg.Record{record})

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

	correlationID := int32(atomic.AddUint32(&p.corrID, 1))
	reqHeader := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: correlationID}
	payload, err := encodeProduceRequest(reqHeader, produceReq)
	if err != nil {
		p.writeHTTPError(w, requestID, session.Topic, http.StatusInternalServerError, "encode_failed", err.Error())
		return
	}

	backendConn, backendAddr, err := p.connectBackend(r.Context())
	if err != nil {
		p.trackOrphans([]orphanInfo{{Topic: session.Topic, Key: session.S3Key, RequestID: requestID, Reason: "kafka_produce_failed"}})
		p.tracker.EmitUploadFailed(requestID, session.Topic, session.S3Key, "backend_unavailable", err.Error(), "kafka_produce", session.TotalUploaded, 0)
		p.writeHTTPError(w, requestID, session.Topic, http.StatusServiceUnavailable, "backend_unavailable", err.Error())
		return
	}
	defer backendConn.Close()

	if _, err := p.forwardToBackend(r.Context(), backendConn, backendAddr, payload); err != nil {
		p.trackOrphans([]orphanInfo{{Topic: session.Topic, Key: session.S3Key, RequestID: requestID, Reason: "kafka_produce_failed"}})
		p.tracker.EmitUploadFailed(requestID, session.Topic, session.S3Key, "backend_error", err.Error(), "kafka_produce", session.TotalUploaded, 0)
		p.writeHTTPError(w, requestID, session.Topic, http.StatusBadGateway, "backend_error", err.Error())
		return
	}

	p.metrics.IncRequests(session.Topic, "ok", "lfs")
	p.metrics.AddUploadBytes(session.TotalUploaded)

	p.tracker.EmitUploadCompleted(requestID, session.Topic, session.Partition, 0, p.s3Bucket, session.S3Key, session.TotalUploaded, shaHex, checksum, string(session.ChecksumAlg), session.ContentType, 0)

	p.deleteUploadSession(sessionID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(env)
}

func (p *lfsProxy) handleHTTPUploadAbort(w http.ResponseWriter, r *http.Request, requestID, sessionID string) {
	session, ok := p.getUploadSession(sessionID)
	if !ok {
		p.writeHTTPError(w, requestID, "", http.StatusNotFound, "upload_not_found", "upload session not found")
		return
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	_ = p.s3Uploader.AbortMultipartUpload(r.Context(), session.S3Key, session.UploadID)
	p.deleteUploadSession(sessionID)
	w.WriteHeader(http.StatusNoContent)
}

func (p *lfsProxy) storeUploadSession(session *uploadSession) {
	if session == nil {
		return
	}
	p.uploadMu.Lock()
	defer p.uploadMu.Unlock()
	p.cleanupUploadSessionsLocked()
	p.uploadSessions[session.ID] = session
}

func (p *lfsProxy) getUploadSession(id string) (*uploadSession, bool) {
	p.uploadMu.Lock()
	defer p.uploadMu.Unlock()
	p.cleanupUploadSessionsLocked()
	session, ok := p.uploadSessions[id]
	return session, ok
}

func (p *lfsProxy) deleteUploadSession(id string) {
	p.uploadMu.Lock()
	defer p.uploadMu.Unlock()
	delete(p.uploadSessions, id)
}

func (p *lfsProxy) cleanupUploadSessionsLocked() {
	now := time.Now().UTC()
	for id, session := range p.uploadSessions {
		if session.ExpiresAt.Before(now) {
			delete(p.uploadSessions, id)
		}
	}
}

func statusForUploadError(err error) (int, string) {
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

func (p *lfsProxy) writeHTTPError(w http.ResponseWriter, requestID, topic string, status int, code, message string) {
	if topic != "" {
		p.logger.Warn("http produce failed", "status", status, "code", code, "requestId", requestID, "topic", topic, "error", message)
	} else {
		p.logger.Warn("http produce failed", "status", status, "code", code, "requestId", requestID, "error", message)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{
		Code:      code,
		Message:   message,
		RequestID: requestID,
	})
}

func (p *lfsProxy) validateHTTPAPIKey(r *http.Request) bool {
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
	// Use constant-time comparison to prevent timing attacks
	return subtle.ConstantTimeCompare([]byte(key), []byte(p.httpAPIKey)) == 1
}

func (p *lfsProxy) validateObjectKey(key string) error {
	if strings.HasPrefix(key, "/") {
		return errors.New("key must be relative")
	}
	if strings.Contains(key, "..") {
		return errors.New("key must not contain '..'")
	}
	ns := strings.TrimSpace(p.s3Namespace)
	if ns != "" && !strings.HasPrefix(key, ns+"/") {
		return errors.New("key outside namespace")
	}
	if !strings.Contains(key, "/lfs/") {
		return errors.New("key must include /lfs/ segment")
	}
	return nil
}

// isValidTopicName validates a Kafka topic name.
// Topics must be 1-249 characters, containing only alphanumeric, dots, underscores, or hyphens.
func (p *lfsProxy) isValidTopicName(topic string) bool {
	if len(topic) == 0 || len(topic) > p.topicMaxLength {
		return false
	}
	return validTopicPattern.MatchString(topic)
}

// getClientIP extracts the client IP address from the request.
// It checks X-Forwarded-For and X-Real-IP headers first, then falls back to RemoteAddr.
func getClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// X-Forwarded-For can contain multiple IPs; take the first one
		if idx := strings.Index(xff, ","); idx > 0 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	// Extract IP from RemoteAddr (host:port format)
	host, _, err := strings.Cut(r.RemoteAddr, ":")
	if err {
		return host
	}
	return r.RemoteAddr
}
