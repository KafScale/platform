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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testHTTPModule builds a fully-configured lfsModule for HTTP handler testing.
// It reuses the fakeS3/fakePresign types from lfs_test.go and populates the
// fields required by the HTTP API handlers.
func testHTTPModule(t *testing.T) *lfsModule {
	t.Helper()
	fs3 := newFakeS3()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := &lfsModule{
		logger:           logger,
		s3Uploader:       &s3Uploader{bucket: "test-bucket", region: "us-east-1", chunkSize: 5 << 20, api: fs3, presign: &fakePresign{}},
		s3Bucket:         "test-bucket",
		s3Namespace:      "test-ns",
		maxBlob:          5 << 30,
		chunkSize:        5 << 20,
		checksumAlg:      "sha256",
		proxyID:          "test-proxy",
		metrics:          newLfsMetrics(),
		tracker:          &LfsOpsTracker{config: TrackerConfig{}, logger: logger},
		httpAPIKey:       "test-secret-key",
		topicMaxLength:   249,
		downloadTTLMax:   2 * time.Minute,
		uploadSessionTTL: 1 * time.Hour,
		uploadSessions:   make(map[string]*uploadSession),
	}
	atomic.StoreUint32(&m.s3Healthy, 1)
	return m
}

// ---------------------------------------------------------------------------
// 1. lfsValidateHTTPAPIKey
// ---------------------------------------------------------------------------

func TestLfsValidateHTTPAPIKey_XAPIKeyHeader(t *testing.T) {
	m := testHTTPModule(t)
	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", nil)
	req.Header.Set("X-API-Key", "test-secret-key")
	if !m.lfsValidateHTTPAPIKey(req) {
		t.Fatal("expected valid X-API-Key header to pass")
	}
}

func TestLfsValidateHTTPAPIKey_BearerToken(t *testing.T) {
	m := testHTTPModule(t)
	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", nil)
	req.Header.Set("Authorization", "Bearer test-secret-key")
	if !m.lfsValidateHTTPAPIKey(req) {
		t.Fatal("expected valid Bearer token to pass")
	}
}

func TestLfsValidateHTTPAPIKey_EmptyKey(t *testing.T) {
	m := testHTTPModule(t)
	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", nil)
	// No auth headers set
	if m.lfsValidateHTTPAPIKey(req) {
		t.Fatal("expected empty key to fail")
	}
}

func TestLfsValidateHTTPAPIKey_WrongKey(t *testing.T) {
	m := testHTTPModule(t)
	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", nil)
	req.Header.Set("X-API-Key", "wrong-key")
	if m.lfsValidateHTTPAPIKey(req) {
		t.Fatal("expected wrong key to fail")
	}
}

func TestLfsValidateHTTPAPIKey_NilRequest(t *testing.T) {
	m := testHTTPModule(t)
	if m.lfsValidateHTTPAPIKey(nil) {
		t.Fatal("expected nil request to fail")
	}
}

func TestLfsValidateHTTPAPIKey_BearerCaseInsensitive(t *testing.T) {
	m := testHTTPModule(t)
	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", nil)
	req.Header.Set("Authorization", "BEARER test-secret-key")
	if !m.lfsValidateHTTPAPIKey(req) {
		t.Fatal("expected case-insensitive Bearer prefix to pass")
	}
}

// ---------------------------------------------------------------------------
// 2. lfsIsValidTopicName
// ---------------------------------------------------------------------------

func TestLfsIsValidTopicName_Valid(t *testing.T) {
	m := testHTTPModule(t)
	valid := []string{"my-topic", "topic.name", "topic_name", "Topic123", "a", "A-B.C_D"}
	for _, name := range valid {
		if !m.lfsIsValidTopicName(name) {
			t.Fatalf("expected %q to be valid", name)
		}
	}
}

func TestLfsIsValidTopicName_Empty(t *testing.T) {
	m := testHTTPModule(t)
	if m.lfsIsValidTopicName("") {
		t.Fatal("expected empty topic to be invalid")
	}
}

func TestLfsIsValidTopicName_TooLong(t *testing.T) {
	m := testHTTPModule(t)
	long := strings.Repeat("a", 250)
	if m.lfsIsValidTopicName(long) {
		t.Fatal("expected 250-char topic to be invalid")
	}
}

func TestLfsIsValidTopicName_ExactMaxLength(t *testing.T) {
	m := testHTTPModule(t)
	exact := strings.Repeat("a", 249)
	if !m.lfsIsValidTopicName(exact) {
		t.Fatal("expected 249-char topic to be valid")
	}
}

func TestLfsIsValidTopicName_SpecialChars(t *testing.T) {
	m := testHTTPModule(t)
	invalid := []string{"topic name", "topic/name", "topic@name", "topic#name", "topic$name", "topic!name"}
	for _, name := range invalid {
		if m.lfsIsValidTopicName(name) {
			t.Fatalf("expected %q to be invalid", name)
		}
	}
}

// ---------------------------------------------------------------------------
// 3. lfsValidateObjectKey
// ---------------------------------------------------------------------------

func TestLfsValidateObjectKey_Valid(t *testing.T) {
	m := testHTTPModule(t)
	err := m.lfsValidateObjectKey("test-ns/my-topic/lfs/2025/01/01/obj-abc")
	if err != nil {
		t.Fatalf("expected valid key, got: %v", err)
	}
}

func TestLfsValidateObjectKey_AbsolutePath(t *testing.T) {
	m := testHTTPModule(t)
	err := m.lfsValidateObjectKey("/test-ns/my-topic/lfs/2025/01/01/obj-abc")
	if err == nil {
		t.Fatal("expected error for absolute path")
	}
	if !strings.Contains(err.Error(), "relative") {
		t.Fatalf("expected 'relative' in error, got: %v", err)
	}
}

func TestLfsValidateObjectKey_ParentRefs(t *testing.T) {
	m := testHTTPModule(t)
	err := m.lfsValidateObjectKey("test-ns/../secret/lfs/data")
	if err == nil {
		t.Fatal("expected error for parent refs")
	}
	if !strings.Contains(err.Error(), "..") {
		t.Fatalf("expected '..' in error, got: %v", err)
	}
}

func TestLfsValidateObjectKey_WrongNamespace(t *testing.T) {
	m := testHTTPModule(t)
	err := m.lfsValidateObjectKey("wrong-ns/my-topic/lfs/2025/01/01/obj-abc")
	if err == nil {
		t.Fatal("expected error for wrong namespace")
	}
	if !strings.Contains(err.Error(), "namespace") {
		t.Fatalf("expected 'namespace' in error, got: %v", err)
	}
}

func TestLfsValidateObjectKey_MissingLFSSegment(t *testing.T) {
	m := testHTTPModule(t)
	err := m.lfsValidateObjectKey("test-ns/my-topic/data/2025/01/01/obj-abc")
	if err == nil {
		t.Fatal("expected error for missing /lfs/ segment")
	}
	if !strings.Contains(err.Error(), "/lfs/") {
		t.Fatalf("expected '/lfs/' in error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 4. lfsGetClientIP
// ---------------------------------------------------------------------------

func TestLfsGetClientIP_XForwardedForSingle(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	ip := lfsGetClientIP(req)
	if ip != "1.2.3.4" {
		t.Fatalf("expected 1.2.3.4, got %s", ip)
	}
}

func TestLfsGetClientIP_XForwardedForMultiple(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Forwarded-For", "1.2.3.4, 5.6.7.8, 9.10.11.12")
	ip := lfsGetClientIP(req)
	if ip != "1.2.3.4" {
		t.Fatalf("expected 1.2.3.4 (first in chain), got %s", ip)
	}
}

func TestLfsGetClientIP_XRealIP(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Real-IP", "10.0.0.1")
	ip := lfsGetClientIP(req)
	if ip != "10.0.0.1" {
		t.Fatalf("expected 10.0.0.1, got %s", ip)
	}
}

func TestLfsGetClientIP_RemoteAddrFallback(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	ip := lfsGetClientIP(req)
	if ip != "192.168.1.1" {
		t.Fatalf("expected 192.168.1.1, got %s", ip)
	}
}

func TestLfsGetClientIP_RemoteAddrNoPort(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "192.168.1.1"
	ip := lfsGetClientIP(req)
	if ip != "192.168.1.1" {
		t.Fatalf("expected 192.168.1.1, got %s", ip)
	}
}

func TestLfsGetClientIP_XForwardedForPrecedence(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Forwarded-For", "1.1.1.1")
	req.Header.Set("X-Real-IP", "2.2.2.2")
	req.RemoteAddr = "3.3.3.3:999"
	ip := lfsGetClientIP(req)
	if ip != "1.1.1.1" {
		t.Fatalf("expected X-Forwarded-For to take precedence, got %s", ip)
	}
}

// ---------------------------------------------------------------------------
// 5. lfsStatusForUploadError
// ---------------------------------------------------------------------------

func TestLfsStatusForUploadError_ExceedsMax(t *testing.T) {
	status, code := lfsStatusForUploadError(errors.New("upload exceeds max size"))
	if status != http.StatusBadRequest || code != "payload_too_large" {
		t.Fatalf("expected 400/payload_too_large, got %d/%s", status, code)
	}
}

func TestLfsStatusForUploadError_EmptyUpload(t *testing.T) {
	status, code := lfsStatusForUploadError(errors.New("empty upload"))
	if status != http.StatusBadRequest || code != "empty_upload" {
		t.Fatalf("expected 400/empty_upload, got %d/%s", status, code)
	}
}

func TestLfsStatusForUploadError_S3KeyRequired(t *testing.T) {
	status, code := lfsStatusForUploadError(errors.New("s3 key required"))
	if status != http.StatusBadRequest || code != "invalid_key" {
		t.Fatalf("expected 400/invalid_key, got %d/%s", status, code)
	}
}

func TestLfsStatusForUploadError_ReaderRequired(t *testing.T) {
	status, code := lfsStatusForUploadError(errors.New("reader required"))
	if status != http.StatusBadRequest || code != "invalid_reader" {
		t.Fatalf("expected 400/invalid_reader, got %d/%s", status, code)
	}
}

func TestLfsStatusForUploadError_Default(t *testing.T) {
	status, code := lfsStatusForUploadError(errors.New("some unknown s3 error"))
	if status != http.StatusBadGateway || code != "s3_upload_failed" {
		t.Fatalf("expected 502/s3_upload_failed, got %d/%s", status, code)
	}
}

// ---------------------------------------------------------------------------
// 6. lfsCORSMiddleware
// ---------------------------------------------------------------------------

func TestLfsCORSMiddleware_Preflight(t *testing.T) {
	m := testHTTPModule(t)
	innerCalled := false
	inner := func(w http.ResponseWriter, r *http.Request) {
		innerCalled = true
	}
	handler := m.lfsCORSMiddleware(inner)

	req := httptest.NewRequest(http.MethodOptions, "/lfs/produce", nil)
	rr := httptest.NewRecorder()
	handler(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rr.Code)
	}
	if innerCalled {
		t.Fatal("inner handler should not be called on OPTIONS preflight")
	}
	if rr.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatal("expected Access-Control-Allow-Origin: *")
	}
	if rr.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Fatal("expected Access-Control-Allow-Methods header")
	}
	if rr.Header().Get("Access-Control-Allow-Headers") == "" {
		t.Fatal("expected Access-Control-Allow-Headers header")
	}
	if rr.Header().Get("Access-Control-Expose-Headers") != "X-Request-ID" {
		t.Fatal("expected Access-Control-Expose-Headers: X-Request-ID")
	}
}

func TestLfsCORSMiddleware_NormalRequest(t *testing.T) {
	m := testHTTPModule(t)
	innerCalled := false
	inner := func(w http.ResponseWriter, r *http.Request) {
		innerCalled = true
		w.WriteHeader(http.StatusOK)
	}
	handler := m.lfsCORSMiddleware(inner)

	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", nil)
	rr := httptest.NewRecorder()
	handler(rr, req)

	if !innerCalled {
		t.Fatal("inner handler should be called for non-OPTIONS requests")
	}
	if rr.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatal("expected CORS headers on normal request")
	}
}

// ---------------------------------------------------------------------------
// 7. handleHTTPProduce
// ---------------------------------------------------------------------------

func TestHandleHTTPProduce_MethodNotAllowed(t *testing.T) {
	m := testHTTPModule(t)
	// Disable API key check for this test
	m.httpAPIKey = ""

	req := httptest.NewRequest(http.MethodGet, "/lfs/produce", nil)
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "method_not_allowed" {
		t.Fatalf("expected code=method_not_allowed, got %s", errResp.Code)
	}
}

func TestHandleHTTPProduce_Unauthorized(t *testing.T) {
	m := testHTTPModule(t)

	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", nil)
	// No auth headers
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestHandleHTTPProduce_MissingTopic(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", strings.NewReader("data"))
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "missing_topic" {
		t.Fatalf("expected code=missing_topic, got %s", errResp.Code)
	}
}

func TestHandleHTTPProduce_InvalidTopic(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", strings.NewReader("data"))
	req.Header.Set("X-Kafka-Topic", "invalid topic!")
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "invalid_topic" {
		t.Fatalf("expected code=invalid_topic, got %s", errResp.Code)
	}
}

func TestHandleHTTPProduce_S3Unhealthy(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	atomic.StoreUint32(&m.s3Healthy, 0)

	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", strings.NewReader("data"))
	req.Header.Set("X-Kafka-Topic", "test-topic")
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "proxy_not_ready" {
		t.Fatalf("expected code=proxy_not_ready, got %s", errResp.Code)
	}
}

func TestHandleHTTPProduce_RequestIDEcho(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	req := httptest.NewRequest(http.MethodGet, "/lfs/produce", nil)
	req.Header.Set("X-Request-ID", "my-custom-id")
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, req)

	if rr.Header().Get("X-Request-ID") != "my-custom-id" {
		t.Fatalf("expected X-Request-ID echoed, got %s", rr.Header().Get("X-Request-ID"))
	}
}

// ---------------------------------------------------------------------------
// 8. handleHTTPDownload
// ---------------------------------------------------------------------------

func TestHandleHTTPDownload_MethodNotAllowed(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	req := httptest.NewRequest(http.MethodGet, "/lfs/download", nil)
	rr := httptest.NewRecorder()
	m.handleHTTPDownload(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
}

func TestHandleHTTPDownload_InvalidJSON(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	req := httptest.NewRequest(http.MethodPost, "/lfs/download", strings.NewReader("not json"))
	rr := httptest.NewRecorder()
	m.handleHTTPDownload(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "invalid_request" {
		t.Fatalf("expected code=invalid_request, got %s", errResp.Code)
	}
}

func TestHandleHTTPDownload_MissingBucketKey(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	body, _ := json.Marshal(lfsDownloadRequest{Bucket: "", Key: ""})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPDownload(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "invalid_request" {
		t.Fatalf("expected code=invalid_request, got %s", errResp.Code)
	}
}

func TestHandleHTTPDownload_WrongBucket(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	body, _ := json.Marshal(lfsDownloadRequest{
		Bucket: "wrong-bucket",
		Key:    "test-ns/topic/lfs/2025/01/01/obj-123",
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPDownload(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "invalid_bucket" {
		t.Fatalf("expected code=invalid_bucket, got %s", errResp.Code)
	}
}

func TestHandleHTTPDownload_PresignMode(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.presignEnabled = true

	const sha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	body, _ := json.Marshal(lfsDownloadRequest{
		Bucket: "test-bucket",
		Key:    "test-ns/topic/lfs/2025/01/01/obj-123",
		Mode:   "presign",
		Integrity: &lfsIntegrityRequest{
			SHA256:      sha,
			ChecksumAlg: "sha256",
			Size:        1024,
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPDownload(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rr.Code, rr.Body.String())
	}
	var resp lfsDownloadResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Mode != "presign" {
		t.Fatalf("expected mode=presign, got %s", resp.Mode)
	}
	if resp.URL == "" {
		t.Fatal("expected non-empty presigned URL")
	}
	if resp.ExpiresAt == "" {
		t.Fatal("expected non-empty expires_at")
	}
	if resp.Integrity == nil {
		t.Fatal("expected integrity block in presign response")
	}
	if resp.Integrity.SHA256 != sha {
		t.Fatalf("expected integrity.sha256=%s, got %s", sha, resp.Integrity.SHA256)
	}
}

func TestHandleHTTPDownload_PresignDisabledByDefault(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	// presignEnabled defaults to false — do not override

	body, _ := json.Marshal(lfsDownloadRequest{
		Bucket: "test-bucket",
		Key:    "test-ns/topic/lfs/2025/01/01/obj-123",
		Mode:   "presign",
		Integrity: &lfsIntegrityRequest{
			SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPDownload(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", rr.Code, rr.Body.String())
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "presign_disabled" {
		t.Fatalf("expected code=presign_disabled, got %s", errResp.Code)
	}
}

func TestHandleHTTPDownload_MissingIntegrity(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.presignEnabled = true

	body, _ := json.Marshal(lfsDownloadRequest{
		Bucket: "test-bucket",
		Key:    "test-ns/topic/lfs/2025/01/01/obj-123",
		Mode:   "presign",
		// Integrity intentionally omitted
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPDownload(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d; body: %s", rr.Code, rr.Body.String())
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "missing_integrity" {
		t.Fatalf("expected code=missing_integrity, got %s", errResp.Code)
	}
}

func TestHandleHTTPDownload_StreamVerifyMatch(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	// Seed the fake S3 with a known payload and compute its real sha256.
	payload := []byte("hello integrity world")
	key := "test-ns/topic/lfs/2025/01/01/obj-verify-ok"
	fs3 := m.s3Uploader.api.(*fakeS3)
	fs3.objects[key] = payload
	h := sha256.New()
	h.Write(payload)
	expectedSHA := hex.EncodeToString(h.Sum(nil))

	body, _ := json.Marshal(lfsDownloadRequest{
		Bucket: "test-bucket",
		Key:    key,
		Mode:   "stream",
		Integrity: &lfsIntegrityRequest{
			SHA256:      expectedSHA,
			ChecksumAlg: "sha256",
			Size:        int64(len(payload)),
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPDownload(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("X-Kafscale-LFS-Checksum"); got != "sha256="+expectedSHA {
		t.Errorf("expected X-Kafscale-LFS-Checksum=sha256=%s, got %q", expectedSHA, got)
	}
	if !bytes.Equal(rr.Body.Bytes(), payload) {
		t.Errorf("response body mismatch")
	}
}

func TestHandleHTTPDownload_StreamVerifyMismatch(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	payload := []byte("legitimate content")
	key := "test-ns/topic/lfs/2025/01/01/obj-tampered"
	fs3 := m.s3Uploader.api.(*fakeS3)
	fs3.objects[key] = payload

	// Client claims a different checksum — simulates tampered object or
	// stale envelope.
	wrongSHA := "0000000000000000000000000000000000000000000000000000000000000000"

	body, _ := json.Marshal(lfsDownloadRequest{
		Bucket: "test-bucket",
		Key:    key,
		Mode:   "stream",
		Integrity: &lfsIntegrityRequest{
			SHA256:      wrongSHA,
			ChecksumAlg: "sha256",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	aborted := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				if r == http.ErrAbortHandler {
					aborted = true
				} else {
					t.Fatalf("unexpected panic: %v", r)
				}
			}
		}()
		m.handleHTTPDownload(rr, req)
	}()

	if !aborted {
		t.Fatalf("expected handler to panic with http.ErrAbortHandler on integrity mismatch, got code=%d body=%s",
			rr.Code, rr.Body.String())
	}
}

func TestHandleHTTPDownload_DefaultModeIsStream(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	// presignEnabled stays false — default mode must be stream, not presign,
	// otherwise requests without explicit mode would hit presign_disabled.

	body, _ := json.Marshal(lfsDownloadRequest{
		Bucket: "test-bucket",
		Key:    "test-ns/topic/lfs/2025/01/01/obj-123",
		// Mode intentionally omitted (empty string → default)
		Integrity: &lfsIntegrityRequest{
			SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	// Stream mode panics with http.ErrAbortHandler on integrity mismatch
	// (which is expected — the fake s3Uploader returns no bytes, so the hash
	// will not match the provided checksum). Go's net/http server recovers
	// from this panic automatically in production; here we recover manually
	// to inspect the response.
	func() {
		defer func() {
			if r := recover(); r != nil {
				if r != http.ErrAbortHandler {
					t.Fatalf("unexpected panic: %v", r)
				}
			}
		}()
		m.handleHTTPDownload(rr, req)
	}()

	// The key assertion: it did NOT short-circuit to presign_disabled, which
	// is what would happen if the default mode were still presign.
	if rr.Code == http.StatusBadRequest {
		var errResp lfsErrorResponse
		if err := json.NewDecoder(rr.Body).Decode(&errResp); err == nil {
			if errResp.Code == "presign_disabled" {
				t.Fatalf("default mode must not be presign; got presign_disabled")
			}
		}
	}

	// Confirm the stream integrity headers were set before the connection
	// was torn down.
	if got := rr.Header().Get("X-Kafscale-LFS-Checksum"); got == "" {
		t.Errorf("expected X-Kafscale-LFS-Checksum header to be set")
	}
}

func TestHandleHTTPDownload_InvalidMode(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	body, _ := json.Marshal(lfsDownloadRequest{
		Bucket: "test-bucket",
		Key:    "test-ns/topic/lfs/2025/01/01/obj-123",
		Mode:   "invalid",
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPDownload(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "invalid_mode" {
		t.Fatalf("expected code=invalid_mode, got %s", errResp.Code)
	}
}

func TestHandleHTTPDownload_InvalidObjectKey(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	body, _ := json.Marshal(lfsDownloadRequest{
		Bucket: "test-bucket",
		Key:    "/absolute/path/lfs/obj",
		Mode:   "presign",
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPDownload(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "invalid_key" {
		t.Fatalf("expected code=invalid_key, got %s", errResp.Code)
	}
}

func TestHandleHTTPDownload_S3Unhealthy(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	atomic.StoreUint32(&m.s3Healthy, 0)

	body, _ := json.Marshal(lfsDownloadRequest{
		Bucket: "test-bucket",
		Key:    "test-ns/topic/lfs/obj",
		Mode:   "presign",
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPDownload(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// 9. handleHTTPUploadInit
// ---------------------------------------------------------------------------

func TestHandleHTTPUploadInit_MethodNotAllowed(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	req := httptest.NewRequest(http.MethodGet, "/lfs/uploads", nil)
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
}

func TestHandleHTTPUploadInit_MissingTopic(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	body, _ := json.Marshal(lfsUploadInitRequest{
		ContentType: "application/octet-stream",
		SizeBytes:   1024,
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "missing_topic" {
		t.Fatalf("expected code=missing_topic, got %s", errResp.Code)
	}
}

func TestHandleHTTPUploadInit_MissingContentType(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	body, _ := json.Marshal(lfsUploadInitRequest{
		Topic:     "my-topic",
		SizeBytes: 1024,
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "missing_content_type" {
		t.Fatalf("expected code=missing_content_type, got %s", errResp.Code)
	}
}

func TestHandleHTTPUploadInit_InvalidSize(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	body, _ := json.Marshal(lfsUploadInitRequest{
		Topic:       "my-topic",
		ContentType: "application/octet-stream",
		SizeBytes:   0,
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "invalid_size" {
		t.Fatalf("expected code=invalid_size, got %s", errResp.Code)
	}
}

func TestHandleHTTPUploadInit_NegativeSize(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	body, _ := json.Marshal(lfsUploadInitRequest{
		Topic:       "my-topic",
		ContentType: "application/octet-stream",
		SizeBytes:   -100,
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "invalid_size" {
		t.Fatalf("expected code=invalid_size, got %s", errResp.Code)
	}
}

func TestHandleHTTPUploadInit_PayloadTooLarge(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.maxBlob = 1000 // Set small limit for test

	body, _ := json.Marshal(lfsUploadInitRequest{
		Topic:       "my-topic",
		ContentType: "application/octet-stream",
		SizeBytes:   2000,
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "payload_too_large" {
		t.Fatalf("expected code=payload_too_large, got %s", errResp.Code)
	}
}

func TestHandleHTTPUploadInit_Success(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	body, _ := json.Marshal(lfsUploadInitRequest{
		Topic:       "my-topic",
		ContentType: "application/octet-stream",
		SizeBytes:   10 << 20,
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rr.Code, rr.Body.String())
	}
	var resp lfsUploadInitResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.UploadID == "" {
		t.Fatal("expected non-empty upload_id")
	}
	if resp.S3Key == "" {
		t.Fatal("expected non-empty s3_key")
	}
	if resp.PartSize <= 0 {
		t.Fatal("expected positive part_size")
	}
	if resp.ExpiresAt == "" {
		t.Fatal("expected non-empty expires_at")
	}
}

func TestHandleHTTPUploadInit_InvalidTopic(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	body, _ := json.Marshal(lfsUploadInitRequest{
		Topic:       "invalid topic!",
		ContentType: "application/octet-stream",
		SizeBytes:   1024,
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "invalid_topic" {
		t.Fatalf("expected code=invalid_topic, got %s", errResp.Code)
	}
}

// ---------------------------------------------------------------------------
// 10. handleHTTPUploadSession routing
// ---------------------------------------------------------------------------

func TestHandleHTTPUploadSession_PartUploadRoute(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	// PUT /lfs/uploads/{id}/parts/1 with no session => not_found (upload_not_found)
	req := httptest.NewRequest(http.MethodPut, "/lfs/uploads/nonexistent/parts/1", strings.NewReader("data"))
	rr := httptest.NewRecorder()
	m.handleHTTPUploadSession(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "upload_not_found" {
		t.Fatalf("expected code=upload_not_found, got %s", errResp.Code)
	}
}

func TestHandleHTTPUploadSession_CompleteRoute(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	// POST /lfs/uploads/{id}/complete with no session => not_found (upload_not_found)
	body, _ := json.Marshal(lfsUploadCompleteRequest{})
	req := httptest.NewRequest(http.MethodPost, "/lfs/uploads/nonexistent/complete", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPUploadSession(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "upload_not_found" {
		t.Fatalf("expected code=upload_not_found, got %s", errResp.Code)
	}
}

func TestHandleHTTPUploadSession_AbortRoute(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	// DELETE /lfs/uploads/{id} with no session => not_found (upload_not_found)
	req := httptest.NewRequest(http.MethodDelete, "/lfs/uploads/nonexistent", nil)
	rr := httptest.NewRecorder()
	m.handleHTTPUploadSession(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "upload_not_found" {
		t.Fatalf("expected code=upload_not_found, got %s", errResp.Code)
	}
}

func TestHandleHTTPUploadSession_InvalidPath(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	// GET /lfs/uploads/some-id/unknown => not_found
	req := httptest.NewRequest(http.MethodGet, "/lfs/uploads/some-id/unknown", nil)
	rr := httptest.NewRecorder()
	m.handleHTTPUploadSession(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "not_found" {
		t.Fatalf("expected code=not_found, got %s", errResp.Code)
	}
}

func TestHandleHTTPUploadSession_EmptyPath(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	req := httptest.NewRequest(http.MethodGet, "/lfs/uploads/", nil)
	rr := httptest.NewRecorder()
	m.handleHTTPUploadSession(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestHandleHTTPUploadSession_InvalidPartNumber(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	req := httptest.NewRequest(http.MethodPut, "/lfs/uploads/some-id/parts/abc", strings.NewReader("data"))
	rr := httptest.NewRecorder()
	m.handleHTTPUploadSession(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "invalid_part" {
		t.Fatalf("expected code=invalid_part, got %s", errResp.Code)
	}
}

func TestHandleHTTPUploadSession_ZeroPartNumber(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	req := httptest.NewRequest(http.MethodPut, "/lfs/uploads/some-id/parts/0", strings.NewReader("data"))
	rr := httptest.NewRecorder()
	m.handleHTTPUploadSession(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "invalid_part" {
		t.Fatalf("expected code=invalid_part, got %s", errResp.Code)
	}
}

// ---------------------------------------------------------------------------
// 11. lfsWriteHTTPError
// ---------------------------------------------------------------------------

func TestLfsWriteHTTPError_JSONFormat(t *testing.T) {
	m := testHTTPModule(t)
	rr := httptest.NewRecorder()
	m.lfsWriteHTTPError(rr, "req-123", "test-topic", http.StatusBadRequest, "test_code", "test message")

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected Content-Type=application/json, got %s", ct)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "test_code" {
		t.Fatalf("expected code=test_code, got %s", errResp.Code)
	}
	if errResp.Message != "test message" {
		t.Fatalf("expected message='test message', got %s", errResp.Message)
	}
	if errResp.RequestID != "req-123" {
		t.Fatalf("expected request_id=req-123, got %s", errResp.RequestID)
	}
}

func TestLfsWriteHTTPError_NoTopic(t *testing.T) {
	m := testHTTPModule(t)
	rr := httptest.NewRecorder()
	m.lfsWriteHTTPError(rr, "req-456", "", http.StatusInternalServerError, "internal", "something went wrong")

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "internal" {
		t.Fatalf("expected code=internal, got %s", errResp.Code)
	}
}

// ---------------------------------------------------------------------------
// 12. Upload session management
// ---------------------------------------------------------------------------

func TestUploadSession_StoreGetDelete(t *testing.T) {
	m := testHTTPModule(t)

	session := &uploadSession{
		ID:        "session-1",
		Topic:     "my-topic",
		S3Key:     "test-ns/my-topic/lfs/2025/01/01/obj-1",
		UploadID:  "s3-upload-id",
		ExpiresAt: time.Now().UTC().Add(1 * time.Hour),
		Parts:     make(map[int32]string),
		PartSizes: make(map[int32]int64),
	}

	// Store
	m.lfsStoreUploadSession(session)

	// Get
	got, ok := m.lfsGetUploadSession("session-1")
	if !ok {
		t.Fatal("expected to find session-1")
	}
	if got.Topic != "my-topic" {
		t.Fatalf("expected topic=my-topic, got %s", got.Topic)
	}

	// Get non-existent
	_, ok = m.lfsGetUploadSession("nonexistent")
	if ok {
		t.Fatal("expected not to find nonexistent session")
	}

	// Delete
	m.lfsDeleteUploadSession("session-1")
	_, ok = m.lfsGetUploadSession("session-1")
	if ok {
		t.Fatal("expected session to be deleted")
	}
}

func TestUploadSession_TTLCleanup(t *testing.T) {
	m := testHTTPModule(t)

	expiredSession := &uploadSession{
		ID:        "expired-session",
		Topic:     "topic-a",
		ExpiresAt: time.Now().UTC().Add(-1 * time.Second), // Already expired
		Parts:     make(map[int32]string),
		PartSizes: make(map[int32]int64),
	}
	activeSession := &uploadSession{
		ID:        "active-session",
		Topic:     "topic-b",
		ExpiresAt: time.Now().UTC().Add(1 * time.Hour),
		Parts:     make(map[int32]string),
		PartSizes: make(map[int32]int64),
	}

	// Store both directly to bypass cleanup during store
	m.uploadMu.Lock()
	m.uploadSessions[expiredSession.ID] = expiredSession
	m.uploadSessions[activeSession.ID] = activeSession
	m.uploadMu.Unlock()

	// Get triggers cleanup
	_, ok := m.lfsGetUploadSession("expired-session")
	if ok {
		t.Fatal("expected expired session to be cleaned up")
	}

	got, ok := m.lfsGetUploadSession("active-session")
	if !ok {
		t.Fatal("expected active session to survive cleanup")
	}
	if got.Topic != "topic-b" {
		t.Fatalf("expected topic-b, got %s", got.Topic)
	}
}

func TestUploadSession_StoreNilSafe(t *testing.T) {
	m := testHTTPModule(t)
	// Should not panic
	m.lfsStoreUploadSession(nil)

	// No sessions should be stored
	m.uploadMu.Lock()
	count := len(m.uploadSessions)
	m.uploadMu.Unlock()
	if count != 0 {
		t.Fatalf("expected 0 sessions, got %d", count)
	}
}

func TestUploadSession_DeleteNonExistent(t *testing.T) {
	m := testHTTPModule(t)
	// Should not panic
	m.lfsDeleteUploadSession("does-not-exist")
}

func TestUploadSession_MultipleSessionsConcurrent(t *testing.T) {
	m := testHTTPModule(t)

	for i := 0; i < 10; i++ {
		session := &uploadSession{
			ID:        "session-" + strings.Repeat("x", i+1),
			Topic:     "topic",
			ExpiresAt: time.Now().UTC().Add(1 * time.Hour),
			Parts:     make(map[int32]string),
			PartSizes: make(map[int32]int64),
		}
		m.lfsStoreUploadSession(session)
	}

	m.uploadMu.Lock()
	count := len(m.uploadSessions)
	m.uploadMu.Unlock()
	if count != 10 {
		t.Fatalf("expected 10 sessions, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// handleHTTPUploadSession with real session: abort flow
// ---------------------------------------------------------------------------

func TestHandleHTTPUploadAbort_WithSession(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	session := &uploadSession{
		ID:        "abort-me",
		Topic:     "my-topic",
		S3Key:     "test-ns/my-topic/lfs/2025/01/01/obj-abort",
		UploadID:  "s3-upload-abort",
		ExpiresAt: time.Now().UTC().Add(1 * time.Hour),
		Parts:     make(map[int32]string),
		PartSizes: make(map[int32]int64),
	}
	m.lfsStoreUploadSession(session)

	req := httptest.NewRequest(http.MethodDelete, "/lfs/uploads/abort-me", nil)
	rr := httptest.NewRecorder()
	m.handleHTTPUploadSession(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d; body: %s", rr.Code, rr.Body.String())
	}

	// Session should be cleaned up
	_, ok := m.lfsGetUploadSession("abort-me")
	if ok {
		t.Fatal("expected session to be deleted after abort")
	}
}

// ---------------------------------------------------------------------------
// Auth integration with handlers
// ---------------------------------------------------------------------------

func TestHandleHTTPDownload_Unauthorized(t *testing.T) {
	m := testHTTPModule(t)
	// httpAPIKey is set by default in testHTTPModule

	body, _ := json.Marshal(lfsDownloadRequest{
		Bucket: "test-bucket",
		Key:    "test-ns/topic/lfs/2025/01/01/obj-123",
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPDownload(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestHandleHTTPUploadInit_Unauthorized(t *testing.T) {
	m := testHTTPModule(t)

	body, _ := json.Marshal(lfsUploadInitRequest{
		Topic:       "my-topic",
		ContentType: "application/octet-stream",
		SizeBytes:   1024,
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestHandleHTTPUploadSession_Unauthorized(t *testing.T) {
	m := testHTTPModule(t)

	req := httptest.NewRequest(http.MethodDelete, "/lfs/uploads/some-id", nil)
	rr := httptest.NewRecorder()
	m.handleHTTPUploadSession(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// handleHTTPUploadInit with invalid JSON
// ---------------------------------------------------------------------------

func TestHandleHTTPUploadInit_InvalidJSON(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""

	req := httptest.NewRequest(http.MethodPost, "/lfs/uploads", strings.NewReader("not json at all"))
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
	var errResp lfsErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "invalid_request" {
		t.Fatalf("expected code=invalid_request, got %s", errResp.Code)
	}
}

func TestHandleHTTPUploadInit_S3Unhealthy(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	atomic.StoreUint32(&m.s3Healthy, 0)

	body, _ := json.Marshal(lfsUploadInitRequest{
		Topic:       "my-topic",
		ContentType: "application/octet-stream",
		SizeBytes:   1024,
	})
	req := httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
}

func TestHandleHTTPUploadSession_S3Unhealthy(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	atomic.StoreUint32(&m.s3Healthy, 0)

	req := httptest.NewRequest(http.MethodDelete, "/lfs/uploads/some-id", nil)
	rr := httptest.NewRecorder()
	m.handleHTTPUploadSession(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
}
