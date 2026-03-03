// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type fakePresignAPI struct {
	url string
	err error
}

func (f fakePresignAPI) PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &v4.PresignedHTTPRequest{URL: f.url}, nil
}

func newReadyProxy(api s3API) *lfsProxy {
	proxy := &lfsProxy{
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		topicMaxLength: defaultTopicMaxLength,
		cacheTTL:       time.Minute,
		metrics:        newLfsMetrics(),
		s3Uploader:     &s3Uploader{bucket: "bucket", chunkSize: minMultipartChunkSize, api: api, presign: fakePresignAPI{url: "https://example.com/object"}},
		s3Bucket:       "bucket",
		s3Namespace:    "default",
		downloadTTLMax: 2 * time.Minute,
	}
	proxy.setReady(true)
	proxy.markS3Healthy(true)
	proxy.touchHealthy()
	return proxy
}

func TestHTTPProduceNotReadyReturnsJSON(t *testing.T) {
	proxy := &lfsProxy{
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		topicMaxLength: defaultTopicMaxLength,
		cacheTTL:       time.Minute,
		metrics:        newLfsMetrics(),
	}

	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", bytes.NewReader([]byte("payload")))
	req.Header.Set(headerTopic, "lfs-demo-topic")
	rec := httptest.NewRecorder()

	proxy.handleHTTPProduce(rec, req)

	resp := rec.Result()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, resp.StatusCode)
	}
	if got := resp.Header.Get(headerRequestID); got == "" {
		t.Fatalf("expected %s header to be set", headerRequestID)
	}
	var body errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Code != "proxy_not_ready" {
		t.Fatalf("unexpected code: %s", body.Code)
	}
	if body.RequestID == "" {
		t.Fatalf("expected request_id in body")
	}
}

func TestHTTPProduceInvalidTopic(t *testing.T) {
	proxy := newReadyProxy(fakeS3API{})
	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", bytes.NewReader([]byte("payload")))
	req.Header.Set(headerTopic, "bad topic")
	rec := httptest.NewRecorder()

	proxy.handleHTTPProduce(rec, req)

	resp := rec.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.StatusCode)
	}
	var body errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Code != "invalid_topic" {
		t.Fatalf("unexpected code: %s", body.Code)
	}
}

func TestHTTPProduceUploadFailureReturnsBadGateway(t *testing.T) {
	proxy := newReadyProxy(failingS3API{err: errors.New("boom")})
	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", bytes.NewReader([]byte("payload")))
	req.Header.Set(headerTopic, "lfs-demo-topic")
	rec := httptest.NewRecorder()

	proxy.handleHTTPProduce(rec, req)

	resp := rec.Result()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("expected status %d, got %d", http.StatusBadGateway, resp.StatusCode)
	}
	var body errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Code != "s3_upload_failed" {
		t.Fatalf("unexpected code: %s", body.Code)
	}
}

func TestHTTPProduceRequestIDPreserved(t *testing.T) {
	proxy := newReadyProxy(failingS3API{err: errors.New("boom")})
	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", bytes.NewReader([]byte("payload")))
	req.Header.Set(headerTopic, "lfs-demo-topic")
	req.Header.Set(headerRequestID, "req-123")
	rec := httptest.NewRecorder()

	proxy.handleHTTPProduce(rec, req)

	resp := rec.Result()
	if got := resp.Header.Get(headerRequestID); got != "req-123" {
		t.Fatalf("expected request id to be preserved, got %q", got)
	}
	var body errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.RequestID != "req-123" {
		t.Fatalf("expected request_id in body to be preserved, got %q", body.RequestID)
	}
}

func TestHTTPProduceUnauthorized(t *testing.T) {
	proxy := newReadyProxy(fakeS3API{})
	proxy.httpAPIKey = "secret"
	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", bytes.NewReader([]byte("payload")))
	req.Header.Set(headerTopic, "lfs-demo-topic")
	rec := httptest.NewRecorder()

	proxy.handleHTTPProduce(rec, req)

	resp := rec.Result()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, resp.StatusCode)
	}
}

func TestHTTPProduceMethodNotAllowed(t *testing.T) {
	proxy := newReadyProxy(fakeS3API{})
	req := httptest.NewRequest(http.MethodGet, "/lfs/produce", nil)
	rec := httptest.NewRecorder()

	proxy.handleHTTPProduce(rec, req)

	resp := rec.Result()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, resp.StatusCode)
	}
}

func TestHTTPDownloadMethodNotAllowed(t *testing.T) {
	proxy := newReadyProxy(fakeS3API{})
	req := httptest.NewRequest(http.MethodGet, "/lfs/download", nil)
	rec := httptest.NewRecorder()

	proxy.handleHTTPDownload(rec, req)

	if rec.Result().StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, rec.Result().StatusCode)
	}
}

func TestHTTPDownloadUnauthorized(t *testing.T) {
	proxy := newReadyProxy(fakeS3API{})
	proxy.httpAPIKey = "secret"
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader([]byte(`{"bucket":"bucket","key":"default/topic/lfs/2026/02/03/obj-1"}`)))
	rec := httptest.NewRecorder()

	proxy.handleHTTPDownload(rec, req)

	if rec.Result().StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, rec.Result().StatusCode)
	}
}

func TestHTTPDownloadInvalidKey(t *testing.T) {
	proxy := newReadyProxy(fakeS3API{})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader([]byte(`{"bucket":"bucket","key":"other/topic/obj-1"}`)))
	rec := httptest.NewRecorder()

	proxy.handleHTTPDownload(rec, req)

	if rec.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Result().StatusCode)
	}
	var body errorResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Code != "invalid_key" {
		t.Fatalf("unexpected code: %s", body.Code)
	}
}

func TestHTTPDownloadPresignOK(t *testing.T) {
	proxy := newReadyProxy(fakeS3API{})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader([]byte(`{"bucket":"bucket","key":"default/topic/lfs/2026/02/03/obj-1","mode":"presign","expires_seconds":120}`)))
	rec := httptest.NewRecorder()

	proxy.handleHTTPDownload(rec, req)

	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}
	var body downloadResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.URL == "" || body.Mode != "presign" {
		t.Fatalf("expected presign response, got %+v", body)
	}
}

func TestHTTPDownloadStreamOK(t *testing.T) {
	proxy := newReadyProxy(fakeS3API{})
	req := httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader([]byte(`{"bucket":"bucket","key":"default/topic/lfs/2026/02/03/obj-1","mode":"stream"}`)))
	rec := httptest.NewRecorder()

	proxy.handleHTTPDownload(rec, req)

	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") == "" {
		t.Fatalf("expected content-type header to be set")
	}
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(payload) == "" {
		t.Fatalf("expected body payload")
	}
}
