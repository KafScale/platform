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

package lfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewProducer(t *testing.T) {
	p := NewProducer("http://localhost:8080")
	if p.endpoint != "http://localhost:8080" {
		t.Errorf("expected endpoint http://localhost:8080, got %s", p.endpoint)
	}
	if p.contentType != "application/octet-stream" {
		t.Errorf("expected default content-type application/octet-stream, got %s", p.contentType)
	}
	if p.maxRetries != 3 {
		t.Errorf("expected default maxRetries 3, got %d", p.maxRetries)
	}
}

func TestNewProducerWithOptions(t *testing.T) {
	client := &http.Client{Timeout: 5 * time.Second}
	p := NewProducer("http://localhost:8080",
		WithHTTPClient(client),
		WithAPIKey("secret-key"),
		WithContentType("video/mp4"),
		WithRetry(5, 2*time.Second),
	)

	if p.client != client {
		t.Error("expected custom HTTP client")
	}
	if p.apiKey != "secret-key" {
		t.Errorf("expected apiKey secret-key, got %s", p.apiKey)
	}
	if p.contentType != "video/mp4" {
		t.Errorf("expected content-type video/mp4, got %s", p.contentType)
	}
	if p.maxRetries != 5 {
		t.Errorf("expected maxRetries 5, got %d", p.maxRetries)
	}
}

func TestProducerProduce(t *testing.T) {
	// Create a mock LFS proxy server
	expectedEnvelope := Envelope{
		Version:     1,
		Bucket:      "test-bucket",
		Key:         "test/topic/lfs/2026/02/01/obj-123",
		Size:        1024,
		SHA256:      "abc123",
		ContentType: "application/octet-stream",
		CreatedAt:   "2026-02-01T12:00:00Z",
		ProxyID:     "test-proxy",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/lfs/produce" {
			t.Errorf("expected /lfs/produce, got %s", r.URL.Path)
		}
		if r.Header.Get("X-Kafka-Topic") != "test-topic" {
			t.Errorf("expected topic test-topic, got %s", r.Header.Get("X-Kafka-Topic"))
		}

		// Read body to simulate upload
		body, _ := io.ReadAll(r.Body)
		if string(body) != "test payload" {
			t.Errorf("expected body 'test payload', got '%s'", string(body))
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(expectedEnvelope)
	}))
	defer server.Close()

	producer := NewProducer(server.URL)
	result, err := producer.Produce(context.Background(), "test-topic", "test-key", strings.NewReader("test payload"))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Envelope.Key != expectedEnvelope.Key {
		t.Errorf("expected key %s, got %s", expectedEnvelope.Key, result.Envelope.Key)
	}
	if result.Envelope.SHA256 != expectedEnvelope.SHA256 {
		t.Errorf("expected sha256 %s, got %s", expectedEnvelope.SHA256, result.Envelope.SHA256)
	}
}

func TestProducerProduceEmptyTopic(t *testing.T) {
	producer := NewProducer("http://localhost:8080")
	_, err := producer.Produce(context.Background(), "", "key", strings.NewReader("data"))

	if err == nil {
		t.Error("expected error for empty topic")
	}
	if !strings.Contains(err.Error(), "topic is required") {
		t.Errorf("expected 'topic is required' error, got: %v", err)
	}
}

func TestProducerProduceNilBody(t *testing.T) {
	producer := NewProducer("http://localhost:8080")
	_, err := producer.Produce(context.Background(), "topic", "key", nil)

	if err == nil {
		t.Error("expected error for nil body")
	}
	if !strings.Contains(err.Error(), "body is required") {
		t.Errorf("expected 'body is required' error, got: %v", err)
	}
}

func TestProducerProduceServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer server.Close()

	producer := NewProducer(server.URL, WithRetry(0, 0))
	_, err := producer.Produce(context.Background(), "test-topic", "key", strings.NewReader("data"))

	if err == nil {
		t.Error("expected error for server error")
	}

	var lfsErr *LfsError
	if !errors.As(err, &lfsErr) {
		t.Errorf("expected LfsError, got %T", err)
	}
}

func TestProducerProduceWithChecksum(t *testing.T) {
	expectedSHA := "expected-sha256"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		env := Envelope{SHA256: expectedSHA}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(env)
	}))
	defer server.Close()

	producer := NewProducer(server.URL)

	// Matching checksum should succeed
	result, err := producer.ProduceWithChecksum(context.Background(), "topic", "key", strings.NewReader("data"), expectedSHA)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Envelope.SHA256 != expectedSHA {
		t.Errorf("expected sha256 %s, got %s", expectedSHA, result.Envelope.SHA256)
	}

	// Mismatched checksum should fail
	_, err = producer.ProduceWithChecksum(context.Background(), "topic", "key", strings.NewReader("data"), "wrong-sha")
	if err == nil {
		t.Error("expected checksum error")
	}
	var checksumErr *ChecksumError
	if !errors.As(err, &checksumErr) {
		t.Errorf("expected ChecksumError, got %T", err)
	}
}

func TestProducerProgress(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		env := Envelope{Size: 1000}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(env)
	}))
	defer server.Close()

	var progressCalls int64
	var lastBytes int64

	producer := NewProducer(server.URL,
		WithProgress(func(bytesSent int64) error {
			atomic.AddInt64(&progressCalls, 1)
			atomic.StoreInt64(&lastBytes, bytesSent)
			return nil
		}),
	)

	// Create a larger payload to trigger multiple progress calls
	payload := bytes.Repeat([]byte("x"), 10000)
	_, err := producer.Produce(context.Background(), "topic", "key", bytes.NewReader(payload))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt64(&progressCalls) == 0 {
		t.Error("expected progress callback to be called")
	}
	if atomic.LoadInt64(&lastBytes) != int64(len(payload)) {
		t.Errorf("expected final bytes %d, got %d", len(payload), atomic.LoadInt64(&lastBytes))
	}
}

func TestProducerProgressCancel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		env := Envelope{}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(env)
	}))
	defer server.Close()

	cancelErr := errors.New("user cancelled")
	producer := NewProducer(server.URL,
		WithProgress(func(bytesSent int64) error {
			if bytesSent > 100 {
				return cancelErr
			}
			return nil
		}),
		WithRetry(0, 0),
	)

	payload := bytes.Repeat([]byte("x"), 10000)
	_, err := producer.Produce(context.Background(), "topic", "key", bytes.NewReader(payload))

	if err == nil {
		t.Error("expected error from progress cancel")
	}
}

func TestProducerRetry(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		count := atomic.AddInt32(&attempts, 1)
		if count < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("status 503: service unavailable"))
			return
		}
		env := Envelope{Key: "success"}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(env)
	}))
	defer server.Close()

	producer := NewProducer(server.URL, WithRetry(3, 10*time.Millisecond))
	result, err := producer.Produce(context.Background(), "topic", "key", strings.NewReader("data"))

	if err != nil {
		t.Fatalf("unexpected error after retries: %v", err)
	}
	if result.Envelope.Key != "success" {
		t.Errorf("expected key 'success', got %s", result.Envelope.Key)
	}
	if atomic.LoadInt32(&attempts) != 3 {
		t.Errorf("expected 3 attempts, got %d", atomic.LoadInt32(&attempts))
	}
}

func TestProducerRetryExhausted(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("status 503: always fails"))
	}))
	defer server.Close()

	producer := NewProducer(server.URL, WithRetry(2, 10*time.Millisecond))
	_, err := producer.Produce(context.Background(), "topic", "key", strings.NewReader("data"))

	if err == nil {
		t.Error("expected error after exhausting retries")
	}
	if !strings.Contains(err.Error(), "max retries exceeded") {
		t.Errorf("expected 'max retries exceeded' error, got: %v", err)
	}
	// Initial attempt + 2 retries = 3 total
	if atomic.LoadInt32(&attempts) != 3 {
		t.Errorf("expected 3 attempts (1 + 2 retries), got %d", atomic.LoadInt32(&attempts))
	}
}

func TestProducerNoRetryOn400(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("bad request"))
	}))
	defer server.Close()

	producer := NewProducer(server.URL, WithRetry(3, 10*time.Millisecond))
	_, err := producer.Produce(context.Background(), "topic", "key", strings.NewReader("data"))

	if err == nil {
		t.Error("expected error for 400 response")
	}
	// 400 errors should not be retried
	if atomic.LoadInt32(&attempts) != 1 {
		t.Errorf("expected 1 attempt (no retry for 400), got %d", atomic.LoadInt32(&attempts))
	}
}

func TestProducerContextCancel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second) // Slow server
		env := Envelope{}
		json.NewEncoder(w).Encode(env)
	}))
	defer server.Close()

	producer := NewProducer(server.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := producer.Produce(ctx, "topic", "key", strings.NewReader("data"))

	if err == nil {
		t.Error("expected error from context timeout")
	}
}

func TestProducerAPIKey(t *testing.T) {
	var receivedKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedKey = r.Header.Get("X-API-Key")
		io.ReadAll(r.Body)
		env := Envelope{}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(env)
	}))
	defer server.Close()

	producer := NewProducer(server.URL, WithAPIKey("my-secret-key"))
	_, err := producer.Produce(context.Background(), "topic", "key", strings.NewReader("data"))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receivedKey != "my-secret-key" {
		t.Errorf("expected API key 'my-secret-key', got '%s'", receivedKey)
	}
}

func TestProducerPartitioned(t *testing.T) {
	var receivedPartition string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPartition = r.Header.Get("X-Kafka-Partition")
		io.ReadAll(r.Body)
		env := Envelope{}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(env)
	}))
	defer server.Close()

	producer := NewProducer(server.URL)
	_, err := producer.ProducePartitioned(context.Background(), "topic", 5, "key", strings.NewReader("data"))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receivedPartition != "5" {
		t.Errorf("expected partition '5', got '%s'", receivedPartition)
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{"nil error", nil, false},
		{"context canceled", context.Canceled, false},
		{"context deadline", context.DeadlineExceeded, false},
		{"checksum error", &ChecksumError{Expected: "a", Actual: "b"}, false},
		{"500 error", &LfsError{Op: "upload", Err: errors.New("status 500: internal error")}, true},
		{"503 error", &LfsError{Op: "upload", Err: errors.New("status 503: unavailable")}, true},
		{"429 error", &LfsError{Op: "upload", Err: errors.New("status 429: rate limited")}, true},
		{"400 error", &LfsError{Op: "upload", Err: errors.New("status 400: bad request")}, false},
		{"connection error", &LfsError{Op: "upload", Err: errors.New("connection refused")}, true},
		{"timeout error", &LfsError{Op: "upload", Err: errors.New("timeout waiting for response")}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryable(tt.err); got != tt.retryable {
				t.Errorf("isRetryable() = %v, want %v", got, tt.retryable)
			}
		})
	}
}
