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
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// ProduceResult contains the result of a successful LFS produce operation.
type ProduceResult struct {
	Envelope  Envelope      // The LFS envelope with S3 location and checksum
	Duration  time.Duration // Time taken for the upload
	BytesSent int64         // Total bytes uploaded
}

// ProgressFunc is called during upload with bytes sent so far.
// Returning an error cancels the upload.
type ProgressFunc func(bytesSent int64) error

// Producer sends large payloads to the LFS proxy via HTTP streaming.
type Producer struct {
	endpoint    string
	client      *http.Client
	apiKey      string
	contentType string
	maxRetries  int
	retryDelay  time.Duration
	progress    ProgressFunc
}

// ProducerOption configures the Producer.
type ProducerOption func(*Producer)

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(client *http.Client) ProducerOption {
	return func(p *Producer) {
		p.client = client
	}
}

// WithAPIKey sets the API key for authenticated requests.
func WithAPIKey(key string) ProducerOption {
	return func(p *Producer) {
		p.apiKey = key
	}
}

// WithContentType sets the Content-Type header for uploads.
func WithContentType(ct string) ProducerOption {
	return func(p *Producer) {
		p.contentType = ct
	}
}

// WithRetry configures retry behavior for transient failures.
func WithRetry(maxRetries int, delay time.Duration) ProducerOption {
	return func(p *Producer) {
		p.maxRetries = maxRetries
		p.retryDelay = delay
	}
}

// WithProgress sets a callback for upload progress.
func WithProgress(fn ProgressFunc) ProducerOption {
	return func(p *Producer) {
		p.progress = fn
	}
}

// NewProducer creates a Producer that sends blobs to the LFS proxy.
//
// The endpoint should be the LFS proxy HTTP URL, e.g., "http://lfs-proxy:8080".
func NewProducer(endpoint string, opts ...ProducerOption) *Producer {
	p := &Producer{
		endpoint:    endpoint,
		client:      &http.Client{Timeout: 0}, // No timeout for large uploads
		contentType: "application/octet-stream",
		maxRetries:  3,
		retryDelay:  time.Second,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Produce streams a payload to the LFS proxy for the given topic.
//
// The reader is streamed directly to the proxy without buffering the entire
// payload in memory. The proxy uploads to S3 and returns an LFS envelope
// that is stored in Kafka.
//
// Example:
//
//	producer := lfs.NewProducer("http://lfs-proxy:8080")
//	file, _ := os.Open("large-video.mp4")
//	defer file.Close()
//
//	result, err := producer.Produce(ctx, "video-uploads", "video-001", file)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Uploaded %d bytes, S3 key: %s\n", result.BytesSent, result.Envelope.Key)
func (p *Producer) Produce(ctx context.Context, topic, key string, body io.Reader) (*ProduceResult, error) {
	if topic == "" {
		return nil, errors.New("topic is required")
	}
	if body == nil {
		return nil, errors.New("body is required")
	}

	var lastErr error
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(p.retryDelay * time.Duration(attempt)):
			}
		}

		result, err := p.doUpload(ctx, topic, key, body)
		if err == nil {
			return result, nil
		}

		// Only retry on transient errors
		if !isRetryable(err) {
			return nil, err
		}
		lastErr = err
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

// ProduceWithChecksum streams a payload and validates the server-computed checksum.
//
// If the server's SHA256 doesn't match the expected checksum, an error is returned.
// This is useful when the client has pre-computed the checksum.
func (p *Producer) ProduceWithChecksum(ctx context.Context, topic, key string, body io.Reader, expectedSHA256 string) (*ProduceResult, error) {
	result, err := p.Produce(ctx, topic, key, body)
	if err != nil {
		return nil, err
	}

	if result.Envelope.SHA256 != expectedSHA256 {
		return nil, &ChecksumError{
			Expected: expectedSHA256,
			Actual:   result.Envelope.SHA256,
		}
	}

	return result, nil
}

// ProducePartitioned sends to a specific partition.
func (p *Producer) ProducePartitioned(ctx context.Context, topic string, partition int32, key string, body io.Reader) (*ProduceResult, error) {
	if topic == "" {
		return nil, errors.New("topic is required")
	}
	if body == nil {
		return nil, errors.New("body is required")
	}

	var lastErr error
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(p.retryDelay * time.Duration(attempt)):
			}
		}

		result, err := p.doUploadPartitioned(ctx, topic, partition, key, body)
		if err == nil {
			return result, nil
		}

		if !isRetryable(err) {
			return nil, err
		}
		lastErr = err
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

func (p *Producer) doUpload(ctx context.Context, topic, key string, body io.Reader) (*ProduceResult, error) {
	return p.doUploadPartitioned(ctx, topic, -1, key, body)
}

func (p *Producer) doUploadPartitioned(ctx context.Context, topic string, partition int32, key string, body io.Reader) (*ProduceResult, error) {
	url := p.endpoint + "/lfs/produce"

	// Wrap body with progress tracking if configured
	var trackedBody io.Reader = body
	if p.progress != nil {
		trackedBody = &progressReader{
			reader:   body,
			progress: p.progress,
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, trackedBody)
	if err != nil {
		return nil, &LfsError{Op: "create_request", Err: err}
	}

	req.Header.Set("X-Kafka-Topic", topic)
	if key != "" {
		req.Header.Set("X-Kafka-Key", base64.StdEncoding.EncodeToString([]byte(key)))
	}
	if partition >= 0 {
		req.Header.Set("X-Kafka-Partition", strconv.Itoa(int(partition)))
	}
	req.Header.Set("Content-Type", p.contentType)

	if p.apiKey != "" {
		req.Header.Set("X-API-Key", p.apiKey)
	}

	start := time.Now()
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, &LfsError{Op: "upload", Err: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, &LfsError{
			Op:  "upload",
			Err: fmt.Errorf("status %d: %s", resp.StatusCode, string(bodyBytes)),
		}
	}

	var env Envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, &LfsError{Op: "decode_response", Err: err}
	}

	return &ProduceResult{
		Envelope:  env,
		Duration:  time.Since(start),
		BytesSent: env.Size,
	}, nil
}

// progressReader wraps a reader and reports progress.
type progressReader struct {
	reader   io.Reader
	progress ProgressFunc
	sent     int64
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	if n > 0 {
		pr.sent += int64(n)
		if pr.progress != nil {
			if perr := pr.progress(pr.sent); perr != nil {
				return n, perr
			}
		}
	}
	return n, err
}

// isRetryable determines if an error is transient and worth retrying.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Context errors are not retryable
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Checksum errors are not retryable
	var checksumErr *ChecksumError
	if errors.As(err, &checksumErr) {
		return false
	}

	// LfsError wrapping HTTP errors might be retryable
	var lfsErr *LfsError
	if errors.As(err, &lfsErr) {
		// Check for retryable HTTP status codes in error message
		errStr := lfsErr.Error()
		// 5xx errors are retryable
		if contains(errStr, "status 5") {
			return true
		}
		// 429 Too Many Requests is retryable
		if contains(errStr, "status 429") {
			return true
		}
		// Connection errors are retryable
		if contains(errStr, "connection") || contains(errStr, "timeout") {
			return true
		}
	}

	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
