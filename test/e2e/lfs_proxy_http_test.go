// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestLfsProxyHTTPProduce(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	s3Server := newFakeS3Server(t)
	t.Cleanup(s3Server.Close)

	brokerAddr, received, closeBackend := startFakeKafkaBackend(t)
	// Start embedded etcd and seed topics for metadata responses
	etcdEndpoints := startLfsProxyEtcd(t, "127.0.0.1", 9092, "http-limited")
	t.Cleanup(closeBackend)

	proxyPort := pickFreePort(t)
	httpPort := pickFreePort(t)
	healthPort := pickFreePort(t)
	proxyCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "lfs-proxy"))
	configureProcessGroup(proxyCmd)
	proxyCmd.Env = append(os.Environ(),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ADDR=127.0.0.1:%s", proxyPort),
		"KAFSCALE_LFS_PROXY_ADVERTISED_HOST=127.0.0.1",
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ADVERTISED_PORT=%s", proxyPort),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_HTTP_ADDR=127.0.0.1:%s", httpPort),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_HEALTH_ADDR=127.0.0.1:%s", healthPort),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_BACKENDS=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS=%s", strings.Join(etcdEndpoints, ",")),
		"KAFSCALE_LFS_PROXY_S3_BUCKET=lfs-e2e",
		"KAFSCALE_LFS_PROXY_S3_REGION=us-east-1",
		fmt.Sprintf("KAFSCALE_LFS_PROXY_S3_ENDPOINT=%s", s3Server.URL),
		"KAFSCALE_LFS_PROXY_S3_ACCESS_KEY=fake",
		"KAFSCALE_LFS_PROXY_S3_SECRET_KEY=fake",
		"KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE=true",
		"KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET=true",
	)
	var proxyLogs bytes.Buffer
	proxyWriterTargets := []io.Writer{&proxyLogs, mustLogFile(t, "lfs-proxy-http.log")}
	proxyCmd.Stdout = io.MultiWriter(proxyWriterTargets...)
	proxyCmd.Stderr = proxyCmd.Stdout
	if err := proxyCmd.Start(); err != nil {
		t.Fatalf("start lfs-proxy: %v", err)
	}
	t.Cleanup(func() {
		_ = signalProcessGroup(proxyCmd, os.Interrupt)
		done := make(chan struct{})
		go func() {
			_ = proxyCmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = signalProcessGroup(proxyCmd, os.Kill)
		}
	})
	waitForPortWithTimeout(t, "127.0.0.1:"+httpPort, 15*time.Second)

	payload := []byte("hello-lfs-stream")
	checksum := sha256.Sum256(payload)
	checksumHex := hex.EncodeToString(checksum[:])

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://127.0.0.1:%s/lfs/produce", httpPort), bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("X-Kafka-Topic", "http-limited")
	req.Header.Set("X-Kafka-Key", base64.StdEncoding.EncodeToString([]byte("key-1")))
	req.Header.Set("X-LFS-Checksum", checksumHex)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http produce failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var env lfs.Envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if env.SHA256 != checksumHex {
		t.Fatalf("checksum mismatch: %s", env.SHA256)
	}

	deadline := time.After(10 * time.Second)
	for {
		select {
		case value := <-received:
			var got lfs.Envelope
			if err := json.Unmarshal(value, &got); err != nil {
				t.Fatalf("expected envelope json: %v", err)
			}
			if got.Key == "" || got.Bucket == "" {
				t.Fatalf("unexpected envelope: %+v", got)
			}
			return
		case <-deadline:
			t.Fatalf("timed out waiting for backend record")
		}
	}
}

func TestLfsProxyHTTPProduceRestart(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	s3Server := newFakeS3Server(t)
	t.Cleanup(s3Server.Close)

	brokerAddr, received, closeBackend := startFakeKafkaBackend(t)
	etcdEndpoints := startLfsProxyEtcd(t, "127.0.0.1", 9092, "http-restart")
	t.Cleanup(closeBackend)

	proxyPort := pickFreePort(t)
	httpPort := pickFreePort(t)
	healthPort := pickFreePort(t)

	startProxy := func() (*exec.Cmd, *bytes.Buffer) {
		proxyCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "lfs-proxy"))
		configureProcessGroup(proxyCmd)
		proxyCmd.Env = append(os.Environ(),
			fmt.Sprintf("KAFSCALE_LFS_PROXY_ADDR=127.0.0.1:%s", proxyPort),
			"KAFSCALE_LFS_PROXY_ADVERTISED_HOST=127.0.0.1",
			fmt.Sprintf("KAFSCALE_LFS_PROXY_ADVERTISED_PORT=%s", proxyPort),
			fmt.Sprintf("KAFSCALE_LFS_PROXY_HTTP_ADDR=127.0.0.1:%s", httpPort),
			fmt.Sprintf("KAFSCALE_LFS_PROXY_HEALTH_ADDR=127.0.0.1:%s", healthPort),
			fmt.Sprintf("KAFSCALE_LFS_PROXY_BACKENDS=%s", brokerAddr),
			fmt.Sprintf("KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS=%s", strings.Join(etcdEndpoints, ",")),
			"KAFSCALE_LFS_PROXY_S3_BUCKET=lfs-e2e",
			"KAFSCALE_LFS_PROXY_S3_REGION=us-east-1",
			fmt.Sprintf("KAFSCALE_LFS_PROXY_S3_ENDPOINT=%s", s3Server.URL),
			"KAFSCALE_LFS_PROXY_S3_ACCESS_KEY=fake",
			"KAFSCALE_LFS_PROXY_S3_SECRET_KEY=fake",
			"KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE=true",
			"KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET=true",
		)
		var proxyLogs bytes.Buffer
		proxyCmd.Stdout = io.MultiWriter(&proxyLogs, mustLogFile(t, "lfs-proxy-http-restart.log"))
		proxyCmd.Stderr = proxyCmd.Stdout
		if err := proxyCmd.Start(); err != nil {
			t.Fatalf("start lfs-proxy: %v", err)
		}
		return proxyCmd, &proxyLogs
	}

	proxyCmd, _ := startProxy()
	defer func() {
		_ = signalProcessGroup(proxyCmd, os.Interrupt)
		_ = proxyCmd.Wait()
	}()
	waitForPortWithTimeout(t, "127.0.0.1:"+httpPort, 15*time.Second)

	slowPayload := bytes.Repeat([]byte("a"), 1024*1024)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://127.0.0.1:%s/lfs/produce", httpPort), newSlowReader(slowPayload, 32*1024, 10*time.Millisecond))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("X-Kafka-Topic", "http-restart")
	req.Header.Set("Content-Type", "application/octet-stream")

	clientErr := make(chan error, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err == nil && resp != nil {
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				clientErr <- nil
				return
			}
			err = fmt.Errorf("status %d", resp.StatusCode)
		}
		clientErr <- err
	}()

	time.Sleep(50 * time.Millisecond)
	_ = signalProcessGroup(proxyCmd, os.Interrupt)
	_ = proxyCmd.Wait()

	<-clientErr

	proxyCmd, _ = startProxy()
	defer func() {
		_ = signalProcessGroup(proxyCmd, os.Interrupt)
		_ = proxyCmd.Wait()
	}()
	waitForPortWithTimeout(t, "127.0.0.1:"+httpPort, 15*time.Second)

	payload := []byte("restart-ok")
	req2, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://127.0.0.1:%s/lfs/produce", httpPort), bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req2.Header.Set("X-Kafka-Topic", "http-restart")
	req2.Header.Set("Content-Type", "application/octet-stream")

	resp, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("http produce failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	deadline := time.After(10 * time.Second)
	for {
		select {
		case value := <-received:
			var got lfs.Envelope
			if err := json.Unmarshal(value, &got); err != nil {
				t.Fatalf("expected envelope json: %v", err)
			}
			if got.Key == "" || got.Bucket == "" {
				t.Fatalf("unexpected envelope: %+v", got)
			}
			return
		case <-deadline:
			t.Fatalf("timed out waiting for backend record")
		}
	}
}

func TestLfsProxyHTTPBackendUnavailable(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	s3Server := newFakeS3Server(t)
	t.Cleanup(s3Server.Close)

	brokerAddr, _, closeBackend := startFakeKafkaBackend(t)
	etcdEndpoints := startLfsProxyEtcd(t, "127.0.0.1", 9092, "http-backend-down")
	t.Cleanup(closeBackend)

	proxyPort := pickFreePort(t)
	httpPort := pickFreePort(t)
	healthPort := pickFreePort(t)
	proxyCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "lfs-proxy"))
	configureProcessGroup(proxyCmd)
	proxyCmd.Env = append(os.Environ(),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ADDR=127.0.0.1:%s", proxyPort),
		"KAFSCALE_LFS_PROXY_ADVERTISED_HOST=127.0.0.1",
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ADVERTISED_PORT=%s", proxyPort),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_HTTP_ADDR=127.0.0.1:%s", httpPort),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_HEALTH_ADDR=127.0.0.1:%s", healthPort),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_BACKENDS=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS=%s", strings.Join(etcdEndpoints, ",")),
		"KAFSCALE_LFS_PROXY_S3_BUCKET=lfs-e2e",
		"KAFSCALE_LFS_PROXY_S3_REGION=us-east-1",
		fmt.Sprintf("KAFSCALE_LFS_PROXY_S3_ENDPOINT=%s", s3Server.URL),
		"KAFSCALE_LFS_PROXY_S3_ACCESS_KEY=fake",
		"KAFSCALE_LFS_PROXY_S3_SECRET_KEY=fake",
		"KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE=true",
		"KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET=true",
	)
	proxyCmd.Stdout = io.MultiWriter(mustLogFile(t, "lfs-proxy-http-backend-down.log"))
	proxyCmd.Stderr = proxyCmd.Stdout
	if err := proxyCmd.Start(); err != nil {
		t.Fatalf("start lfs-proxy: %v", err)
	}
	t.Cleanup(func() {
		_ = signalProcessGroup(proxyCmd, os.Interrupt)
		_ = proxyCmd.Wait()
	})
	waitForPortWithTimeout(t, "127.0.0.1:"+httpPort, 15*time.Second)

	closeBackend()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://127.0.0.1:%s/lfs/produce", httpPort), bytes.NewReader([]byte("payload")))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("X-Kafka-Topic", "http-backend-down")
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http produce failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusBadGateway {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, string(body))
	}
	var body httpErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if body.Code == "" {
		t.Fatalf("expected error code in response")
	}
}

func newSlowReader(payload []byte, chunk int, delay time.Duration) io.Reader {
	return &slowReader{payload: payload, chunk: chunk, delay: delay}
}

type slowReader struct {
	payload []byte
	chunk   int
	delay   time.Duration
	idx     int
}

func (r *slowReader) Read(p []byte) (int, error) {
	if r.idx >= len(r.payload) {
		return 0, io.EOF
	}
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	end := r.idx + r.chunk
	if end > len(r.payload) {
		end = len(r.payload)
	}
	n := copy(p, r.payload[r.idx:end])
	r.idx += n
	return n, nil
}

type fakeS3Server struct {
	*httptest.Server
	mu      sync.Mutex
	buckets map[string]struct{}
	uploads map[string]*multipartUpload
	objects map[string][]byte
	counter int64
}

type multipartUpload struct {
	bucket string
	key    string
	data   []byte
}

func newFakeS3Server(t *testing.T) *fakeS3Server {
	t.Helper()
	fs := &fakeS3Server{
		buckets: make(map[string]struct{}),
		uploads: make(map[string]*multipartUpload),
		objects: make(map[string][]byte),
	}
	handler := http.NewServeMux()
	handler.HandleFunc("/", fs.serve)
	fs.Server = httptest.NewServer(handler)
	return fs
}

func (f *fakeS3Server) serve(w http.ResponseWriter, r *http.Request) {
	bucket, key := splitBucketKey(r.URL.Path)
	switch r.Method {
	case http.MethodHead:
		f.headBucket(w, bucket)
		return
	case http.MethodPut:
		if r.URL.Query().Get("partNumber") != "" && r.URL.Query().Get("uploadId") != "" {
			f.uploadPart(w, r, bucket, key)
			return
		}
		if key == "" {
			f.putBucket(w, bucket)
			return
		}
		f.putObject(w, r, bucket, key)
		return
	case http.MethodPost:
		if _, ok := r.URL.Query()["uploads"]; ok {
			f.createMultipart(w, bucket, key)
			return
		}
		if r.URL.Query().Get("uploadId") != "" {
			f.completeMultipart(w, r.URL.Query().Get("uploadId"))
			return
		}
	}
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

func (f *fakeS3Server) headBucket(w http.ResponseWriter, bucket string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.buckets[bucket]; !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3Server) putBucket(w http.ResponseWriter, bucket string) {
	f.mu.Lock()
	f.buckets[bucket] = struct{}{}
	f.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3Server) putObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.objects[bucket+"/"+key] = body
	f.buckets[bucket] = struct{}{}
	f.mu.Unlock()
	w.Header().Set("ETag", "\"fake\"")
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3Server) createMultipart(w http.ResponseWriter, bucket, key string) {
	f.mu.Lock()
	f.counter++
	uploadID := fmt.Sprintf("upload-%d", f.counter)
	f.uploads[uploadID] = &multipartUpload{bucket: bucket, key: key}
	f.buckets[bucket] = struct{}{}
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, "<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>", uploadID)
}

func (f *fakeS3Server) uploadPart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	upload := f.uploads[uploadID]
	if upload != nil {
		upload.data = append(upload.data, body...)
	}
	f.mu.Unlock()
	w.Header().Set("ETag", "\"part\"")
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3Server) completeMultipart(w http.ResponseWriter, uploadID string) {
	f.mu.Lock()
	upload := f.uploads[uploadID]
	if upload != nil {
		f.objects[upload.bucket+"/"+upload.key] = upload.data
		delete(f.uploads, uploadID)
	}
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, "<CompleteMultipartUploadResult><ETag>\"fake\"</ETag></CompleteMultipartUploadResult>")
}

func splitBucketKey(path string) (string, string) {
	trimmed := strings.TrimPrefix(path, "/")
	if trimmed == "" {
		return "", ""
	}
	parts := strings.SplitN(trimmed, "/", 2)
	bucket := parts[0]
	if len(parts) == 1 {
		return bucket, ""
	}
	return bucket, parts[1]
}

func waitForPortWithTimeout(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		select {
		case <-deadline:
			t.Fatalf("broker did not start listening on %s: %v", addr, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func startFakeKafkaBackend(t *testing.T) (string, <-chan []byte, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	received := make(chan []byte, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleKafkaConn(t, conn, received)
		}
	}()
	return addr, received, func() {
		_ = ln.Close()
		<-done
	}
}

func handleKafkaConn(t *testing.T, conn net.Conn, received chan<- []byte) {
	t.Helper()
	defer conn.Close()
	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		return
	}
	header, req, err := protocol.ParseRequest(frame.Payload)
	if err != nil {
		return
	}
	prodReq, ok := req.(*protocol.ProduceRequest)
	if !ok {
		return
	}
	if len(prodReq.Topics) > 0 && len(prodReq.Topics[0].Partitions) > 0 {
		records := prodReq.Topics[0].Partitions[0].Records
		value := extractFirstRecordValue(records)
		if len(value) > 0 {
			select {
			case received <- value:
			default:
			}
		}
	}
	respPayload, _ := buildProduceResponse(prodReq, header.CorrelationID, header.APIVersion)
	_ = protocol.WriteFrame(conn, respPayload)
}

func buildProduceResponse(req *protocol.ProduceRequest, correlationID int32, version int16) ([]byte, error) {
	topics := make([]protocol.ProduceTopicResponse, 0, len(req.Topics))
	for _, topic := range req.Topics {
		parts := make([]protocol.ProducePartitionResponse, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			parts = append(parts, protocol.ProducePartitionResponse{
				Partition:       part.Partition,
				ErrorCode:       protocol.NONE,
				BaseOffset:      0,
				LogAppendTimeMs: 0,
				LogStartOffset:  0,
			})
		}
		topics = append(topics, protocol.ProduceTopicResponse{Name: topic.Name, Partitions: parts})
	}
	resp := &protocol.ProduceResponse{CorrelationID: correlationID, Topics: topics, ThrottleMs: 0}
	return protocol.EncodeProduceResponse(resp, version)
}

func extractFirstRecordValue(records []byte) []byte {
	if len(records) == 0 {
		return nil
	}
	var batch kmsg.RecordBatch
	if err := batch.ReadFrom(records); err != nil {
		return nil
	}
	raw := batch.Records
	recordsOut := make([]kmsg.Record, int(batch.NumRecords))
	recordsOut = readRawRecordsInto(recordsOut, raw)
	if len(recordsOut) == 0 {
		return nil
	}
	return recordsOut[0].Value
}

func readRawRecordsInto(rs []kmsg.Record, in []byte) []kmsg.Record {
	for i := range rs {
		length, used := binary.Varint(in)
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

type httpErrorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}
