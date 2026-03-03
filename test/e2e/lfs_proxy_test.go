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

//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestLfsProxyKafkaProtocol tests the LFS proxy with native Kafka protocol.
// Uses franz-go client to produce messages with LFS_BLOB header.
func TestLfsProxyKafkaProtocol(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// Start fake S3 server
	s3Server := newFakeS3Server(t)
	t.Cleanup(s3Server.Close)

	// Start fake Kafka backend
	brokerAddr, received, closeBackend := startFakeKafkaBackend(t)
	// Start embedded etcd and seed topics for metadata responses
	etcdEndpoints := startLfsProxyEtcd(t, "127.0.0.1", 9092, "lfs-test-topic", "regular-topic", "checksum-test")
	t.Cleanup(closeBackend)

	// Start LFS proxy
	proxyPort := pickFreePort(t)
	healthPort := pickFreePort(t)
	proxyCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "lfs-proxy"))
	configureProcessGroup(proxyCmd)
	proxyCmd.Env = append(os.Environ(),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ADDR=127.0.0.1:%s", proxyPort),
		"KAFSCALE_LFS_PROXY_ADVERTISED_HOST=127.0.0.1",
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ADVERTISED_PORT=%s", proxyPort),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_HEALTH_ADDR=127.0.0.1:%s", healthPort),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_BACKENDS=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS=%s", strings.Join(etcdEndpoints, ",")),
		"KAFSCALE_LFS_PROXY_S3_BUCKET=lfs-test",
		"KAFSCALE_LFS_PROXY_S3_REGION=us-east-1",
		fmt.Sprintf("KAFSCALE_LFS_PROXY_S3_ENDPOINT=%s", s3Server.URL),
		"KAFSCALE_LFS_PROXY_S3_ACCESS_KEY=fake",
		"KAFSCALE_LFS_PROXY_S3_SECRET_KEY=fake",
		"KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE=true",
		"KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET=true",
	)
	var proxyLogs bytes.Buffer
	proxyWriterTargets := []io.Writer{&proxyLogs, mustLogFile(t, "lfs-proxy-kafka.log")}
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
	waitForPortWithTimeout(t, "127.0.0.1:"+proxyPort, 15*time.Second)

	// Create franz-go client pointing to proxy
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:"+proxyPort),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close()

	// Generate random blob
	blob := make([]byte, 1024)
	if _, err := rand.Read(blob); err != nil {
		t.Fatalf("generate blob: %v", err)
	}

	// Produce with LFS_BLOB header
	record := &kgo.Record{
		Topic: "lfs-test-topic",
		Key:   []byte("test-key"),
		Value: blob,
		Headers: []kgo.RecordHeader{
			{Key: "LFS_BLOB", Value: nil},
		},
	}
	res := client.ProduceSync(ctx, record)
	if err := res.FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// Wait for backend to receive the envelope
	deadline := time.After(10 * time.Second)
	for {
		select {
		case value := <-received:
			// Should receive an LFS envelope, not the original blob
			if !lfs.IsLfsEnvelope(value) {
				t.Fatalf("expected LFS envelope, got: %s", string(value))
			}

			var env lfs.Envelope
			if err := json.Unmarshal(value, &env); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}

			// Verify envelope fields
			if env.Version != 1 {
				t.Errorf("Version = %d, want 1", env.Version)
			}
			if env.Bucket != "lfs-test" {
				t.Errorf("Bucket = %s, want lfs-test", env.Bucket)
			}
			if env.Size != int64(len(blob)) {
				t.Errorf("Size = %d, want %d", env.Size, len(blob))
			}

			// Verify checksum matches
			expectedHash := sha256.Sum256(blob)
			expectedChecksum := hex.EncodeToString(expectedHash[:])
			if env.SHA256 != expectedChecksum {
				t.Errorf("SHA256 = %s, want %s", env.SHA256, expectedChecksum)
			}

			// Verify blob was stored in S3
			s3Key := env.Key
			s3Server.mu.Lock()
			storedBlob, ok := s3Server.objects["lfs-test/"+s3Key]
			s3Server.mu.Unlock()
			if !ok {
				t.Errorf("blob not found in S3 at key: %s", s3Key)
			} else if !bytes.Equal(storedBlob, blob) {
				t.Errorf("stored blob does not match original")
			}

			return
		case <-deadline:
			t.Fatalf("timed out waiting for backend record")
		}
	}
}

// TestLfsProxyPassthrough tests that non-LFS messages pass through unchanged.
func TestLfsProxyPassthrough(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// Start fake S3 server
	s3Server := newFakeS3Server(t)
	t.Cleanup(s3Server.Close)

	// Start fake Kafka backend
	brokerAddr, received, closeBackend := startFakeKafkaBackend(t)
	// Start embedded etcd and seed topics for metadata responses
	etcdEndpoints := startLfsProxyEtcd(t, "127.0.0.1", 9092, "regular-topic")
	t.Cleanup(closeBackend)

	// Start LFS proxy
	proxyPort := pickFreePort(t)
	healthPort := pickFreePort(t)
	proxyCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "lfs-proxy"))
	configureProcessGroup(proxyCmd)
	proxyCmd.Env = append(os.Environ(),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ADDR=127.0.0.1:%s", proxyPort),
		"KAFSCALE_LFS_PROXY_ADVERTISED_HOST=127.0.0.1",
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ADVERTISED_PORT=%s", proxyPort),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_HEALTH_ADDR=127.0.0.1:%s", healthPort),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_BACKENDS=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS=%s", strings.Join(etcdEndpoints, ",")),
		"KAFSCALE_LFS_PROXY_S3_BUCKET=lfs-test",
		"KAFSCALE_LFS_PROXY_S3_REGION=us-east-1",
		fmt.Sprintf("KAFSCALE_LFS_PROXY_S3_ENDPOINT=%s", s3Server.URL),
		"KAFSCALE_LFS_PROXY_S3_ACCESS_KEY=fake",
		"KAFSCALE_LFS_PROXY_S3_SECRET_KEY=fake",
		"KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE=true",
		"KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET=true",
	)
	var proxyLogs bytes.Buffer
	proxyWriterTargets := []io.Writer{&proxyLogs, mustLogFile(t, "lfs-proxy-passthrough.log")}
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
	waitForPortWithTimeout(t, "127.0.0.1:"+proxyPort, 15*time.Second)

	// Create franz-go client
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:"+proxyPort),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close()

	// Produce without LFS_BLOB header (regular message)
	plainValue := []byte("regular message without LFS")
	record := &kgo.Record{
		Topic: "regular-topic",
		Key:   []byte("key"),
		Value: plainValue,
	}
	res := client.ProduceSync(ctx, record)
	if err := res.FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// Wait for backend to receive the message
	deadline := time.After(10 * time.Second)
	for {
		select {
		case value := <-received:
			// Should receive the original message unchanged
			if lfs.IsLfsEnvelope(value) {
				t.Fatalf("expected plain message, got LFS envelope")
			}
			if !bytes.Equal(value, plainValue) {
				t.Errorf("value = %q, want %q", value, plainValue)
			}
			return
		case <-deadline:
			t.Fatalf("timed out waiting for backend record")
		}
	}
}

// TestLfsProxyChecksumValidation tests that checksum validation works.
func TestLfsProxyChecksumValidation(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// Start fake S3 server
	s3Server := newFakeS3Server(t)
	t.Cleanup(s3Server.Close)

	// Start fake Kafka backend
	brokerAddr, _, closeBackend := startFakeKafkaBackend(t)
	// Start embedded etcd and seed topics for metadata responses
	etcdEndpoints := startLfsProxyEtcd(t, "127.0.0.1", 9092, "checksum-test")
	t.Cleanup(closeBackend)

	// Start LFS proxy
	proxyPort := pickFreePort(t)
	healthPort := pickFreePort(t)
	proxyCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "lfs-proxy"))
	configureProcessGroup(proxyCmd)
	proxyCmd.Env = append(os.Environ(),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ADDR=127.0.0.1:%s", proxyPort),
		"KAFSCALE_LFS_PROXY_ADVERTISED_HOST=127.0.0.1",
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ADVERTISED_PORT=%s", proxyPort),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_HEALTH_ADDR=127.0.0.1:%s", healthPort),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_BACKENDS=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS=%s", strings.Join(etcdEndpoints, ",")),
		"KAFSCALE_LFS_PROXY_S3_BUCKET=lfs-test",
		"KAFSCALE_LFS_PROXY_S3_REGION=us-east-1",
		fmt.Sprintf("KAFSCALE_LFS_PROXY_S3_ENDPOINT=%s", s3Server.URL),
		"KAFSCALE_LFS_PROXY_S3_ACCESS_KEY=fake",
		"KAFSCALE_LFS_PROXY_S3_SECRET_KEY=fake",
		"KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE=true",
		"KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET=true",
	)
	var proxyLogs bytes.Buffer
	proxyWriterTargets := []io.Writer{&proxyLogs, mustLogFile(t, "lfs-proxy-checksum.log")}
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
	waitForPortWithTimeout(t, "127.0.0.1:"+proxyPort, 15*time.Second)

	// Create franz-go client
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:"+proxyPort),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer client.Close()

	// Produce with wrong checksum in LFS_BLOB header
	blob := []byte("test blob data")
	wrongChecksum := "0000000000000000000000000000000000000000000000000000000000000000"
	record := &kgo.Record{
		Topic: "checksum-test",
		Key:   []byte("key"),
		Value: blob,
		Headers: []kgo.RecordHeader{
			{Key: "LFS_BLOB", Value: []byte(wrongChecksum)},
		},
	}
	res := client.ProduceSync(ctx, record)
	err = res.FirstErr()

	// Should fail with checksum error
	if err == nil {
		t.Fatalf("expected checksum error, got nil")
	}
	t.Logf("got expected error: %v", err)
}

// TestLfsProxyHealthEndpoint tests the health endpoints.
func TestLfsProxyHealthEndpoint(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// Start fake S3 server
	s3Server := newFakeS3Server(t)
	t.Cleanup(s3Server.Close)

	// Start fake Kafka backend
	brokerAddr, _, closeBackend := startFakeKafkaBackend(t)
	// Start embedded etcd and seed topics for metadata responses
	etcdEndpoints := startLfsProxyEtcd(t, "127.0.0.1", 9092, "health-check")
	t.Cleanup(closeBackend)

	// Start LFS proxy
	proxyPort := pickFreePort(t)
	healthPort := pickFreePort(t)
	proxyCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "lfs-proxy"))
	configureProcessGroup(proxyCmd)
	proxyCmd.Env = append(os.Environ(),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ADDR=127.0.0.1:%s", proxyPort),
		"KAFSCALE_LFS_PROXY_ADVERTISED_HOST=127.0.0.1",
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ADVERTISED_PORT=%s", proxyPort),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_HEALTH_ADDR=127.0.0.1:%s", healthPort),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_BACKENDS=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS=%s", strings.Join(etcdEndpoints, ",")),
		"KAFSCALE_LFS_PROXY_S3_BUCKET=lfs-test",
		"KAFSCALE_LFS_PROXY_S3_REGION=us-east-1",
		fmt.Sprintf("KAFSCALE_LFS_PROXY_S3_ENDPOINT=%s", s3Server.URL),
		"KAFSCALE_LFS_PROXY_S3_ACCESS_KEY=fake",
		"KAFSCALE_LFS_PROXY_S3_SECRET_KEY=fake",
		"KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE=true",
		"KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET=true",
	)
	var proxyLogs bytes.Buffer
	proxyWriterTargets := []io.Writer{&proxyLogs, mustLogFile(t, "lfs-proxy-health.log")}
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
	waitForPortWithTimeout(t, "127.0.0.1:"+healthPort, 15*time.Second)

	// Test /livez endpoint
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/livez", healthPort))
	if err != nil {
		t.Fatalf("livez request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/livez status = %d, want 200", resp.StatusCode)
	}

	// Test /readyz endpoint
	resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:%s/readyz", healthPort))
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/readyz status = %d, want 200", resp.StatusCode)
	}
}
