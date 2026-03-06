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
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLfsProxyBrokerE2E(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	s3Server := newFakeS3Server(t)
	t.Cleanup(s3Server.Close)

	etcd, endpoints := startEmbeddedEtcd(t)
	t.Cleanup(func() {
		etcd.Close()
	})

	brokerAddr := freeAddr(t)
	metricsAddr := freeAddr(t)
	controlAddr := freeAddr(t)

	brokerHost, brokerPort := splitHostPort(t, brokerAddr)
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{
			NodeID: 0,
			Host:   brokerHost,
			Port:   brokerPort,
		}},
	}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("create etcd store: %v", err)
	}

	topic := "lfs-broker-topic"
	if _, err := store.CreateTopic(ctx, metadata.TopicSpec{
		Name:              topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	brokerCmd, brokerLogs := startBrokerWithEtcd(t, ctx, brokerAddr, metricsAddr, controlAddr, endpoints)
	t.Cleanup(func() { stopBroker(t, brokerCmd) })
	waitForBroker(t, brokerLogs, brokerAddr)

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
		fmt.Sprintf("KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS=%s", strings.Join(endpoints, ",")),
		"KAFSCALE_LFS_PROXY_S3_BUCKET=lfs-e2e-broker",
		"KAFSCALE_LFS_PROXY_S3_REGION=us-east-1",
		fmt.Sprintf("KAFSCALE_LFS_PROXY_S3_ENDPOINT=%s", s3Server.URL),
		"KAFSCALE_LFS_PROXY_S3_ACCESS_KEY=fake",
		"KAFSCALE_LFS_PROXY_S3_SECRET_KEY=fake",
		"KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE=true",
		"KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET=true",
	)
	var proxyLogs bytes.Buffer
	proxyWriterTargets := []io.Writer{&proxyLogs, mustLogFile(t, "lfs-proxy-broker.log")}
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

	producer, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:"+proxyPort),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup("lfs-proxy-broker-e2e"),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	consumerClosed := false
	defer func() {
		if !consumerClosed {
			consumer.CloseAllowingRebalance()
		}
	}()

	blob := make([]byte, 1024)
	if _, err := rand.Read(blob); err != nil {
		t.Fatalf("generate blob: %v", err)
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   []byte("test-key"),
		Value: blob,
		Headers: []kgo.RecordHeader{
			{Key: "LFS_BLOB", Value: nil},
		},
	}
	res := producer.ProduceSync(ctx, record)
	if err := res.FirstErr(); err != nil {
		t.Fatalf("produce: %v\nproxy logs:\n%s\nbroker logs:\n%s", err, proxyLogs.String(), brokerLogs.String())
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for broker record\nproxy logs:\n%s\nbroker logs:\n%s", proxyLogs.String(), brokerLogs.String())
		}
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %+v\nproxy logs:\n%s\nbroker logs:\n%s", errs, proxyLogs.String(), brokerLogs.String())
		}
		var got []byte
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Topic != topic || got != nil {
				return
			}
			got = append([]byte(nil), r.Value...)
		})
		if got == nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if !lfs.IsLfsEnvelope(got) {
			t.Fatalf("expected LFS envelope, got: %s", string(got))
		}
		var env lfs.Envelope
		if err := json.Unmarshal(got, &env); err != nil {
			t.Fatalf("decode envelope: %v", err)
		}
		expectedHash := sha256.Sum256(blob)
		expectedChecksum := hex.EncodeToString(expectedHash[:])
		if env.SHA256 != expectedChecksum {
			t.Fatalf("SHA256 = %s, want %s", env.SHA256, expectedChecksum)
		}
		s3Key := env.Key
		s3Server.mu.Lock()
		storedBlob, ok := s3Server.objects["lfs-e2e-broker/"+s3Key]
		s3Server.mu.Unlock()
		if !ok {
			t.Fatalf("blob not found in S3 at key: %s", s3Key)
		}
		if !bytes.Equal(storedBlob, blob) {
			t.Fatalf("stored blob does not match original")
		}
		consumer.CloseAllowingRebalance()
		consumerClosed = true
		return
	}
}

func splitHostPort(t *testing.T, addr string) (string, int32) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split addr %s: %v", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port %s: %v", portStr, err)
	}
	return host, int32(port)
}
