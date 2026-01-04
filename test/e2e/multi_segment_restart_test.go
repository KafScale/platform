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
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestMultiSegmentRestartDurability(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	etcd, endpoints := startEmbeddedEtcd(t)
	defer etcd.Close()
	waitForEtcd(t, endpoints)
	if err := waitForEtcdClient(t, endpoints[0]); err != nil {
		t.Fatalf("etcd client probe failed: %v", err)
	}
	t.Logf("embedded etcd endpoints: %v", endpoints)

	t.Setenv("KAFSCALE_SEGMENT_BYTES", "1024")
	t.Setenv("KAFSCALE_FLUSH_INTERVAL_MS", "50")
	t.Setenv("KAFSCALE_S3_NAMESPACE", fmt.Sprintf("durability-%d", time.Now().UnixNano()))
	setEnvDefault(t, "KAFSCALE_S3_BUCKET", "kafscale")
	setEnvDefault(t, "KAFSCALE_S3_REGION", "us-east-1")
	setEnvDefault(t, "KAFSCALE_S3_ENDPOINT", "http://127.0.0.1:9000")
	setEnvDefault(t, "KAFSCALE_S3_PATH_STYLE", "true")
	setEnvDefault(t, "KAFSCALE_S3_ACCESS_KEY", "minioadmin")
	setEnvDefault(t, "KAFSCALE_S3_SECRET_KEY", "minioadmin")
	setEnvDefault(t, "KAFSCALE_ETCD_ENDPOINTS", strings.Join(endpoints, ","))
	bucket := envOrDefault("KAFSCALE_S3_BUCKET", "kafscale")

	brokerAddr := freeAddr(t)
	metricsAddr := freeAddr(t)
	controlAddr := freeAddr(t)

	brokerCmd, brokerLogs := startBrokerWithEtcdS3(t, ctx, brokerAddr, metricsAddr, controlAddr)
	waitForBroker(t, brokerLogs, brokerAddr)

	topic := fmt.Sprintf("durability-%d", time.Now().UnixNano())
	messageCount := 12
	payload := strings.Repeat("x", 600)
	expected := make([]string, 0, messageCount)

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableIdempotentWrite(),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelWarn, func() string { return "franz/producer " })),
	)
	if err != nil {
		stopBroker(t, brokerCmd)
		t.Fatalf("create producer: %v\nbroker logs:\n%s", err, brokerLogs.String())
	}
	producerClosed := false
	defer func() {
		if !producerClosed {
			producer.Close()
		}
	}()

	for i := 0; i < messageCount; i++ {
		value := fmt.Sprintf("msg-%03d-%s", i, payload)
		expected = append(expected, value)
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(value)}).FirstErr(); err != nil {
			stopBroker(t, brokerCmd)
			t.Fatalf("produce %d failed: %v\nbroker logs:\n%s", i, err, brokerLogs.String())
		}
	}
	producer.Close()
	producerClosed = true
	logEtcdOffset(t, endpoints[0], topic)
	logEtcdSnapshot(t, endpoints[0], topic)
	printS3Layout(t, bucket, []string{topic})
	stopBroker(t, brokerCmd)
	waitForPortClosed(t, brokerAddr)
	waitForPortClosed(t, metricsAddr)
	waitForPortClosed(t, controlAddr)
	if err := waitForEtcdClient(t, endpoints[0]); err != nil {
		t.Fatalf("etcd client probe failed after broker stop: %v", err)
	}

	brokerCmd, brokerLogs = startBrokerWithEtcdS3(t, ctx, brokerAddr, metricsAddr, controlAddr)
	waitForBroker(t, brokerLogs, brokerAddr)
	if err := waitForEtcdClient(t, endpoints[0]); err != nil {
		t.Fatalf("etcd client probe failed after broker restart: %v", err)
	}
	waitForEtcd(t, endpoints)
	logEtcdOffset(t, endpoints[0], topic)
	logEtcdSnapshot(t, endpoints[0], topic)
	printS3Layout(t, bucket, []string{topic})

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.ConsumerGroup(fmt.Sprintf("durability-%d", time.Now().UnixNano())),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelWarn, func() string { return "franz/consumer " })),
	)
	if err != nil {
		stopBroker(t, brokerCmd)
		t.Fatalf("create consumer: %v\nbroker logs:\n%s", err, brokerLogs.String())
	}
	consumerClosed := false
	defer func() {
		if !consumerClosed {
			consumer.Close()
		}
	}()

	seen := 0
	deadline := time.Now().Add(20 * time.Second)
	for seen < messageCount {
		if time.Now().After(deadline) {
			logEtcdOffset(t, endpoints[0], topic)
			logEtcdSnapshot(t, endpoints[0], topic)
			printS3Layout(t, bucket, []string{topic})
			stopBroker(t, brokerCmd)
			t.Fatalf("timed out waiting for records (%d/%d). broker logs:\n%s", seen, messageCount, brokerLogs.String())
		}
		fetchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		fetches := consumer.PollFetches(fetchCtx)
		cancel()
		if errs := fetches.Errors(); len(errs) > 0 {
			allTimeouts := true
			for _, fetchErr := range errs {
				if !errors.Is(fetchErr.Err, context.DeadlineExceeded) {
					allTimeouts = false
					break
				}
			}
			if allTimeouts {
				t.Logf("fetch timed out waiting for broker response; retrying (%d/%d)", seen, messageCount)
				continue
			}
			logEtcdOffset(t, endpoints[0], topic)
			logEtcdSnapshot(t, endpoints[0], topic)
			printS3Layout(t, bucket, []string{topic})
			stopBroker(t, brokerCmd)
			t.Fatalf("fetch errors: %+v\nbroker logs:\n%s", errs, brokerLogs.String())
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if seen >= messageCount {
				return
			}
			got := string(record.Value)
			if got != expected[seen] {
				stopBroker(t, brokerCmd)
				t.Fatalf("record %d mismatch: expected %q got %q", seen, expected[seen], got)
			}
			seen++
		})
	}

	consumer.Close()
	consumerClosed = true
	time.Sleep(500 * time.Millisecond)
	stopBroker(t, brokerCmd)
}

func waitForEtcd(t *testing.T, endpoints []string) {
	t.Helper()
	if len(endpoints) == 0 {
		t.Fatalf("no etcd endpoints provided")
	}
	endpoint := strings.TrimPrefix(strings.TrimPrefix(endpoints[0], "http://"), "https://")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", endpoint, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("etcd not reachable at %s", endpoints[0])
}

func waitForEtcdClient(t *testing.T, endpoint string) error {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		return err
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = cli.Get(ctx, "/health")
	return err
}

func logEtcdOffset(t *testing.T, endpoint, topic string) {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Logf("etcd offset probe failed to connect: %v", err)
		return
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	key := fmt.Sprintf("/kafscale/topics/%s/partitions/0/next_offset", topic)
	resp, err := cli.Get(ctx, key)
	if err != nil {
		t.Logf("etcd offset probe failed: %v", err)
		return
	}
	if len(resp.Kvs) == 0 {
		t.Logf("etcd offset probe: %s missing", key)
		return
	}
	t.Logf("etcd offset probe: %s=%s", key, strings.TrimSpace(string(resp.Kvs[0].Value)))
}

func logEtcdSnapshot(t *testing.T, endpoint, topic string) {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Logf("etcd snapshot probe failed to connect: %v", err)
		return
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := cli.Get(ctx, "/kafscale/metadata/snapshot")
	if err != nil {
		t.Logf("etcd snapshot probe failed: %v", err)
		return
	}
	if len(resp.Kvs) == 0 {
		t.Log("etcd snapshot probe: snapshot key missing")
		return
	}
	payload := string(resp.Kvs[0].Value)
	t.Logf("etcd snapshot probe: bytes=%d topic_present=%t", len(payload), strings.Contains(payload, fmt.Sprintf("\"Name\":\"%s\"", topic)))
}

func setEnvDefault(t *testing.T, name, value string) {
	t.Helper()
	if strings.TrimSpace(os.Getenv(name)) == "" {
		t.Setenv(name, value)
	}
}

func startBrokerWithEtcdS3(t *testing.T, ctx context.Context, brokerAddr, metricsAddr, controlAddr string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	brokerCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "broker"))
	configureProcessGroup(brokerCmd)
	brokerCmd.Env = append(os.Environ(),
		"KAFSCALE_AUTO_CREATE_TOPICS=true",
		"KAFSCALE_AUTO_CREATE_PARTITIONS=1",
		"KAFSCALE_TRACE_KAFKA=true",
		"KAFSCALE_LOG_LEVEL=debug",
		fmt.Sprintf("KAFSCALE_BROKER_ADDR=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_METRICS_ADDR=%s", metricsAddr),
		fmt.Sprintf("KAFSCALE_CONTROL_ADDR=%s", controlAddr),
	)
	var brokerLogs bytes.Buffer
	logWriter := io.MultiWriter(&brokerLogs, mustLogFile(t, "broker-durability.log"))
	brokerCmd.Stdout = logWriter
	brokerCmd.Stderr = logWriter
	if err := brokerCmd.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	return brokerCmd, &brokerLogs
}
