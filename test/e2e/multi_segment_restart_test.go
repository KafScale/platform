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
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
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

	t.Setenv("KAFSCALE_SEGMENT_BYTES", "1024")
	t.Setenv("KAFSCALE_FLUSH_INTERVAL_MS", "50")
	t.Setenv("KAFSCALE_S3_NAMESPACE", fmt.Sprintf("durability-%d", time.Now().UnixNano()))
	setEnvDefault(t, "KAFSCALE_S3_BUCKET", "kafscale")
	setEnvDefault(t, "KAFSCALE_S3_REGION", "us-east-1")
	setEnvDefault(t, "KAFSCALE_S3_ENDPOINT", "http://127.0.0.1:9000")
	setEnvDefault(t, "KAFSCALE_S3_PATH_STYLE", "true")
	setEnvDefault(t, "KAFSCALE_S3_ACCESS_KEY", "minioadmin")
	setEnvDefault(t, "KAFSCALE_S3_SECRET_KEY", "minioadmin")

	brokerAddr := freeAddr(t)
	metricsAddr := freeAddr(t)
	controlAddr := freeAddr(t)

	brokerCmd, brokerLogs := startBrokerWithEtcdS3(t, ctx, brokerAddr, metricsAddr, controlAddr, endpoints)
	waitForBroker(t, brokerLogs, brokerAddr)

	topic := fmt.Sprintf("durability-%d", time.Now().UnixNano())
	messageCount := 12
	payload := strings.Repeat("x", 600)
	expected := make([]string, 0, messageCount)

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableIdempotentWrite(),
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
	stopBroker(t, brokerCmd)

	brokerAddr = freeAddr(t)
	metricsAddr = freeAddr(t)
	controlAddr = freeAddr(t)
	brokerCmd, brokerLogs = startBrokerWithEtcdS3(t, ctx, brokerAddr, metricsAddr, controlAddr, endpoints)
	waitForBroker(t, brokerLogs, brokerAddr)

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
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
			stopBroker(t, brokerCmd)
			t.Fatalf("timed out waiting for records (%d/%d). broker logs:\n%s", seen, messageCount, brokerLogs.String())
		}
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
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
	stopBroker(t, brokerCmd)
}

func setEnvDefault(t *testing.T, name, value string) {
	t.Helper()
	if strings.TrimSpace(os.Getenv(name)) == "" {
		t.Setenv(name, value)
	}
}

func startBrokerWithEtcdS3(t *testing.T, ctx context.Context, brokerAddr, metricsAddr, controlAddr string, endpoints []string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	brokerCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "broker"))
	brokerCmd.Env = append(os.Environ(),
		"KAFSCALE_AUTO_CREATE_TOPICS=true",
		"KAFSCALE_AUTO_CREATE_PARTITIONS=1",
		fmt.Sprintf("KAFSCALE_BROKER_ADDR=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_METRICS_ADDR=%s", metricsAddr),
		fmt.Sprintf("KAFSCALE_CONTROL_ADDR=%s", controlAddr),
		fmt.Sprintf("KAFSCALE_ETCD_ENDPOINTS=%s", strings.Join(endpoints, ",")),
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
