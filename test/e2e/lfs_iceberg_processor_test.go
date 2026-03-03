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

func TestLfsIcebergProcessorE2E(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	required := []string{
		"KAFSCALE_E2E_S3_ENDPOINT",
		"KAFSCALE_E2E_S3_BUCKET",
		"KAFSCALE_E2E_S3_REGION",
		"KAFSCALE_E2E_S3_ACCESS_KEY",
		"KAFSCALE_E2E_S3_SECRET_KEY",
		"ICEBERG_PROCESSOR_CATALOG_URI",
		"ICEBERG_PROCESSOR_WAREHOUSE",
	}
	for _, key := range required {
		if os.Getenv(key) == "" {
			t.Skipf("%s not set", key)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)

	etcd, endpoints := startEmbeddedEtcd(t)
	t.Cleanup(func() { etcd.Close() })

	brokerAddr := freeAddr(t)
	metricsAddr := freeAddr(t)
	controlAddr := freeAddr(t)

	brokerCmd, brokerLogs := startBrokerWithEtcdS3ForIceberg(t, ctx, brokerAddr, metricsAddr, controlAddr, endpoints)
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
		fmt.Sprintf("KAFSCALE_LFS_PROXY_S3_BUCKET=%s", os.Getenv("KAFSCALE_E2E_LFS_BUCKET")),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_S3_REGION=%s", os.Getenv("KAFSCALE_E2E_S3_REGION")),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_S3_ENDPOINT=%s", os.Getenv("KAFSCALE_E2E_S3_ENDPOINT")),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_S3_ACCESS_KEY=%s", os.Getenv("KAFSCALE_E2E_S3_ACCESS_KEY")),
		fmt.Sprintf("KAFSCALE_LFS_PROXY_S3_SECRET_KEY=%s", os.Getenv("KAFSCALE_E2E_S3_SECRET_KEY")),
		"KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE=true",
		"KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET=true",
	)
	if os.Getenv("KAFSCALE_E2E_LFS_BUCKET") == "" {
		proxyCmd.Env = append(proxyCmd.Env, fmt.Sprintf("KAFSCALE_LFS_PROXY_S3_BUCKET=%s", os.Getenv("KAFSCALE_E2E_S3_BUCKET")))
	}
	var proxyLogs bytes.Buffer
	proxyCmd.Stdout = io.MultiWriter(&proxyLogs, mustLogFile(t, "lfs-iceberg-proxy.log"))
	proxyCmd.Stderr = proxyCmd.Stdout
	if err := proxyCmd.Start(); err != nil {
		t.Fatalf("start lfs-proxy: %v", err)
	}
	t.Cleanup(func() { _ = signalProcessGroup(proxyCmd, os.Interrupt) })
	waitForPortWithTimeout(t, fmt.Sprintf("127.0.0.1:%s", proxyPort), 10*time.Second)

	configPath := writeIcebergProcessorConfig(t, brokerAddr, endpoints)
	processorCmd := exec.CommandContext(ctx, "go", "run", "./cmd/processor", "-config", configPath)
	processorCmd.Dir = filepath.Join(repoRoot(t), "addons", "processors", "iceberg-processor")
	configureProcessGroup(processorCmd)
	var processorLogs bytes.Buffer
	processorCmd.Stdout = io.MultiWriter(&processorLogs, mustLogFile(t, "lfs-iceberg-processor.log"))
	processorCmd.Stderr = processorCmd.Stdout
	if err := processorCmd.Start(); err != nil {
		t.Fatalf("start iceberg-processor: %v", err)
	}
	t.Cleanup(func() { _ = signalProcessGroup(processorCmd, os.Interrupt) })

	producer, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:"+proxyPort),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	topic := "lfs-iceberg-topic"
	record := &kgo.Record{
		Topic:   topic,
		Key:     []byte("k1"),
		Value:   []byte("hello world"),
		Headers: []kgo.RecordHeader{{Key: "LFS_BLOB", Value: []byte("1")}},
	}
	if res := producer.ProduceSync(ctx, record); res.FirstErr() != nil {
		t.Fatalf("produce: %v", res.FirstErr())
	}

	waitForLog(t, &processorLogs, "sink write failed", 30*time.Second)
	if strings.Contains(processorLogs.String(), "sink write failed") {
		t.Fatalf("processor reported sink write failure")
	}
}

func startBrokerWithEtcdS3ForIceberg(t *testing.T, ctx context.Context, brokerAddr, metricsAddr, controlAddr string, endpoints []string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	brokerCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "broker"))
	configureProcessGroup(brokerCmd)
	brokerCmd.Env = append(os.Environ(),
		"KAFSCALE_AUTO_CREATE_TOPICS=true",
		"KAFSCALE_AUTO_CREATE_PARTITIONS=1",
		fmt.Sprintf("KAFSCALE_BROKER_ADDR=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_METRICS_ADDR=%s", metricsAddr),
		fmt.Sprintf("KAFSCALE_CONTROL_ADDR=%s", controlAddr),
		fmt.Sprintf("KAFSCALE_ETCD_ENDPOINTS=%s", strings.Join(endpoints, ",")),
		fmt.Sprintf("KAFSCALE_S3_BUCKET=%s", os.Getenv("KAFSCALE_E2E_S3_BUCKET")),
		fmt.Sprintf("KAFSCALE_S3_REGION=%s", os.Getenv("KAFSCALE_E2E_S3_REGION")),
		fmt.Sprintf("KAFSCALE_S3_ENDPOINT=%s", os.Getenv("KAFSCALE_E2E_S3_ENDPOINT")),
		fmt.Sprintf("KAFSCALE_S3_ACCESS_KEY=%s", os.Getenv("KAFSCALE_E2E_S3_ACCESS_KEY")),
		fmt.Sprintf("KAFSCALE_S3_SECRET_KEY=%s", os.Getenv("KAFSCALE_E2E_S3_SECRET_KEY")),
		"KAFSCALE_S3_PATH_STYLE=true",
	)
	var brokerLogs bytes.Buffer
	logWriter := io.MultiWriter(&brokerLogs, mustLogFile(t, "broker-lfs-iceberg.log"))
	brokerCmd.Stdout = logWriter
	brokerCmd.Stderr = logWriter
	if err := brokerCmd.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	return brokerCmd, &brokerLogs
}

func writeIcebergProcessorConfig(t *testing.T, brokerAddr string, endpoints []string) string {
	t.Helper()
	config := fmt.Sprintf(`s3:
  bucket: %s
  namespace: default
  region: %s
  endpoint: %s
  path_style: true
iceberg:
  catalog:
    type: %s
    uri: %s
    token: "%s"
  warehouse: %s
offsets:
  backend: etcd
  lease_ttl_seconds: 30
  key_prefix: processors
discovery:
  mode: auto
etcd:
  endpoints:
    - %s
schema:
  mode: "off"
mappings:
  - topic: lfs-iceberg-topic
    table: default.lfs_iceberg_topic
    mode: append
    create_table_if_missing: true
    lfs:
      mode: resolve
      max_inline_size: 1048576
      store_metadata: true
      validate_checksum: true
      resolve_concurrency: 2
`,
		os.Getenv("KAFSCALE_E2E_S3_BUCKET"),
		os.Getenv("KAFSCALE_E2E_S3_REGION"),
		os.Getenv("KAFSCALE_E2E_S3_ENDPOINT"),
		envOrDefault("ICEBERG_PROCESSOR_CATALOG_TYPE", "rest"),
		os.Getenv("ICEBERG_PROCESSOR_CATALOG_URI"),
		os.Getenv("ICEBERG_PROCESSOR_CATALOG_TOKEN"),
		os.Getenv("ICEBERG_PROCESSOR_WAREHOUSE"),
		endpoints[0],
	)

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(config), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func waitForLog(t *testing.T, logs *bytes.Buffer, needle string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), needle) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestLfsIcebergQueryValidation(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}
	cmdLine := os.Getenv("KAFSCALE_E2E_QUERY_CMD")
	if cmdLine == "" {
		t.Skip("KAFSCALE_E2E_QUERY_CMD not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	cmd := exec.CommandContext(ctx, "sh", "-c", cmdLine)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("query command failed: %v", err)
	}
}
