// Copyright 2026 KafScale team.
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
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestSimpleOffsetCommitE2E(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E") != "1" {
		t.Skip("set KAFSCALE_E2E=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	etcd, endpoints := startEmbeddedEtcd(t)
	t.Cleanup(etcd.Close)
	brokerAddr, proxyAddr := freeAddr(t), freeAddr(t)
	_, brokerPort, _ := net.SplitHostPort(brokerAddr)
	_, proxyPort, _ := net.SplitHostPort(proxyAddr)
	start := func(component string, settings []string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), component+".log")
		log, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", component))
		configureProcessGroup(cmd)
		cmd.Env = os.Environ()
		for _, entry := range append([]string{"KAFSCALE_ACL_ENABLED=false", "KAFSCALE_PROXY_IDENTITY_KEY_FILE=", "KAFSCALE_PROXY_TLS_CERT_FILE=", "KAFSCALE_PROXY_TLS_KEY_FILE=", "KAFSCALE_PROXY_TLS_CLIENT_CA_FILE=", "KAFSCALE_PROXY_TLS_PRINCIPALS_FILE=", "KAFSCALE_PROXY_LFS_ENABLED=false"}, settings...) {
			key, value, _ := strings.Cut(entry, "=")
			cmd.Env = upsertEnv(cmd.Env, key, value)
		}
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() {
			stopBroker(t, cmd)
			log.Close()
			if t.Failed() {
				data, _ := os.ReadFile(path)
				t.Logf("%s logs: %s", component, data)
			}
		})
	}
	start("broker", []string{"KAFSCALE_AUTO_CREATE_TOPICS=true", "KAFSCALE_AUTO_CREATE_PARTITIONS=1", "KAFSCALE_USE_MEMORY_S3=1", "KAFSCALE_BROKER_ADDR=" + brokerAddr, "KAFSCALE_BROKER_HOST=127.0.0.1", "KAFSCALE_BROKER_PORT=" + brokerPort, "KAFSCALE_METRICS_ADDR=" + freeAddr(t), "KAFSCALE_CONTROL_ADDR=" + freeAddr(t), "KAFSCALE_ETCD_ENDPOINTS=" + strings.Join(endpoints, ",")})
	waitForPortWithin(t, brokerAddr, time.Minute)
	start("proxy", []string{"KAFSCALE_PROXY_ADDR=" + proxyAddr, "KAFSCALE_PROXY_BACKENDS=" + brokerAddr, "KAFSCALE_PROXY_ADVERTISED_HOST=127.0.0.1", "KAFSCALE_PROXY_ADVERTISED_PORT=" + proxyPort, "KAFSCALE_PROXY_ETCD_ENDPOINTS=" + strings.Join(endpoints, ","), "KAFSCALE_PROXY_HEALTH_ADDR="})
	waitForPortWithin(t, proxyAddr, time.Minute)

	image := envOrDefault("KAFSCALE_KAFKA_CLI_IMAGE", "apache/kafka:3.9.1")
	if err := exec.CommandContext(ctx, "docker", "image", "inspect", image).Run(); err != nil && os.Getenv("KAFSCALE_KAFKA_HOME") == "" {
		if err := exec.CommandContext(ctx, "docker", "pull", image).Run(); err != nil {
			t.Fatalf("kafka cli image %s not available: %v", image, err)
		}
	}
	for name, addr := range map[string]string{"broker": brokerAddr, "proxy": proxyAddr} {
		t.Run(name, func(t *testing.T) {
			topic, group := "simple-"+name, "simple-group-"+name
			client, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.DisableIdempotentWrite())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			for i := range 3 {
				if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(fmt.Sprint(i))}).FirstErr(); err != nil {
					t.Fatal(err)
				}
			}
			commit := kmsg.NewPtrOffsetCommitRequest()
			commit.Group, commit.Generation, commit.MemberID = group, -1, ""
			part := kmsg.NewOffsetCommitRequestTopicPartition()
			part.Offset = 2
			commit.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: topic, Partitions: []kmsg.OffsetCommitRequestTopicPartition{part}}}
			resp, err := commit.RequestWith(ctx, client)
			if err != nil || resp.Topics[0].Partitions[0].ErrorCode != 0 {
				t.Fatal("assign commit", resp, err)
			}
			if got := cliCurrentOffset(t, ctx, image, addr, group); got != "2" {
				t.Fatalf("describe after assign commit: %q", got)
			}
			runKafkaCLI(t, ctx, image, fmt.Sprintf("/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %s --group %s --topic %s --reset-offsets --to-earliest --execute", addr, group, topic))
			if got := cliCurrentOffset(t, ctx, image, addr, group); got != "0" {
				t.Fatalf("describe after reset: %q", got)
			}
			fresh := group + "-never-seen"
			runKafkaCLI(t, ctx, image, fmt.Sprintf("/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %s --group %s --topic %s --reset-offsets --to-earliest --execute", addr, fresh, topic))
			if got := cliCurrentOffset(t, ctx, image, addr, fresh); got != "0" {
				t.Fatalf("describe after reset of a new group: %q", got)
			}
		})
	}
}

func runKafkaCLI(t *testing.T, ctx context.Context, image, script string) string {
	t.Helper()
	if home := os.Getenv("KAFSCALE_KAFKA_HOME"); home != "" {
		return string(runCmdGetOutput(t, ctx, "sh", "-c", strings.ReplaceAll(script, "/opt/kafka", home)))
	}
	args := []string{"run", "--rm"}
	if runtime.GOOS == "linux" {
		args = append(args, "--network=host")
	}
	args = append(args, image, "sh", "-c", script)
	return string(runCmdGetOutput(t, ctx, "docker", args...))
}

func cliCurrentOffset(t *testing.T, ctx context.Context, image, addr, group string) string {
	t.Helper()
	output := runKafkaCLI(t, ctx, image, fmt.Sprintf("/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server %s --group %s --describe", addr, group))
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 3 && fields[0] == group {
			return fields[3]
		}
	}
	t.Fatalf("no offsets for %s:\n%s", group, output)
	return ""
}

func waitForPortWithin(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	for deadline := time.Now().Add(timeout); ; time.Sleep(100 * time.Millisecond) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not start listening: %v", addr, err)
		}
	}
}
