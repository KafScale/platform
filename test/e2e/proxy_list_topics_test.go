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
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProxyListTopicsAfterCreateE2E(t *testing.T) {
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

	client, err := kgo.NewClient(kgo.SeedBrokers(proxyAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	for i := range 10 {
		topic := fmt.Sprintf("listed-%d", i)
		if _, err := admin.CreateTopic(ctx, 3, 1, nil, topic); err != nil {
			t.Fatal(err)
		}
		listed, err := admin.ListTopics(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !listed.Has(topic) {
			t.Fatalf("%s missing from list after create: %v", topic, listed.Names())
		}
		if got := len(listed[topic].Partitions); got != 3 {
			t.Fatalf("%s listed with %d partitions", topic, got)
		}
	}
}
