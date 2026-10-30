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
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/KafScale/platform/pkg/metadata"
)

func TestTopicRecreateE2E(t *testing.T) {
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
	start("broker", []string{"KAFSCALE_AUTO_CREATE_TOPICS=false", "KAFSCALE_USE_MEMORY_S3=1", "KAFSCALE_BROKER_ADDR=" + brokerAddr, "KAFSCALE_BROKER_HOST=127.0.0.1", "KAFSCALE_BROKER_PORT=" + brokerPort, "KAFSCALE_METRICS_ADDR=" + freeAddr(t), "KAFSCALE_CONTROL_ADDR=" + freeAddr(t), "KAFSCALE_ETCD_ENDPOINTS=" + strings.Join(endpoints, ",")})
	waitForPortWithin(t, brokerAddr, time.Minute)
	start("proxy", []string{"KAFSCALE_PROXY_ADDR=" + proxyAddr, "KAFSCALE_PROXY_BACKENDS=" + brokerAddr, "KAFSCALE_PROXY_ADVERTISED_HOST=127.0.0.1", "KAFSCALE_PROXY_ADVERTISED_PORT=" + proxyPort, "KAFSCALE_PROXY_ETCD_ENDPOINTS=" + strings.Join(endpoints, ","), "KAFSCALE_PROXY_HEALTH_ADDR="})
	waitForPortWithin(t, proxyAddr, time.Minute)

	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer etcdClient.Close()

	for name, addr := range map[string]string{"broker": brokerAddr, "proxy": proxyAddr} {
		t.Run(name, func(t *testing.T) {
			topic := "recreate-" + name
			client, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.DisableIdempotentWrite())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			admin := kadm.NewClient(client)
			create := func() {
				t.Helper()
				resp, err := admin.CreateTopic(ctx, 1, 1, nil, topic)
				if err != nil || resp.Err != nil {
					t.Fatalf("create topic: %v %v", err, resp.Err)
				}
			}
			produce := func(value string) {
				t.Helper()
				if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(value)}).FirstErr(); err != nil {
					t.Fatal(err)
				}
			}
			create()
			for range 5 {
				produce("old")
			}
			deleted, err := admin.DeleteTopic(ctx, topic)
			if err != nil || deleted.Err != nil {
				t.Fatalf("delete topic: %v %v", err, deleted.Err)
			}
			record, err := etcdClient.Get(ctx, metadata.DeletedTopicKey(topic, 0))
			if err != nil || len(record.Kvs) != 1 || !strings.Contains(string(record.Kvs[0].Value), `"prefix":"default/`+topic+`/"`) {
				t.Fatalf("deleted topic record: %v %+v", err, record)
			}
			create()
			ends, err := admin.ListEndOffsets(ctx, topic)
			if err != nil {
				t.Fatal(err)
			}
			if end, _ := ends.Lookup(topic, 0); end.Err != nil || end.Offset != 0 {
				t.Fatalf("end offset after re-create: %+v", end)
			}
			produce("new")
			consumer, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
			if err != nil {
				t.Fatal(err)
			}
			defer consumer.Close()
			fetches := consumer.PollFetches(ctx)
			if err := fetches.Err(); err != nil {
				t.Fatal(err)
			}
			records := fetches.Records()
			if len(records) != 1 || records[0].Offset != 0 || string(records[0].Value) != "new" {
				for _, r := range records {
					t.Logf("offset %d value %q", r.Offset, r.Value)
				}
				t.Fatalf("got %d records after re-create, want only the new one at offset 0", len(records))
			}
		})
	}
}
