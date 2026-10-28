// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").

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

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProxyPlainAutoCreateE2E(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E_AUTH") != "1" {
		t.Skip("set KAFSCALE_E2E_AUTH=1")
	}
	for _, enabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
			start("broker", []string{fmt.Sprintf("KAFSCALE_AUTO_CREATE_TOPICS=%t", enabled), "KAFSCALE_AUTO_CREATE_PARTITIONS=3", "KAFSCALE_USE_MEMORY_S3=1", "KAFSCALE_BROKER_ADDR=" + brokerAddr, "KAFSCALE_BROKER_HOST=127.0.0.1", "KAFSCALE_BROKER_PORT=" + brokerPort, "KAFSCALE_METRICS_ADDR=" + freeAddr(t), "KAFSCALE_CONTROL_ADDR=" + freeAddr(t), "KAFSCALE_ETCD_ENDPOINTS=" + strings.Join(endpoints, ",")})
			waitForPort(t, brokerAddr)
			start("proxy", []string{"KAFSCALE_PROXY_ADDR=" + proxyAddr, "KAFSCALE_PROXY_BACKENDS=" + brokerAddr, "KAFSCALE_PROXY_ADVERTISED_HOST=127.0.0.1", "KAFSCALE_PROXY_ADVERTISED_PORT=" + proxyPort, "KAFSCALE_PROXY_ETCD_ENDPOINTS=" + strings.Join(endpoints, ","), "KAFSCALE_PROXY_HEALTH_ADDR="})
			waitForPort(t, proxyAddr)
			// Use exact wire versions; v1 has no opt-in field and franz-go leaves it
			// false by default. Both must reach the same broker policy as direct clients.
			for _, test := range []struct {
				version int16
				flag    bool
				topic   string
			}{{1, false, "qa-legacy"}, {4, false, "qa-default"}, {4, true, "qa-explicit"}} {
				conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				req := kmsg.NewPtrMetadataRequest()
				req.Version, req.AllowAutoTopicCreation = test.version, test.flag
				req.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(test.topic)}}
				request := kmsg.NewRequestFormatter().AppendRequest(nil, req, 41)[4:]
				if err := protocol.WriteFrame(conn, request); err != nil {
					conn.Close()
					t.Fatal(err)
				}
				frame, err := protocol.ReadFrame(conn)
				conn.Close()
				if err != nil {
					t.Fatal(err)
				}
				resp := kmsg.NewPtrMetadataResponse()
				resp.Version = test.version
				if len(frame.Payload) < 4 {
					t.Fatal("short response")
				}
				if err := resp.ReadFrom(frame.Payload[4:]); err != nil {
					t.Fatal(err)
				}
				if len(resp.Topics) != 1 {
					t.Fatal(resp)
				}
				topic := resp.Topics[0]
				if enabled {
					if topic.ErrorCode != 0 || len(topic.Partitions) != 3 || resp.Brokers[0].Host != "127.0.0.1" || fmt.Sprint(resp.Brokers[0].Port) != proxyPort {
						t.Fatal(resp)
					}
				} else if topic.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
					t.Fatal(resp)
				}
			}
			if !enabled {
				return
			}
			producer, err := kgo.NewClient(kgo.SeedBrokers(proxyAddr), kgo.DisableIdempotentWrite())
			if err != nil {
				t.Fatal(err)
			}
			defer producer.Close()
			requestCtx, stop := context.WithTimeout(ctx, 30*time.Second)
			defer stop()
			if err := producer.ProduceSync(requestCtx, &kgo.Record{Topic: "qa-franz", Value: []byte("auto-created")}).FirstErr(); err != nil {
				t.Fatal("produce through plain proxy", err)
			}
			consumer, err := kgo.NewClient(kgo.SeedBrokers(proxyAddr), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"qa-franz": {0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart(), 2: kgo.NewOffset().AtStart()}}))
			if err != nil {
				t.Fatal(err)
			}
			defer consumer.Close()
			fetches := consumer.PollFetches(requestCtx)
			if len(fetches.Errors()) != 0 {
				t.Fatal(fetches.Errors())
			}
			records := fetches.Records()
			if len(records) != 1 || string(records[0].Value) != "auto-created" {
				t.Fatal(records)
			}
		})
	}
}
