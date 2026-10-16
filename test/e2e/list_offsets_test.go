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
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestListOffsetsUnknownTopicThroughProxy(t *testing.T) {
	if !parseBoolEnv("KAFSCALE_E2E") {
		t.Skip("set KAFSCALE_E2E=1 to run broker/proxy integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	etcd, endpoints := startEmbeddedEtcd(t)
	t.Cleanup(etcd.Close)
	start := func(component string, env []string) {
		t.Helper()
		logPath := filepath.Join(t.TempDir(), component+".log")
		log, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", component))
		configureProcessGroup(cmd)
		cmd.Env = append(os.Environ(), env...)
		cmd.Stdout = log
		cmd.Stderr = log
		if err := cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() {
			stopBroker(t, cmd)
			log.Close()
			if t.Failed() {
				if data, err := os.ReadFile(logPath); err == nil {
					t.Logf("%s logs:\n%s", component, data)
				}
			}
		})
	}
	waitReady := func(addr string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) && ctx.Err() == nil {
			conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
			if err == nil {
				conn.Close()
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("process did not listen on %s", addr)
	}
	brokerAddr, proxyAddr := freeAddr(t), freeAddr(t)
	_, brokerPort, _ := net.SplitHostPort(brokerAddr)
	_, proxyPort, _ := net.SplitHostPort(proxyAddr)
	start("broker", []string{
		"KAFSCALE_USE_MEMORY_S3=1", "KAFSCALE_AUTO_CREATE_TOPICS=true",
		"KAFSCALE_BROKER_ADDR=" + brokerAddr, "KAFSCALE_BROKER_HOST=127.0.0.1", "KAFSCALE_BROKER_PORT=" + brokerPort,
		"KAFSCALE_METRICS_ADDR=" + freeAddr(t), "KAFSCALE_CONTROL_ADDR=" + freeAddr(t),
		"KAFSCALE_ETCD_ENDPOINTS=" + strings.Join(endpoints, ","),
	})
	waitReady(brokerAddr)
	start("proxy", []string{
		"KAFSCALE_PROXY_ADDR=" + proxyAddr, "KAFSCALE_PROXY_BACKENDS=" + brokerAddr,
		"KAFSCALE_PROXY_ADVERTISED_HOST=127.0.0.1", "KAFSCALE_PROXY_ADVERTISED_PORT=" + proxyPort,
		"KAFSCALE_PROXY_ETCD_ENDPOINTS=" + strings.Join(endpoints, ","), "KAFSCALE_PROXY_HEALTH_ADDR=",
	})
	waitReady(proxyAddr)
	for _, endpoint := range []struct{ name, addr string }{{"broker", brokerAddr}, {"proxy", proxyAddr}} {
		for _, version := range []int16{0, 4} {
			t.Run(fmt.Sprintf("%s_v%d", endpoint.name, version), func(t *testing.T) {
				conn, err := net.DialTimeout("tcp", endpoint.addr, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				correlation := int32(0)
				exchange := func(req kmsg.Request, resp kmsg.Response) {
					t.Helper()
					correlation++
					payload := binary.BigEndian.AppendUint16(nil, uint16(req.Key()))
					payload = binary.BigEndian.AppendUint16(payload, uint16(req.GetVersion()))
					payload = binary.BigEndian.AppendUint32(payload, uint32(correlation))
					payload = binary.BigEndian.AppendUint16(payload, 0xffff)
					payload = req.AppendTo(payload)
					if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
						t.Fatal(err)
					}
					if err := protocol.WriteFrame(conn, payload); err != nil {
						t.Fatal(err)
					}
					frame, err := protocol.ReadFrame(conn)
					if err != nil {
						t.Fatal(err)
					}
					if len(frame.Payload) < 4 || int32(binary.BigEndian.Uint32(frame.Payload[:4])) != correlation {
						t.Fatal("invalid correlation ID")
					}
					resp.SetVersion(req.GetVersion())
					if err := resp.ReadFrom(frame.Payload[4:]); err != nil {
						t.Fatal(err)
					}
				}
				topic := fmt.Sprintf("offsets-%s-v%d", endpoint.name, version)
				lookup := func(timestamp int64, want int16) {
					t.Helper()
					req := kmsg.NewPtrListOffsetsRequest()
					req.SetVersion(version)
					req.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: 0, Timestamp: timestamp, MaxNumOffsets: 1}}}}
					resp := kmsg.NewPtrListOffsetsResponse()
					exchange(req, resp)
					if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
						t.Fatalf("invalid response: %+v", resp)
					}
					p := resp.Topics[0].Partitions[0]
					if p.ErrorCode != want {
						t.Fatalf("timestamp=%d got=%d want=%d", timestamp, p.ErrorCode, want)
					}
					if want == protocol.NONE {
						if version == 0 {
							if len(p.OldStyleOffsets) != 1 || p.OldStyleOffsets[0] != 0 {
								t.Fatalf("invalid v0 offset: %+v", p)
							}
						} else if p.Offset != 0 {
							t.Fatalf("invalid offset: %+v", p)
						}
					} else if version == 0 {
						if len(p.OldStyleOffsets) != 0 {
							t.Fatal("error returned offsets")
						}
					} else if p.Offset != -1 {
						t.Fatalf("error offset=%d", p.Offset)
					}
				}
				lookup(-1, protocol.UNKNOWN_TOPIC_OR_PARTITION)
				lookup(-2, protocol.UNKNOWN_TOPIC_OR_PARTITION)
				create := kmsg.NewPtrCreateTopicsRequest()
				create.SetVersion(0)
				create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
				created := kmsg.NewPtrCreateTopicsResponse()
				exchange(create, created)
				if len(created.Topics) != 1 || created.Topics[0].ErrorCode != protocol.NONE {
					t.Fatalf("explicit creation failed after lookups: %+v", created)
				}
				lookup(-1, protocol.NONE)
				lookup(-2, protocol.NONE)
			})
		}
	}
}
