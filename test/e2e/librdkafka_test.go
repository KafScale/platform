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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
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

func TestLibrdkafkaCompatibilityE2E(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E") != "1" {
		t.Skip("set KAFSCALE_E2E=1")
	}
	kcatBinary := os.Getenv("KAFSCALE_KCAT_BIN")
	if kcatBinary == "" {
		kcatBinary = "kcat"
	}
	path, err := exec.LookPath(kcatBinary)
	if err != nil {
		t.Fatalf("kcat is required for this test: %v", err)
	}
	version, err := exec.Command(path, "-V").CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("client: %s", version)
	options, err := exec.Command(path, "-F", os.DevNull, "-X", "list").CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	var classicOption []string
	for _, line := range strings.Split(string(options), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == "group.protocol" {
			classicOption = []string{"-X", "group.protocol=classic"}
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	etcd, endpoints := startEmbeddedEtcd(t)
	defer etcd.Close()
	brokerAddr, proxyAddr := freeAddr(t), freeAddr(t)
	brokerCmd, brokerLogs := startBrokerWithEtcd(t, ctx, brokerAddr, freeAddr(t), freeAddr(t), endpoints)
	defer stopBroker(t, brokerCmd)
	waitForBroker(t, brokerLogs, brokerAddr)
	_, proxyPort, _ := net.SplitHostPort(proxyAddr)
	logFile, err := os.Create(filepath.Join(t.TempDir(), "proxy.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	proxyCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "proxy"))
	configureProcessGroup(proxyCmd)
	proxyCmd.Env = os.Environ()
	for _, entry := range []string{"KAFSCALE_PROXY_ADDR=" + proxyAddr, "KAFSCALE_PROXY_BACKENDS=" + brokerAddr, "KAFSCALE_PROXY_ETCD_ENDPOINTS=" + strings.Join(endpoints, ","), "KAFSCALE_PROXY_ADVERTISED_HOST=127.0.0.1", "KAFSCALE_PROXY_ADVERTISED_PORT=" + proxyPort, "KAFSCALE_PROXY_HEALTH_ADDR=", "KAFSCALE_PROXY_LFS_ENABLED=false", "KAFSCALE_PROXY_IDENTITY_KEY_FILE=", "KAFSCALE_PROXY_TLS_CERT_FILE=", "KAFSCALE_PROXY_TLS_KEY_FILE=", "KAFSCALE_PROXY_TLS_CLIENT_CA_FILE=", "KAFSCALE_PROXY_TLS_PRINCIPALS_FILE="} {
		key, value, _ := strings.Cut(entry, "=")
		proxyCmd.Env = upsertEnv(proxyCmd.Env, key, value)
	}
	var proxyLogs brokerLogBuffer
	writer := io.MultiWriter(logFile, &proxyLogs)
	proxyCmd.Stdout, proxyCmd.Stderr = writer, writer
	if err := proxyCmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer stopBroker(t, proxyCmd)
	for deadline := time.Now().Add(20 * time.Second); ; {
		conn, err := net.DialTimeout("tcp", proxyAddr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("proxy startup: %v\n%s", err, proxyLogs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	run := func(t *testing.T, addr, input string, args ...string) string {
		t.Helper()
		commandCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		args = append([]string{"-F", os.DevNull, "-b", addr, "-X", "socket.timeout.ms=5000"}, args...)
		cmd := exec.CommandContext(commandCtx, path, args...)
		cmd.Stdin = strings.NewReader(input)
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("kcat %v: %v\n%s", args, err, stderr.String())
		}
		return stdout.String()
	}
	for _, target := range []struct{ name, addr string }{{"broker", brokerAddr}, {"proxy", proxyAddr}} {
		t.Run(target.name, func(t *testing.T) {
			topic := fmt.Sprintf("kcat-%s-%d", target.name, time.Now().UnixNano())
			const messages = "first\nsecond\nthird\n"
			run(t, target.addr, messages, "-P", "-q", "-t", topic, "-p", "0", "-X", "enable.idempotence=false", "-X", "message.timeout.ms=5000")
			t.Run("reject-corrupt-produce", func(t *testing.T) {
				fetch := kmsg.NewPtrFetchRequest()
				fetch.Version, fetch.ReplicaID, fetch.MaxBytes = 4, -1, 1<<20
				fetch.Topics = []kmsg.FetchRequestTopic{{Topic: topic, Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 0, PartitionMaxBytes: 1 << 20}}}}
				before := directGroupRequest(t, target.addr, fetch).(*kmsg.FetchResponse).Topics[0].Partitions[0]
				if before.ErrorCode != 0 || before.HighWatermark != 3 || len(before.RecordBatches) < 61 {
					t.Fatalf("initial records: %+v", before)
				}
				badCRC := bytes.Clone(before.RecordBatches)
				binary.BigEndian.PutUint32(badCRC[17:21], 0)
				badPayload := bytes.Clone(before.RecordBatches)
				badPayload[len(badPayload)-1] ^= 1
				for _, version := range []int16{7, 9} {
					for _, tc := range []struct {
						name    string
						records []byte
					}{
						{"zero-crc", badCRC},
						{"corrupt-payload", badPayload},
						{"valid-then-bad-crc", append(bytes.Clone(before.RecordBatches), badCRC...)},
					} {
						t.Run(fmt.Sprintf("v%d/%s", version, tc.name), func(t *testing.T) {
							req := kmsg.NewPtrProduceRequest()
							req.Version, req.Acks, req.TimeoutMillis = version, -1, 5000
							req.Topics = []kmsg.ProduceRequestTopic{{Topic: topic, Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: tc.records}}}}
							resp := directGroupRequest(t, target.addr, req).(*kmsg.ProduceResponse)
							if part := resp.Topics[0].Partitions[0]; part.ErrorCode != protocol.CORRUPT_MESSAGE {
								t.Fatalf("corrupt produce: %+v", part)
							}
							after := directGroupRequest(t, target.addr, fetch).(*kmsg.FetchResponse).Topics[0].Partitions[0]
							if after.ErrorCode != 0 || after.HighWatermark != before.HighWatermark || !bytes.Equal(after.RecordBatches, before.RecordBatches) {
								t.Fatalf("corrupt produce changed stored records: %+v", after)
							}
						})
					}
				}
			})
			if got := run(t, target.addr, "", "-C", "-q", "-t", topic, "-p", "0", "-o", "beginning", "-c", "3", "-e"); got != messages {
				t.Fatalf("partition consume: %q", got)
			}
			group := "group-" + topic
			groupArgs := []string{"-q", "-c", "3", "-o", "beginning", "-X", "auto.offset.reset=earliest", "-X", "auto.commit.interval.ms=100"}
			groupArgs = append(groupArgs, classicOption...)
			groupArgs = append(groupArgs, "-G", group, topic)
			if got := run(t, target.addr, "", groupArgs...); got != messages {
				t.Fatalf("group consume: %q", got)
			}
			fetch := kmsg.NewPtrOffsetFetchRequest()
			fetch.Version, fetch.Group = 5, group
			fetched := directGroupRequest(t, target.addr, fetch).(*kmsg.OffsetFetchResponse)
			if fetched.ErrorCode != 0 || len(fetched.Topics) != 1 || len(fetched.Topics[0].Partitions) != 1 || fetched.Topics[0].Partitions[0].Offset != 3 {
				t.Fatalf("group checkpoint: %+v", fetched)
			}
			t.Run("disconnect-during-join", func(t *testing.T) { testJoinDisconnect(t, target.addr, topic) })
			t.Run("two-members", func(t *testing.T) {
				multiTopic, multiGroup := topic+"-two", group+"-two"
				create := kmsg.NewPtrCreateTopicsRequest()
				create.Version, create.TimeoutMillis = 2, 5000
				create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: multiTopic, NumPartitions: 2, ReplicationFactor: 1}}
				if resp := directGroupRequest(t, target.addr, create).(*kmsg.CreateTopicsResponse); len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
					t.Fatalf("create topic: %+v", resp)
				}
				var outputs [2]brokerLogBuffer
				var errors [2]brokerLogBuffer
				startConsumer := func(index int) {
					args := []string{"-F", os.DevNull, "-b", target.addr, "-q", "-u", "-f", "%p:%s\n", "-X", "auto.offset.reset=earliest", "-X", "session.timeout.ms=6000", "-X", "max.poll.interval.ms=15000"}
					args = append(args, classicOption...)
					args = append(args, "-G", multiGroup, multiTopic)
					cmd := exec.CommandContext(ctx, path, args...)
					configureProcessGroup(cmd)
					cmd.Stdout, cmd.Stderr = &outputs[index], &errors[index]
					if err := cmd.Start(); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { stopBroker(t, cmd) })
				}
				waitMembers := func(count int) {
					deadline := time.Now().Add(25 * time.Second)
					for time.Now().Before(deadline) {
						describe := kmsg.NewPtrDescribeGroupsRequest()
						describe.Version, describe.Groups = 3, []string{multiGroup}
						resp := directGroupRequest(t, target.addr, describe).(*kmsg.DescribeGroupsResponse)
						if len(resp.Groups) == 1 && resp.Groups[0].ErrorCode == 0 && resp.Groups[0].State == "Stable" && len(resp.Groups[0].Members) == count {
							seen := map[int32]bool{}
							valid := true
							for _, member := range resp.Groups[0].Members {
								var assignment kmsg.ConsumerMemberAssignment
								if err := assignment.ReadFrom(member.MemberAssignment); err != nil || len(assignment.Topics) != 1 || len(assignment.Topics[0].Partitions) == 0 {
									valid = false
									break
								}
								for _, partition := range assignment.Topics[0].Partitions {
									if seen[partition] {
										valid = false
									}
									seen[partition] = true
								}
							}
							if valid && len(seen) == 2 {
								return
							}
						}
						time.Sleep(100 * time.Millisecond)
					}
					t.Fatalf("group never assigned partitions to %d members: %s %s", count, errors[0].String(), errors[1].String())
				}
				startConsumer(0)
				waitMembers(1)
				startConsumer(1)
				waitMembers(2)
				for partition := 0; partition < 2; partition++ {
					run(t, target.addr, fmt.Sprintf("partition-%d\n", partition), "-P", "-q", "-t", multiTopic, "-p", fmt.Sprint(partition), "-X", "enable.idempotence=false")
				}
				deadline := time.Now().Add(15 * time.Second)
				for time.Now().Before(deadline) && (outputs[0].String() == "" || outputs[1].String() == "") {
					time.Sleep(100 * time.Millisecond)
				}
				first, second := outputs[0].String(), outputs[1].String()
				if first == "" || second == "" || first == second || !strings.Contains(first+second, "0:partition-0") || !strings.Contains(first+second, "1:partition-1") {
					t.Fatalf("both members must consume distinct assigned partitions: %q %q; %s %s", first, second, errors[0].String(), errors[1].String())
				}
			})

		})
	}
}
