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
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/KafScale/platform/pkg/metadata"
)

func TestConsumerGroupMetadataPersistsInEtcd(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	etcd, endpoints := startEmbeddedEtcd(t)
	defer etcd.Close()

	brokerAddr := freeAddr(t)
	metricsAddr := freeAddr(t)
	controlAddr := freeAddr(t)

	brokerCmd, brokerLogs := startBrokerWithEtcd(t, ctx, brokerAddr, metricsAddr, controlAddr, endpoints)
	waitForBroker(t, brokerLogs, brokerAddr)

	topic := fmt.Sprintf("orders-%d", time.Now().UnixNano())
	groupID := fmt.Sprintf("franz-etcd-%d", time.Now().UnixNano())
	sessionTimeout := 22 * time.Second
	rebalanceTimeout := 35 * time.Second

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.SessionTimeout(sessionTimeout),
		kgo.RebalanceTimeout(rebalanceTimeout),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		stopBroker(t, brokerCmd)
		t.Fatalf("create consumer: %v\nbroker logs:\n%s", err, brokerLogs.String())
	}
	consumerClosed := false
	defer func() {
		if !consumerClosed {
			consumer.CloseAllowingRebalance()
		}
	}()

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

	msg := []byte("hello-etcd")
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: msg}).FirstErr(); err != nil {
		stopBroker(t, brokerCmd)
		t.Fatalf("produce failed: %v\nbroker logs:\n%s", err, brokerLogs.String())
	}

	consumeDeadline := time.Now().Add(15 * time.Second)
	for {
		if time.Now().After(consumeDeadline) {
			stopBroker(t, brokerCmd)
			t.Fatalf("timed out waiting for consumer group join")
		}
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			stopBroker(t, brokerCmd)
			t.Fatalf("fetch errors: %+v\nbroker logs:\n%s", errs, brokerLogs.String())
		}
		seen := false
		fetches.EachRecord(func(record *kgo.Record) {
			seen = true
		})
		if seen {
			break
		}
	}

	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{
		Endpoints: endpoints,
	})
	if err != nil {
		stopBroker(t, brokerCmd)
		t.Fatalf("create etcd store: %v", err)
	}

	verifyGroup := func(stage string) {
		deadline := time.Now().Add(10 * time.Second)
		for {
			group, err := store.FetchConsumerGroup(ctx, groupID)
			if err != nil {
				stopBroker(t, brokerCmd)
				t.Fatalf("fetch consumer group (%s): %v", stage, err)
			}
			if group != nil {
				if group.RebalanceTimeoutMs != int32(rebalanceTimeout/time.Millisecond) {
					t.Fatalf("rebalance timeout mismatch (%s): %d", stage, group.RebalanceTimeoutMs)
				}
				member := group.Members[group.Leader]
				if member == nil {
					t.Fatalf("expected leader member persisted (%s)", stage)
				}
				if member.SessionTimeoutMs != int32(sessionTimeout/time.Millisecond) {
					t.Fatalf("session timeout mismatch (%s): %d", stage, member.SessionTimeoutMs)
				}
				return
			}
			if time.Now().After(deadline) {
				stopBroker(t, brokerCmd)
				t.Fatalf("consumer group not found in etcd (%s)", stage)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	verifyGroup("before-restart")
	stopBroker(t, brokerCmd)
	consumer.CloseAllowingRebalance()
	consumerClosed = true
	producer.Close()
	producerClosed = true

	brokerAddr = freeAddr(t)
	metricsAddr = freeAddr(t)
	controlAddr = freeAddr(t)
	brokerCmd, brokerLogs = startBrokerWithEtcd(t, ctx, brokerAddr, metricsAddr, controlAddr, endpoints)
	waitForBroker(t, brokerLogs, brokerAddr)
	verifyGroup("after-restart")
	stopBroker(t, brokerCmd)
}

// brokerLogBuffer allows readiness checks while os/exec streams process output.
type brokerLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *brokerLogBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *brokerLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func startBrokerWithEtcd(t *testing.T, ctx context.Context, brokerAddr, metricsAddr, controlAddr string, endpoints []string) (*exec.Cmd, *brokerLogBuffer) {
	t.Helper()
	brokerCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "broker"))
	configureProcessGroup(brokerCmd)
	brokerCmd.Env = append(os.Environ(),
		"KAFSCALE_AUTO_CREATE_TOPICS=true",
		"KAFSCALE_AUTO_CREATE_PARTITIONS=1",
		"KAFSCALE_USE_MEMORY_S3=1",
		fmt.Sprintf("KAFSCALE_BROKER_ADDR=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_METRICS_ADDR=%s", metricsAddr),
		fmt.Sprintf("KAFSCALE_CONTROL_ADDR=%s", controlAddr),
		fmt.Sprintf("KAFSCALE_ETCD_ENDPOINTS=%s", strings.Join(endpoints, ",")),
	)
	var brokerLogs brokerLogBuffer
	logWriter := io.MultiWriter(&brokerLogs, mustLogFile(t, "broker-etcd.log"))
	brokerCmd.Stdout = logWriter
	brokerCmd.Stderr = logWriter
	if err := brokerCmd.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	return brokerCmd, &brokerLogs
}

func stopBroker(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = signalProcessGroup(cmd, os.Interrupt)
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = signalProcessGroup(cmd, os.Kill)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on free port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}

// A raw protocol client creates membership without a background heartbeat loop,
// then SIGKILL leaves it in etcd for a replacement broker to expire.
func TestDeleteConsumerGroupAfterCoordinatorCrash(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E") != "1" {
		t.Skip("set KAFSCALE_E2E=1 to run integration harness")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	etcd, endpoints := startEmbeddedEtcd(t)
	defer etcd.Close()
	brokerAddr, metricsAddr, controlAddr := freeAddr(t), freeAddr(t), freeAddr(t)
	brokerCmd, logs := startBrokerWithEtcd(t, ctx, brokerAddr, metricsAddr, controlAddr, endpoints)
	t.Cleanup(func() { stopBroker(t, brokerCmd) })
	waitForBroker(t, logs, brokerAddr)
	client, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, group := range []string{"crashed", "live"} {
		join := kmsg.NewPtrJoinGroupRequest()
		join.Group, join.ProtocolType = group, "consumer"
		join.SessionTimeoutMillis = 2000
		if group == "live" {
			join.SessionTimeoutMillis = 60000
		}
		join.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range"}}
		joined, err := join.RequestWith(ctx, client)
		if err != nil || joined.ErrorCode != 0 {
			t.Fatalf("join: %+v, %v", joined, err)
		}
		commit := kmsg.NewPtrOffsetCommitRequest()
		commit.Group, commit.MemberID, commit.Generation = group, joined.MemberID, joined.Generation
		commit.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "orders", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 42}}}}
		committed, err := commit.RequestWith(ctx, client)
		if err != nil || len(committed.Topics) != 1 || len(committed.Topics[0].Partitions) != 1 || committed.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatalf("commit: %+v, %v", committed, err)
		}
	}
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	before, err := store.FetchConsumerGroup(ctx, "crashed")
	if err != nil || before == nil || len(before.Members) != 1 {
		t.Fatalf("persisted membership: %+v, %v", before, err)
	}
	if err := signalProcessGroup(brokerCmd, os.Kill); err != nil {
		t.Fatal(err)
	}
	_ = brokerCmd.Wait()
	client.Close()
	// Wait only for the recorded member's session deadline, without joining again.
	for _, member := range before.Members {
		heartbeat, err := time.Parse(time.RFC3339Nano, member.HeartbeatAt)
		if err != nil {
			t.Fatal(err)
		}
		if remaining := time.Until(heartbeat.Add(time.Duration(member.SessionTimeoutMs)*time.Millisecond + 20*time.Millisecond)); remaining > 0 {
			time.Sleep(remaining)
		}
	}
	brokerCmd, logs = startBrokerWithEtcd(t, ctx, brokerAddr, metricsAddr, controlAddr, endpoints)
	waitForBroker(t, logs, brokerAddr)
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	del := kmsg.NewPtrDeleteGroupsRequest()
	del.Groups = []string{"crashed", "live"}
	deleted, err := del.RequestWith(ctx, admin)
	if err != nil || len(deleted.Groups) != 2 || deleted.Groups[0].ErrorCode != 0 || deleted.Groups[1].ErrorCode != 68 {
		t.Fatalf("delete after crash: %+v, %v", deleted, err)
	}
	if group, err := store.FetchConsumerGroup(ctx, "crashed"); err != nil || group != nil {
		t.Fatal(group, err)
	}
	if offsets, err := store.ListGroupConsumerOffsets(ctx, "crashed"); err != nil || len(offsets) != 0 {
		t.Fatal(offsets, err)
	}
	live, err := store.FetchConsumerGroup(ctx, "live")
	if err != nil || live == nil || len(live.Members) != 1 {
		t.Fatal(live, err)
	}
	if offsets, err := store.ListGroupConsumerOffsets(ctx, "live"); err != nil || len(offsets) != 1 || offsets[0].Offset != 42 {
		t.Fatal(offsets, err)
	}
}
