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
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestCoordinatorBrokerRestartE2E(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E") != "1" {
		t.Skip("set KAFSCALE_E2E=1")
	}
	bin := filepath.Join(t.TempDir(), "broker")
	build := exec.Command("go", "build", "-o", bin, filepath.Join(repoRoot(t), "cmd", "broker"))
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	for _, crash := range []bool{false, true} {
		t.Run(fmt.Sprintf("crash=%v", crash), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			etcd, endpoints := startEmbeddedEtcd(t)
			defer etcd.Close()
			store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			addresses := []string{freeAddr(t), freeAddr(t)}
			snapshot := metadata.ClusterMetadata{ControllerID: 1}
			for i, addr := range addresses {
				host, portString, _ := net.SplitHostPort(addr)
				port, _ := strconv.Atoi(portString)
				snapshot.Brokers = append(snapshot.Brokers, protocol.MetadataBroker{NodeID: int32(i + 1), Host: host, Port: int32(port)})
			}
			encoded, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.EtcdClient().Put(ctx, "/kafscale/metadata/snapshot", string(encoded)); err != nil {
				t.Fatal(err)
			}
			start := func(id int) (*exec.Cmd, *brokerLogBuffer, <-chan error) {
				t.Helper()
				cmd := exec.CommandContext(ctx, bin)
				configureProcessGroup(cmd)
				cmd.Env = os.Environ()
				for _, entry := range []string{
					"KAFSCALE_BROKER_ID=" + strconv.Itoa(id), "KAFSCALE_BROKER_ADDR=" + addresses[id-1],
					"KAFSCALE_METRICS_ADDR=" + freeAddr(t), "KAFSCALE_CONTROL_ADDR=" + freeAddr(t),
					"KAFSCALE_ETCD_ENDPOINTS=" + strings.Join(endpoints, ","), "KAFSCALE_USE_MEMORY_S3=1", "KAFSCALE_LOG_LEVEL=debug",
				} {
					key, value, _ := strings.Cut(entry, "=")
					cmd.Env = upsertEnv(cmd.Env, key, value)
				}
				logs := &brokerLogBuffer{}
				cmd.Stdout, cmd.Stderr = logs, logs
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				exited := make(chan error, 1)
				go func() { exited <- cmd.Wait(); close(exited) }()
				t.Cleanup(func() {
					_ = cmd.Process.Signal(os.Interrupt)
					select {
					case <-exited:
					case <-time.After(2 * time.Second):
						_ = cmd.Process.Kill()
						<-exited
					}
				})
				waitForBroker(t, logs, addresses[id-1])
				return cmd, logs, exited
			}
			a, logsA, exited := start(1)
			admin, err := kgo.NewClient(kgo.SeedBrokers(addresses[0]))
			if err != nil {
				t.Fatal(err)
			}
			defer admin.Close()
			const topic, groupID = "coordinator-restart", "restart-client"
			created, err := kadm.NewClient(admin).CreateTopic(ctx, 1, 1, nil, topic)
			if err != nil || created.Err != nil {
				t.Fatalf("create: %v %v", err, created.Err)
			}
			assigned := make(chan struct{}, 8)
			clientLogs := &brokerLogBuffer{}
			consumer, err := kgo.NewClient(kgo.SeedBrokers(addresses[0]), kgo.ConsumerGroup(groupID), kgo.ConsumeTopics(topic),
				kgo.WithLogger(kgo.BasicLogger(clientLogs, kgo.LogLevelDebug, nil)),
				kgo.Balancers(kgo.RangeBalancer()),
				kgo.SessionTimeout(45*time.Second), kgo.HeartbeatInterval(time.Second), kgo.RebalanceTimeout(10*time.Second),
				kgo.RetryBackoffFn(func(int) time.Duration { return 100 * time.Millisecond }),
				kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
					if len(partitions[topic]) > 0 {
						select {
						case assigned <- struct{}{}:
						default:
						}
					}
				}))
			if err != nil {
				t.Fatal(err)
			}
			pollCtx, stopPoll := context.WithCancel(ctx)
			defer func() { stopPoll(); consumer.Close() }()
			go func() {
				for pollCtx.Err() == nil {
					consumer.PollFetches(pollCtx)
				}
			}()
			select {
			case <-assigned:
			case <-time.After(15 * time.Second):
				t.Fatalf("initial join failed\n%s", logsA.String())
			}
			_, logsB, _ := start(2)
			find := kmsg.NewPtrFindCoordinatorRequest()
			find.Version, find.CoordinatorKey = 3, groupID
			found := directGroupRequest(t, addresses[1], find).(*kmsg.FindCoordinatorResponse)
			if found.ErrorCode != 0 || found.NodeID != 1 {
				t.Fatalf("initial coordinator: %+v", found)
			}
			began := time.Now()
			signal := os.Interrupt
			if crash {
				signal = os.Kill
			}
			if err := a.Process.Signal(signal); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-exited:
				if !crash && err != nil {
					t.Fatalf("graceful stop: %v\n%s", err, logsA.String())
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("old coordinator did not drain client connections after releasing leases\n%s", logsA.String())
			}
			if !crash {
				// A live peer must advertise itself immediately after release,
				// and can recover the same member before broker 1 returns.
				found := directGroupRequest(t, addresses[1], find).(*kmsg.FindCoordinatorResponse)
				if found.ErrorCode != 0 || found.NodeID != 2 {
					t.Fatalf("handoff discovery: %+v", found)
				}
				group, err := store.FetchConsumerGroup(ctx, groupID)
				if err != nil || group == nil {
					t.Fatalf("stored group: %v %v", group, err)
				}
				heartbeat := kmsg.NewPtrHeartbeatRequest()
				heartbeat.Group, heartbeat.MemberID, heartbeat.Generation = groupID, group.Leader, group.GenerationId
				if resp := directGroupRequest(t, addresses[1], heartbeat).(*kmsg.HeartbeatResponse); resp.ErrorCode != 0 {
					t.Fatalf("peer takeover: %+v", resp)
				}
			}
			_, restartedLogs, _ := start(1)
			// Require an actual fresh join rather than only an assignment restored from etcd.
			for len(assigned) > 0 {
				<-assigned
			}
			consumer.ForceRebalance()
			select {
			case <-assigned:
			case <-time.After(time.Until(began.Add(8 * time.Second))):
				t.Logf("client logs:\n%s", clientLogs.String())
				t.Fatalf("group did not rejoin within 8s (45s session)\nold:\n%s\npeer:\n%s\nrestarted:\n%s", logsA.String(), logsB.String(), restartedLogs.String())
			}
			var owner int32
			for i, addr := range addresses {
				resp := directGroupRequest(t, addr, find).(*kmsg.FindCoordinatorResponse)
				if resp.ErrorCode != 0 || resp.NodeID < 1 || resp.NodeID > 2 || (!crash && resp.NodeID != 2) {
					t.Fatalf("coordinator after restart: %+v", resp)
				}
				if i == 0 {
					owner = resp.NodeID
				} else if resp.NodeID != owner {
					t.Fatalf("brokers disagree about coordinator: %d != %d", owner, resp.NodeID)
				}
			}
			group, err := store.FetchConsumerGroup(ctx, groupID)
			if err != nil || group == nil || group.State != "stable" || len(group.Members) != 1 {
				t.Fatalf("recovered group: %v %v", group, err)
			}
			heartbeat := kmsg.NewPtrHeartbeatRequest()
			heartbeat.Group, heartbeat.MemberID, heartbeat.Generation = groupID, group.Leader, group.GenerationId
			for i, addr := range addresses {
				resp := directGroupRequest(t, addr, heartbeat).(*kmsg.HeartbeatResponse)
				want := int16(protocol.NOT_COORDINATOR)
				if int32(i+1) == owner {
					want = protocol.NONE
				}
				if resp.ErrorCode != want {
					t.Fatalf("broker %d heartbeat: %d, want %d", i+1, resp.ErrorCode, want)
				}
			}
			t.Logf("coordinator %d, client rejoined in %s with a 45s session", owner, time.Since(began))
		})
	}
}
