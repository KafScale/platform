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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestBrokerStartupChild(t *testing.T) {
	mode := os.Getenv("KAFSCALE_STARTUP_TEST_CHILD")
	if mode == "" {
		return
	}
	if mode == "invalid" {
		fmt.Fprintln(os.Stderr, "invalid broker configuration")
		os.Exit(1)
	}
	if mode == "collision" {
		fmt.Fprintln(os.Stderr, "listen tcp: bind: address already in use")
		os.Exit(1)
	}
	addresses := strings.Split(os.Getenv("KAFSCALE_STARTUP_TEST_ADDRS"), ",")
	delayedFailure := false
	for i, addr := range addresses {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			if mode == "delayed-metrics" && i == 1 {
				delayedFailure = true
				continue
			}
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer listener.Close()
		if i == 1 {
			fmt.Println("metrics server listening on " + addr)
			go http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/metrics" {
					http.NotFound(w, r)
					return
				}
				fmt.Fprintln(w, "# TYPE kafscale_broker_uptime_seconds gauge\nkafscale_broker_uptime_seconds 1")
			}))
			continue
		}
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				conn.Close()
			}
		}()
	}
	if mode != "wait" {
		fmt.Println("broker listening on " + addresses[0])
	}
	if delayedFailure {
		time.Sleep(250 * time.Millisecond)
		fmt.Fprintln(os.Stderr, "metrics server error: bind: address already in use")
		os.Exit(1)
	}
	select {}
}

func startupChildFactory(t *testing.T, mode string, commands *[]*exec.Cmd) func(brokerAddresses) *exec.Cmd {
	t.Helper()
	return func(addresses brokerAddresses) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestBrokerStartupChild$")
		configureProcessGroup(cmd)
		cmd.Env = append(os.Environ(), "KAFSCALE_STARTUP_TEST_CHILD="+mode, "KAFSCALE_STARTUP_TEST_ADDRS="+strings.Join([]string{addresses.kafka, addresses.metrics, addresses.control}, ","))
		*commands = append(*commands, cmd)
		t.Cleanup(func() { stopStartedCommand(cmd); startedCommands.Delete(cmd) })
		return cmd
	}
}

func TestBrokerStartupRetriesOccupiedPorts(t *testing.T) {
	for _, role := range []string{"kafka", "metrics", "control"} {
		t.Run(role, func(t *testing.T) {
			occupied, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer occupied.Close()
			addresses := newBrokerAddresses(t)
			switch role {
			case "kafka":
				addresses.kafka = occupied.Addr().String()
			case "metrics":
				addresses.metrics = occupied.Addr().String()
			case "control":
				addresses.control = occupied.Addr().String()
			}
			var commands []*exec.Cmd
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, _, actual, err := startBrokerAttempts(ctx, addresses, startupChildFactory(t, "ready", &commands), func() brokerAddresses { return newBrokerAddresses(t) }, nil, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if len(commands) != 2 || actual.kafka == addresses.kafka || actual.metrics == addresses.metrics || actual.control == addresses.control {
				t.Fatalf("attempts=%d endpoints=%+v original=%+v", len(commands), actual, addresses)
			}
			if commands[0].ProcessState == nil {
				t.Fatal("failed process was not reaped before retry")
			}
		})
	}
}

func TestBrokerStartupRetryLimits(t *testing.T) {
	for _, tc := range []struct {
		mode     string
		attempts int
	}{{"collision", 3}, {"invalid", 1}, {"wait", 1}} {
		t.Run(tc.mode, func(t *testing.T) {
			timeout := 20 * time.Second
			if tc.mode == "wait" {
				timeout = 2 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			var commands []*exec.Cmd
			_, _, _, err := startBrokerAttempts(ctx, newBrokerAddresses(t), startupChildFactory(t, tc.mode, &commands), func() brokerAddresses { return newBrokerAddresses(t) }, nil, io.Discard)
			if err == nil || len(commands) != tc.attempts {
				t.Fatalf("attempts=%d err=%v", len(commands), err)
			}
			for _, cmd := range commands {
				if cmd.ProcessState == nil {
					t.Fatal("failed process was not reaped")
				}
			}
		})
	}
}

func TestBrokerStartupCleanupDoesNotRewait(t *testing.T) {
	var commands []*exec.Cmd
	cmd := startupChildFactory(t, "invalid", &commands)(newBrokerAddresses(t))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("expected helper failure")
	}
	stopStartedCommand(cmd)
	if _, registered := startedCommands.Load(cmd); registered {
		t.Fatal("cleanup tried to wait again on a reaped process")
	}
}

func TestBrokerStartupCancellationStopsChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	addresses := newBrokerAddresses(t)
	cmd, _, actual := startBrokerWithEtcd(t, ctx, addresses.kafka, addresses.metrics, addresses.control, nil)
	cancel()
	select {
	case <-commandCompletionFor(cmd).done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled broker process was not reaped")
	}
	deadline := time.Now().Add(5 * time.Second)
	for _, addr := range []string{actual.kafka, actual.metrics, actual.control} {
		for {
			conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
			if err != nil {
				break
			}
			conn.Close()
			if time.Now().After(deadline) {
				t.Fatalf("broker child retained listener %s after cancellation", addr)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func TestBrokerStartupRepairsSnapshotAfterCollision(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "host.docker.internal"} {
		t.Run(host, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			etcd, endpoints := startEmbeddedEtcd(t)
			defer etcd.Close()
			occupied, err := net.Listen("tcp", ":0")
			if err != nil {
				t.Fatal(err)
			}
			defer occupied.Close()
			addresses := newBrokerAddresses(t)
			addresses.kafka = net.JoinHostPort("127.0.0.1", fmt.Sprint(occupied.Addr().(*net.TCPAddr).Port))
			client, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			oldPort := int32(occupied.Addr().(*net.TCPAddr).Port)
			snapshot := metadata.ClusterMetadata{Brokers: []protocol.MetadataBroker{
				{NodeID: 1, Host: host, Port: oldPort},
				{NodeID: 2, Host: "other-broker", Port: oldPort},
			}}
			data, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.Put(ctx, "/kafscale/metadata/snapshot", string(data)); err != nil {
				t.Fatal(err)
			}
			cmd, _, actual := startBrokerWithEtcd(t, ctx, addresses.kafka, addresses.metrics, addresses.control, endpoints, "KAFSCALE_BROKER_ID=1", "KAFSCALE_BROKER_HOST="+host)
			defer stopBroker(t, cmd)
			if actual.kafka == addresses.kafka {
				t.Fatal("broker retained occupied address")
			}
			req := kmsg.NewPtrMetadataRequest()
			req.Version = 12
			response := directGroupRequest(t, actual.kafka, req).(*kmsg.MetadataResponse)
			_, newPort, _ := net.SplitHostPort(actual.kafka)
			if len(response.Brokers) != 2 || response.Brokers[0].NodeID != 1 || response.Brokers[0].Host != host || fmt.Sprint(response.Brokers[0].Port) != newPort {
				t.Fatalf("incorrect advertised metadata: %+v", response)
			}
			persisted, err := client.Get(ctx, "/kafscale/metadata/snapshot")
			if err != nil {
				t.Fatal(err)
			}
			var repaired metadata.ClusterMetadata
			if len(persisted.Kvs) != 1 {
				t.Fatal("snapshot missing")
			}
			if err := json.Unmarshal(persisted.Kvs[0].Value, &repaired); err != nil {
				t.Fatal(err)
			}
			if len(repaired.Brokers) != 2 || repaired.Brokers[0].Host != host || fmt.Sprint(repaired.Brokers[0].Port) != newPort || !reflect.DeepEqual(repaired.Brokers[1], snapshot.Brokers[1]) {
				t.Fatalf("incorrect persisted repair: %+v", repaired.Brokers)
			}
		})
	}
}

func TestBrokerStartupRejectsUnrelatedMetrics(t *testing.T) {
	// Even another broker's HTTP metrics must not count as readiness while the bind
	// failure is delayed well beyond the old two successful TCP polls.
	unrelated := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "# TYPE kafscale_broker_uptime_seconds gauge\nkafscale_broker_uptime_seconds 1")
	}))
	defer unrelated.Close()
	addresses := newBrokerAddresses(t)
	addresses.metrics = strings.TrimPrefix(unrelated.URL, "http://")
	var commands []*exec.Cmd
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, actual, err := startBrokerAttempts(ctx, addresses, startupChildFactory(t, "delayed-metrics", &commands), func() brokerAddresses { return newBrokerAddresses(t) }, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 2 || actual.metrics == addresses.metrics {
		t.Fatalf("accepted unrelated metrics server: attempts=%d actual=%+v", len(commands), actual)
	}
	if commands[0].ProcessState == nil {
		t.Fatal("failed process was not reaped")
	}
}

func TestBrokerStartupRetriesAuxiliaryListeners(t *testing.T) {
	for _, role := range []string{"metrics", "control"} {
		t.Run(role, func(t *testing.T) {
			occupied, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer occupied.Close()
			addresses := newBrokerAddresses(t)
			if role == "metrics" {
				addresses.metrics = occupied.Addr().String()
			} else {
				addresses.control = occupied.Addr().String()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			// Also preserve Docker fixtures' advertised-host / wildcard-listen contract.
			cmd, _, actual := startBrokerWithEtcd(t, ctx, addresses.kafka, addresses.metrics, addresses.control, nil, "KAFSCALE_BROKER_HOST=host.docker.internal")
			defer stopBroker(t, cmd)
			if actual.kafka == addresses.kafka {
				t.Fatal("auxiliary collision did not retry the process")
			}
			req := kmsg.NewPtrMetadataRequest()
			req.Version = 12
			response := directGroupRequest(t, actual.kafka, req).(*kmsg.MetadataResponse)
			if len(response.Brokers) != 1 || response.Brokers[0].Host != "host.docker.internal" {
				t.Fatalf("advertised host lost: %+v", response)
			}
			_, port, _ := net.SplitHostPort(actual.kafka)
			if fmt.Sprint(response.Brokers[0].Port) != port {
				t.Fatalf("advertised port lost: %+v", response)
			}
		})
	}
}
