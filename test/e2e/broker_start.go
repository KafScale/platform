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
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/pkg/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type brokerAddresses struct{ kafka, metrics, control string }

func newBrokerAddresses(t *testing.T) brokerAddresses {
	addresses := allocateLocalAddresses(t, 3)
	return brokerAddresses{addresses[0], addresses[1], addresses[2]}
}

type commandCompletion struct {
	done chan struct{}
	err  error
}

var startedCommands sync.Map

// One goroutine owns Wait, so startup failures, crash tests and cleanup can all
// observe exit without racing Wait or leaving a failed child behind.
func commandCompletionFor(cmd *exec.Cmd) *commandCompletion {
	state := &commandCompletion{done: make(chan struct{})}
	stored, loaded := startedCommands.LoadOrStore(cmd, state)
	if loaded {
		return stored.(*commandCompletion)
	}
	go func() { state.err = cmd.Wait(); close(state.done) }()
	return state
}

func stopStartedCommand(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	if _, registered := startedCommands.Load(cmd); !registered && cmd.ProcessState != nil {
		// A previous owner already reaped it. Never signal an old process
		// group ID after its completion record was removed during cleanup.
		return
	}
	exit := commandCompletionFor(cmd)
	select {
	case <-exit.done:
		return
	default:
	}
	_ = signalProcessGroup(cmd, os.Interrupt)
	select {
	case <-exit.done:
	case <-time.After(2 * time.Second):
		_ = signalProcessGroup(cmd, os.Kill)
		<-exit.done
	}
}

func startBrokerProcess(t *testing.T, ctx context.Context, addresses brokerAddresses, env, endpoints []string, logName string) (*exec.Cmd, *brokerLogBuffer, brokerAddresses) {
	t.Helper()
	var writer io.Writer = mustLogFile(t, logName)
	if parseBoolEnv("KAFSCALE_E2E_DEBUG") {
		writer = io.MultiWriter(writer, os.Stdout)
	}
	factory := func(addresses brokerAddresses) *exec.Cmd {
		cmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "broker"))
		configureProcessGroup(cmd)
		// Cancellation must also stop the broker spawned by go run.
		cmd.Cancel = func() error { return signalProcessGroup(cmd, os.Kill) }
		cmd.Env = append(os.Environ(), env...)
		listenAddr := addresses.kafka
		for _, entry := range env {
			if strings.HasPrefix(entry, "KAFSCALE_BROKER_HOST=") {
				// Docker CLI fixtures advertise the host gateway and listen on
				// all interfaces. Keep that contract after changing the port.
				_, port, _ := net.SplitHostPort(addresses.kafka)
				listenAddr = ":" + port
			}
		}
		for key, value := range map[string]string{"KAFSCALE_BROKER_ADDR": addresses.kafka, "KAFSCALE_METRICS_ADDR": addresses.metrics, "KAFSCALE_CONTROL_ADDR": addresses.control} {
			cmd.Env = upsertEnv(cmd.Env, key, value)
		}
		cmd.Env = upsertEnv(cmd.Env, "KAFSCALE_BROKER_ADDR", listenAddr)
		return cmd
	}
	beforeRetry := func(old, next brokerAddresses) error {
		t.Logf("broker bind collision; retrying with %s", next.kafka)
		return moveSnapshotBrokerAddress(ctx, endpoints, old.kafka, next.kafka)
	}
	cmd, logs, actual, err := startBrokerAttempts(ctx, addresses, factory, func() brokerAddresses { return newBrokerAddresses(t) }, beforeRetry, writer)
	if err != nil {
		t.Fatalf("start broker: %v\n%s", err, logs.String())
	}
	t.Cleanup(func() { stopStartedCommand(cmd); startedCommands.Delete(cmd) })
	return cmd, logs, actual
}

func bindCollision(logs string) bool {
	return strings.Contains(strings.ToLower(logs), "address already in use")
}

// Only bind collisions are retried. Ordinary startup errors and cancellation
// fail immediately; a failed process is reaped before any endpoint is reused.
func startBrokerAttempts(ctx context.Context, addresses brokerAddresses, factory func(brokerAddresses) *exec.Cmd, next func() brokerAddresses, beforeRetry func(brokerAddresses, brokerAddresses) error, writer io.Writer) (*exec.Cmd, *brokerLogBuffer, brokerAddresses, error) {
	var logs *brokerLogBuffer
	for attempt := 1; attempt <= 3; attempt++ {
		logs = &brokerLogBuffer{}
		if err := ctx.Err(); err != nil {
			return nil, logs, addresses, err
		}
		cmd := factory(addresses)
		cmd.Stdout, cmd.Stderr = io.MultiWriter(logs, writer), io.MultiWriter(logs, writer)
		if err := cmd.Start(); err != nil {
			return nil, logs, addresses, err
		}
		exit := commandCompletionFor(cmd)
		err := waitBrokerStartup(ctx, logs, addresses, exit)
		if err == nil {
			return cmd, logs, addresses, nil
		}
		stopStartedCommand(cmd)
		startedCommands.Delete(cmd)
		if ctx.Err() != nil {
			return nil, logs, addresses, ctx.Err()
		}
		if !bindCollision(logs.String()) || attempt == 3 {
			return nil, logs, addresses, fmt.Errorf("attempt %d: %w", attempt, err)
		}
		updated := next()
		if beforeRetry != nil {
			if err := beforeRetry(addresses, updated); err != nil {
				return nil, logs, addresses, err
			}
		}
		addresses = updated
	}
	panic("unreachable")
}

func waitBrokerStartup(ctx context.Context, logs *brokerLogBuffer, addresses brokerAddresses, exit *commandCompletion) error {
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	ready := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-exit.done:
			return fmt.Errorf("broker exited during startup: %v", exit.err)
		case <-deadline.C:
			return fmt.Errorf("broker startup timed out")
		case <-ticker.C:
			output := logs.String()
			if bindCollision(output) {
				return fmt.Errorf("broker port collision")
			}
			if strings.Contains(output, "metrics server error") || strings.Contains(output, "control server listen error") {
				return fmt.Errorf("broker auxiliary listener failed")
			}
			_, port, _ := net.SplitHostPort(addresses.kafka)
			if !strings.Contains(output, "broker listening on "+addresses.kafka) && !strings.Contains(output, "broker listening on [::]:"+port) && !strings.Contains(output, "broker listening on 0.0.0.0:"+port) {
				continue
			}
			listening := true
			for _, addr := range []string{addresses.kafka, addresses.metrics, addresses.control} {
				conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
				if err != nil {
					listening = false
					break
				}
				conn.Close()
			}
			// Require two successful polls: asynchronous auxiliary bind failures
			// must be observed even if the Kafka listener was ready first.
			if listening && ready {
				return nil
			}
			ready = listening
		}
	}
}

func splitCSVEndpoints(value string) []string {
	var endpoints []string
	for _, entry := range strings.Split(value, ",") {
		if entry = strings.TrimSpace(entry); entry != "" {
			endpoints = append(endpoints, entry)
		}
	}
	return endpoints
}

// Existing snapshots (including restart and multi-broker fixtures) must advertise
// the replacement port. CAS preserves concurrent changes by another fixture.
func moveSnapshotBrokerAddress(parent context.Context, endpoints []string, old, next string) error {
	if len(endpoints) == 0 || old == next {
		return nil
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 3 * time.Second})
	if err != nil {
		return err
	}
	defer client.Close()
	oldHost, oldPort, err := net.SplitHostPort(old)
	if err != nil {
		return err
	}
	host, portString, err := net.SplitHostPort(next)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		return err
	}
	const key = "/kafscale/metadata/snapshot"
	for attempt := 0; attempt < 5; attempt++ {
		response, err := client.Get(ctx, key)
		if err != nil {
			return err
		}
		if len(response.Kvs) == 0 {
			return nil
		}
		var snapshot metadata.ClusterMetadata
		if err := json.Unmarshal(response.Kvs[0].Value, &snapshot); err != nil {
			return err
		}
		changed := false
		for i := range snapshot.Brokers {
			broker := &snapshot.Brokers[i]
			if broker.Host == oldHost && strconv.Itoa(int(broker.Port)) == oldPort {
				broker.Host, broker.Port, changed = host, int32(port), true
			}
		}
		if !changed {
			return nil
		}
		data, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		txn, err := client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", response.Kvs[0].ModRevision)).Then(clientv3.OpPut(key, string(data))).Commit()
		if err != nil {
			return err
		}
		if txn.Succeeded {
			return nil
		}
	}
	return fmt.Errorf("metadata snapshot changed during broker port replacement")
}
