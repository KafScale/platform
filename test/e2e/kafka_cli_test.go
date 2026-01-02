// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

type kafkaCliHarness struct {
	ctx              context.Context
	brokerAddr       string
	brokerPort       string
	brokerListenAddr string
	brokerLogs       *bytes.Buffer
	image            string
	bootstrap        string
	producerCmd      string
	consumerCmd      string
}

func newKafkaCliHarness(t *testing.T) *kafkaCliHarness {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	requireBinaries(t, "docker")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	brokerPort := pickFreePort(t)
	brokerListenAddr := ":" + brokerPort
	brokerAddr := "127.0.0.1:" + brokerPort
	metricsAddr := "127.0.0.1:" + pickFreePort(t)
	controlAddr := "127.0.0.1:" + pickFreePort(t)
	advertisedHost := dockerAdvertisedHost(brokerAddr)

	brokerCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "broker"))
	brokerCmd.Env = append(os.Environ(),
		"KAFSCALE_AUTO_CREATE_TOPICS=true",
		"KAFSCALE_AUTO_CREATE_PARTITIONS=1",
		fmt.Sprintf("KAFSCALE_BROKER_ADDR=%s", brokerListenAddr),
		fmt.Sprintf("KAFSCALE_BROKER_HOST=%s", advertisedHost),
		fmt.Sprintf("KAFSCALE_METRICS_ADDR=%s", metricsAddr),
		fmt.Sprintf("KAFSCALE_CONTROL_ADDR=%s", controlAddr),
	)
	var brokerLogs bytes.Buffer
	debugLogs := parseBoolEnv("KAFSCALE_E2E_DEBUG")
	brokerWriterTargets := []io.Writer{&brokerLogs, mustLogFile(t, "broker.log")}
	if debugLogs {
		brokerWriterTargets = append(brokerWriterTargets, os.Stdout)
	}
	logWriter := io.MultiWriter(brokerWriterTargets...)
	brokerCmd.Stdout = logWriter
	brokerCmd.Stderr = logWriter
	if err := brokerCmd.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() {
		_ = brokerCmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() {
			_ = brokerCmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = brokerCmd.Process.Kill()
		}
	})

	t.Log("waiting for broker readiness")
	waitForBrokerReady(t, &brokerLogs, brokerListenAddr, brokerAddr)
	t.Logf("broker ready on %s", brokerAddr)

	image := envOrDefault("KAFSCALE_KAFKA_CLI_IMAGE", "apache/kafka:3.9.1")
	t.Logf("kafka cli image: %s", image)
	if err := exec.CommandContext(ctx, "docker", "image", "inspect", image).Run(); err != nil {
		t.Logf("docker image %s not found; pulling...", image)
		if pullErr := exec.CommandContext(ctx, "docker", "pull", image).Run(); pullErr != nil {
			t.Fatalf("docker image %s not found and pull failed: %v", image, pullErr)
		}
	}

	producerCmd := envOrDefault("KAFSCALE_KAFKA_CLI_CMD", "/opt/kafka/bin/kafka-console-producer.sh")
	consumerCmd := envOrDefault("KAFSCALE_KAFKA_CLI_CONSUMER_CMD", "/opt/kafka/bin/kafka-console-consumer.sh")
	bootstrap := envOrDefault("KAFSCALE_KAFKA_CLI_BOOTSTRAP", defaultDockerBootstrap("127.0.0.1:"+brokerPort))
	t.Logf("kafka cli bootstrap: %s", bootstrap)

	return &kafkaCliHarness{
		ctx:              ctx,
		brokerAddr:       brokerAddr,
		brokerPort:       brokerPort,
		brokerListenAddr: brokerListenAddr,
		brokerLogs:       &brokerLogs,
		image:            image,
		bootstrap:        bootstrap,
		producerCmd:      producerCmd,
		consumerCmd:      consumerCmd,
	}
}

func TestKafkaCliProduce(t *testing.T) {
	harness := newKafkaCliHarness(t)
	ctx := harness.ctx
	image := harness.image
	producerCmd := harness.producerCmd
	consumerCmd := harness.consumerCmd
	bootstrap := harness.bootstrap
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	topic := fmt.Sprintf("cli-%08x", rng.Uint32())
	messageCount := 10
	messages := make([]string, messageCount)
	quoted := make([]string, messageCount)
	for i := 0; i < messageCount; i++ {
		msg := fmt.Sprintf("cli-%08x-%02d", rng.Uint32(), i)
		messages[i] = msg
		quoted[i] = strconv.Quote(msg)
	}

	args := []string{"run", "--rm"}
	if runtime.GOOS == "linux" {
		args = append(args, "--network=host")
	}
	produceScript := fmt.Sprintf("printf '%%s\\n' %s | %s --bootstrap-server %s --topic %s --producer-property enable.idempotence=false", strings.Join(quoted, " "), producerCmd, bootstrap, topic)
	args = append(args, image, "sh", "-c", produceScript)
	t.Log("kafka cli: running producer")
	roundTripStart := time.Now()
	runCmdGetOutput(t, ctx, "docker", args...)
	produceDone := time.Now()

	consumeArgs := []string{"run", "--rm"}
	if runtime.GOOS == "linux" {
		consumeArgs = append(consumeArgs, "--network=host")
	}
	consumeScript := fmt.Sprintf("%s --bootstrap-server %s --topic %s --from-beginning --max-messages %d", consumerCmd, bootstrap, topic, messageCount)
	consumeArgs = append(consumeArgs, image, "sh", "-c", consumeScript)
	t.Log("kafka cli: running consumer")
	consumed := runCmdGetOutput(t, ctx, "docker", consumeArgs...)
	consumeDone := time.Now()
	expected := make(map[string]struct{}, messageCount)
	for _, msg := range messages {
		expected[msg] = struct{}{}
	}
	records := 0
	for _, line := range strings.Split(strings.TrimSpace(string(consumed)), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "Processed a total of") {
			continue
		}
		records++
		if _, ok := expected[trimmed]; !ok {
			t.Fatalf("unexpected kafka CLI record %q\n%s", trimmed, string(consumed))
		}
		delete(expected, trimmed)
	}
	if records != messageCount {
		t.Fatalf("expected %d records from kafka CLI consumer, got %d\n%s", messageCount, records, string(consumed))
	}
	if len(expected) != 0 {
		missing := make([]string, 0, len(expected))
		for key := range expected {
			missing = append(missing, key)
		}
		t.Fatalf("missing kafka CLI records: %s\n%s", strings.Join(missing, ", "), string(consumed))
	}

	bucket := envOrDefault("KAFSCALE_S3_BUCKET", "kafscale")
	t.Logf("kafka CLI produce/consume round trip succeeded in %s (s3 bucket=%s)", formatDuration(time.Since(roundTripStart)), bucket)
	var table strings.Builder
	fmt.Fprintf(&table, "%-30s %-12s %-12s %-12s\n", "message", "topic", "produced", "consumed")
	for _, msg := range messages {
		fmt.Fprintf(&table, "%-30s %-12s %-12s %-12s\n",
			msg,
			topic,
			formatOffset(roundTripStart, produceDone),
			formatOffset(roundTripStart, consumeDone),
		)
	}
	t.Log("\n" + table.String())
	printS3Layout(t, bucket, []string{topic})

	apiVersionsCmd := envOrDefault("KAFSCALE_KAFKA_API_VERSIONS_CMD", "/opt/kafka/bin/kafka-broker-api-versions.sh")
	apiArgs := []string{"run", "--rm"}
	if runtime.GOOS == "linux" {
		apiArgs = append(apiArgs, "--network=host")
	}
	apiScript := fmt.Sprintf("%s --bootstrap-server %s", apiVersionsCmd, bootstrap)
	apiArgs = append(apiArgs, image, "sh", "-c", apiScript)
	t.Log("kafka cli: checking api versions")
	apiOutput := runCmdGetOutput(t, ctx, "docker", apiArgs...)
	apiMax, ok := parseApiVersionMax(string(apiOutput), "ApiVersions")
	if !ok {
		t.Fatalf("api versions output missing ApiVersions entry:\n%s", string(apiOutput))
	}
	if apiMax < 3 {
		t.Fatalf("expected ApiVersions max >= 3, got %d\n%s", apiMax, string(apiOutput))
	}
}

func TestKafkaCliAdminTopics(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	requireBinaries(t, "docker")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokerPort := pickFreePort(t)
	brokerListenAddr := ":" + brokerPort
	brokerAddr := "127.0.0.1:" + brokerPort
	metricsAddr := "127.0.0.1:" + pickFreePort(t)
	controlAddr := "127.0.0.1:" + pickFreePort(t)
	advertisedHost := dockerAdvertisedHost(brokerAddr)

	brokerCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "broker"))
	brokerCmd.Env = append(os.Environ(),
		"KAFSCALE_AUTO_CREATE_TOPICS=false",
		"KAFSCALE_AUTO_CREATE_PARTITIONS=1",
		"KAFSCALE_ALLOW_ADMIN_APIS=true",
		fmt.Sprintf("KAFSCALE_BROKER_ADDR=%s", brokerListenAddr),
		fmt.Sprintf("KAFSCALE_BROKER_HOST=%s", advertisedHost),
		fmt.Sprintf("KAFSCALE_METRICS_ADDR=%s", metricsAddr),
		fmt.Sprintf("KAFSCALE_CONTROL_ADDR=%s", controlAddr),
	)
	var brokerLogs bytes.Buffer
	debugLogs := parseBoolEnv("KAFSCALE_E2E_DEBUG")
	brokerWriterTargets := []io.Writer{&brokerLogs, mustLogFile(t, "broker.log")}
	if debugLogs {
		brokerWriterTargets = append(brokerWriterTargets, os.Stdout)
	}
	logWriter := io.MultiWriter(brokerWriterTargets...)
	brokerCmd.Stdout = logWriter
	brokerCmd.Stderr = logWriter
	if err := brokerCmd.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() {
		_ = brokerCmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() {
			_ = brokerCmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = brokerCmd.Process.Kill()
		}
	})

	t.Log("waiting for broker readiness")
	waitForBrokerReady(t, &brokerLogs, brokerListenAddr, brokerAddr)
	t.Logf("broker ready on %s", brokerAddr)

	image := envOrDefault("KAFSCALE_KAFKA_CLI_IMAGE", "apache/kafka:3.9.1")
	t.Logf("kafka cli image: %s", image)
	if err := exec.CommandContext(ctx, "docker", "image", "inspect", image).Run(); err != nil {
		t.Logf("docker image %s not found; pulling...", image)
		if pullErr := exec.CommandContext(ctx, "docker", "pull", image).Run(); pullErr != nil {
			t.Fatalf("docker image %s not found and pull failed: %v", image, pullErr)
		}
	}

	topicsCmd := envOrDefault("KAFSCALE_KAFKA_TOPICS_CMD", "/opt/kafka/bin/kafka-topics.sh")
	bootstrap := envOrDefault("KAFSCALE_KAFKA_CLI_BOOTSTRAP", defaultDockerBootstrap("127.0.0.1:"+brokerPort))
	topic := fmt.Sprintf("admin-%08x", rand.New(rand.NewSource(time.Now().UnixNano())).Uint32())

	runArgs := []string{"run", "--rm"}
	if runtime.GOOS == "linux" {
		runArgs = append(runArgs, "--network=host")
	}

	createScript := fmt.Sprintf("%s --bootstrap-server %s --create --topic %s --partitions 1 --replication-factor 1", topicsCmd, bootstrap, topic)
	t.Log("kafka cli: creating topic")
	runCmdGetOutput(t, ctx, "docker", append(runArgs, image, "sh", "-c", createScript)...)

	listScript := fmt.Sprintf("%s --bootstrap-server %s --list", topicsCmd, bootstrap)
	listOutput := runCmdGetOutput(t, ctx, "docker", append(runArgs, image, "sh", "-c", listScript)...)
	if !strings.Contains(string(listOutput), topic) {
		t.Fatalf("expected topic %s in list output:\n%s", topic, string(listOutput))
	}

	deleteScript := fmt.Sprintf("%s --bootstrap-server %s --delete --topic %s", topicsCmd, bootstrap, topic)
	t.Log("kafka cli: deleting topic")
	runCmdGetOutput(t, ctx, "docker", append(runArgs, image, "sh", "-c", deleteScript)...)

	listOutput = runCmdGetOutput(t, ctx, "docker", append(runArgs, image, "sh", "-c", listScript)...)
	if strings.Contains(string(listOutput), topic) {
		t.Fatalf("expected topic %s to be deleted:\n%s", topic, string(listOutput))
	}
}

func TestKafkaCliListOffsets(t *testing.T) {
	harness := newKafkaCliHarness(t)
	ctx := harness.ctx
	image := harness.image
	producerCmd := harness.producerCmd
	bootstrap := harness.bootstrap

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	topic := fmt.Sprintf("cli-offsets-%08x", rng.Uint32())
	messageCount := 3
	quoted := make([]string, messageCount)
	for i := 0; i < messageCount; i++ {
		quoted[i] = strconv.Quote(fmt.Sprintf("offset-%08x-%02d", rng.Uint32(), i))
	}

	args := []string{"run", "--rm"}
	if runtime.GOOS == "linux" {
		args = append(args, "--network=host")
	}
	produceScript := fmt.Sprintf("printf '%%s\\n' %s | %s --bootstrap-server %s --topic %s --producer-property enable.idempotence=false", strings.Join(quoted, " "), producerCmd, bootstrap, topic)
	args = append(args, image, "sh", "-c", produceScript)
	t.Log("kafka cli: producing messages for list offsets")
	runCmdGetOutput(t, ctx, "docker", args...)

	offsetCmd := envOrDefault("KAFSCALE_KAFKA_GET_OFFSETS_CMD", "/opt/kafka/bin/kafka-get-offsets.sh")
	offsetScript := fmt.Sprintf("%s --bootstrap-server %s --topic %s --time -1", offsetCmd, bootstrap, topic)
	offsetArgs := []string{"run", "--rm"}
	if runtime.GOOS == "linux" {
		offsetArgs = append(offsetArgs, "--network=host")
	}
	offsetArgs = append(offsetArgs, image, "sh", "-c", offsetScript)
	t.Log("kafka cli: fetching latest offsets")
	output := strings.TrimSpace(string(runCmdGetOutput(t, ctx, "docker", offsetArgs...)))
	if output == "" {
		t.Fatalf("expected offset output, got empty response")
	}

	found := false
	for _, line := range strings.Split(output, "\n") {
		parts := strings.Split(strings.TrimSpace(line), ":")
		if len(parts) != 3 {
			continue
		}
		if parts[0] != topic {
			continue
		}
		offset, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			t.Fatalf("parse offset from %q: %v", line, err)
		}
		if offset != int64(messageCount) {
			t.Fatalf("expected latest offset %d got %d (line %q)", messageCount, offset, line)
		}
		found = true
	}
	if !found {
		t.Fatalf("no offsets for topic %s in output:\n%s", topic, output)
	}
}

func defaultDockerBootstrap(addr string) string {
	if runtime.GOOS == "linux" {
		return addr
	}
	host, port, found := strings.Cut(addr, ":")
	if !found || host == "" || port == "" {
		return "host.docker.internal:39092"
	}
	return "host.docker.internal:" + port
}

func dockerAdvertisedHost(addr string) string {
	if runtime.GOOS == "linux" {
		host, _, found := strings.Cut(addr, ":")
		if found && host != "" {
			return host
		}
		return "127.0.0.1"
	}
	return "host.docker.internal"
}

func waitForBrokerReady(t *testing.T, logs *bytes.Buffer, listenAddr, connectAddr string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	msg := fmt.Sprintf("broker listening on %s", listenAddr)
	altMsg := fmt.Sprintf("broker listening on [::]%s", strings.TrimPrefix(listenAddr, "127.0.0.1"))
	for {
		if strings.Contains(logs.String(), msg) || strings.Contains(logs.String(), altMsg) {
			waitForPort(t, connectAddr)
			return
		}
		select {
		case <-deadline:
			t.Fatalf("broker failed to start: logs:\n%s", logs.String())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func parseApiVersionMax(output, apiName string) (int, bool) {
	normalized := strings.ReplaceAll(output, "\r", "\n")
	for _, line := range strings.Split(normalized, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, apiName+"(") {
			continue
		}
		fields := strings.Fields(trimmed)
		for i := 0; i < len(fields)-1; i++ {
			if fields[i] != "to" {
				continue
			}
			maxVersion, err := strconv.Atoi(fields[i+1])
			if err != nil {
				return 0, false
			}
			return maxVersion, true
		}
	}
	return 0, false
}
