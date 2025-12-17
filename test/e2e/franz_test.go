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
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	testS3Bucket    = "kafscale"
	testS3Region    = "us-east-1"
	testS3Endpoint  = "http://127.0.0.1:9000"
	testS3PathStyle = "true"
	testS3AccessKey = "minioadmin"
	testS3SecretKey = "minioadmin"
)

func TestFranzGoProduceConsume(t *testing.T) {
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	brokerAddr := "127.0.0.1:39092"
	metricsAddr := "127.0.0.1:39093"
	controlAddr := "127.0.0.1:39094"

	brokerCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "broker"))
	brokerCmd.Env = append(os.Environ(),
		"KAFSCALE_LOG_LEVEL=debug",
		"KAFSCALE_TRACE_KAFKA=true",
		"KAFSCALE_AUTO_CREATE_TOPICS=true",
		"KAFSCALE_AUTO_CREATE_PARTITIONS=1",
		fmt.Sprintf("KAFSCALE_BROKER_ADDR=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_METRICS_ADDR=%s", metricsAddr),
		fmt.Sprintf("KAFSCALE_CONTROL_ADDR=%s", controlAddr),
		fmt.Sprintf("KAFSCALE_S3_BUCKET=%s", testS3Bucket),
		fmt.Sprintf("KAFSCALE_S3_REGION=%s", testS3Region),
		fmt.Sprintf("KAFSCALE_S3_ENDPOINT=%s", testS3Endpoint),
		fmt.Sprintf("KAFSCALE_S3_PATH_STYLE=%s", testS3PathStyle),
		fmt.Sprintf("KAFSCALE_S3_ACCESS_KEY=%s", testS3AccessKey),
		fmt.Sprintf("KAFSCALE_S3_SECRET_KEY=%s", testS3SecretKey),
	)
	var brokerLogs bytes.Buffer
	var franzLogs bytes.Buffer
	logWriter := io.MultiWriter(&brokerLogs, os.Stdout, mustLogFile(t, "broker.log"))
	franzLogWriter := io.MultiWriter(&franzLogs, os.Stdout, mustLogFile(t, "franz.log"))
	newFranzLogger := func(component string) kgo.Logger {
		return kgo.BasicLogger(franzLogWriter, kgo.LogLevelDebug, func() string {
			return fmt.Sprintf("franz/%s ", component)
		})
	}
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
	waitForBroker(t, &brokerLogs, brokerAddr)

	const topic = "orders"
	t.Log("creating franz-go producer")
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableIdempotentWrite(),
		kgo.WithLogger(newFranzLogger("producer")),
	)
	if err != nil {
		t.Fatalf("create producer: %v\nbroker logs:\n%s\nfranz logs:\n%s", err, brokerLogs.String(), franzLogs.String())
	}
	defer producer.Close()

	for i := 0; i < 5; i++ {
		t.Logf("producing record %d", i)
		value := []byte(fmt.Sprintf("msg-%d", i))
		res := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: value})
		if err := res.FirstErr(); err != nil {
			t.Fatalf("produce %d failed: %v\nbroker logs:\n%s\nfranz logs:\n%s", i, err, brokerLogs.String(), franzLogs.String())
		}
		t.Logf("produce %d acked", i)
	}

	t.Log("creating franz-go consumer")
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.ConsumerGroup("franz-e2e-consumer"),
		kgo.ConsumeTopics(topic),
		kgo.BlockRebalanceOnPoll(),
		kgo.WithLogger(newFranzLogger("consumer")),
	)
	if err != nil {
		t.Fatalf("create consumer: %v\nbroker logs:\n%s\nfranz logs:\n%s", err, brokerLogs.String(), franzLogs.String())
	}
	defer consumer.Close()

	received := make(map[string]struct{})
	deadline := time.Now().Add(8 * time.Second)

	for len(received) < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for records (got %d). broker logs:\n%s\nfranz logs:\n%s", len(received), brokerLogs.String(), franzLogs.String())
		}
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %+v\nbroker logs:\n%s\nfranz logs:\n%s", errs, brokerLogs.String(), franzLogs.String())
		}
		fetches.EachRecord(func(record *kgo.Record) {
			t.Logf("consumed %s", record.Value)
			received[string(record.Value)] = struct{}{}
		})
	}
	t.Log("franz-go produce/consume round trip succeeded")
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("determine repo root: %v", err)
	}
	return root
}

func mustLogFile(t *testing.T, name string) io.Writer {
	t.Helper()
	dir := filepath.Join(repoRoot(t), "test", "e2e", "logs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create log dir: %v", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("%s-%d.log", strings.TrimSuffix(name, ".log"), time.Now().UnixNano()))
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create log file: %v", err)
	}
	t.Logf("streaming broker logs to %s", path)
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func waitForPort(t *testing.T, addr string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		select {
		case <-deadline:
			t.Fatalf("broker did not start listening on %s: %v", addr, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func waitForBroker(t *testing.T, logs *bytes.Buffer, addr string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	msg := fmt.Sprintf("broker listening on %s", addr)
	for {
		if strings.Contains(logs.String(), msg) {
			waitForPort(t, addr)
			return
		}
		select {
		case <-deadline:
			t.Fatalf("broker failed to start: logs:\n%s", logs.String())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func runCmdGetOutput(t *testing.T, ctx context.Context, name string, args ...string) []byte {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("command %s %s failed: %v\n%s", name, strings.Join(args, " "), err, buf.String())
	}
	return buf.Bytes()
}
