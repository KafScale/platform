//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFranzGoProduceConsume(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
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
		"KAFSCALE_AUTO_CREATE_TOPICS=true",
		"KAFSCALE_AUTO_CREATE_PARTITIONS=1",
		fmt.Sprintf("KAFSCALE_BROKER_ADDR=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_METRICS_ADDR=%s", metricsAddr),
		fmt.Sprintf("KAFSCALE_CONTROL_ADDR=%s", controlAddr),
	)
	var brokerLogs bytes.Buffer
	var franzLogs bytes.Buffer
	debugLogs := parseBoolEnv("KAFSCALE_E2E_DEBUG")
	brokerWriterTargets := []io.Writer{&brokerLogs, mustLogFile(t, "broker.log")}
	franzWriterTargets := []io.Writer{&franzLogs, mustLogFile(t, "franz.log")}
	if debugLogs {
		brokerWriterTargets = append(brokerWriterTargets, os.Stdout)
		franzWriterTargets = append(franzWriterTargets, os.Stdout)
	}
	logWriter := io.MultiWriter(brokerWriterTargets...)
	franzLogWriter := io.MultiWriter(franzWriterTargets...)
	traceKafka := parseBoolEnv("KAFSCALE_TRACE_KAFKA")
	newFranzLogger := func(component string) kgo.Logger {
		level := kgo.LogLevelWarn
		if traceKafka {
			level = kgo.LogLevelDebug
		}
		return kgo.BasicLogger(franzLogWriter, level, func() string {
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
	roundTripStart := time.Now()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	const (
		messageCount     = 10
		messagesPerTopic = 5
	)
	topicCount := (messageCount + messagesPerTopic - 1) / messagesPerTopic
	topics := make([]string, topicCount)
	topicPrefix := fmt.Sprintf("orders-%08x", rng.Uint32())
	for i := range topics {
		topics[i] = fmt.Sprintf("%s-%d", topicPrefix, i)
	}
	type messageInfo struct {
		value      string
		topic      string
		producedAt time.Time
		consumedAt time.Time
	}
	messages := make([]*messageInfo, messageCount)
	messageIndex := make(map[string]*messageInfo, messageCount)
	for i := 0; i < messageCount; i++ {
		topic := topics[i/messagesPerTopic]
		val := fmt.Sprintf("msg-%02d-%08x", i, rng.Uint32())
		info := &messageInfo{value: val, topic: topic}
		messages[i] = info
		messageIndex[val] = info
	}
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
	producerClosed := false
	defer func() {
		if !producerClosed {
			producer.Close()
		}
	}()

	for i, msg := range messages {
		t.Logf("producing record %d topic=%s value=%s", i, msg.topic, msg.value)
		res := producer.ProduceSync(ctx, &kgo.Record{Topic: msg.topic, Value: []byte(msg.value)})
		if err := res.FirstErr(); err != nil {
			t.Fatalf("produce %d failed: %v\nbroker logs:\n%s\nfranz logs:\n%s", i, err, brokerLogs.String(), franzLogs.String())
		}
		msg.producedAt = time.Now()
		t.Logf("produce %d acked", i)
	}

	t.Log("creating franz-go consumer")
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.ConsumerGroup("franz-e2e-consumer"),
		kgo.ConsumeTopics(topics...),
		kgo.BlockRebalanceOnPoll(),
		kgo.WithLogger(newFranzLogger("consumer")),
	)
	if err != nil {
		t.Fatalf("create consumer: %v\nbroker logs:\n%s\nfranz logs:\n%s", err, brokerLogs.String(), franzLogs.String())
	}
	consumerClosed := false
	defer func() {
		if !consumerClosed {
			consumer.CloseAllowingRebalance()
		}
	}()

	received := make(map[string]struct{})
	deadline := time.Now().Add(15 * time.Second)
	consumedAll := false

	for len(received) < messageCount {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for records (got %d). broker logs:\n%s\nfranz logs:\n%s", len(received), brokerLogs.String(), franzLogs.String())
		}
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %+v\nbroker logs:\n%s\nfranz logs:\n%s", errs, brokerLogs.String(), franzLogs.String())
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if consumedAll {
				return
			}
			val := string(record.Value)
			info, ok := messageIndex[val]
			if !ok {
				t.Fatalf("received unexpected record %q on topic %s", val, record.Topic)
			}
			info.consumedAt = time.Now()
			t.Logf("consumed %s from %s", record.Value, record.Topic)
			received[val] = struct{}{}
			if len(received) >= messageCount {
				consumedAll = true
			}
		})
		if consumedAll {
			break
		}
	}
	t.Logf("received %d/%d unique records", len(received), messageCount)
	bucket := envOrDefault("KAFSCALE_S3_BUCKET", "kafscale")
	closeWithTimeout(t, "consumer", func() { consumer.CloseAllowingRebalance() })
	consumerClosed = true
	closeWithTimeout(t, "producer", func() { producer.Close() })
	producerClosed = true

	totalDuration := time.Since(roundTripStart)
	t.Logf("franz-go produce/consume round trip succeeded in %s (s3 bucket=%s)", formatDuration(totalDuration), bucket)
	var table strings.Builder
	fmt.Fprintf(&table, "%-30s %-12s %-12s %-12s\n", "message", "topic", "produced", "consumed")
	for _, msg := range messages {
		fmt.Fprintf(&table, "%-30s %-12s %-12s %-12s\n",
			msg.value,
			msg.topic,
			formatOffset(roundTripStart, msg.producedAt),
			formatOffset(roundTripStart, msg.consumedAt),
		)
	}
	t.Log("\n" + table.String())
	printS3Layout(t, bucket, topics)
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

func parseBoolEnv(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func formatOffset(start, ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	return ts.Sub(start).Truncate(time.Millisecond).String()
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	return d.Truncate(time.Millisecond).String()
}

func closeWithTimeout(t *testing.T, name string, closeFn func()) {
	done := make(chan struct{})
	go func() {
		closeFn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s close timed out", name)
	}
}
func envOrDefault(name, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		return val
	}
	return fallback
}

func printS3Layout(t *testing.T, bucket string, topics []string) {
	if _, err := exec.LookPath("aws"); err != nil {
		t.Log("skipping S3 verification: aws CLI not found in PATH")
		return
	}
	endpoint := envOrDefault("KAFSCALE_S3_ENDPOINT", "http://127.0.0.1:9000")
	accessKey := envOrDefault("KAFSCALE_S3_ACCESS_KEY", "minioadmin")
	secretKey := envOrDefault("KAFSCALE_S3_SECRET_KEY", "minioadmin")
	t.Logf("verifying S3 objects in bucket %s", bucket)
	time.Sleep(2 * time.Second)
	for _, topic := range topics {
		prefix := fmt.Sprintf("%s/", topic)
		var out bytes.Buffer
		var lastErr error
		for attempt := 1; attempt <= 10; attempt++ {
			listCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			cmd := exec.CommandContext(listCtx, "aws",
				"--endpoint-url", endpoint,
				"s3", "ls", fmt.Sprintf("s3://%s/%s", bucket, prefix),
				"--recursive")
			cmd.Env = append(os.Environ(),
				"AWS_ACCESS_KEY_ID="+accessKey,
				"AWS_SECRET_ACCESS_KEY="+secretKey,
				"AWS_DEFAULT_REGION="+envOrDefault("KAFSCALE_S3_REGION", "us-east-1"),
				"AWS_EC2_METADATA_DISABLED=true",
			)
			out.Reset()
			cmd.Stdout = &out
			cmd.Stderr = &out
			lastErr = cmd.Run()
			cancel()
			if lastErr == nil && strings.TrimSpace(out.String()) != "" {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if lastErr != nil {
			t.Fatalf("s3 ls %s failed after retries: %v\n%s", prefix, lastErr, out.String())
		}
		t.Logf("objects under s3://%s/%s:\n%s", bucket, prefix, out.String())
	}
}
