//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFranzGoProduceConsume(t *testing.T) {
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}
	ensureBinary(t, "docker")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	minio := startMinio(t, ctx)
	t.Cleanup(func() { minio.Stop(t, context.Background()) })
	minio.ensureBucket(t, ctx, "kafscale-e2e")

	brokerAddr := "127.0.0.1:39092"
	metricsAddr := "127.0.0.1:39093"
	controlAddr := "127.0.0.1:39094"

	brokerCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "broker"))
	brokerCmd.Env = append(os.Environ(),
		"KAFSCALE_LOG_LEVEL=debug",
		"KAFSCALE_S3_BUCKET=kafscale-e2e",
		"KAFSCALE_S3_REGION=us-east-1",
		fmt.Sprintf("KAFSCALE_S3_ENDPOINT=%s", minio.Endpoint),
		"KAFSCALE_S3_PATH_STYLE=true",
		fmt.Sprintf("KAFSCALE_BROKER_ADDR=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_METRICS_ADDR=%s", metricsAddr),
		fmt.Sprintf("KAFSCALE_CONTROL_ADDR=%s", controlAddr),
	)
	var brokerLogs bytes.Buffer
	logWriter := io.MultiWriter(&brokerLogs, os.Stdout)
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
		case <-time.After(5 * time.Second):
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
		kgo.RequiredAcks(kgo.LeaderAck()),
	)
	if err != nil {
		t.Fatalf("create producer: %v\nlogs:\n%s", err, brokerLogs.String())
	}
	defer producer.Close()

	for i := 0; i < 5; i++ {
		t.Logf("producing record %d", i)
		value := []byte(fmt.Sprintf("msg-%d", i))
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: value}).FirstErr(); err != nil {
			t.Fatalf("produce %d failed: %v\nlogs:\n%s", i, err, brokerLogs.String())
		}
	}

	t.Log("creating franz-go consumer")
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.ConsumerGroup("franz-e2e-consumer"),
		kgo.ConsumeTopics(topic),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		t.Fatalf("create consumer: %v\nlogs:\n%s", err, brokerLogs.String())
	}
	defer consumer.Close()

	received := make(map[string]struct{})
	deadline := time.Now().Add(15 * time.Second)

	for len(received) < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for records (got %d). broker logs:\n%s", len(received), brokerLogs.String())
		}
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %+v\nlogs:\n%s", errs, brokerLogs.String())
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

type minioInstance struct {
	Endpoint string
	id       string
	managed  bool
}

func startMinio(t *testing.T, ctx context.Context) *minioInstance {
	t.Helper()
	if existing := findExistingMinio(t, ctx); existing != "" {
		return &minioInstance{
			Endpoint: "http://127.0.0.1:19000",
			id:       existing,
			managed:  false,
		}
	}
	name := fmt.Sprintf("kafscale-minio-%d", time.Now().UnixNano())
	args := []string{
		"run", "-d", "--rm",
		"-p", "19000:9000",
		"-e", "MINIO_ROOT_USER=minio",
		"-e", "MINIO_ROOT_PASSWORD=minio123",
		"--name", name,
		"minio/minio",
		"server", "/data", "--address", ":9000", "--console-address", ":9001",
	}
	out := runCmdGetOutput(t, ctx, "docker", args...)
	id := strings.TrimSpace(string(out))
	inst := &minioInstance{
		Endpoint: "http://127.0.0.1:19000",
		id:       id,
		managed:  true,
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		req, _ := http.NewRequestWithContext(waitCtx, http.MethodGet, inst.Endpoint+"/minio/health/ready", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			break
		}
		if waitCtx.Err() != nil {
			t.Fatalf("minio not ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	return inst
}

func (m *minioInstance) Stop(t *testing.T, ctx context.Context) {
	t.Helper()
	if m.id == "" || !m.managed {
		return
	}
	runCmdIgnoreErr(t, ctx, "docker", "stop", m.id)
}

func (m *minioInstance) ensureBucket(t *testing.T, ctx context.Context, bucket string) {
	t.Helper()
	creds := credentials.NewStaticCredentialsProvider("minio", "minio123", "")
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(creds),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(m.Endpoint)
		o.UsePathStyle = true
	})
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
		t.Fatalf("create bucket: %v", err)
	}
}

func waitForPort(t *testing.T, addr string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
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
	deadline := time.After(30 * time.Second)
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

func findExistingMinio(t *testing.T, ctx context.Context) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "docker", "ps", "-q", "--filter", "ancestor=minio/minio", "--filter", "name=kafscale-minio")
	output, err := cmd.Output()
	if err != nil {
		return ""
	}
	id := strings.TrimSpace(string(output))
	if id == "" {
		return ""
	}
	lines := strings.Split(id, "\n")
	if len(lines) > 0 {
		return strings.TrimSpace(lines[0])
	}
	return ""
}
