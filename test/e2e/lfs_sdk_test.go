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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestLfsSDKKindE2E(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" || !parseBoolEnvLocal("KAFSCALE_E2E_KIND") {
		t.Skipf("set %s=1 and KAFSCALE_E2E_KIND=1 to run kind integration test", enableEnv)
	}

	brokerAddr := strings.TrimSpace(os.Getenv("KAFSCALE_E2E_BROKER_ADDR"))
	if brokerAddr == "" {
		t.Skip("KAFSCALE_E2E_BROKER_ADDR not set")
	}

	httpURL := lfsProxyHTTPURL(t)
	if httpURL == "" {
		t.Skip("LFS proxy HTTP URL not configured (set LFS_PROXY_HTTP_URL or LFS_PROXY_SERVICE_HOST)")
	}

	cfg, err := s3ConfigFromEnv()
	if err != nil {
		t.Skip(err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	topic := envOrDefaultLocal("LFS_DEMO_TOPIC", "lfs-demo-topic")
	payloadSize := ensureMinBlobSize(envOrDefaultLocal("LFS_DEMO_BLOB_SIZE", "2097152"))
	payload := buildPayload(payloadSize)
	checksum := sha256.Sum256(payload)
	checksumHex := hex.EncodeToString(checksum[:])

	producer := lfs.NewProducer(httpURL)
	result, err := producer.Produce(ctx, topic, fmt.Sprintf("sdk-e2e-%d", rand.Int63()), bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("produce via lfs proxy: %v", err)
	}
	if result.Envelope.SHA256 != "" && result.Envelope.SHA256 != checksumHex {
		t.Fatalf("checksum mismatch: expected %s got %s", checksumHex, result.Envelope.SHA256)
	}

	s3Client, err := lfs.NewS3Client(ctx, cfg)
	if err != nil {
		t.Fatalf("create s3 client: %v", err)
	}
	consumer := lfs.NewConsumer(s3Client)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(fmt.Sprintf("lfs-sdk-e2e-%d", rand.Int63())),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("create kafka client: %v", err)
	}
	t.Cleanup(client.Close)

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("poll fetches: %v", err)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			resolved, err := consumer.Unwrap(ctx, record.Value)
			if err != nil {
				t.Fatalf("unwrap lfs record: %v", err)
			}
			if !bytes.Equal(resolved, payload) {
				t.Fatalf("resolved payload mismatch: got %d bytes", len(resolved))
			}
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for LFS envelope on topic %s", topic)
}

func lfsProxyHTTPURL(t *testing.T) string {
	t.Helper()
	if url := strings.TrimSpace(os.Getenv("LFS_PROXY_HTTP_URL")); url != "" {
		return url
	}
	host := strings.TrimSpace(os.Getenv("LFS_PROXY_SERVICE_HOST"))
	if host == "" {
		return ""
	}
	port := envOrDefaultLocal("LFS_PROXY_HTTP_PORT", "8080")
	path := envOrDefaultLocal("LFS_PROXY_HTTP_PATH", "/lfs/produce")
	return fmt.Sprintf("http://%s:%s%s", host, port, path)
}

func s3ConfigFromEnv() (lfs.S3Config, error) {
	bucket := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_BUCKET"))
	region := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_REGION"))
	endpoint := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_ENDPOINT"))
	accessKey := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_ACCESS_KEY"))
	secretKey := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_S3_SECRET_KEY"))
	if bucket == "" || region == "" || endpoint == "" || accessKey == "" || secretKey == "" {
		return lfs.S3Config{}, fmt.Errorf("set KAFSCALE_LFS_PROXY_S3_BUCKET/REGION/ENDPOINT/ACCESS_KEY/SECRET_KEY")
	}
	forcePathStyle := parseBoolEnvLocal("KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE")
	return lfs.S3Config{
		Bucket:          bucket,
		Region:          region,
		Endpoint:        endpoint,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		ForcePathStyle:  forcePathStyle,
	}, nil
}

func ensureMinBlobSize(raw string) int {
	val, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || val <= 0 {
		return 2 * 1024 * 1024
	}
	if val < 1024*1024 {
		return 2 * 1024 * 1024
	}
	return val
}

func buildPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	return payload
}

func parseBoolEnvLocal(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func envOrDefaultLocal(name, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		return val
	}
	return fallback
}
