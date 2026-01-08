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

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	mode := strings.ToLower(envOrDefault("KAFSCALE_E2E_MODE", "produce"))
	brokerAddr := strings.TrimSpace(os.Getenv("KAFSCALE_E2E_BROKER_ADDR"))
	topic := strings.TrimSpace(os.Getenv("KAFSCALE_E2E_TOPIC"))
	count := parseEnvInt("KAFSCALE_E2E_COUNT", 1)
	timeout := time.Duration(parseEnvInt("KAFSCALE_E2E_TIMEOUT_SEC", 40)) * time.Second

	switch mode {
	case "produce":
		if brokerAddr == "" || topic == "" {
			log.Fatalf("KAFSCALE_E2E_BROKER_ADDR and KAFSCALE_E2E_TOPIC are required")
		}
		if count <= 0 {
			log.Fatalf("KAFSCALE_E2E_COUNT must be > 0")
		}
		client, err := kgo.NewClient(
			kgo.SeedBrokers(brokerAddr),
			kgo.AllowAutoTopicCreation(),
		)
		if err != nil {
			log.Fatalf("create producer client: %v", err)
		}
		defer client.Close()
		produceCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := produceMessages(produceCtx, client, topic, count); err != nil {
			log.Fatalf("produce: %v", err)
		}
		log.Printf("produced %d messages to %s", count, topic)
	case "consume":
		if brokerAddr == "" || topic == "" {
			log.Fatalf("KAFSCALE_E2E_BROKER_ADDR and KAFSCALE_E2E_TOPIC are required")
		}
		if count <= 0 {
			log.Fatalf("KAFSCALE_E2E_COUNT must be > 0")
		}
		partition := parseEnvInt32("KAFSCALE_E2E_PARTITION", -1)
		offsetOverride := parseEnvInt64("KAFSCALE_E2E_OFFSET", -1)
		var opts []kgo.Opt
		opts = append(opts, kgo.SeedBrokers(brokerAddr))
		if partition >= 0 {
			offset := kgo.NewOffset().AtStart()
			if offsetOverride >= 0 {
				offset = kgo.NewOffset().At(offsetOverride)
			}
			partitions := map[string]map[int32]kgo.Offset{
				topic: {partition: offset},
			}
			opts = append(opts, kgo.ConsumePartitions(partitions))
		} else {
			opts = append(opts,
				kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
		}
		client, err := kgo.NewClient(
			opts...,
		)
		if err != nil {
			log.Fatalf("create consumer client: %v", err)
		}
		defer client.Close()
		if err := consumeMessages(context.Background(), client, topic, count, timeout); err != nil {
			log.Fatalf("consume: %v", err)
		}
		log.Printf("consumed %d messages from %s", count, topic)
	case "metrics":
		metricsAddr := strings.TrimSpace(os.Getenv("KAFSCALE_E2E_METRICS_ADDR"))
		if metricsAddr == "" {
			log.Fatalf("KAFSCALE_E2E_METRICS_ADDR is required")
		}
		waitTimeout := time.Duration(parseEnvInt("KAFSCALE_E2E_METRICS_TIMEOUT_SEC", 60)) * time.Second
		if err := waitForS3Health(metricsAddr, waitTimeout); err != nil {
			log.Fatalf("metrics: %v", err)
		}
		log.Printf("s3 health is healthy via %s", metricsAddr)
	case "probe":
		addrs := parseCSVAddrs(os.Getenv("KAFSCALE_E2E_ADDRS"))
		if len(addrs) == 0 {
			log.Fatalf("KAFSCALE_E2E_ADDRS is required for probe mode")
		}
		retries := parseEnvInt("KAFSCALE_E2E_PROBE_RETRIES", 5)
		if retries < 1 {
			retries = 1
		}
		sleep := time.Duration(parseEnvInt("KAFSCALE_E2E_PROBE_SLEEP_MS", 200)) * time.Millisecond
		if err := probeAddrs(addrs, 2*time.Second, retries, sleep, dialAddr); err != nil {
			log.Fatalf("probe: %v", err)
		}
		log.Printf("probed %d addresses", len(addrs))
	default:
		log.Fatalf("unknown KAFSCALE_E2E_MODE %q", mode)
	}
}

func produceMessages(ctx context.Context, client *kgo.Client, topic string, count int) error {
	for i := 0; i < count; i++ {
		msg := fmt.Sprintf("restart-%d", i)
		res := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(msg)})
		if err := res.FirstErr(); err != nil {
			return err
		}
	}
	return nil
}

func consumeMessages(ctx context.Context, client *kgo.Client, topic string, count int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	received := 0
	for received < count {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %d messages (got %d)", count, received)
		}
		fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		fetches := client.PollFetches(fetchCtx)
		cancel()
		if errs := fetches.Errors(); len(errs) > 0 {
			if !allTransientFetchErrors(errs) {
				return fmt.Errorf("fetch errors: %+v", errs)
			}
			continue
		}
		fetches.EachRecord(func(record *kgo.Record) {
			received++
		})
	}
	return nil
}

func allTransientFetchErrors(errs []kgo.FetchError) bool {
	for _, fetchErr := range errs {
		err := fetchErr.Err
		if err == nil {
			continue
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			continue
		}
		if kerr.IsRetriable(err) {
			continue
		}
		return false
	}
	return true
}

type dialFunc func(addr string, timeout time.Duration) error

func dialAddr(addr string, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err
	}
	return conn.Close()
}

func probeAddrs(addrs []string, timeout time.Duration, retries int, sleep time.Duration, dial dialFunc) error {
	for _, addr := range addrs {
		var lastErr error
		for attempt := 0; attempt < retries; attempt++ {
			if err := dial(addr, timeout); err == nil {
				lastErr = nil
				break
			} else {
				lastErr = err
			}
			time.Sleep(sleep)
		}
		if lastErr != nil {
			return fmt.Errorf("probe %s failed: %w", addr, lastErr)
		}
	}
	return nil
}

func waitForS3Health(metricsAddr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	url := fmt.Sprintf("http://%s/metrics", metricsAddr)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr == nil && strings.Contains(string(body), `kafscale_s3_health_state{state="healthy"} 1`) {
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timed out waiting for s3 health metrics at %s", url)
}

func parseCSVAddrs(raw string) []string {
	parts := strings.Split(raw, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		addrs = append(addrs, part)
	}
	return addrs
}

func envOrDefault(name, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		return val
	}
	return fallback
}

func parseEnvInt(name string, fallback int) int {
	val := strings.TrimSpace(os.Getenv(name))
	if val == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseEnvInt32(name string, fallback int32) int32 {
	val := strings.TrimSpace(os.Getenv(name))
	if val == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return fallback
	}
	return int32(parsed)
}

func parseEnvInt64(name string, fallback int64) int64 {
	val := strings.TrimSpace(os.Getenv(name))
	if val == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
