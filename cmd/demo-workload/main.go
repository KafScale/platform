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
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	brokerAddr := strings.TrimSpace(envOrDefault("KAFSCALE_DEMO_BROKER_ADDR", "127.0.0.1:39092"))
	topics := parseTopics(envOrDefault("KAFSCALE_DEMO_TOPICS", "demo-topic-1,demo-topic-2"))
	if len(topics) == 0 {
		log.Fatal("no topics configured")
	}
	rate := parseRate(envOrDefault("KAFSCALE_DEMO_MESSAGES_PER_SEC", "5"))
	group := strings.TrimSpace(envOrDefault("KAFSCALE_DEMO_GROUP", "kafscale-demo"))

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("init client: %v", err)
	}
	defer client.Close()

	log.Printf("demo workload connected to %s (topics=%s, rate=%d msg/s)", brokerAddr, strings.Join(topics, ","), rate)

	go produceLoop(ctx, client, topics, rate)
	consumeLoop(ctx, client)
}

func produceLoop(ctx context.Context, client *kgo.Client, topics []string, rate int) {
	if rate < 1 {
		rate = 1
	}
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	i := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			topic := topics[i%len(topics)]
			msg := fmt.Sprintf("demo-%d", i)
			if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(msg)}).FirstErr(); err != nil {
				log.Printf("produce error on %s: %v", topic, err)
			} else {
				log.Printf("produced %s to %s", msg, topic)
			}
			i++
		}
	}
}

func consumeLoop(ctx context.Context, client *kgo.Client) {
	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				log.Printf("consume error on %s[%d]: %v", err.Topic, err.Partition, err.Err)
			}
			continue
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			log.Printf("consumed %s from %s[%d]@%d", string(rec.Value), rec.Topic, rec.Partition, rec.Offset)
		}
	}
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func parseTopics(raw string) []string {
	parts := strings.Split(raw, ",")
	topics := make([]string, 0, len(parts))
	for _, part := range parts {
		topic := strings.TrimSpace(part)
		if topic != "" {
			topics = append(topics, topic)
		}
	}
	return topics
}

func parseRate(raw string) int {
	rate := 0
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return 5
		}
		rate = rate*10 + int(ch-'0')
	}
	if rate < 1 {
		return 5
	}
	return rate
}
