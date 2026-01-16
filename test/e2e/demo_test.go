// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kafscalev1alpha1 "github.com/novatechflow/kafscale/api/v1alpha1"
	consolepkg "github.com/novatechflow/kafscale/internal/console"
	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/operator"
)

func TestDemoStack(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E") != "1" || !parseBoolEnv("KAFSCALE_E2E_DEMO") {
		t.Skip("demo mode disabled")
	}

	demoCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	e, endpoints := startEmbeddedEtcd(t)
	defer e.Close()

	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
			UID:       "demo-cluster",
		},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{},
			S3: kafscalev1alpha1.S3Spec{
				Bucket: envOrDefault("KAFSCALE_S3_BUCKET", "kafscale"),
				Region: envOrDefault("KAFSCALE_S3_REGION", "us-east-1"),
			},
			Etcd: kafscalev1alpha1.EtcdSpec{
				Endpoints: endpoints,
			},
		},
	}
	brokerAddr := "127.0.0.1:39092"
	metricsAddr := "127.0.0.1:39093"
	controlAddr := "127.0.0.1:39094"
	releasePort(t, brokerAddr)

	topicNames := demoTopicNames()
	meta := operator.BuildClusterMetadata(cluster, buildDemoTopics(topicNames, cluster.Name))
	overrideBrokerEndpoints(&meta, brokerAddr)
	if err := operator.PublishMetadataSnapshot(demoCtx, endpoints, meta); err != nil {
		t.Fatalf("publish snapshot: %v", err)
	}

	brokerCmd := exec.CommandContext(demoCtx, "go", "run", filepath.Join(repoRoot(t), "cmd", "broker"))
	configureProcessGroup(brokerCmd)
	brokerCmd.Env = append(os.Environ(),
		"KAFSCALE_AUTO_CREATE_TOPICS=true",
		"KAFSCALE_AUTO_CREATE_PARTITIONS=1",
		fmt.Sprintf("KAFSCALE_BROKER_ADDR=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_METRICS_ADDR=%s", metricsAddr),
		fmt.Sprintf("KAFSCALE_CONTROL_ADDR=%s", controlAddr),
		fmt.Sprintf("KAFSCALE_ETCD_ENDPOINTS=%s", strings.Join(endpoints, ",")),
	)
	var brokerLogs bytes.Buffer
	brokerCmd.Stdout = io.MultiWriter(&brokerLogs, mustLogFile(t, "demo-broker.log"))
	brokerCmd.Stderr = brokerCmd.Stdout
	if err := brokerCmd.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() {
		_ = signalProcessGroup(brokerCmd, os.Interrupt)
		_ = brokerCmd.Wait()
	})
	t.Log("waiting for broker readiness")
	waitForBroker(t, &brokerLogs, brokerAddr)

	store, err := metadata.NewEtcdStore(demoCtx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{
		Endpoints: endpoints,
	})
	if err != nil {
		t.Fatalf("init etcd store: %v", err)
	}
	consoleAddr := "127.0.0.1:48080"
	releasePort(t, consoleAddr)
	consoleLogger := log.New(io.Discard, "", 0)
	consoleUser := strings.TrimSpace(os.Getenv("KAFSCALE_UI_USERNAME"))
	consolePass := strings.TrimSpace(os.Getenv("KAFSCALE_UI_PASSWORD"))
	consoleOpts := consolepkg.ServerOptions{
		Store:  store,
		Logger: consoleLogger,
		Auth: consolepkg.AuthConfig{
			Username: consoleUser,
			Password: consolePass,
		},
	}
	brokerMetricsURL := fmt.Sprintf("http://%s/metrics", metricsAddr)
	operatorMetricsURL := strings.TrimSpace(os.Getenv("KAFSCALE_CONSOLE_OPERATOR_METRICS_URL"))
	brokerProvider := consolepkg.NewPromMetricsClient(brokerMetricsURL)
	if operatorMetricsURL != "" {
		consoleOpts.Metrics = consolepkg.NewCompositeMetricsProvider(brokerProvider, operatorMetricsURL)
	} else {
		consoleOpts.Metrics = brokerProvider
	}
	if err := consolepkg.StartServer(demoCtx, consoleAddr, consoleOpts); err != nil {
		t.Fatalf("start console: %v", err)
	}
	consoleURL := fmt.Sprintf("http://%s", consoleAddr)
	waitForHTTP(t, consoleURL+"/healthz", 5*time.Second)

	producerLog := mustLogFile(t, "demo-producer.log")
	producerLogger := log.New(io.MultiWriter(producerLog, os.Stdout), "", log.LstdFlags)

	go func() {
		var producer *kgo.Client
		var err error
		for retries := 0; retries < 5; retries++ {
			producer, err = kgo.NewClient(
				kgo.SeedBrokers(brokerAddr),
				kgo.AllowAutoTopicCreation(),
				kgo.WithLogger(kgo.BasicLogger(io.Discard, kgo.LogLevelWarn, nil)),
			)
			if err == nil {
				break
			}
			producerLogger.Printf("producer init failed (attempt %d): %v", retries+1, err)
			time.Sleep(time.Second)
		}
		if err != nil {
			producerLogger.Printf("producer init exhausted retries: %v", err)
			return
		}
		defer producer.Close()
		producerLogger.Printf("producer connected to %s", brokerAddr)
		producer.ForceMetadataRefresh()

		rate := parseEnvInt("KAFSCALE_DEMO_MESSAGES_PER_SEC", 5)
		if rate < 1 {
			rate = 1
		}
		interval := time.Second / time.Duration(rate)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-demoCtx.Done():
				return
			case <-ticker.C:
				msg := fmt.Sprintf("demo-%d", i)
				topic := topicNames[i%len(topicNames)]
				err := producer.ProduceSync(demoCtx, &kgo.Record{Topic: topic, Value: []byte(msg)}).FirstErr()
				if err != nil {
					producerLogger.Printf("produce error on %s: %v", topic, err)
					continue
				}
				i++
			}
		}
	}()

	maybeOpenConsoleUI(t, consoleURL+"/ui/")
	t.Logf("demo stack running at %s (broker=%s). Press Ctrl+C to stop.", consoleURL+"/ui/", brokerAddr)
	<-demoCtx.Done()
	t.Log("demo shut down requested")
}

func demoTopicNames() []string {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	count := rng.Intn(3) + 2 // 2-4 topics
	names := make([]string, count)
	for i := 0; i < count; i++ {
		names[i] = fmt.Sprintf("topic-%08x-%d", rng.Uint32(), i)
	}
	return names
}

func buildDemoTopics(names []string, clusterName string) []kafscalev1alpha1.KafscaleTopic {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	topics := make([]kafscalev1alpha1.KafscaleTopic, 0, len(names))
	for i, name := range names {
		partitions := int32(rng.Intn(3) + 1)
		if i == 0 && partitions < 3 {
			partitions = 3
		}
		topics = append(topics, kafscalev1alpha1.KafscaleTopic{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: kafscalev1alpha1.KafscaleTopicSpec{
				ClusterRef: clusterName,
				Partitions: partitions,
			},
		})
	}
	return topics
}

func overrideBrokerEndpoints(meta *metadata.ClusterMetadata, addr string) {
	if meta == nil {
		return
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
		portStr = "39092"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 39092
	}
	for i := range meta.Brokers {
		meta.Brokers[i].Host = host
		meta.Brokers[i].Port = int32(port)
	}
	if meta.ControllerID < 0 && len(meta.Brokers) > 0 {
		meta.ControllerID = meta.Brokers[0].NodeID
	}
}

func parseEnvInt(name string, fallback int) int {
	val := strings.TrimSpace(os.Getenv(name))
	if val == "" {
		return fallback
	}
	if parsed, err := strconv.Atoi(val); err == nil {
		return parsed
	}
	return fallback
}
