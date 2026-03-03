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

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	consolepkg "github.com/KafScale/platform/internal/console"
	"github.com/KafScale/platform/pkg/metadata"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	addr := envOrDefault("KAFSCALE_CONSOLE_HTTP_ADDR", ":8080")
	store, err := buildMetadataStore(ctx)
	if err != nil {
		log.Fatalf("metadata store init failed: %v", err)
	}
	opts := consolepkg.ServerOptions{
		Store:  store,
		Logger: log.Default(),
		Auth:   authConfigFromEnv(),
	}
	if metricsProvider := buildMetricsProvider(store); metricsProvider != nil {
		opts.Metrics = metricsProvider
	}

	// Initialize LFS components if enabled
	if lfsHandlers, lfsConsumer := buildLFSComponents(ctx); lfsHandlers != nil {
		opts.LFSHandlers = lfsHandlers
		if lfsConsumer != nil {
			lfsConsumer.Start()
			defer lfsConsumer.Close()
		}
	}

	if err := consolepkg.StartServer(ctx, addr, opts); err != nil {
		log.Fatalf("console server failed: %v", err)
	}

	<-ctx.Done()
	log.Println("kafscale console shutting down")
}

func envOrDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	warnDefaultEnv(key, fallback)
	return fallback
}

func warnDefaultEnv(key, fallback string) {
	if !isProdEnv() {
		return
	}
	log.Printf("using default for unset env %s=%s", key, fallback)
}

func isProdEnv() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("KAFSCALE_ENV")), "prod")
}

func buildMetadataStore(ctx context.Context) (metadata.Store, error) {
	cfg, ok := consoleEtcdConfigFromEnv()
	if !ok {
		return nil, nil
	}
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, cfg)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func buildMetricsProvider(store metadata.Store) consolepkg.MetricsProvider {
	metricsURL := strings.TrimSpace(os.Getenv("KAFSCALE_CONSOLE_BROKER_METRICS_URL"))
	operatorURL := strings.TrimSpace(os.Getenv("KAFSCALE_CONSOLE_OPERATOR_METRICS_URL"))
	if metricsURL == "" && store == nil && operatorURL == "" {
		return nil
	}
	var brokerProvider consolepkg.MetricsProvider
	if store != nil {
		brokerProvider = consolepkg.NewAggregatedPromMetricsClient(store, metricsURL)
	} else if metricsURL != "" {
		brokerProvider = consolepkg.NewPromMetricsClient(metricsURL)
	}
	if operatorURL != "" {
		return consolepkg.NewCompositeMetricsProvider(brokerProvider, operatorURL)
	}
	return brokerProvider
}

func authConfigFromEnv() consolepkg.AuthConfig {
	return consolepkg.AuthConfig{
		Username: strings.TrimSpace(os.Getenv("KAFSCALE_UI_USERNAME")),
		Password: strings.TrimSpace(os.Getenv("KAFSCALE_UI_PASSWORD")),
	}
}

func consoleEtcdConfigFromEnv() (metadata.EtcdStoreConfig, bool) {
	endpoints := strings.TrimSpace(os.Getenv("KAFSCALE_CONSOLE_ETCD_ENDPOINTS"))
	if endpoints == "" {
		return metadata.EtcdStoreConfig{}, false
	}
	return metadata.EtcdStoreConfig{
		Endpoints: strings.Split(endpoints, ","),
		Username:  os.Getenv("KAFSCALE_CONSOLE_ETCD_USERNAME"),
		Password:  os.Getenv("KAFSCALE_CONSOLE_ETCD_PASSWORD"),
	}, true
}

func buildLFSComponents(ctx context.Context) (*consolepkg.LFSHandlers, *consolepkg.LFSConsumer) {
	enabled := strings.EqualFold(strings.TrimSpace(os.Getenv("KAFSCALE_CONSOLE_LFS_ENABLED")), "true")
	if !enabled {
		return nil, nil
	}

	// LFS configuration
	lfsCfg := consolepkg.LFSConfig{
		Enabled:      true,
		TrackerTopic: envOrDefault("KAFSCALE_LFS_TRACKER_TOPIC", "__lfs_ops_state"),
		KafkaBrokers: splitCSV(os.Getenv("KAFSCALE_CONSOLE_KAFKA_BROKERS")),
		S3Bucket:     strings.TrimSpace(os.Getenv("KAFSCALE_CONSOLE_LFS_S3_BUCKET")),
		S3Region:     strings.TrimSpace(os.Getenv("KAFSCALE_CONSOLE_LFS_S3_REGION")),
		S3Endpoint:   strings.TrimSpace(os.Getenv("KAFSCALE_CONSOLE_LFS_S3_ENDPOINT")),
		S3AccessKey:  strings.TrimSpace(os.Getenv("KAFSCALE_CONSOLE_LFS_S3_ACCESS_KEY")),
		S3SecretKey:  strings.TrimSpace(os.Getenv("KAFSCALE_CONSOLE_LFS_S3_SECRET_KEY")),
		PresignTTL:   300, // 5 minutes default
	}

	if ttl := strings.TrimSpace(os.Getenv("KAFSCALE_CONSOLE_LFS_S3_PRESIGN_TTL")); ttl != "" {
		if parsed, err := strconv.Atoi(ttl); err == nil && parsed > 0 {
			lfsCfg.PresignTTL = parsed
		}
	}

	// Create handlers
	handlers := consolepkg.NewLFSHandlers(lfsCfg, log.Default())

	// Create S3 client if configured
	if lfsCfg.S3Bucket != "" {
		s3Cfg := consolepkg.LFSS3Config{
			Bucket:         lfsCfg.S3Bucket,
			Region:         lfsCfg.S3Region,
			Endpoint:       lfsCfg.S3Endpoint,
			AccessKey:      lfsCfg.S3AccessKey,
			SecretKey:      lfsCfg.S3SecretKey,
			ForcePathStyle: lfsCfg.S3Endpoint != "",
		}
		if s3Client, err := consolepkg.NewLFSS3Client(ctx, s3Cfg, log.Default()); err == nil && s3Client != nil {
			handlers.SetS3Client(s3Client)
		} else if err != nil {
			log.Printf("lfs s3 client init failed: %v", err)
		}
	}

	// Create consumer if Kafka brokers configured
	var consumer *consolepkg.LFSConsumer
	if len(lfsCfg.KafkaBrokers) > 0 {
		consumerCfg := consolepkg.LFSConsumerConfig{
			Brokers: lfsCfg.KafkaBrokers,
			Topic:   lfsCfg.TrackerTopic,
			GroupID: "kafscale-console-lfs",
		}
		var err error
		consumer, err = consolepkg.NewLFSConsumer(ctx, consumerCfg, handlers, log.Default())
		if err != nil {
			log.Printf("lfs consumer init failed: %v", err)
		} else if consumer != nil {
			handlers.SetConsumer(consumer)
		}
	}

	log.Printf("lfs console components initialized: s3_bucket=%s tracker_topic=%s",
		lfsCfg.S3Bucket, lfsCfg.TrackerTopic)

	return handlers, consumer
}

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		val := strings.TrimSpace(part)
		if val != "" {
			out = append(out, val)
		}
	}
	return out
}
