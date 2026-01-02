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
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	consolepkg "github.com/novatechflow/kafscale/internal/console"
	"github.com/novatechflow/kafscale/pkg/metadata"
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
	if metricsProvider := buildMetricsProvider(); metricsProvider != nil {
		opts.Metrics = metricsProvider
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

func buildMetricsProvider() consolepkg.MetricsProvider {
	metricsURL := strings.TrimSpace(os.Getenv("KAFSCALE_CONSOLE_BROKER_METRICS_URL"))
	if metricsURL == "" {
		return nil
	}
	return consolepkg.NewPromMetricsClient(metricsURL)
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
