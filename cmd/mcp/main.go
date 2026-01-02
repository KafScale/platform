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
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	console "github.com/novatechflow/kafscale/internal/console"
	"github.com/novatechflow/kafscale/internal/mcpserver"
	"github.com/novatechflow/kafscale/pkg/metadata"
)

var version = "dev"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	addr := envOrDefault("KAFSCALE_MCP_HTTP_ADDR", ":8090")
	sessionTimeout, err := parseDurationEnv("KAFSCALE_MCP_SESSION_TIMEOUT")
	if err != nil {
		log.Fatalf("invalid KAFSCALE_MCP_SESSION_TIMEOUT: %v", err)
	}

	store, err := buildMetadataStore(ctx)
	if err != nil {
		log.Fatalf("metadata store init failed: %v", err)
	}
	if store == nil {
		log.Printf("warning: KAFSCALE_MCP_ETCD_ENDPOINTS not set; metadata tools will be unavailable")
	}
	metricsProvider := buildMetricsProvider()
	if metricsProvider == nil {
		log.Printf("warning: KAFSCALE_MCP_BROKER_METRICS_URL not set; metrics tool will be unavailable")
	}

	server := mcpserver.NewServer(mcpserver.Options{
		Store:   store,
		Metrics: metricsProvider,
		Logger:  log.Default(),
		Version: version,
	})

	slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var handler http.Handler = mcp.NewStreamableHTTPHandler(func(_ *http.Request) *mcp.Server {
		return server
	}, &mcp.StreamableHTTPOptions{
		SessionTimeout: sessionTimeout,
		Logger:         slogLogger,
	})

	if token := strings.TrimSpace(os.Getenv("KAFSCALE_MCP_AUTH_TOKEN")); token != "" {
		handler = mcpserver.RequireBearerToken(token, handler)
	} else {
		log.Printf("warning: KAFSCALE_MCP_AUTH_TOKEN not set; MCP server is unauthenticated")
	}

	mux := http.NewServeMux()
	mux.Handle("/mcp", handler)
	mux.Handle("/mcp/", handler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("mcp server listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("mcp server error: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Println("kafscale mcp shutting down")
}

func envOrDefault(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	warnDefaultEnv(key, fallback)
	return fallback
}

func parseDurationEnv(key string) (time.Duration, error) {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return 0, nil
	}
	return time.ParseDuration(val)
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
	cfg, ok := mcpEtcdConfigFromEnv()
	if !ok {
		return nil, nil
	}
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, cfg)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func buildMetricsProvider() console.MetricsProvider {
	metricsURL := strings.TrimSpace(os.Getenv("KAFSCALE_MCP_BROKER_METRICS_URL"))
	if metricsURL == "" {
		return nil
	}
	return console.NewPromMetricsClient(metricsURL)
}

func mcpEtcdConfigFromEnv() (metadata.EtcdStoreConfig, bool) {
	endpoints := strings.TrimSpace(os.Getenv("KAFSCALE_MCP_ETCD_ENDPOINTS"))
	if endpoints == "" {
		return metadata.EtcdStoreConfig{}, false
	}
	return metadata.EtcdStoreConfig{
		Endpoints: strings.Split(endpoints, ","),
		Username:  os.Getenv("KAFSCALE_MCP_ETCD_USERNAME"),
		Password:  os.Getenv("KAFSCALE_MCP_ETCD_PASSWORD"),
	}, true
}
