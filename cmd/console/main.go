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
		Auth: consolepkg.AuthConfig{
			Username: strings.TrimSpace(os.Getenv("KAFSCALE_UI_USERNAME")),
			Password: strings.TrimSpace(os.Getenv("KAFSCALE_UI_PASSWORD")),
		},
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
	return fallback
}

func buildMetadataStore(ctx context.Context) (metadata.Store, error) {
	endpoints := strings.TrimSpace(os.Getenv("KAFSCALE_CONSOLE_ETCD_ENDPOINTS"))
	if endpoints == "" {
		return nil, nil
	}
	cfg := metadata.EtcdStoreConfig{
		Endpoints: strings.Split(endpoints, ","),
		Username:  os.Getenv("KAFSCALE_CONSOLE_ETCD_USERNAME"),
		Password:  os.Getenv("KAFSCALE_CONSOLE_ETCD_PASSWORD"),
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
