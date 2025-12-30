package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/config"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/processor"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/server"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config/config.yaml", "Path to processor config")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	runner, err := processor.New(cfg)
	if err != nil {
		log.Fatalf("failed to build processor: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metricsAddr := envOrDefault("KAFSCALE_METRICS_ADDR", ":9093")
	server.Start(ctx, metricsAddr)

	if err := runner.Run(ctx); err != nil {
		log.Fatalf("processor stopped with error: %v", err)
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
