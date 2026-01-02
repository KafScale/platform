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
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
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
