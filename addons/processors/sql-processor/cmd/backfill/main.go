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
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/discovery"
)

func main() {
	configPath := flag.String("config", "config/config.yaml", "path to config yaml")
	mode := flag.String("mode", "all", "manifest|time-index|all")
	interval := flag.Duration("interval", 0, "repeat interval (0 for one-shot)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	rawCfg := cfg
	rawCfg.Manifest.Enabled = false
	rawCfg.TimeIndex.Enabled = false
	lister, err := discovery.New(rawCfg)
	if err != nil {
		log.Fatalf("init lister: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	buildManifest := strings.EqualFold(*mode, "manifest") || strings.EqualFold(*mode, "all")
	buildTimeIndex := strings.EqualFold(*mode, "time-index") || strings.EqualFold(*mode, "all")

	var manifestBuilder *discovery.ManifestBuilder
	if buildManifest {
		manifestBuilder, err = discovery.NewManifestBuilder(cfg, lister)
		if err != nil {
			log.Fatalf("init manifest builder: %v", err)
		}
	}

	var timeIndexBuilder *discovery.TimeIndexBuilder
	if buildTimeIndex {
		timeIndexBuilder, err = discovery.NewTimeIndexBuilder(cfg, lister)
		if err != nil {
			log.Fatalf("init time index builder: %v", err)
		}
	}

	if *interval <= 0 {
		runOnce(ctx, manifestBuilder, timeIndexBuilder)
		return
	}

	if manifestBuilder != nil {
		go manifestBuilder.Run(ctx, *interval)
	}
	if timeIndexBuilder != nil {
		go timeIndexBuilder.Run(ctx, *interval)
	}
	<-ctx.Done()
}

func runOnce(ctx context.Context, manifestBuilder *discovery.ManifestBuilder, timeIndexBuilder *discovery.TimeIndexBuilder) {
	if manifestBuilder != nil {
		if err := manifestBuilder.Build(ctx); err != nil {
			log.Fatalf("build manifest: %v", err)
		}
	}
	if timeIndexBuilder != nil {
		if err := timeIndexBuilder.Build(ctx); err != nil {
			log.Fatalf("build time index: %v", err)
		}
	}
	if _, ok := ctx.Deadline(); ok {
		time.Sleep(0)
	}
}
