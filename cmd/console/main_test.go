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
	"net"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/etcd/server/v3/embed"

	"github.com/novatechflow/kafscale/pkg/metadata"
)

func TestBuildMetadataStoreUsesEtcdEnv(t *testing.T) {
	etcd, endpoints := startEmbeddedEtcd(t)
	defer etcd.Close()

	t.Setenv("KAFSCALE_CONSOLE_ETCD_ENDPOINTS", strings.Join(endpoints, ","))
	t.Setenv("KAFSCALE_CONSOLE_ETCD_USERNAME", "user")
	t.Setenv("KAFSCALE_CONSOLE_ETCD_PASSWORD", "pass")

	store, err := buildMetadataStore(context.Background())
	if err != nil {
		t.Fatalf("build metadata store: %v", err)
	}
	if _, ok := store.(*metadata.EtcdStore); !ok {
		t.Fatalf("expected etcd store when KAFSCALE_CONSOLE_ETCD_ENDPOINTS is set")
	}
}

func TestBuildMetricsProvider(t *testing.T) {
	t.Setenv("KAFSCALE_CONSOLE_BROKER_METRICS_URL", "")
	if got := buildMetricsProvider(nil); got != nil {
		t.Fatalf("expected nil metrics provider when env unset")
	}

	t.Setenv("KAFSCALE_CONSOLE_BROKER_METRICS_URL", "http://127.0.0.1:9093/metrics")
	if got := buildMetricsProvider(nil); got == nil {
		t.Fatalf("expected metrics provider when env set")
	}
}

func TestConsoleEtcdConfigFromEnv(t *testing.T) {
	t.Setenv("KAFSCALE_CONSOLE_ETCD_ENDPOINTS", "http://a:2379,http://b:2379")
	t.Setenv("KAFSCALE_CONSOLE_ETCD_USERNAME", "user")
	t.Setenv("KAFSCALE_CONSOLE_ETCD_PASSWORD", "pass")
	cfg, ok := consoleEtcdConfigFromEnv()
	if !ok {
		t.Fatalf("expected config to be enabled")
	}
	if len(cfg.Endpoints) != 2 || cfg.Endpoints[0] != "http://a:2379" || cfg.Endpoints[1] != "http://b:2379" {
		t.Fatalf("unexpected endpoints: %v", cfg.Endpoints)
	}
	if cfg.Username != "user" || cfg.Password != "pass" {
		t.Fatalf("unexpected credentials: %+v", cfg)
	}
}

func TestAuthConfigFromEnv(t *testing.T) {
	t.Setenv("KAFSCALE_UI_USERNAME", " user ")
	t.Setenv("KAFSCALE_UI_PASSWORD", " pass ")
	cfg := authConfigFromEnv()
	if cfg.Username != "user" || cfg.Password != "pass" {
		t.Fatalf("unexpected auth config: %+v", cfg)
	}
}

func TestEnvOrDefaultConsoleAddr(t *testing.T) {
	t.Setenv("KAFSCALE_CONSOLE_HTTP_ADDR", "127.0.0.1:9090")
	if got := envOrDefault("KAFSCALE_CONSOLE_HTTP_ADDR", ":8080"); got != "127.0.0.1:9090" {
		t.Fatalf("expected env override, got %q", got)
	}
}

func startEmbeddedEtcd(t *testing.T) (*embed.Etcd, []string) {
	t.Helper()
	if err := ensureEtcdPortsFree(); err != nil {
		t.Skipf("skipping embedded etcd test: %v", err)
	}
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.Logger = "zap"
	setEtcdPorts(t, cfg, "22379", "22380")

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("skipping embedded etcd test: %v", err)
		}
		t.Fatalf("start embedded etcd: %v", err)
	}
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		e.Server.Stop()
		t.Fatalf("etcd server took too long to start")
	}
	clientURL := e.Clients[0].Addr().String()
	return e, []string{fmt.Sprintf("http://%s", clientURL)}
}

func ensureEtcdPortsFree() error {
	if err := killProcessesOnPort("22379"); err != nil {
		return err
	}
	if err := killProcessesOnPort("22380"); err != nil {
		return err
	}
	if err := portAvailable("127.0.0.1:22379"); err != nil {
		return err
	}
	if err := portAvailable("127.0.0.1:22380"); err != nil {
		return err
	}
	return nil
}

func setEtcdPorts(t *testing.T, cfg *embed.Config, clientPort, peerPort string) {
	t.Helper()
	clientURL, err := url.Parse("http://127.0.0.1:" + clientPort)
	if err != nil {
		t.Fatalf("parse client url: %v", err)
	}
	peerURL, err := url.Parse("http://127.0.0.1:" + peerPort)
	if err != nil {
		t.Fatalf("parse peer url: %v", err)
	}
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.Name = "default"
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
}

func killProcessesOnPort(port string) error {
	out, err := exec.Command("lsof", "-nP", "-iTCP:"+port, "-sTCP:LISTEN", "-t").Output()
	if err != nil {
		return nil
	}
	pids := strings.Fields(string(out))
	for _, pidStr := range pids {
		pid, convErr := strconv.Atoi(strings.TrimSpace(pidStr))
		if convErr != nil {
			continue
		}
		_ = syscall.Kill(pid, syscall.SIGTERM)
		time.Sleep(100 * time.Millisecond)
		if alive := syscall.Kill(pid, 0); alive == nil {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	return nil
}

func portAvailable(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("port %s already in use", addr)
	}
	_ = ln.Close()
	return nil
}
