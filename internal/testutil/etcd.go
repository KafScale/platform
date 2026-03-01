// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package testutil

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
)

// StartEmbeddedEtcd launches an embedded etcd server on random ports and
// registers cleanup via t.Cleanup. Returns the endpoints (e.g. ["http://127.0.0.1:PORT"]).
func StartEmbeddedEtcd(t *testing.T) []string {
	t.Helper()

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Logger = "zap"
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{etcdLogPath(t)}

	clientPort := freeLocalPort(t)
	peerPort := freeLocalPort(t)
	cfg.ListenClientUrls = []url.URL{mustURL(t, fmt.Sprintf("http://127.0.0.1:%d", clientPort))}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.ListenPeerUrls = []url.URL{mustURL(t, fmt.Sprintf("http://127.0.0.1:%d", peerPort))}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("skipping: embedded etcd not permitted: %v", err)
		}
		t.Fatalf("start embedded etcd: %v", err)
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		e.Server.Stop()
		t.Fatalf("embedded etcd took too long to start")
	}

	t.Cleanup(func() {
		e.Close()
	})

	clientURL := e.Clients[0].Addr().String()
	return []string{fmt.Sprintf("http://%s", clientURL)}
}

func freeLocalPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate free port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func mustURL(t *testing.T, raw string) url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url %s: %v", raw, err)
	}
	return *parsed
}

func etcdLogPath(t *testing.T) string {
	t.Helper()
	dir := os.TempDir()
	return filepath.Join(dir, fmt.Sprintf("etcd-test-%d.log", time.Now().UnixNano()))
}
