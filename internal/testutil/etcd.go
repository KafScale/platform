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
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
)

var embeddedEtcdStartMu sync.Mutex

// StartEmbeddedEtcd launches an embedded etcd server on random ports and
// registers cleanup via t.Cleanup. Returns the endpoints (e.g. ["http://127.0.0.1:PORT"]).
func StartEmbeddedEtcd(t *testing.T) []string {
	t.Helper()

	embeddedEtcdStartMu.Lock()
	defer embeddedEtcdStartMu.Unlock()

	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 50 * time.Millisecond)
		}
		endpoints, err := startEmbeddedEtcdOnce(t)
		if err == nil {
			return endpoints
		}
		lastErr = err
		if !isEmbeddedEtcdBindError(err) {
			break
		}
	}
	if lastErr != nil && strings.Contains(lastErr.Error(), "operation not permitted") {
		t.Skipf("skipping: embedded etcd not permitted: %v", lastErr)
	}
	t.Fatalf("start embedded etcd: %v", lastErr)
	return nil
}

func startEmbeddedEtcdOnce(t *testing.T) ([]string, error) {
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
		return nil, err
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		e.Server.Stop()
		return nil, fmt.Errorf("embedded etcd took too long to start")
	}

	t.Cleanup(func() {
		e.Close()
	})

	clientURL := e.Clients[0].Addr().String()
	return []string{fmt.Sprintf("http://%s", clientURL)}, nil
}

func isEmbeddedEtcdBindError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "bind: address already in use") ||
		strings.Contains(msg, "address already in use")
}

func freeLocalPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate free port: %v", err)
	}
	defer func() { _ = ln.Close() }()
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
