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

package metadata

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"

	"github.com/KafScale/platform/internal/testutil"
)

func TestEtcdStoreReconnectsQuicklyAfterOutage(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	backend := strings.TrimPrefix(endpoints[0], "http://")
	proxy := startToggleProxy(t, backend)

	ctx := context.Background()
	store, err := NewEtcdStore(ctx, ClusterMetadata{}, EtcdStoreConfig{
		Endpoints:   []string{"http://" + proxy.addr()},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewEtcdStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	putCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	_, err = store.EtcdClient().Put(putCtx, "/kafscale/reconnect-test", "ok")
	cancel()
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	proxy.setBlocked(true)
	outageUntil := time.Now().Add(20 * time.Second)
	for time.Now().Before(outageUntil) {
		failCtx, cancelFail := context.WithTimeout(ctx, 300*time.Millisecond)
		_, _ = store.EtcdClient().Get(failCtx, "/kafscale/reconnect-test")
		cancelFail()
		time.Sleep(50 * time.Millisecond)
	}
	proxy.setBlocked(false)

	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		getCtx, cancelGet := context.WithTimeout(ctx, 400*time.Millisecond)
		_, lastErr = store.EtcdClient().Get(getCtx, "/kafscale/reconnect-test")
		cancelGet()
		if lastErr == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("etcd did not recover within 3s after a 20s outage: %v", lastErr)
}

type toggleProxy struct {
	ln      net.Listener
	target  string
	mu      sync.Mutex
	blocked bool
	conns   []net.Conn
}

func startToggleProxy(t *testing.T, target string) *toggleProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &toggleProxy{ln: ln, target: target}
	go p.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return p
}

func (p *toggleProxy) addr() string {
	return p.ln.Addr().String()
}

func (p *toggleProxy) setBlocked(blocked bool) {
	p.mu.Lock()
	p.blocked = blocked
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	if blocked {
		for _, c := range conns {
			_ = c.Close()
		}
	}
}

func (p *toggleProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.blocked {
			p.mu.Unlock()
			_ = c.Close()
			continue
		}
		p.conns = append(p.conns, c)
		p.mu.Unlock()
		go p.pipe(c)
	}
}

func (p *toggleProxy) pipe(client net.Conn) {
	backend, err := net.DialTimeout("tcp", p.target, time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	p.conns = append(p.conns, backend)
	p.mu.Unlock()
	go func() {
		_, _ = io.Copy(backend, client)
		_ = backend.Close()
		_ = client.Close()
	}()
	_, _ = io.Copy(client, backend)
	_ = backend.Close()
	_ = client.Close()
}

func TestEtcdClientConfigSetsReconnectBackoff(t *testing.T) {
	cfg := EtcdClientConfig(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 4 * time.Second,
	})
	if cfg.DialTimeout != 4*time.Second {
		t.Fatalf("DialTimeout = %s, want 4s", cfg.DialTimeout)
	}
	if cfg.DialKeepAliveTime != 0 {
		t.Fatalf("DialKeepAliveTime = %s, want 0", cfg.DialKeepAliveTime)
	}
	if cfg.DialKeepAliveTimeout != 0 {
		t.Fatalf("DialKeepAliveTimeout = %s, want 0", cfg.DialKeepAliveTimeout)
	}
	if cfg.PermitWithoutStream {
		t.Fatal("PermitWithoutStream must stay false; etcd rejects idle pings")
	}
	if len(cfg.DialOptions) == 0 {
		t.Fatal("expected reconnect DialOptions")
	}
	if etcdConnectParams.Backoff.MaxDelay != etcdReconnectMaxDelay {
		t.Fatalf("MaxDelay = %s, want %s", etcdConnectParams.Backoff.MaxDelay, etcdReconnectMaxDelay)
	}
	if etcdConnectParams.Backoff.MaxDelay >= backoff.DefaultConfig.MaxDelay {
		t.Fatalf("MaxDelay %s is not shorter than the gRPC default %s", etcdConnectParams.Backoff.MaxDelay, backoff.DefaultConfig.MaxDelay)
	}
	if etcdConnectParams.MinConnectTimeout != etcdMinConnectTimeout {
		t.Fatalf("MinConnectTimeout = %s, want %s", etcdConnectParams.MinConnectTimeout, etcdMinConnectTimeout)
	}
}

func TestEtcdClientIdleConnectionSurvivesKeepaliveStrikeWindow(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	cfg := EtcdClientConfig(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 2 * time.Second,
	})
	cli, err := clientv3.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	ctx := context.Background()
	putCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	_, err = cli.Put(putCtx, "/kafscale/idle-keepalive", "ok")
	cancel()
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	conn := cli.ActiveConnection()
	if conn == nil {
		t.Fatal("missing active connection")
	}
	if got := conn.GetState(); got != connectivity.Ready {
		t.Fatalf("connection state %s, want READY", got)
	}

	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		state := conn.GetState()
		if state != connectivity.Ready {
			t.Fatalf("idle connection left READY after %s: %s", 25*time.Second-time.Until(deadline), state)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if cli.ActiveConnection() != conn {
		t.Fatal("idle client replaced its connection during the strike window")
	}

	getCtx, cancelGet := context.WithTimeout(ctx, 3*time.Second)
	resp, err := cli.Get(getCtx, "/kafscale/idle-keepalive")
	cancelGet()
	if err != nil {
		t.Fatalf("get after idle window: %v", err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "ok" {
		t.Fatalf("unexpected get result: %+v", resp.Kvs)
	}
	if conn.GetState() != connectivity.Ready {
		t.Fatalf("connection state after get %s, want READY", conn.GetState())
	}
}
