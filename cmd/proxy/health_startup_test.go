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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/internal/testutil"
	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/protocol"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const proxyHelperEnv = "KAFSCALE_PROXY_TEST_RUN_MAIN"

func TestProxyMainHelperProcess(t *testing.T) {
	if os.Getenv(proxyHelperEnv) != "1" {
		t.Skip("helper process for TestProxyHealthServesWhileEtcdBlocksStartup")
	}
	main()
}

type heldEtcdGate struct {
	ln      net.Listener
	target  string
	release chan struct{}
	mu      sync.Mutex
	conns   []net.Conn
}

func startHeldEtcdGate(t *testing.T, target string) *heldEtcdGate {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &heldEtcdGate{ln: ln, target: target, release: make(chan struct{})}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			g.mu.Lock()
			g.conns = append(g.conns, c)
			g.mu.Unlock()
			go g.serve(c)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		g.mu.Lock()
		for _, c := range g.conns {
			_ = c.Close()
		}
		g.mu.Unlock()
	})
	return g
}

func (g *heldEtcdGate) serve(c net.Conn) {
	<-g.release
	upstream, err := net.Dial("tcp", g.target)
	if err != nil {
		_ = c.Close()
		return
	}
	g.mu.Lock()
	g.conns = append(g.conns, upstream)
	g.mu.Unlock()
	go func() {
		_, _ = io.Copy(upstream, c)
		_ = upstream.Close()
	}()
	_, _ = io.Copy(c, upstream)
	_ = c.Close()
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func healthStatus(client *http.Client, url string) int {
	resp, err := client.Get(url)
	if err != nil {
		return 0
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

func waitForHealthStatus(t *testing.T, client *http.Client, url string, want int, timeout time.Duration, logs *lockedBuffer) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	got := 0
	for time.Now().Before(deadline) {
		got = healthStatus(client, url)
		if got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s: want status %d within %s, last status %d\nproxy output:\n%s", url, want, timeout, got, logs.snapshot().String())
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) snapshot() *bytes.Buffer {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.NewBuffer(append([]byte(nil), b.buf.Bytes()...))
}

func TestProxyHealthServesWhileEtcdBlocksStartup(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	etcdAddr := strings.TrimPrefix(endpoints[0], "http://")

	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()
	snapshot := metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 0, Host: "127.0.0.1", Port: 19092}},
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Put(ctx, "/kafscale/metadata/snapshot", string(raw)); err != nil {
		t.Fatal(err)
	}

	gate := startHeldEtcdGate(t, etcdAddr)
	proxyAddr := freeTCPAddr(t)
	healthAddr := freeTCPAddr(t)

	cmd := exec.Command(os.Args[0], "-test.run=^TestProxyMainHelperProcess$")
	cmd.Env = append(os.Environ(),
		proxyHelperEnv+"=1",
		"KAFSCALE_PROXY_ETCD_ENDPOINTS=http://"+gate.ln.Addr().String(),
		"KAFSCALE_PROXY_ADDR="+proxyAddr,
		"KAFSCALE_PROXY_HEALTH_ADDR="+healthAddr,
		"KAFSCALE_PROXY_ADVERTISED_HOST=127.0.0.1",
		"KAFSCALE_PROXY_READYZ_FETCH_PROBE=false",
		"KAFSCALE_PROXY_BACKENDS=",
		"KAFSCALE_PROXY_LFS_ENABLED=false",
	)
	logs := &lockedBuffer{}
	cmd.Stdout = logs
	cmd.Stderr = logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})

	client := &http.Client{Timeout: 2 * time.Second}
	livez := "http://" + healthAddr + "/livez"
	readyz := "http://" + healthAddr + "/readyz"

	waitForHealthStatus(t, client, livez, http.StatusOK, 3*time.Second, logs)
	if got := healthStatus(client, readyz); got != http.StatusServiceUnavailable {
		t.Fatalf("readyz while etcd blocks startup: want 503, got %d\nproxy output:\n%s", got, logs.snapshot().String())
	}
	if conn, err := net.DialTimeout("tcp", proxyAddr, 200*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Fatalf("kafka listener accepted connections before startup finished")
	}

	close(gate.release)

	waitForHealthStatus(t, client, readyz, http.StatusOK, 30*time.Second, logs)
	if got := healthStatus(client, livez); got != http.StatusOK {
		t.Fatalf("livez after startup: want 200, got %d", got)
	}
	conn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("kafka listener after ready: %v\nproxy output:\n%s", err, logs.snapshot().String())
	}
	_ = conn.Close()
}
