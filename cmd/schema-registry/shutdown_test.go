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
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/internal/testutil"
	"github.com/KafScale/platform/v2/pkg/schemaregistry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func status(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

func TestServeDrainsOnSignal(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: testutil.StartEmbeddedEtcd(t), DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	reg, err := schemaregistry.New(schemaregistry.Config{Client: cli, Namespace: "default", InlineLimit: schemaregistry.DefaultInlineLimit})
	if err != nil {
		t.Fatal(err)
	}
	api := schemaregistry.NewServer(reg, 5*time.Second)
	started := make(chan struct{})
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /slow", func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		_, _ = io.WriteString(w, "done")
	})
	mux.Handle("/", api)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + ln.Addr().String()
	ctx, signal := context.WithCancel(context.Background())
	defer signal()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, ln, mux, api.SetDraining, 2*time.Second, 5*time.Second) }()

	if got := status(t, base+"/readyz"); got != http.StatusOK {
		t.Fatalf("readyz before the signal = %d", got)
	}
	slow := make(chan string, 1)
	go func() {
		resp, err := http.Get(base + "/slow")
		if err != nil {
			slow <- "error: " + err.Error()
			return
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		slow <- string(body)
	}()
	<-started
	signal()
	deadline := time.Now().Add(time.Second)
	for status(t, base+"/readyz") != http.StatusServiceUnavailable {
		if time.Now().After(deadline) {
			t.Fatal("readyz still passes after the signal")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := status(t, base+"/subjects"); got != http.StatusOK {
		t.Fatalf("request during the drain period = %d", got)
	}
	select {
	case err := <-done:
		t.Fatalf("serve returned during the drain period: %v", err)
	default:
	}
	close(release)
	if got := <-slow; got != "done" {
		t.Fatalf("in-flight request got %q", got)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return after the drain period")
	}
	if _, err := http.Get(base + "/readyz"); err == nil {
		t.Fatal("listener still accepts connections after shutdown")
	}
}

func TestServeShutdownTimeout(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /slow", func(http.ResponseWriter, *http.Request) {
		close(started)
		<-release
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, signal := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, ln, mux, func() {}, 0, 100*time.Millisecond) }()
	go func() {
		if resp, err := http.Get("http://" + ln.Addr().String() + "/slow"); err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-started
	signal()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("serve returned no error when a request outlived the shutdown timeout")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not give up after the shutdown timeout")
	}
}
