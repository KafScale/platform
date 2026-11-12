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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func etcdOutageProxy(t *testing.T) (*proxy, <-chan struct{}) {
	t.Helper()
	endpoints := testutil.StartEmbeddedEtcd(t)
	addr, done := ackingBackend(t, "orders")
	putProxyTestSnapshot(t, endpoints, metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{brokerAt(t, 0, addr)},
		Topics:  []protocol.MetadataTopic{{Topic: kmsg.StringPtr("orders"), Partitions: []protocol.MetadataPartition{{Partition: 0, Leader: 0}}}},
	})
	ctx := context.Background()
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("NewEtcdStore: %v", err)
	}
	p := lfsTestProxy(store, time.Minute)
	if backends, err := p.currentBackends(ctx); err != nil || len(backends) != 1 || backends[0] != addr {
		t.Fatalf("discovery with etcd up: %v %v", backends, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close etcd client: %v", err)
	}
	return p, done
}

func TestEtcdOutageWithStaleCacheIsNoBackend(t *testing.T) {
	p, _ := etcdOutageProxy(t)
	ctx := context.Background()
	atomic.StoreInt64(&p.lastHealthy, time.Now().Add(-2*time.Minute).UnixNano())
	if backends, err := p.currentBackends(ctx); err == nil {
		t.Errorf("discovery succeeded with etcd down: %v", backends)
	}
	p.refreshMetadataCache(ctx)
	if p.haveBackend(ctx) {
		t.Error("readiness found a backend from a stale cache with etcd down")
	}
	m, fs3 := lfsTestModuleFor(t, p)
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, lfsProduceHTTPRequest("orders"))
	if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "backend_unavailable") {
		t.Errorf("produce: %d %s", rr.Code, rr.Body.String())
	}
	rr = httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, lfsUploadInitHTTPRequest("orders"))
	if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "backend_unavailable") {
		t.Errorf("upload init: %d %s", rr.Code, rr.Body.String())
	}
	if fs3.puts != 0 || fs3.multipartInit != 0 {
		t.Errorf("uploaded with etcd down and a stale cache: puts=%d multipart=%d", fs3.puts, fs3.multipartInit)
	}
	if p.cacheFresh() {
		t.Error("a failed etcd refresh made the backend cache fresh again")
	}
}

func TestEtcdBlipWithFreshCacheStillServes(t *testing.T) {
	p, done := etcdOutageProxy(t)
	ctx := context.Background()
	p.touchHealthy()
	before := atomic.LoadInt64(&p.lastHealthy)
	if _, err := p.currentBackends(ctx); err == nil {
		t.Error("discovery succeeded with etcd down")
	}
	if atomic.LoadInt64(&p.lastHealthy) != before {
		t.Error("a failed etcd refresh moved lastHealthy")
	}
	if !p.haveBackend(ctx) {
		t.Fatal("readiness lost the backend during an etcd blip with a fresh cache")
	}
	conn, addr, err := p.connectBackend(ctx)
	if err != nil {
		t.Fatalf("Kafka listener could not reach a broker from the fresh cache: %v", err)
	}
	_ = conn.Close()
	if addr == "" {
		t.Fatal("no broker address")
	}
	m, fs3 := lfsTestModuleFor(t, p)
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, lfsProduceHTTPRequest("orders"))
	waitDone(t, done)
	if rr.Code != http.StatusOK {
		t.Fatalf("produce during an etcd blip: %d %s", rr.Code, rr.Body.String())
	}
	if fs3.puts != 1 {
		t.Fatalf("puts=%d", fs3.puts)
	}
}
