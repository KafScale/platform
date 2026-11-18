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

package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc/backoff"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
)

func TestSnapshotResolverCache(t *testing.T) {
	inner := &stubResolver{topics: []string{"orders"}, partitions: map[string][]int32{"orders": {0, 1}}}
	resolver := NewSnapshotResolver(inner, 10*time.Second)
	ctx := context.Background()

	topics, err := resolver.Topics(ctx)
	if err != nil || len(topics) != 1 {
		t.Fatalf("expected topics, got %v err=%v", topics, err)
	}
	partitions, err := resolver.Partitions(ctx, "orders")
	if err != nil || len(partitions) != 2 {
		t.Fatalf("expected partitions, got %v err=%v", partitions, err)
	}
	if inner.calls == 0 {
		t.Fatalf("expected resolver calls")
	}
}

func TestEtcdResolverSnapshot(t *testing.T) {
	embedCfg := embed.NewConfig()
	embedCfg.Dir = t.TempDir()
	embedCfg.LogLevel = "error"
	embedCfg.Logger = "zap"
	embedCfg.LogOutputs = []string{filepath.Join(embedCfg.Dir, "etcd.log")}
	clientURL := mustLocalURL(t, freeLocalPort(t))
	peerURL := mustLocalURL(t, freeLocalPort(t))
	embedCfg.ListenClientUrls = []url.URL{clientURL}
	embedCfg.AdvertiseClientUrls = []url.URL{clientURL}
	embedCfg.ListenPeerUrls = []url.URL{peerURL}
	embedCfg.AdvertisePeerUrls = []url.URL{peerURL}
	embedCfg.InitialCluster = embedCfg.InitialClusterFromName(embedCfg.Name)
	e, err := embed.StartEtcd(embedCfg)
	if err != nil {
		t.Fatalf("start etcd: %v", err)
	}
	defer e.Close()

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(5 * time.Second):
		t.Fatalf("etcd start timeout")
	}

	endpoint := "http://" + e.Clients[0].Addr().String()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	defer cli.Close()

	payload := snapshotPayload{
		Topics: []snapshotTopic{
			{Name: "orders", Partitions: []snapshotPartition{{PartitionIndex: 0}, {PartitionIndex: 1}}},
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	_, err = cli.Put(context.Background(), "/kafscale/metadata/snapshot", string(data))
	if err != nil {
		t.Fatalf("put snapshot: %v", err)
	}

	cfg := config.Config{
		Metadata: config.MetaConfig{
			Discovery: "etcd",
			Etcd:      config.EtcdConfig{Endpoints: []string{endpoint}},
			Snapshot:  config.SnapshotConfig{Key: "/kafscale/metadata/snapshot"},
		},
	}
	resolver, err := NewResolver(cfg)
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	topics, err := resolver.Topics(context.Background())
	if err != nil {
		t.Fatalf("topics: %v", err)
	}
	if len(topics) != 1 || topics[0] != "orders" {
		t.Fatalf("unexpected topics: %v", topics)
	}
}

func TestSnapshotToMaps(t *testing.T) {
	snap := Snapshot{
		Topics: []Topic{
			{Name: "orders", Partitions: []int32{0, 1}},
			{Name: "payments", Partitions: []int32{2}},
		},
	}
	topics, partitions := snapshotToMaps(snap)
	if len(topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(topics))
	}
	if len(partitions["orders"]) != 2 || len(partitions["payments"]) != 1 {
		t.Fatalf("unexpected partitions: %+v", partitions)
	}
}

func TestNewResolverConfigErrors(t *testing.T) {
	cfg := config.Config{Metadata: config.MetaConfig{Discovery: "etcd"}}
	if _, err := NewResolver(cfg); err == nil {
		t.Fatalf("expected error for missing etcd endpoints")
	}
}

func TestEtcdClientConfigSetsReconnectBackoff(t *testing.T) {
	defaultCfg := EtcdClientConfig(clientv3.Config{
		Endpoints: []string{"http://127.0.0.1:2379"},
	})
	if defaultCfg.DialTimeout != 5*time.Second {
		t.Fatalf("DialTimeout = %s, want 5s default", defaultCfg.DialTimeout)
	}

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
	params := etcdConnectParams(cfg.DialTimeout)
	if params.Backoff.BaseDelay != 100*time.Millisecond {
		t.Fatalf("BaseDelay = %s, want 100ms", params.Backoff.BaseDelay)
	}
	if params.Backoff.MaxDelay != etcdReconnectMaxDelay {
		t.Fatalf("MaxDelay = %s, want %s", params.Backoff.MaxDelay, etcdReconnectMaxDelay)
	}
	if params.Backoff.MaxDelay >= backoff.DefaultConfig.MaxDelay {
		t.Fatalf("MaxDelay %s is not shorter than the gRPC default %s", params.Backoff.MaxDelay, backoff.DefaultConfig.MaxDelay)
	}
	if params.MinConnectTimeout != etcdMinConnectTimeout {
		t.Fatalf("MinConnectTimeout = %s, want %s when DialTimeout is %s", params.MinConnectTimeout, etcdMinConnectTimeout, cfg.DialTimeout)
	}
	if got := etcdConnectParams(10 * time.Second).MinConnectTimeout; got != 10*time.Second {
		t.Fatalf("MinConnectTimeout = %s, want 10s when DialTimeout is 10s", got)
	}
}

type stubResolver struct {
	topics     []string
	partitions map[string][]int32
	calls      int
}

func (s *stubResolver) Topics(ctx context.Context) ([]string, error) {
	s.calls++
	return s.topics, nil
}

func (s *stubResolver) Partitions(ctx context.Context, topic string) ([]int32, error) {
	s.calls++
	return s.partitions[topic], nil
}

func freeLocalPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

func mustLocalURL(t *testing.T, port int) url.URL {
	t.Helper()
	parsed, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	return *parsed
}
