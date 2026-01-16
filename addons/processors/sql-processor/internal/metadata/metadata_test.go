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
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

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
