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

//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kafscalev1alpha1 "github.com/novatechflow/kafscale/api/v1alpha1"
	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/operator"

	"go.etcd.io/etcd/server/v3/embed"
)

func TestSnapshotPublishAndBrokerConsumption(t *testing.T) {
	e, endpoints := startEmbeddedEtcd(t)
	defer e.Close()

	ctx := context.Background()

	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "production",
			Namespace: "default",
			UID:       "cluster-uid",
		},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{},
			S3: kafscalev1alpha1.S3Spec{
				Bucket: "test",
				Region: "us-east-1",
			},
			Etcd: kafscalev1alpha1.EtcdSpec{
				Endpoints: endpoints,
			},
		},
	}

	topic := kafscalev1alpha1.KafscaleTopic{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orders",
			Namespace: "default",
		},
		Spec: kafscalev1alpha1.KafscaleTopicSpec{
			ClusterRef: "production",
			Partitions: 3,
		},
	}

	snapshot := operator.BuildClusterMetadata(cluster, []kafscalev1alpha1.KafscaleTopic{topic})
	if err := operator.PublishMetadataSnapshot(ctx, endpoints, snapshot); err != nil {
		t.Fatalf("publish snapshot: %v", err)
	}

	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{
		Endpoints: endpoints,
	})
	if err != nil {
		t.Fatalf("create etcd store: %v", err)
	}

	meta, err := store.Metadata(ctx, []string{"orders"})
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if len(meta.Brokers) == 0 {
		t.Fatalf("expected brokers in snapshot, got none")
	}
	if len(meta.Topics) != 1 || meta.Topics[0].Name != "orders" {
		t.Fatalf("snapshot missing topic: %+v", meta.Topics)
	}
	if meta.ClusterID == nil || *meta.ClusterID != "cluster-uid" {
		t.Fatalf("cluster id mismatch: %+v", meta.ClusterID)
	}
}

func startEmbeddedEtcd(t *testing.T) (*embed.Etcd, []string) {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Logger = "zap"
	cfg.LogLevel = "info"
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
		t.Fatalf("start etcd: %v", err)
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

func freeLocalPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate port: %v", err)
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
	dir := filepath.Join(repoRoot(t), "test", "e2e", "logs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create log dir: %v", err)
	}
	return filepath.Join(dir, fmt.Sprintf("embedded-etcd-%d.log", time.Now().UnixNano()))
}
