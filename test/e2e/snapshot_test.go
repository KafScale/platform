package e2e

import (
	"context"
	"fmt"
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
	cfg.LogLevel = "error"

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
