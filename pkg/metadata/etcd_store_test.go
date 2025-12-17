package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/novatechflow/kafscale/pkg/protocol"
)

func TestEtcdStoreCreateTopicPersistsSnapshot(t *testing.T) {
	e, endpoints := startEmbeddedEtcd(t)
	defer e.Close()

	ctx := context.Background()
	initial := ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "broker-0", Port: 9092},
		},
		ControllerID: 1,
	}
	store, err := NewEtcdStore(ctx, initial, EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("NewEtcdStore: %v", err)
	}

	_, err = store.CreateTopic(ctx, TopicSpec{
		Name:              "orders",
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	waitForTopicInSnapshot(t, endpoints, "orders")
}

func TestEtcdStoreDeleteTopicRemovesOffsets(t *testing.T) {
	e, endpoints := startEmbeddedEtcd(t)
	defer e.Close()

	ctx := context.Background()
	initial := ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "broker-0", Port: 9092},
		},
		ControllerID: 1,
		Topics: []protocol.MetadataTopic{
			{
				Name: "orders",
				Partitions: []protocol.MetadataPartition{
					{PartitionIndex: 0, LeaderID: 1, ReplicaNodes: []int32{1}, ISRNodes: []int32{1}},
					{PartitionIndex: 1, LeaderID: 1, ReplicaNodes: []int32{1}, ISRNodes: []int32{1}},
				},
			},
		},
	}
	store, err := NewEtcdStore(ctx, initial, EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("NewEtcdStore: %v", err)
	}

	if err := store.UpdateOffsets(ctx, "orders", 0, 10); err != nil {
		t.Fatalf("UpdateOffsets: %v", err)
	}
	if err := store.CommitConsumerOffset(ctx, "group-a", "orders", 0, 5, "meta"); err != nil {
		t.Fatalf("CommitConsumerOffset: %v", err)
	}

	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}

	waitForTopicRemoval(t, endpoints, "orders")

	cli := newEtcdClient(t, endpoints)
	defer cli.Close()

	ctxTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Get(ctxTimeout, "/kafscale/topics/orders/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("get offsets prefix: %v", err)
	}
	if resp.Count != 0 {
		t.Fatalf("expected offsets to be deleted, got %d keys", resp.Count)
	}

	ctxTimeout, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err = cli.Get(ctxTimeout, "/kafscale/consumers/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("get consumers prefix: %v", err)
	}
	for _, kv := range resp.Kvs {
		if string(kv.Key) == "/kafscale/consumers/group-a/offsets/orders/0" {
			t.Fatalf("consumer offset still present after delete")
		}
	}
}

func startEmbeddedEtcd(t *testing.T) (*embed.Etcd, []string) {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.Logger = "zap"

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("skipping etcd store tests: %v", err)
		}
		t.Fatalf("start embedded etcd: %v", err)
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

func waitForTopicInSnapshot(t *testing.T, endpoints []string, topic string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		meta, err := loadSnapshot(endpoints)
		if err == nil && topicExists(meta, topic) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("topic %s was not persisted to snapshot", topic)
}

func waitForTopicRemoval(t *testing.T, endpoints []string, topic string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		meta, err := loadSnapshot(endpoints)
		if err == nil && !topicExists(meta, topic) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("topic %s still present in snapshot", topic)
}

func loadSnapshot(endpoints []string) (*ClusterMetadata, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := cli.Get(ctx, snapshotKey())
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("snapshot missing")
	}
	var meta ClusterMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func topicExists(meta *ClusterMetadata, topic string) bool {
	if meta == nil {
		return false
	}
	for _, t := range meta.Topics {
		if t.Name == topic && t.ErrorCode == 0 {
			return true
		}
	}
	return false
}

func newEtcdClient(t *testing.T, endpoints []string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("new etcd client: %v", err)
	}
	return cli
}
