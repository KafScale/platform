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

package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	metadatapb "github.com/novatechflow/kafscale/pkg/gen/metadata"
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

func TestEtcdStoreTopicConfigAndPartitions(t *testing.T) {
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
	if _, err := store.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	cfg, err := store.FetchTopicConfig(ctx, "orders")
	if err != nil {
		t.Fatalf("FetchTopicConfig: %v", err)
	}
	cfg.RetentionMs = 120000
	if err := store.UpdateTopicConfig(ctx, cfg); err != nil {
		t.Fatalf("UpdateTopicConfig: %v", err)
	}
	updated, err := store.FetchTopicConfig(ctx, "orders")
	if err != nil {
		t.Fatalf("FetchTopicConfig: %v", err)
	}
	if updated.RetentionMs != 120000 {
		t.Fatalf("unexpected retention: %d", updated.RetentionMs)
	}
	if err := store.CreatePartitions(ctx, "orders", 2); err != nil {
		t.Fatalf("CreatePartitions: %v", err)
	}

	cli := newEtcdClient(t, endpoints)
	defer cli.Close()
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Get(ctxTimeout, PartitionStateKey("orders", 1))
	if err != nil {
		t.Fatalf("get partition state: %v", err)
	}
	if resp.Count == 0 {
		t.Fatalf("expected partition state for new partition")
	}
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

func TestEtcdStoreConsumerGroupPersistence(t *testing.T) {
	e, endpoints := startEmbeddedEtcd(t)
	defer e.Close()

	ctx := context.Background()
	store, err := NewEtcdStore(ctx, ClusterMetadata{}, EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("NewEtcdStore: %v", err)
	}

	group := &metadatapb.ConsumerGroup{
		GroupId:      "group-1",
		State:        "stable",
		ProtocolType: "consumer",
		Protocol:     "range",
		Leader:       "member-1",
		GenerationId: 3,
		Members: map[string]*metadatapb.GroupMember{
			"member-1": {
				Subscriptions: []string{"orders"},
				Assignments: []*metadatapb.Assignment{
					{Topic: "orders", Partitions: []int32{0, 1}},
				},
			},
		},
	}
	if err := store.PutConsumerGroup(ctx, group); err != nil {
		t.Fatalf("PutConsumerGroup: %v", err)
	}
	loaded, err := store.FetchConsumerGroup(ctx, "group-1")
	if err != nil {
		t.Fatalf("FetchConsumerGroup: %v", err)
	}
	if loaded == nil || loaded.GenerationId != 3 || loaded.Leader != "member-1" {
		t.Fatalf("unexpected group data: %#v", loaded)
	}
	groups, err := store.ListConsumerGroups(ctx)
	if err != nil {
		t.Fatalf("ListConsumerGroups: %v", err)
	}
	if len(groups) != 1 || groups[0].GetGroupId() != "group-1" {
		t.Fatalf("unexpected list groups: %#v", groups)
	}
	if err := store.DeleteConsumerGroup(ctx, "group-1"); err != nil {
		t.Fatalf("DeleteConsumerGroup: %v", err)
	}
	loaded, err = store.FetchConsumerGroup(ctx, "group-1")
	if err != nil {
		t.Fatalf("FetchConsumerGroup after delete: %v", err)
	}
	if loaded != nil {
		t.Fatalf("expected group deleted, got %#v", loaded)
	}
}

func startEmbeddedEtcd(t *testing.T) (*embed.Etcd, []string) {
	t.Helper()
	if err := ensureEtcdPortsFree(); err != nil {
		t.Skipf("skipping etcd store tests: %v", err)
	}
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.Logger = "zap"
	setEtcdPorts(t, cfg, "32379", "32380")

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

func ensureEtcdPortsFree() error {
	if err := killProcessesOnPort("32379"); err != nil {
		return err
	}
	if err := killProcessesOnPort("32380"); err != nil {
		return err
	}
	if err := portAvailable("127.0.0.1:32379"); err != nil {
		return err
	}
	if err := portAvailable("127.0.0.1:32380"); err != nil {
		return err
	}
	return nil
}

func setEtcdPorts(t *testing.T, cfg *embed.Config, clientPort, peerPort string) {
	t.Helper()
	clientURL, err := url.Parse("http://127.0.0.1:" + clientPort)
	if err != nil {
		t.Fatalf("parse client url: %v", err)
	}
	peerURL, err := url.Parse("http://127.0.0.1:" + peerPort)
	if err != nil {
		t.Fatalf("parse peer url: %v", err)
	}
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.Name = "default"
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
}

func killProcessesOnPort(port string) error {
	out, err := exec.Command("lsof", "-nP", "-iTCP:"+port, "-sTCP:LISTEN", "-t").Output()
	if err != nil {
		return nil
	}
	pids := strings.Fields(string(out))
	for _, pidStr := range pids {
		pid, convErr := strconv.Atoi(strings.TrimSpace(pidStr))
		if convErr != nil {
			continue
		}
		_ = syscall.Kill(pid, syscall.SIGTERM)
		time.Sleep(100 * time.Millisecond)
		if alive := syscall.Kill(pid, 0); alive == nil {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	return nil
}

func portAvailable(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("port %s already in use", addr)
	}
	_ = ln.Close()
	return nil
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
