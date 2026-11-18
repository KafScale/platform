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
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

func preferredLeaderTestProxy(t *testing.T, ctx context.Context) *proxy {
	t.Helper()
	endpoints := testutil.StartEmbeddedEtcd(t)
	putProxyTestSnapshot(t, endpoints, metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{brokerAt(t, 0, "127.0.0.1:19090"), brokerAt(t, 1, "127.0.0.1:19091"), brokerAt(t, 2, "127.0.0.1:19092")},
		Topics: []protocol.MetadataTopic{{
			Topic:      kmsg.StringPtr("orders"),
			Partitions: []protocol.MetadataPartition{{Partition: 0, Leader: 0}, {Partition: 1, Leader: 1}, {Partition: 2, Leader: 2}},
		}},
	})
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	if _, err := cli.Put(ctx, metadata.PartitionLeasePrefix()+"/orders/0", strconv.Itoa(1)); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	p := lfsTestProxy(store, time.Minute)
	if p.router, err = metadata.NewPartitionRouter(ctx, store.EtcdClient(), p.logger); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.router.Stop)
	p.refreshMetadataCache(ctx)
	return p
}

func TestProduceRoutesUnownedPartitionsToPreferredLeader(t *testing.T) {
	ctx := context.Background()
	p := preferredLeaderTestProxy(t, ctx)
	groups := p.groupPartitionsByBroker(ctx, makeProduceRequest(map[string][]int32{"orders": {0, 1, 2}}), nil)
	if _, ok := groups[""]; ok {
		t.Fatalf("unowned partitions fell back to any broker: %v", mapKeys(groups))
	}
	parts := make(map[string][]int32)
	for addr, req := range groups {
		for _, topic := range req.Topics {
			for _, part := range topic.Partitions {
				parts[addr] = append(parts[addr], part.Partition)
			}
		}
	}
	if len(parts["127.0.0.1:19091"]) != 2 || len(parts["127.0.0.1:19092"]) != 1 || parts["127.0.0.1:19092"][0] != 2 {
		t.Fatalf("produce groups = %v, want owner broker 1 for 0 and 1, preferred broker 2 for 2", parts)
	}
}

func TestFetchRoutesUnownedPartitionsToPreferredLeader(t *testing.T) {
	ctx := context.Background()
	p := preferredLeaderTestProxy(t, ctx)
	req := &kmsg.FetchRequest{Topics: []kmsg.FetchRequestTopic{{
		Topic:      "orders",
		Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 0}, {Partition: 1}, {Partition: 2}},
	}}}
	groups := p.groupFetchPartitionsByBroker(ctx, req, nil)
	parts := make(map[string][]int32)
	for addr, sub := range groups {
		for _, topic := range sub.Topics {
			for _, part := range topic.Partitions {
				parts[addr] = append(parts[addr], part.Partition)
			}
		}
	}
	if len(parts[""]) != 0 || len(parts["127.0.0.1:19091"]) != 2 || len(parts["127.0.0.1:19092"]) != 1 {
		t.Fatalf("fetch groups = %v, want owner broker 1 for 0 and 1, preferred broker 2 for 2", parts)
	}
}
