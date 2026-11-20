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

	"github.com/KafScale/platform/v2/internal/testutil"
	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/protocol"
)

type preferredMetadataStore struct {
	metadata.Store
	snapshot *metadata.ClusterMetadata
}

func (s preferredMetadataStore) Metadata(context.Context, []string) (*metadata.ClusterMetadata, error) {
	return s.snapshot, nil
}

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

func TestPreferredLeaderFallbacks(t *testing.T) {
	ctx := context.Background()
	p := lfsTestProxy(nil, time.Minute)
	if got := p.preferredLeaders(ctx, "orders"); got != nil {
		t.Fatalf("leaders without a store: %v", got)
	}
	p.store = &failingMetadataStore{fail: true}
	if got := p.preferredLeaders(ctx, "orders"); got != nil {
		t.Fatalf("leaders with failed metadata: %v", got)
	}
	p.store = preferredMetadataStore{snapshot: &metadata.ClusterMetadata{Topics: []protocol.MetadataTopic{
		{}, {Topic: kmsg.StringPtr("other")},
		{Topic: kmsg.StringPtr("orders"), ErrorCode: protocol.UNKNOWN_TOPIC_OR_PARTITION},
	}}}
	if got := p.preferredLeaders(ctx, "orders"); got != nil {
		t.Fatalf("leaders for an unavailable topic: %v", got)
	}
	if got := p.preferredLeaderAddr(ctx, nil, 0); got != "" {
		t.Fatalf("missing preferred leader address = %q", got)
	}
}

func TestProduceRetriesCurrentOwnerAfterStaleOwnerRejects(t *testing.T) {
	f := newPartitionRoutingFixture(t, 0, 1)
	// The lease has moved to broker 1, but the request was already grouped for
	// broker 0. The snapshot's preferred leader is still broker 0.
	f.moveOwner(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := makeProduceRequest(map[string][]int32{"orders": {0}})
	req.Version, req.Acks = 3, 1
	header := &protocol.RequestHeader{APIKey: 0, APIVersion: 3, CorrelationID: 91}
	pool := newConnPool(time.Second)
	defer pool.Close()
	raw, err := f.p.forwardProduce(ctx, header, req, nil, map[string]*kmsg.ProduceRequest{f.brokers[0].addr: req}, pool)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := parseProduceResponse(raw, 3)
	if err != nil {
		t.Fatal(err)
	}
	if code := resp.Topics[0].Partitions[0].ErrorCode; code != 0 {
		t.Fatalf("ownership change escaped to the client: error %d", code)
	}
	if f.brokers[0].produced.Load() != 0 || f.brokers[1].produced.Load() != 1 {
		t.Fatal("request was not accepted exactly once by the current owner")
	}
}

func TestFetchRetriesCurrentOwnerAfterStaleOwnerRejects(t *testing.T) {
	f := newPartitionRoutingFixture(t, 0, 1)
	f.moveOwner(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := makeFetchRequest(map[string][]int32{"orders": {0}})
	req.Version = 11
	header := &protocol.RequestHeader{APIKey: 1, APIVersion: 11, CorrelationID: 92}
	pool := newConnPool(time.Second)
	defer pool.Close()
	raw, err := f.p.forwardFetch(ctx, header, req, nil, map[string]*kmsg.FetchRequest{f.brokers[0].addr: req}, pool)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := parseFetchResponse(raw, 11)
	if err != nil {
		t.Fatal(err)
	}
	if code := resp.Topics[0].Partitions[0].ErrorCode; code != 0 {
		t.Fatalf("ownership change escaped to the client: error %d", code)
	}
	if f.brokers[0].fetched.Load() != 0 || f.brokers[1].fetched.Load() != 1 {
		t.Fatal("fetch did not reach the current owner")
	}
}

func TestProduceDoesNotReplayAmbiguousOutcome(t *testing.T) {
	f := newPartitionRoutingFixture(t, 0, 0)
	f.brokers[0].produceError.Store(int32(protocol.REQUEST_TIMED_OUT))
	f.brokers[1].owns.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := makeProduceRequest(map[string][]int32{"orders": {0}})
	req.Version, req.Acks = 3, 1
	header := &protocol.RequestHeader{APIKey: 0, APIVersion: 3, CorrelationID: 93}
	pool := newConnPool(time.Second)
	defer pool.Close()
	raw, err := f.p.forwardProduce(ctx, header, req, nil, map[string]*kmsg.ProduceRequest{f.brokers[0].addr: req}, pool)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := parseProduceResponse(raw, 3)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Topics[0].Partitions[0].ErrorCode != protocol.REQUEST_TIMED_OUT || f.brokers[0].produced.Load() != 1 || f.brokers[1].produced.Load() != 0 {
		t.Fatal("ambiguous durable produce was replayed or falsely acknowledged")
	}
}

func TestListOffsetsRoutesUnownedPartitionsToPreferredLeader(t *testing.T) {
	ctx := context.Background()
	p := preferredLeaderTestProxy(t, ctx)
	groups := p.groupByPartitionOwner(ctx, []topicPartition{{"orders", 0}, {"orders", 1}, {"orders", 2}})
	if len(groups[""]) != 0 || len(groups["127.0.0.1:19091"]) != 2 || !groups["127.0.0.1:19092"][topicPartition{"orders", 2}] {
		t.Fatalf("list offsets groups = %v, want owner broker 1 for 0 and 1, preferred broker 2 for 2", groups)
	}
}
