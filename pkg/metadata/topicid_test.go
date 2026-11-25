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

package metadata

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/KafScale/platform/v2/internal/testutil"
	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestTopicIDLifecycle(t *testing.T) {
	ctx := context.Background()
	endpoints := testutil.StartEmbeddedEtcd(t)
	seed := ClusterMetadata{Brokers: []protocol.MetadataBroker{{NodeID: 1}}}
	store := newTestEtcdStore(t, ctx, seed, endpoints)
	topic, err := store.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	id := topic.TopicID
	if id == ([16]byte{}) || id == TopicIDForName("orders") || id[6]>>4 != 4 || id[8]>>6 != 2 {
		t.Fatalf("not a random v4 UUID: %x", id)
	}
	if err := store.CreatePartitions(ctx, "orders", 3); err != nil {
		t.Fatal(err)
	}
	restarted := newTestEtcdStore(t, ctx, seed, endpoints)
	got, err := restarted.Metadata(ctx, []string{"orders"})
	if err != nil || len(got.Topics) != 1 || got.Topics[0].TopicID != id || len(got.Topics[0].Partitions) != 3 {
		t.Fatalf("identity changed after restart/expansion: %+v, %v", got, err)
	}
	if err := restarted.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	recreated, err := restarted.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	if recreated.TopicID == id || recreated.TopicID == TopicIDForName("orders") {
		t.Fatal("recreated topic reused ID")
	}
	// A fresh process must read the committed replacement, regardless of its seed.
	latest := newTestEtcdStore(t, ctx, seed, endpoints)
	got, err = latest.Metadata(ctx, []string{"orders"})
	if err != nil || got.Topics[0].TopicID != recreated.TopicID {
		t.Fatalf("replacement ID not durable: %+v %v", got, err)
	}
}

func TestConcurrentTopicCreationKeepsWinningID(t *testing.T) {
	ctx := context.Background()
	endpoints := testutil.StartEmbeddedEtcd(t)
	seed := ClusterMetadata{Brokers: []protocol.MetadataBroker{{NodeID: 1}}}
	stores := make([]*EtcdStore, 8)
	for i := range stores {
		stores[i] = newTestEtcdStore(t, ctx, seed, endpoints)
	}
	type result struct {
		topic *protocol.MetadataTopic
		err   error
	}
	results := make(chan result, len(stores))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, store := range stores {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			topic, err := store.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 1})
			results <- result{topic, err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var winner [16]byte
	successes := 0
	for result := range results {
		if result.err == nil {
			winner = result.topic.TopicID
			successes++
		} else if !errors.Is(result.err, ErrTopicExists) {
			t.Fatal(result.err)
		}
	}
	if successes != 1 {
		t.Fatalf("%d successful creates", successes)
	}
	fresh := newTestEtcdStore(t, ctx, seed, endpoints)
	got, err := fresh.Metadata(ctx, nil)
	if err != nil || len(got.Topics) != 1 || got.Topics[0].TopicID != winner {
		t.Fatalf("winner not persisted: %+v %v", got, err)
	}
}

func TestLegacyTopicIDSurvivesUpgrade(t *testing.T) {
	for _, legacyID := range [][16]byte{{}, {9}} {
		t.Run(fmt.Sprintf("%x", legacyID), func(t *testing.T) {
			ctx := context.Background()
			endpoints := testutil.StartEmbeddedEtcd(t)
			seed := ClusterMetadata{Brokers: []protocol.MetadataBroker{{NodeID: 1}}, Topics: []protocol.MetadataTopic{{Topic: kmsg.StringPtr("orders"), TopicID: legacyID, Partitions: []protocol.MetadataPartition{{Partition: 0, Leader: 1}}}}}
			cli := newEtcdClient(t, endpoints)
			defer cli.Close()
			writeSnapshotFixture(t, cli, seed)
			store := newTestEtcdStore(t, ctx, seed, endpoints)
			if err := store.CreatePartitions(ctx, "orders", 2); err != nil {
				t.Fatal(err)
			}
			if _, err := store.CreateTopic(ctx, TopicSpec{Name: "other", NumPartitions: 1}); err != nil {
				t.Fatal(err)
			}
			restarted := newTestEtcdStore(t, ctx, ClusterMetadata{}, endpoints)
			got, err := restarted.Metadata(ctx, []string{"orders"})
			expected := legacyID
			if expected == ([16]byte{}) {
				expected = TopicIDForName("orders")
			}
			if err != nil || got.Topics[0].TopicID != expected {
				t.Fatalf("legacy ID changed: %+v %v", got, err)
			}
		})
	}
}
