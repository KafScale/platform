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

package operator

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	kafscalev1 "github.com/KafScale/platform/v2/api/v1"
	"github.com/KafScale/platform/v2/internal/testutil"
	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOperatorTopicIDLifecycle(t *testing.T) {
	ctx := context.Background()
	endpoints := testutil.StartEmbeddedEtcd(t)
	cluster := &kafscalev1.KafscaleCluster{}
	topic := kafscalev1.KafscaleTopic{ObjectMeta: metav1.ObjectMeta{Name: "orders", UID: "first-resource"}, Spec: kafscalev1.KafscaleTopicSpec{Partitions: 1}}
	render := func() metadata.ClusterMetadata {
		return BuildClusterMetadataForBrokers(cluster, []kafscalev1.KafscaleTopic{topic}, 1)
	}
	// Independent controllers render different candidates; all must converge on
	// the ID committed by the first successful etcd transaction.
	var wg sync.WaitGroup
	errors := make(chan error, 4)
	for range 4 {
		desired := render()
		wg.Add(1)
		go func() { defer wg.Done(); errors <- PublishMetadataSnapshot(ctx, endpoints, desired) }()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	first, err := readSnapshotFromEtcd(ctx, endpoints)
	if err != nil || len(first.Topics) != 1 {
		t.Fatal(first, err)
	}
	id := first.Topics[0].TopicID
	if id == ([16]byte{}) || id == metadata.TopicIDForName("orders") {
		t.Fatalf("not a new ID: %x", id)
	}
	topic.Spec.Partitions = 3
	for range 3 {
		if err := PublishMetadataSnapshot(ctx, endpoints, render()); err != nil {
			t.Fatal(err)
		}
	}
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// A broker mutation must preserve the operator resource UID as well as IDs.
	if _, err := store.CreateTopic(ctx, metadata.TopicSpec{Name: "api-created", NumPartitions: 1}); err != nil {
		t.Fatal(err)
	}
	current, err := readSnapshotFromEtcd(ctx, endpoints)
	if err != nil || current.Topics[0].TopicID != id || len(current.Topics[0].Partitions) != 3 || current.TopicResourceUIDs["orders"] != "first-resource" {
		t.Fatalf("ID/resource changed: %+v %v", current, err)
	}
	topic.UID = "replacement-resource"
	if err := PublishMetadataSnapshot(ctx, endpoints, render()); err != nil {
		t.Fatal(err)
	}
	recreated, err := readSnapshotFromEtcd(ctx, endpoints)
	if err != nil || recreated.Topics[0].TopicID == id || recreated.TopicResourceUIDs["orders"] != "replacement-resource" {
		t.Fatalf("recreated resource retained identity: %+v %v", recreated, err)
	}
	replacementID := recreated.Topics[0].TopicID
	if err := PublishMetadataSnapshot(ctx, endpoints, render()); err != nil {
		t.Fatal(err)
	}
	recreated, err = readSnapshotFromEtcd(ctx, endpoints)
	if err != nil || recreated.Topics[0].TopicID != replacementID || len(recreated.Topics) != 2 {
		t.Fatalf("reconcile changed identity: %+v %v", recreated, err)
	}
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := PublishMetadataSnapshot(ctx, endpoints, render()); err != nil {
		t.Fatal(err)
	}
	recreated, err = readSnapshotFromEtcd(ctx, endpoints)
	if err != nil || recreated.Topics[0].TopicID == replacementID {
		t.Fatalf("deleted topic retained ID: %+v %v", recreated, err)
	}
}

func TestOperatorPreservesLegacyTopicID(t *testing.T) {
	for _, oldID := range [][16]byte{{}, {7}} {
		ctx := context.Background()
		endpoints := testutil.StartEmbeddedEtcd(t)
		cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
		if err != nil {
			t.Fatal(err)
		}
		existing := metadata.ClusterMetadata{Topics: []protocol.MetadataTopic{{Topic: kmsg.StringPtr("orders"), TopicID: oldID}}}
		payload, err := json.Marshal(existing)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cli.Put(ctx, "/kafscale/metadata/snapshot", string(payload)); err != nil {
			t.Fatal(err)
		}
		cli.Close()
		desired := BuildClusterMetadataForBrokers(&kafscalev1.KafscaleCluster{}, []kafscalev1.KafscaleTopic{{ObjectMeta: metav1.ObjectMeta{Name: "orders", UID: "adopted-resource"}, Spec: kafscalev1.KafscaleTopicSpec{Partitions: 1}}}, 1)
		for range 2 {
			if err := PublishMetadataSnapshot(ctx, endpoints, desired); err != nil {
				t.Fatal(err)
			}
		}
		got, err := readSnapshotFromEtcd(ctx, endpoints)
		expected := oldID
		if expected == ([16]byte{}) {
			expected = metadata.TopicIDForName("orders")
		}
		if err != nil || got.Topics[0].TopicID != expected {
			t.Fatalf("legacy ID changed: %+v %v", got, err)
		}
	}
}
