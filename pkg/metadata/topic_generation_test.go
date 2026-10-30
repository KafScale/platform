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
	"errors"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/KafScale/platform/internal/testutil"
)

func TestTopicGenerationRetireAndRestore(t *testing.T) {
	ctx := context.Background()
	store, err := NewEtcdStore(ctx, ClusterMetadata{}, EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	generation := func() int64 {
		t.Helper()
		got, err := store.TopicGeneration(ctx, "orders")
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if generation() != 0 || TopicStorageName("orders", 0) != "orders" || TopicStorageName("orders", 2) != "orders@2" {
		t.Fatal("unexpected initial generation or storage name")
	}
	if err := store.RetireTopic(ctx, DeletedTopic{Name: "orders", Generation: 1}); !errors.Is(err, ErrTopicGenerationChanged) {
		t.Fatalf("expected generation conflict, got %v", err)
	}
	if err := store.RetireTopic(ctx, DeletedTopic{Name: "orders", Generation: 0}); err != nil {
		t.Fatal(err)
	}
	if err := store.RetireTopic(ctx, DeletedTopic{Name: "orders", Generation: 0}); !errors.Is(err, ErrTopicGenerationChanged) {
		t.Fatalf("expected generation conflict, got %v", err)
	}
	if err := store.RetireTopic(ctx, DeletedTopic{Name: "orders", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if generation() != 2 {
		t.Fatalf("generation %d, want 2", generation())
	}
	if err := store.RestoreTopicGeneration(ctx, "orders", 0); !errors.Is(err, ErrTopicGenerationChanged) {
		t.Fatalf("expected generation conflict, got %v", err)
	}
	if err := store.RestoreTopicGeneration(ctx, "orders", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.RestoreTopicGeneration(ctx, "orders", 0); err != nil {
		t.Fatal(err)
	}
	if generation() != 0 {
		t.Fatalf("generation %d, want 0", generation())
	}
	resp, err := store.client.Get(ctx, DeletedTopicPrefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatalf("records left after restore: %d", len(resp.Kvs))
	}
	store.client.Close()
	if _, err := store.TopicGeneration(ctx, "orders"); err == nil {
		t.Fatal("expected error from closed client")
	}
	if err := store.RetireTopic(ctx, DeletedTopic{Name: "orders"}); err == nil {
		t.Fatal("expected error from closed client")
	}
	if err := store.RestoreTopicGeneration(ctx, "orders", 0); err == nil {
		t.Fatal("expected error from closed client")
	}
}
