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
	"github.com/KafScale/platform/pkg/protocol"
	"testing"
)

func TestUpdateOffsetsRejectsDeletedTopic(t *testing.T) {
	ctx := context.Background()
	for _, etcd := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "etcd"}[etcd], func(t *testing.T) {
			var store Store = NewInMemoryStore(ClusterMetadata{Brokers: []protocol.MetadataBroker{{NodeID: 1}}})
			if etcd {
				store = newRetireStore(t, ctx)
			} else {
				_, _ = store.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 1})
			}
			if err := store.DeleteTopic(ctx, "orders"); err != nil {
				t.Fatal(err)
			}
			if err := store.UpdateOffsets(ctx, "orders", 0, 99); !errors.Is(err, ErrUnknownTopic) {
				t.Fatalf("deleted offset update: %v", err)
			}
			if etcd {
				s := store.(*EtcdStore)
				r, err := s.client.Get(ctx, offsetKey("orders", 0))
				if err != nil || len(r.Kvs) != 0 {
					t.Fatalf("stale offset persisted: %v %v", r, err)
				}
			}
		})
	}
}

func TestOffsetCallbacksFenceRetiredStorage(t *testing.T) {
	ctx := context.Background()
	s := newRetireStore(t, ctx)
	if err := s.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateTopic(ctx, TopicSpec{Name: "orders", NumPartitions: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateOffsetsForStorage(ctx, "orders", 0, 999, "orders"); !errors.Is(err, ErrTopicGenerationChanged) {
		t.Fatal(err)
	}
	if err := s.UpdateOffsetsForStorage(ctx, "orders", 0, 7, "orders@1"); err != nil {
		t.Fatal(err)
	}
	if next, err := s.NextOffset(ctx, "orders", 0); err != nil || next != 8 {
		t.Fatalf("next %d: %v", next, err)
	}
	// A delete after the read but before its write must fail the transaction.
	if err := s.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := s.commitGuarded(ctx, generationIs(TopicGenerationKey("orders"), 1), nil); !errors.Is(err, ErrTopicGenerationChanged) {
		t.Fatal(err)
	}
}

func TestOffsetUpdateCorruptMetadata(t *testing.T) {
	ctx := context.Background()
	s := newRetireStore(t, ctx)
	for _, key := range []string{snapshotKey(), TopicGenerationKey("orders")} {
		_, _ = s.client.Put(ctx, key, "broken")
		if err := s.UpdateOffsets(ctx, "orders", 0, 1); err == nil {
			t.Fatalf("accepted corrupt %s", key)
		}
		_, _ = s.client.Delete(ctx, key)
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.UpdateOffsets(cancelCtx, "orders", 0, 1); err == nil {
		t.Fatal("accepted canceled update")
	}

}

func TestMissingInvalidTopicLookup(t *testing.T) {
	ctx := context.Background()
	for _, store := range []Store{NewInMemoryStore(ClusterMetadata{}), newRetireStore(t, ctx)} {
		meta, err := store.Metadata(ctx, []string{"bad/name"})
		if err != nil || meta.Topics[0].ErrorCode != protocol.INVALID_TOPIC_EXCEPTION {
			t.Fatalf("invalid metadata: %v %v", meta, err)
		}
		if _, err := store.NextOffset(ctx, "bad/name", 0); !errors.Is(err, ErrInvalidTopic) {
			t.Fatalf("invalid offset lookup: %v", err)
		}
	}
}
