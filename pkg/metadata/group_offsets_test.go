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
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
)

func TestListGroupConsumerOffsets(t *testing.T) {
	for _, backend := range []string{"memory", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var store Store = NewInMemoryStore(ClusterMetadata{})
			if backend == "etcd" {
				store = newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t))
			}
			want := []ConsumerOffset{{Group: "g", Topic: "a", Partition: 0, Offset: 42, Metadata: "checkpoint"}, {Group: "g", Topic: "z", Partition: 2, Offset: 17, Metadata: ""}}
			for _, offset := range append(append([]ConsumerOffset{}, want...), ConsumerOffset{Group: "g-other", Topic: "private", Offset: 99}) {
				if err := store.CommitConsumerOffset(ctx, offset.Group, offset.Topic, offset.Partition, offset.Offset, offset.Metadata); err != nil {
					t.Fatal(err)
				}
			}
			got, err := store.ListGroupConsumerOffsets(ctx, "g")
			sort.Slice(got, func(i, j int) bool { return got[i].Topic < got[j].Topic })
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatal(got, err)
			}
			got, err = store.ListGroupConsumerOffsets(ctx, "missing")
			if err != nil || len(got) != 0 {
				t.Fatal(got, err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := store.ListGroupConsumerOffsets(canceled, "g"); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if backend == "etcd" {
				etcd := store.(*EtcdStore)
				if _, err := etcd.client.Put(ctx, ConsumerOffsetKey("g-other", "corrupt", 0), "not-json"); err != nil {
					t.Fatal(err)
				}
				if got, err := store.ListGroupConsumerOffsets(ctx, "g"); err != nil || len(got) != 2 {
					t.Fatal("read another group's offset prefix", got, err)
				}
				if _, err := etcd.client.Put(ctx, ConsumerOffsetKey("g", "corrupt", 0), "not-json"); err != nil {
					t.Fatal(err)
				}
				if _, err := store.ListGroupConsumerOffsets(ctx, "g"); err == nil {
					t.Fatal("corrupt offset silently ignored")
				}
			}
		})
	}
}

func TestDeleteConsumerGroupRemovesOnlyItsOffsets(t *testing.T) {
	for _, backend := range []string{"memory", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var store Store = NewInMemoryStore(ClusterMetadata{})
			if backend == "etcd" {
				store = newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t))
			}
			for _, id := range []string{"g", "g-other"} {
				if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: id, State: "empty"}); err != nil {
					t.Fatal(err)
				}
				for _, partition := range []int32{0, 1} {
					if err := store.CommitConsumerOffset(ctx, id, "orders", partition, 42, "checkpoint"); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := store.DeleteConsumerGroup(ctx, "g"); err != nil {
				t.Fatal(err)
			}
			if group, err := store.FetchConsumerGroup(ctx, "g"); err != nil || group != nil {
				t.Fatal(group, err)
			}
			if offsets, err := store.ListGroupConsumerOffsets(ctx, "g"); err != nil || len(offsets) != 0 {
				t.Fatal(offsets, err)
			}
			if group, err := store.FetchConsumerGroup(ctx, "g-other"); err != nil || group == nil {
				t.Fatal(group, err)
			}
			offsets, err := store.ListGroupConsumerOffsets(ctx, "g-other")
			if err != nil || len(offsets) != 2 {
				t.Fatal(offsets, err)
			}
			for _, offset := range offsets {
				if offset.Offset != 42 || offset.Metadata != "checkpoint" {
					t.Fatal(offset)
				}
			}
		})
	}
}
