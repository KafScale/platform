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
	"testing"

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
)

func TestDeleteConsumerGroupAndOffsets(t *testing.T) {
	for _, backend := range []string{"memory", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var store Store = NewInMemoryStore(ClusterMetadata{})
			if backend == "etcd" {
				store = newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t))
			}
			if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "g", State: "empty"}); err != nil {
				t.Fatal(err)
			}
			for _, group := range []string{"g", "g-other"} {
				if err := store.CommitConsumerOffset(ctx, group, "orders", 0, 42, "metadata"); err != nil {
					t.Fatal(err)
				}
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if err := DeleteConsumerGroupAndOffsets(canceled, store, "g"); err == nil {
				t.Fatal("canceled deletion succeeded")
			}
			if group, err := store.FetchConsumerGroup(ctx, "g"); err != nil || group == nil {
				t.Fatal("canceled deletion changed group", group, err)
			}
			if err := DeleteConsumerGroupAndOffsets(ctx, store, "g"); err != nil {
				t.Fatal(err)
			}
			if group, err := store.FetchConsumerGroup(ctx, "g"); err != nil || group != nil {
				t.Fatal(group, err)
			}
			if offsets, err := store.ListGroupConsumerOffsets(ctx, "g"); err != nil || len(offsets) != 0 {
				t.Fatal(offsets, err)
			}
			if offsets, err := store.ListGroupConsumerOffsets(ctx, "g-other"); err != nil || len(offsets) != 1 {
				t.Fatal(offsets, err)
			}
			if err := DeleteConsumerGroupAndOffsets(ctx, store, "g"); err != nil {
				t.Fatal("deletion not idempotent", err)
			}
		})
	}
}
