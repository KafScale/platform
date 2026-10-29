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

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
)

type legacyCommitStore struct{ Store }

func TestCommitConsumerOffsetIfGroupExists(t *testing.T) {
	ctx := context.Background()
	stores := map[string]Store{
		"memory": NewInMemoryStore(ClusterMetadata{}),
		"etcd":   newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t)),
	}
	for name, store := range stores {
		if err := CommitConsumerOffsetIfGroupExists(ctx, store, "g", "orders", 0, 1, ""); !errors.Is(err, ErrConsumerGroupDeleted) {
			t.Fatal(name, err)
		}
		if offsets, _ := store.ListGroupConsumerOffsets(ctx, "g"); len(offsets) != 0 {
			t.Fatal(name, offsets)
		}
		if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "g"}); err != nil {
			t.Fatal(err)
		}
		if err := CommitConsumerOffsetIfGroupExists(ctx, store, "g", "orders", 0, 7, "m"); err != nil {
			t.Fatal(name, err)
		}
		if offset, meta, err := store.FetchConsumerOffset(ctx, "g", "orders", 0); err != nil || offset != 7 || meta != "m" {
			t.Fatal(name, offset, meta, err)
		}
	}
	legacy := legacyCommitStore{NewInMemoryStore(ClusterMetadata{})}
	if err := CommitConsumerOffsetIfGroupExists(ctx, legacy, "g", "orders", 0, 3, ""); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := stores["etcd"].(*EtcdStore).CommitConsumerOffsetIfGroupExists(canceled, "g", "orders", 0, 1, ""); err == nil {
		t.Fatal("expected canceled")
	}
	if err := stores["memory"].(*InMemoryStore).CommitConsumerOffsetIfGroupExists(canceled, "g", "orders", 0, 1, ""); err == nil {
		t.Fatal("expected canceled")
	}
}
