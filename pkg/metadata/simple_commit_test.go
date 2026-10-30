// Copyright 2026 KafScale team.
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
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func TestCommitSimpleConsumerOffset(t *testing.T) {
	ctx := context.Background()
	stores := map[string]Store{
		"memory": NewInMemoryStore(ClusterMetadata{}),
		"etcd":   newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t)),
	}
	old := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	for name, store := range stores {
		if err := CommitSimpleConsumerOffset(ctx, store, "absent", "orders", 0, 3, "m"); err != nil {
			t.Fatal(name, err)
		}
		if offset, meta, err := store.FetchConsumerOffset(ctx, "absent", "orders", 0); err != nil || offset != 3 || meta != "m" {
			t.Fatal(name, offset, meta, err)
		}
		if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "empty", State: "empty", EmptySince: old}); err != nil {
			t.Fatal(err)
		}
		if err := CommitSimpleConsumerOffset(ctx, store, "empty", "orders", 0, 4, ""); err != nil {
			t.Fatal(name, err)
		}
		if group, _ := store.FetchConsumerGroup(ctx, "empty"); group == nil || group.EmptySince == old {
			t.Fatal(name, "commit did not reset retention clock", group)
		}
		if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "active", Members: map[string]*metadatapb.GroupMember{"m1": {}}}); err != nil {
			t.Fatal(err)
		}
		if err := CommitSimpleConsumerOffset(ctx, store, "active", "orders", 0, 5, ""); !errors.Is(err, ErrConsumerGroupNotEmpty) {
			t.Fatal(name, err)
		}
		if offsets, _ := store.ListGroupConsumerOffsets(ctx, "active"); len(offsets) != 0 {
			t.Fatal(name, offsets)
		}
	}
	legacy := legacyCommitStore{NewInMemoryStore(ClusterMetadata{})}
	if err := CommitSimpleConsumerOffset(ctx, legacy, "g", "orders", 0, 1, ""); err != nil {
		t.Fatal(err)
	}
}

func TestCommitSimpleConsumerOffsetRacesJoin(t *testing.T) {
	ctx := context.Background()
	endpoints := testutil.StartEmbeddedEtcd(t)
	base := newTestEtcdStore(t, ctx, ClusterMetadata{}, endpoints)
	joined := false
	interceptor := func(callCtx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if strings.HasSuffix(method, "/Txn") && !joined {
			joined = true
			if err := base.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "g", GenerationId: 1, Members: map[string]*metadatapb.GroupMember{"m1": {}}}); err != nil {
				return err
			}
		}
		return invoke(callCtx, method, req, reply, cc, opts...)
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialOptions: []grpc.DialOption{grpc.WithChainUnaryInterceptor(interceptor)}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	store := &EtcdStore{client: client}
	if err := store.CommitSimpleConsumerOffset(ctx, "g", "orders", 0, 9, ""); !errors.Is(err, ErrConsumerGroupNotEmpty) {
		t.Fatal(err)
	}
	if offsets, _ := base.ListGroupConsumerOffsets(ctx, "g"); len(offsets) != 0 {
		t.Fatal(offsets)
	}
	if group, _ := base.FetchConsumerGroup(ctx, "g"); group == nil || len(group.Members) != 1 {
		t.Fatal("join was overwritten", group)
	}
}
