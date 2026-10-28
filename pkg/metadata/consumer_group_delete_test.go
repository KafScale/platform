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
	"fmt"
	"strings"
	"testing"

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Embedding only the preexisting Store contract must not expose the optional
// deletion method, even when the wrapped built-in store implements it.
type legacyDeletionStore struct {
	Store
	listErr, errorOnDelete error
}

var _ Store = legacyDeletionStore{}

func (s legacyDeletionStore) ListGroupConsumerOffsets(ctx context.Context, group string) ([]ConsumerOffset, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.Store.ListGroupConsumerOffsets(ctx, group)
}
func (s legacyDeletionStore) DeleteConsumerGroup(ctx context.Context, group string) error {
	if s.errorOnDelete != nil {
		return s.errorOnDelete
	}
	return s.Store.DeleteConsumerGroup(ctx, group)
}

func TestOptionalConsumerGroupDeletion(t *testing.T) {
	ctx := context.Background()
	base := NewInMemoryStore(ClusterMetadata{})
	legacy := legacyDeletionStore{Store: base}
	if _, ok := any(legacy).(ConsumerGroupDeleter); ok {
		t.Fatal("Store requires the optional capability")
	}
	if err := base.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "g"}); err != nil {
		t.Fatal(err)
	}
	if err := DeleteConsumerGroupAndOffsets(ctx, legacy, "g"); err != nil {
		t.Fatal(err)
	}
	if group, _ := base.FetchConsumerGroup(ctx, "g"); group != nil {
		t.Fatal("legacy metadata-only deletion failed")
	}
	if err := base.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "g"}); err != nil {
		t.Fatal(err)
	}
	if err := base.CommitConsumerOffset(ctx, "g", "orders", 0, 42, "checkpoint"); err != nil {
		t.Fatal(err)
	}
	if err := DeleteConsumerGroupAndOffsets(ctx, legacy, "g"); !errors.Is(err, ErrAtomicGroupDeletionUnsupported) {
		t.Fatal(err)
	}
	if group, _ := base.FetchConsumerGroup(ctx, "g"); group == nil {
		t.Fatal("unsupported deletion mutated group")
	}
	if offset, _, _ := base.FetchConsumerOffset(ctx, "g", "orders", 0); offset != 42 {
		t.Fatal("unsupported deletion changed offsets")
	}
	sentinel := errors.New("fixture")
	if err := DeleteConsumerGroupAndOffsets(ctx, legacyDeletionStore{Store: base, listErr: sentinel}, "g"); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if err := DeleteConsumerGroupAndOffsets(ctx, legacyDeletionStore{Store: base, errorOnDelete: sentinel}, "missing"); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	// Invalid internal keys and other groups must remain untouched in memory.
	base.consumerOffsets["invalid-key"] = 99
	if err := DeleteConsumerGroupAndOffsets(ctx, base, "g"); err != nil {
		t.Fatal(err)
	}
	if base.consumerOffsets["invalid-key"] != 99 {
		t.Fatal("removed unrelated internal key")
	}
}

func TestOwnedConsumerOffsetKey(t *testing.T) {
	prefix := "/kafscale/consumers/a/offsets/"
	for _, tc := range []struct {
		key   string
		owned bool
	}{{prefix + "orders/0", true}, {prefix + "orders/-1", true}, {prefix + "orders/no", false}, {prefix + "orders/2147483648", false}, {prefix + "victim/metadata", false}, {prefix + "victim/offsets/orders/0", false}, {prefix + "/0", false}, {"/other/orders/0", false}, {prefix, false}} {
		if got := ownedConsumerOffsetKey(tc.key, prefix); got != tc.owned {
			t.Fatal(tc, got)
		}
	}
}

func TestEtcdDeletionPreservesNestedGroupIDs(t *testing.T) {
	ctx := context.Background()
	store := newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t))
	for _, target := range []string{"a", "a/offsets/victim"} {
		t.Run(target, func(t *testing.T) {
			if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: target}); err != nil {
				t.Fatal(err)
			}
			victims := []string{target + "/offsets/mid", target + "/offsets/zzz", target + "-other"}
			for _, victim := range victims {
				if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: victim}); err != nil {
					t.Fatal(err)
				}
				if err := store.CommitConsumerOffset(ctx, victim, "orders", 0, 99, "victim"); err != nil {
					t.Fatal(err)
				}
			}
			// More offsets than etcd's default transaction-operation limit; compacted
			// ranges must still leave the interleaved foreign group keys untouched.
			for i := 0; i < 150; i++ {
				for _, topic := range []string{"alpha", "zeta"} {
					if err := store.CommitConsumerOffset(ctx, target, topic, int32(i), 42, ""); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := store.DeleteConsumerGroupAndOffsets(ctx, target); err != nil {
				t.Fatal(err)
			}
			if group, err := store.FetchConsumerGroup(ctx, target); err != nil || group != nil {
				t.Fatal(group, err)
			}
			for _, topic := range []string{"alpha", "zeta"} {
				if offset, _, err := store.FetchConsumerOffset(ctx, target, topic, 0); err != nil || offset != 0 {
					t.Fatal(offset, err)
				}
			}
			for _, victim := range victims {
				if group, err := store.FetchConsumerGroup(ctx, victim); err != nil || group == nil || group.GroupId != victim {
					t.Fatal("deleted foreign group", victim, group, err)
				}
				if offset, meta, err := store.FetchConsumerOffset(ctx, victim, "orders", 0); err != nil || offset != 99 || meta != "victim" {
					t.Fatal("deleted foreign offset", victim, offset, err)
				}
			}
		})
	}
	// A target with no offsets may still have nested foreign groups.
	if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "empty/offsets/child"}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteConsumerGroupAndOffsets(ctx, "empty"); err != nil {
		t.Fatal(err)
	}
	if group, _ := store.FetchConsumerGroup(ctx, "empty/offsets/child"); group == nil {
		t.Fatal("empty parent deleted child")
	}
}

func TestEtcdDeletionFencesConcurrentWrites(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()
	base := newTestEtcdStore(t, ctx, ClusterMetadata{}, endpoints)
	for _, mode := range []string{"insert_foreign", "change_group", "txn_error"} {
		t.Run(mode, func(t *testing.T) {
			groupID := "race-" + mode
			if err := base.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: groupID}); err != nil {
				t.Fatal(err)
			}
			if err := base.CommitConsumerOffset(ctx, groupID, "alpha", 0, 42, ""); err != nil {
				t.Fatal(err)
			}
			attempts := 0
			interceptor := func(callCtx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if strings.HasSuffix(method, "/Txn") {
					attempts++
					switch mode {
					case "txn_error":
						return status.Error(codes.PermissionDenied, "fixture transaction denied")
					case "insert_foreign":
						if attempts == 1 {
							if err := base.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: groupID + "/offsets/victim"}); err != nil {
								return err
							}
							if err := base.CommitConsumerOffset(ctx, groupID+"/offsets/victim", "orders", 0, 99, ""); err != nil {
								return err
							}
						}
					case "change_group":
						if err := base.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: groupID, GenerationId: int32(attempts)}); err != nil {
							return err
						}
					}
				}
				return invoke(callCtx, method, req, reply, cc, opts...)
			}
			cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialOptions: []grpc.DialOption{grpc.WithChainUnaryInterceptor(interceptor)}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { cli.Close() })
			store := &EtcdStore{client: cli}
			err = store.DeleteConsumerGroupAndOffsets(ctx, groupID)
			switch mode {
			case "insert_foreign":
				if err != nil || attempts != 2 {
					t.Fatal("did not retry after concurrent insertion", attempts, err)
				}
				if group, _ := base.FetchConsumerGroup(ctx, groupID+"/offsets/victim"); group == nil {
					t.Fatal("deleted concurrently inserted foreign group")
				}
				if offset, _, _ := base.FetchConsumerOffset(ctx, groupID+"/offsets/victim", "orders", 0); offset != 99 {
					t.Fatal("deleted concurrent foreign offset")
				}
			case "change_group":
				if !errors.Is(err, ErrConsumerGroupChanged) || attempts != 3 {
					t.Fatal(attempts, err)
				}
			case "txn_error":
				if err == nil {
					t.Fatal("transaction error ignored")
				}
			}
			if err != nil {
				if group, readErr := base.FetchConsumerGroup(ctx, groupID); readErr != nil || group == nil {
					t.Fatal("failed transaction removed group", group, readErr)
				}
				if offset, _, readErr := base.FetchConsumerOffset(ctx, groupID, "alpha", 0); readErr != nil || offset != 42 {
					t.Fatal("failed transaction removed offsets", offset, readErr)
				}
			}
		})
	}
}

func ExampleConsumerGroupDeleter() {
	var store Store = NewInMemoryStore(ClusterMetadata{})
	_, supportsAtomicDeletion := store.(ConsumerGroupDeleter)
	fmt.Println(supportsAtomicDeletion)
	// Output: true
}
