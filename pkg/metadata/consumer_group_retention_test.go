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
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConsumerGroupRetentionStores(t *testing.T) {
	ctx := context.Background()
	stores := map[string]Store{"memory": NewInMemoryStore(ClusterMetadata{}), "etcd": newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t))}
	for name, store := range stores {
		t.Run(name, func(t *testing.T) {
			now := time.Now().UTC()
			old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
			for _, id := range []string{"expired", "fresh", "active", "expired/offsets/child", "legacy"} {
				if err := store.CommitConsumerOffset(ctx, id, "orders", 0, 42, ""); err != nil {
					t.Fatal(err)
				}
				if id == "legacy" {
					continue
				}
				group := &metadatapb.ConsumerGroup{GroupId: id, State: "empty", EmptySince: old}
				if id == "fresh" || id == "expired/offsets/child" {
					group.EmptySince = now.Format(time.RFC3339Nano)
				}
				if id == "active" {
					group.Members = map[string]*metadatapb.GroupMember{"member": {}}
				}
				if err := store.PutConsumerGroup(ctx, group); err != nil {
					t.Fatal(err)
				}
			}
			retainer := store.(ConsumerGroupRetainer)
			cutoff := now.Add(-24 * time.Hour)
			// A current raw commit must protect even old group metadata. Age the etcd
			// offset fixture as well so this represents a genuinely inactive group.
			if etcd, ok := store.(*EtcdStore); ok {
				payload, _ := json.Marshal(consumerOffsetRecord{Offset: 42, CommittedAt: old})
				if _, err := etcd.client.Put(ctx, consumerOffsetKey("expired", "orders", 0), string(payload)); err != nil {
					t.Fatal(err)
				}
			}
			if deleted, err := retainer.ExpireConsumerGroup(ctx, "expired", cutoff, now); err != nil || !deleted {
				t.Fatalf("expiry: %v, %v", deleted, err)
			}
			if group, err := store.FetchConsumerGroup(ctx, "expired"); err != nil || group != nil {
				t.Fatalf("expired metadata remains: %+v, %v", group, err)
			}
			if offsets, err := store.ListGroupConsumerOffsets(ctx, "expired"); err != nil || len(offsets) != 0 {
				t.Fatalf("expired offsets remain: %+v, %v", offsets, err)
			}
			for _, id := range []string{"fresh", "active", "expired/offsets/child", "legacy", "missing"} {
				if deleted, err := retainer.ExpireConsumerGroup(ctx, id, cutoff, now); err != nil || deleted {
					t.Fatalf("unexpected deletion %s: %v, %v", id, deleted, err)
				}
			}
			legacy, err := store.FetchConsumerGroup(ctx, "legacy")
			if err != nil || legacy == nil || emptySince(legacy).IsZero() {
				t.Fatalf("legacy clock not persisted: %+v, %v", legacy, err)
			}
			if deleted, err := retainer.ExpireConsumerGroup(ctx, "legacy", now.Add(time.Hour), now.Add(2*time.Hour)); err != nil || !deleted {
				t.Fatalf("legacy expiry after retention: %v, %v", deleted, err)
			}
			if offsets, err := store.ListGroupConsumerOffsets(ctx, "expired/offsets/child"); err != nil || len(offsets) != 1 {
				t.Fatalf("nested group's offsets removed: %+v, %v", offsets, err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := retainer.ExpireConsumerGroup(canceled, "fresh", cutoff, now); err == nil {
				t.Fatal("canceled expiry succeeded")
			}
		})
	}
}

func TestConsumerGroupRetentionClockReset(t *testing.T) {
	ctx := context.Background()
	stores := map[string]Store{"memory": NewInMemoryStore(ClusterMetadata{}), "etcd": newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t))}
	for name, store := range stores {
		t.Run(name, func(t *testing.T) {
			old := time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano)
			group := &metadatapb.ConsumerGroup{GroupId: "clock", State: "empty", EmptySince: old}
			if err := store.PutConsumerGroup(ctx, group); err != nil {
				t.Fatal(err)
			}
			// Persisting Empty metadata again must not postpone expiry.
			group.EmptySince = ""
			if err := store.PutConsumerGroup(ctx, group); err != nil {
				t.Fatal(err)
			}
			saved, _ := store.FetchConsumerGroup(ctx, "clock")
			if saved.EmptySince != old {
				t.Fatalf("metadata write reset clock: %+v", saved)
			}
			if err := store.CommitConsumerOffset(ctx, "clock", "orders", 0, 7, ""); err != nil {
				t.Fatal(err)
			}
			saved, _ = store.FetchConsumerGroup(ctx, "clock")
			if emptySince(saved).Before(time.Now().Add(-time.Minute)) {
				t.Fatal("raw commit did not reset clock")
			}
			if err := CommitConsumerOffsetIfGroupExists(ctx, store, "clock", "orders", 0, 8, ""); err != nil {
				t.Fatal(err)
			}
			saved, _ = store.FetchConsumerGroup(ctx, "clock")
			if emptySince(saved).Before(time.Now().Add(-time.Minute)) {
				t.Fatal("conditional commit did not reset clock")
			}
			group.Members = map[string]*metadatapb.GroupMember{"live": {}}
			if err := store.PutConsumerGroup(ctx, group); err != nil {
				t.Fatal(err)
			}
			saved, _ = store.FetchConsumerGroup(ctx, "clock")
			if saved.EmptySince != "" {
				t.Fatal("rejoin did not clear clock")
			}
			group.Members = nil
			group.EmptySince = old
			if err := store.PutConsumerGroup(ctx, group); err != nil {
				t.Fatal(err)
			}
			saved, _ = store.FetchConsumerGroup(ctx, "clock")
			if emptySince(saved).Before(time.Now().Add(-time.Minute)) {
				t.Fatal("leaving did not start a new clock")
			}
			if deleted, err := store.(ConsumerGroupRetainer).ExpireConsumerGroup(ctx, "clock", time.Now().Add(-time.Hour), time.Now()); err != nil || deleted {
				t.Fatalf("reset group expired: %v, %v", deleted, err)
			}
		})
	}
}

func TestEtcdRetentionFencesConcurrentChanges(t *testing.T) {
	ctx := context.Background()
	endpoints := testutil.StartEmbeddedEtcd(t)
	base := newTestEtcdStore(t, ctx, ClusterMetadata{}, endpoints)
	for _, mode := range []string{"join", "commit", "legacy_commit", "initialize"} {
		t.Run(mode, func(t *testing.T) {
			id := "retention-" + mode
			old := time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano)
			group := &metadatapb.ConsumerGroup{GroupId: id, State: "empty", EmptySince: old}
			if mode == "initialize" {
				group.EmptySince = ""
			}
			encoded, _ := EncodeConsumerGroup(group)
			if _, err := base.client.Put(ctx, ConsumerGroupKey(id), string(encoded)); err != nil {
				t.Fatal(err)
			}
			payload, _ := json.Marshal(consumerOffsetRecord{Offset: 42, CommittedAt: old})
			if _, err := base.client.Put(ctx, consumerOffsetKey(id, "orders", 0), string(payload)); err != nil {
				t.Fatal(err)
			}
			changed := false
			interceptor := func(callCtx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if strings.HasSuffix(method, "/Txn") && !changed {
					changed = true
					switch mode {
					case "join":
						group.Members = map[string]*metadatapb.GroupMember{"new": {}}
						if err := base.PutConsumerGroup(ctx, group); err != nil {
							return err
						}
					case "legacy_commit":
						fresh, _ := json.Marshal(consumerOffsetRecord{Offset: 99, CommittedAt: time.Now().UTC().Format(time.RFC3339Nano)})
						if _, err := base.client.Put(ctx, consumerOffsetKey(id, "orders", 0), string(fresh)); err != nil {
							return err
						}
					default:
						if err := base.CommitConsumerOffset(ctx, id, "orders", 0, 99, ""); err != nil {
							return err
						}
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
			if deleted, err := store.ExpireConsumerGroup(ctx, id, time.Now().Add(-24*time.Hour), time.Now()); deleted || !errors.Is(err, ErrConsumerGroupChanged) {
				t.Fatalf("unfenced %s: %v, %v", mode, deleted, err)
			}
			if got, _, err := base.FetchConsumerOffset(ctx, id, "orders", 0); err != nil || (mode != "join" && got != 99) || (mode == "join" && got != 42) {
				t.Fatalf("concurrent %s lost offset: %d, %v", mode, got, err)
			}
		})
	}
}

func TestEtcdRetentionHonorsLegacyCommitTime(t *testing.T) {
	ctx := context.Background()
	store := newTestEtcdStore(t, ctx, ClusterMetadata{}, testutil.StartEmbeddedEtcd(t))
	old := time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano)
	if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "legacy-write", State: "empty", EmptySince: old}); err != nil {
		t.Fatal(err)
	}
	fresh := time.Now().UTC().Format(time.RFC3339Nano)
	payload, _ := json.Marshal(consumerOffsetRecord{Offset: 42, CommittedAt: fresh})
	if _, err := store.client.Put(ctx, consumerOffsetKey("legacy-write", "orders", 0), string(payload)); err != nil {
		t.Fatal(err)
	}
	if deleted, err := store.ExpireConsumerGroup(ctx, "legacy-write", time.Now().Add(-time.Hour), time.Now()); err != nil || deleted {
		t.Fatalf("fresh legacy commit expired: %v, %v", deleted, err)
	}
	group, _ := store.FetchConsumerGroup(ctx, "legacy-write")
	if group.EmptySince != fresh {
		t.Fatalf("legacy commit did not reset persisted clock: %+v", group)
	}
}

func TestRetentionCommitFencesAndStoreErrors(t *testing.T) {
	ctx := context.Background()
	endpoints := testutil.StartEmbeddedEtcd(t)
	base := newTestEtcdStore(t, ctx, ClusterMetadata{}, endpoints)
	for _, mode := range []string{"read_error", "write_error", "changed"} {
		t.Run(mode, func(t *testing.T) {
			id := "commit-" + mode
			if err := base.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: id, State: "empty"}); err != nil {
				t.Fatal(err)
			}
			attempts := 0
			interceptor := func(callCtx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if mode == "read_error" && strings.HasSuffix(method, "/Range") {
					return status.Error(codes.PermissionDenied, "fixture read denied")
				}
				if strings.HasSuffix(method, "/Txn") {
					attempts++
					if mode == "write_error" {
						return status.Error(codes.PermissionDenied, "fixture write denied")
					}
					if mode == "changed" {
						if err := base.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: id, GenerationId: int32(attempts)}); err != nil {
							return err
						}
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
			err = store.CommitConsumerOffsetIfGroupExists(ctx, id, "orders", 0, 99, "")
			if mode == "changed" {
				if !errors.Is(err, ErrConsumerGroupChanged) || attempts != 3 {
					t.Fatalf("unbounded or unfenced retry: attempts=%d err=%v", attempts, err)
				}
			} else if err == nil {
				t.Fatal("store error ignored")
			}
			if offsets, err := base.ListGroupConsumerOffsets(ctx, id); err != nil || len(offsets) != 0 {
				t.Fatalf("failed commit changed offsets: %+v, %v", offsets, err)
			}
		})
	}
}
