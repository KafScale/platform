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

package broker

import (
	"context"
	"testing"
	"time"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

func TestCoordinatorRetentionLifecycle(t *testing.T) {
	ctx := context.Background()
	for _, disabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "enabled", true: "disabled"}[disabled], func(t *testing.T) {
			store := metadata.NewInMemoryStore(disconnectGroupMetadata())
			retention := time.Hour
			if disabled {
				retention = -1
			}
			deleted := 0
			c := NewGroupCoordinator(store, protocol.MetadataBroker{NodeID: 7}, &CoordinatorConfig{CleanupInterval: time.Hour, OffsetsRetention: retention, GroupDeleted: func(string) { deleted++ }})
			t.Cleanup(c.Stop)
			now := time.Now()
			old := now.Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano)
			if err := store.CommitConsumerOffset(ctx, "expired", "orders", 0, 42, ""); err != nil {
				t.Fatal(err)
			}
			if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "expired", State: "empty", EmptySince: old}); err != nil {
				t.Fatal(err)
			}
			active := joinTestGroup(t, c, "")
			syncTestGroup(t, c, active.MemberID, active.Generation, 0)
			if err := store.CommitConsumerOffset(ctx, "readers", "orders", 0, 7, ""); err != nil {
				t.Fatal(err)
			}
			c.cleanupRetainedGroups(now)
			group, err := store.FetchConsumerGroup(ctx, "expired")
			if err != nil || (group == nil) == disabled {
				t.Fatalf("disabled=%t expiry: %+v, %v", disabled, group, err)
			}
			if group, err := store.FetchConsumerGroup(ctx, "readers"); err != nil || group == nil || len(group.Members) != 1 {
				t.Fatalf("active group expired: %+v, %v", group, err)
			}
			if (deleted == 0) != disabled {
				t.Fatalf("deletion callback count=%d disabled=%t", deleted, disabled)
			}
			// Recovery must preserve the original Empty timestamp, not restart it.
			restarted := NewGroupCoordinator(store, protocol.MetadataBroker{NodeID: 7}, &CoordinatorConfig{CleanupInterval: time.Hour, OffsetsRetention: retention})
			t.Cleanup(restarted.Stop)
			restarted.cleanupRetainedGroups(now.Add(time.Minute))
			if group, _ := store.FetchConsumerGroup(ctx, "expired"); (group == nil) == disabled {
				t.Fatal("restart changed retention result")
			}
		})
	}
}

func TestRetentionRecoversExpiredSessions(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewInMemoryStore(disconnectGroupMetadata())
	old := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "abandoned", State: "stable", GenerationId: 1, Members: map[string]*metadatapb.GroupMember{"dead": {HeartbeatAt: old, SessionTimeoutMs: 1000}}}); err != nil {
		t.Fatal(err)
	}
	c := newTestCoordinator(t, store)
	c.cleanupRetainedGroups(time.Now())
	group, err := store.FetchConsumerGroup(ctx, "abandoned")
	if err != nil || group == nil || len(group.Members) != 0 || group.EmptySince == "" {
		t.Fatalf("restart did not transition expired sessions to Empty: %+v, %v", group, err)
	}
	// It receives a full retention period from becoming Empty, not from its
	// last heartbeat or the restart's first scan.
	if group.State != "empty" {
		t.Fatalf("unexpected state: %+v", group)
	}
}

func TestRetentionRequiresOwnership(t *testing.T) {
	for _, allow := range []bool{false, true} {
		t.Run(map[bool]string{false: "other-owner", true: "acquire"}[allow], func(t *testing.T) {
			store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
			old := time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano)
			if err := store.PutConsumerGroup(context.Background(), &metadatapb.ConsumerGroup{GroupId: "expired", State: "empty", EmptySince: old}); err != nil {
				t.Fatal(err)
			}
			owned := false
			c := NewGroupCoordinator(store, protocol.MetadataBroker{NodeID: 7}, &CoordinatorConfig{CleanupInterval: time.Hour, OffsetsRetention: time.Hour, OwnsGroup: func(string) bool { return owned }, AcquireGroup: func(context.Context, string) error {
				if allow {
					owned = true
					return nil
				}
				return metadata.ErrNotOwner
			}})
			t.Cleanup(c.Stop)
			c.cleanupRetainedGroups(time.Now())
			group, _ := store.FetchConsumerGroup(context.Background(), "expired")
			if (group == nil) != allow {
				t.Fatalf("allow=%t group=%+v", allow, group)
			}
		})
	}
}
