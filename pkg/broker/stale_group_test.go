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

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestDeleteGroupsExpiresStaleMembers(t *testing.T) {
	for _, backend := range []string{"memory", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var store metadata.Store = metadata.NewInMemoryStore(metadata.ClusterMetadata{})
			if backend == "etcd" {
				etcd, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = etcd.Close() })
				store = etcd
			}
			for _, cached := range []bool{false, true} {
				for _, tc := range []struct {
					name      string
					age       time.Duration
					timeout   int32
					heartbeat string
					mixed     bool
					want      int16
				}{
					{name: "expired", age: time.Hour, timeout: 1000},
					{name: "fresh", timeout: 60000, want: protocol.NON_EMPTY_GROUP},
					{name: "mixed", age: time.Hour, timeout: 1000, mixed: true, want: protocol.NON_EMPTY_GROUP},
					{name: "default timeout expired", age: time.Hour},
					{name: "default timeout fresh", want: protocol.NON_EMPTY_GROUP},
					{name: "long session still alive", age: time.Hour, timeout: int32((2 * time.Hour) / time.Millisecond), want: protocol.NON_EMPTY_GROUP},
					{name: "missing heartbeat", heartbeat: "missing"},
					{name: "malformed heartbeat", heartbeat: "not-a-time"},
				} {
					name := "stored/"
					if cached {
						name = "cached/"
					}
					t.Run(name+tc.name, func(t *testing.T) {
						heartbeat := time.Now().Add(-tc.age).UTC().Format(time.RFC3339Nano)
						if tc.heartbeat != "" {
							heartbeat = tc.heartbeat
						}
						if heartbeat == "missing" {
							heartbeat = ""
						}
						members := map[string]*metadatapb.GroupMember{"old": {HeartbeatAt: heartbeat, SessionTimeoutMs: tc.timeout}}
						if tc.mixed {
							members["live"] = &metadatapb.GroupMember{HeartbeatAt: time.Now().UTC().Format(time.RFC3339Nano), SessionTimeoutMs: 60000}
						}
						seed := &metadatapb.ConsumerGroup{GroupId: "stale", State: groupStateStableStr, GenerationId: 4, Leader: "old", ProtocolType: "consumer", Protocol: "range", Members: members}
						if err := store.PutConsumerGroup(ctx, seed); err != nil {
							t.Fatal(err)
						}
						if err := store.CommitConsumerOffset(ctx, "stale", "orders", 0, 42, "checkpoint"); err != nil {
							t.Fatal(err)
						}
						c := newTestCoordinator(t, store)
						if cached {
							if _, err := c.loadGroupIfMissing(ctx, "stale"); err != nil {
								t.Fatal(err)
							}
						}
						req := kmsg.NewPtrDeleteGroupsRequest()
						req.Groups = []string{"stale"}
						resp, err := c.DeleteGroups(ctx, req)
						if err != nil || len(resp.Groups) != 1 || resp.Groups[0].ErrorCode != tc.want {
							t.Fatalf("delete: %+v, %v; want %d", resp, err, tc.want)
						}
						group, err := store.FetchConsumerGroup(ctx, "stale")
						if err != nil {
							t.Fatal(err)
						}
						offsets, err := store.ListGroupConsumerOffsets(ctx, "stale")
						if err != nil {
							t.Fatal(err)
						}
						if tc.want == 0 {
							if group != nil || len(offsets) != 0 || c.groups["stale"] != nil {
								t.Fatalf("stale group survived deletion: %+v, %+v", group, offsets)
							}
						} else {
							if c.groups["stale"] == nil {
								t.Fatal("surviving stored members were not cached for background expiry")
							}
							if group == nil || len(offsets) != 1 || offsets[0].Offset != 42 || offsets[0].Metadata != "checkpoint" {
								t.Fatal(group, offsets)
							}
							if tc.mixed {
								if len(group.Members) != 1 || group.Members["live"] == nil || group.Leader != "live" || group.GenerationId != 5 || group.State != groupStatePreparingStr {
									t.Fatalf("partial expiry: %+v", group)
								}
							} else if len(group.Members) != 1 || group.GenerationId != 4 {
								t.Fatalf("fresh membership changed: %+v", group)
							}
						}
						// Keep table cases independent while using one embedded etcd instance.
						if err := store.DeleteConsumerGroup(ctx, "stale"); err != nil {
							t.Fatal(err)
						}
					})
				}
			}
		})
	}
}

func TestDeleteStaleGroupPersistenceFailures(t *testing.T) {
	for _, fail := range []string{"put", "delete"} {
		t.Run(fail, func(t *testing.T) {
			ctx := context.Background()
			store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
			if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "stale", State: groupStateStableStr, GenerationId: 4, Leader: "dead", Protocol: "range", Members: map[string]*metadatapb.GroupMember{"dead": {HeartbeatAt: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano), SessionTimeoutMs: 1000}}}); err != nil {
				t.Fatal(err)
			}
			if err := store.CommitConsumerOffset(ctx, "stale", "orders", 0, 42, "checkpoint"); err != nil {
				t.Fatal(err)
			}
			c := newTestCoordinator(t, coordinatorFaultStore{Store: store, fail: fail})
			req := kmsg.NewPtrDeleteGroupsRequest()
			req.Groups = []string{"stale"}
			resp, err := c.DeleteGroups(ctx, req)
			if err != nil || resp.Groups[0].ErrorCode != protocol.UNKNOWN_SERVER_ERROR {
				t.Fatal(resp, err)
			}
			offsets, err := store.ListGroupConsumerOffsets(ctx, "stale")
			if err != nil || len(offsets) != 1 || offsets[0].Offset != 42 {
				t.Fatal(offsets, err)
			}
			if fail == "delete" {
				group, err := store.FetchConsumerGroup(ctx, "stale")
				if err != nil || group == nil || len(group.Members) != 0 || group.State != groupStateEmptyStr || group.GenerationId != 5 || group.Leader != "" || group.Protocol != "" {
					t.Fatalf("expiry must persist before deletion: %+v, %v", group, err)
				}
			}
			c.store = store
			resp, err = c.DeleteGroups(ctx, req)
			if err != nil || resp.Groups[0].ErrorCode != 0 {
				t.Fatalf("retry: %+v, %v", resp, err)
			}
			if group, err := store.FetchConsumerGroup(ctx, "stale"); err != nil || group != nil {
				t.Fatal(group, err)
			}
			if offsets, err := store.ListGroupConsumerOffsets(ctx, "stale"); err != nil || len(offsets) != 0 {
				t.Fatal(offsets, err)
			}
		})
	}
}
