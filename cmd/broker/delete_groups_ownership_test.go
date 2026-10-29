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

package main

import (
	"context"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/protobuf/proto"
)

func TestDeleteGroupsRequiresCoordinatorLease(t *testing.T) {
	ctx := context.Background()
	store, err := metadata.NewEtcdStore(ctx, defaultMetadata(), metadata.EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	a := newHandler(store, storage.NewMemoryS3Client(), protocol.MetadataBroker{NodeID: 1}, testLogger())
	b := newHandler(store, storage.NewMemoryS3Client(), protocol.MetadataBroker{NodeID: 2}, testLogger())
	for _, h := range []*handler{a, b} {
		t.Cleanup(func() { h.coordinator.Stop(); h.groupLeaseManager.ReleaseAll(); h.leaseManager.ReleaseAll() })
	}
	for _, tc := range []struct {
		name, heartbeat string
		takeover        int16
	}{
		{"fresh", time.Now().UTC().Format(time.RFC3339Nano), protocol.NON_EMPTY_GROUP},
		// An apparent hour of clock skew cannot authorize a non-owner to expire members.
		{"past clock skew", time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano), protocol.NONE},
		{"future clock skew", time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), protocol.NON_EMPTY_GROUP},
		{"legacy heartbeat", "", protocol.NON_EMPTY_GROUP},
	} {
		t.Run(tc.name, func(t *testing.T) {
			groupID := tc.name
			if err := a.groupLeaseManager.Acquire(ctx, groupID); err != nil {
				t.Fatal(err)
			}
			seed := &metadatapb.ConsumerGroup{GroupId: groupID, State: "stable", GenerationId: 4, Leader: "member", ProtocolType: "consumer", Members: map[string]*metadatapb.GroupMember{"member": {HeartbeatAt: tc.heartbeat, SessionTimeoutMs: 60000}}}
			if err := store.PutConsumerGroup(ctx, seed); err != nil {
				t.Fatal(err)
			}
			if err := store.CommitConsumerOffset(ctx, groupID, "orders", 0, 42, "checkpoint"); err != nil {
				t.Fatal(err)
			}
			if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "empty", State: "empty"}); err != nil {
				t.Fatal(err)
			}
			req := kmsg.NewPtrDeleteGroupsRequest()
			req.Groups = []string{groupID, "empty", ""}
			header := &protocol.RequestHeader{APIKey: protocol.APIKeyDeleteGroups, APIVersion: 2}
			payload, err := b.Handle(ctx, header, req)
			if err != nil {
				t.Fatal(err)
			}
			resp := decodeKmsgResponse(t, 2, payload, kmsg.NewPtrDeleteGroupsResponse)
			for i, code := range []int16{protocol.NOT_COORDINATOR, protocol.NONE, protocol.INVALID_REQUEST} {
				if len(resp.Groups) != 3 || resp.Groups[i].Group != req.Groups[i] || resp.Groups[i].ErrorCode != code {
					t.Fatal(resp)
				}
			}
			// Defense in depth: bypassing the handler still cannot mutate another owner's group.
			direct, err := b.coordinator.DeleteGroups(ctx, req)
			if err != nil || direct.Groups[0].ErrorCode != protocol.NOT_COORDINATOR {
				t.Fatal(direct, err)
			}
			stored, err := store.FetchConsumerGroup(ctx, groupID)
			if err != nil || !proto.Equal(stored, seed) {
				t.Fatal("non-owner changed membership", stored, err)
			}
			offsets, err := store.ListGroupConsumerOffsets(ctx, groupID)
			if err != nil || len(offsets) != 1 || offsets[0].Offset != 42 {
				t.Fatal(offsets, err)
			}
			// Once the old owner relinquishes its lease, B may recover this group.
			a.groupLeaseManager.Release(groupID)
			req.Groups = []string{groupID}
			payload, err = b.Handle(ctx, header, req)
			if err != nil {
				t.Fatal(err)
			}
			resp = decodeKmsgResponse(t, 2, payload, kmsg.NewPtrDeleteGroupsResponse)
			if resp.Groups[0].ErrorCode != tc.takeover || !b.groupLeaseManager.Owns(groupID) {
				t.Fatal(resp)
			}
		})
	}
}
