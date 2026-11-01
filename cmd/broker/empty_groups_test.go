// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package main

import (
	"context"
	"testing"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/broker"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestOffsetOnlyGroupVisibilityUsesGroupACLs(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"service","allow":[{"action":"group_read","resource":"group","name":"allowed"}]},{"name":"inspector","allow":[{"action":"group_read","resource":"group","name":"*"}]}]}`)
	store := metadata.NewInMemoryStore(defaultMetadata())
	h := newTestHandler(store)
	t.Cleanup(h.coordinator.Stop)
	for _, group := range []string{"allowed", "private"} {
		if err := store.CommitConsumerOffset(context.Background(), group, "orders", 0, 42, ""); err != nil {
			t.Fatal(err)
		}
	}
	ctx := broker.ContextWithConnInfo(context.Background(), &broker.ConnContext{Principal: "service"})
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyListGroups, APIVersion: 4, ClientID: kmsg.StringPtr("spoofed")}
	data, err := h.Handle(ctx, header, kmsg.NewPtrListGroupsRequest())
	if err != nil {
		t.Fatal(err)
	}
	listed := decodeKmsgResponse(t, 4, data, kmsg.NewPtrListGroupsResponse)
	if listed.ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED || len(listed.Groups) != 0 {
		t.Fatal(listed)
	}
	inspector := broker.ContextWithConnInfo(context.Background(), &broker.ConnContext{Principal: "inspector"})
	data, err = h.Handle(inspector, header, kmsg.NewPtrListGroupsRequest())
	if err != nil {
		t.Fatal(err)
	}
	listed = decodeKmsgResponse(t, 4, data, kmsg.NewPtrListGroupsResponse)
	if listed.ErrorCode != 0 || len(listed.Groups) != 2 {
		t.Fatal(listed)
	}
	for _, group := range listed.Groups {
		if group.GroupState != "Empty" {
			t.Fatal(group)
		}
	}
	header.APIKey, header.APIVersion = protocol.APIKeyDescribeGroups, 5
	describe := kmsg.NewPtrDescribeGroupsRequest()
	describe.Groups = []string{"allowed", "private"}
	data, err = h.Handle(ctx, header, describe)
	if err != nil {
		t.Fatal(err)
	}
	described := decodeKmsgResponse(t, 5, data, kmsg.NewPtrDescribeGroupsResponse)
	if described.Groups[0].ErrorCode != 0 || described.Groups[0].State != "Empty" || described.Groups[1].ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatal(described)
	}
}

func TestDeleteGroupsCannotCrossNestedGroupACL(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"admin-a","allow":[{"action":"group_admin","resource":"group","name":"a"}]}]}`)
	ctx := context.Background()
	store, err := metadata.NewEtcdStore(ctx, defaultMetadata(), metadata.EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	victim := "a/offsets/victim"
	for _, group := range []string{"a", victim} {
		if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: group, State: "empty"}); err != nil {
			t.Fatal(err)
		}
		if err := store.CommitConsumerOffset(ctx, group, "orders", 0, 42, "checkpoint"); err != nil {
			t.Fatal(err)
		}
	}
	h := newTestHandler(store)
	t.Cleanup(h.coordinator.Stop)
	ctx = broker.ContextWithConnInfo(ctx, &broker.ConnContext{Principal: "admin-a"})
	req := kmsg.NewPtrDeleteGroupsRequest()
	req.Groups = []string{"a", victim}
	data, err := h.Handle(ctx, &protocol.RequestHeader{APIKey: protocol.APIKeyDeleteGroups, APIVersion: 2}, req)
	if err != nil {
		t.Fatal(err)
	}
	resp := decodeKmsgResponse(t, 2, data, kmsg.NewPtrDeleteGroupsResponse)
	if resp.Groups[0].ErrorCode != 0 || resp.Groups[1].ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED {
		t.Fatal(resp)
	}
	if record, err := store.FetchConsumerGroup(ctx, victim); err != nil || record == nil {
		t.Fatal("deletion crossed group ACL", record, err)
	}
	if offset, meta, err := store.FetchConsumerOffset(ctx, victim, "orders", 0); err != nil || offset != 42 || meta != "checkpoint" {
		t.Fatal("deletion crossed offset ACL", offset, meta, err)
	}
}

func TestDescribeGroupsOlderVersions(t *testing.T) {
	for _, entry := range generateApiVersions() {
		if entry.ApiKey == protocol.APIKeyDescribeGroups && (entry.MinVersion != 0 || entry.MaxVersion != 5) {
			t.Fatalf("DescribeGroups advertised %d..%d", entry.MinVersion, entry.MaxVersion)
		}
	}
	store := metadata.NewInMemoryStore(defaultMetadata())
	h := newTestHandler(store)
	t.Cleanup(h.coordinator.Stop)
	if err := store.CommitConsumerOffset(context.Background(), "idle", "orders", 0, 42, ""); err != nil {
		t.Fatal(err)
	}
	for version := int16(0); version <= 5; version++ {
		req := kmsg.NewPtrDescribeGroupsRequest()
		req.Version = version
		req.Groups = []string{"idle", "missing"}
		data, err := h.Handle(context.Background(), &protocol.RequestHeader{APIKey: protocol.APIKeyDescribeGroups, APIVersion: version}, req)
		if err != nil {
			t.Fatal(err)
		}
		resp := decodeKmsgResponse(t, version, data, kmsg.NewPtrDescribeGroupsResponse)
		if len(resp.Groups) != 2 || resp.Groups[0].ErrorCode != 0 || resp.Groups[0].State != "Empty" || resp.Groups[0].ProtocolType != "consumer" {
			t.Fatalf("v%d idle group %+v", version, resp.Groups)
		}
		dead := resp.Groups[1]
		if dead.ErrorCode != 0 || dead.State != "Dead" || dead.ProtocolType != "" || dead.Protocol != "" || len(dead.Members) != 0 {
			t.Fatalf("v%d missing group %+v", version, dead)
		}
	}
}
