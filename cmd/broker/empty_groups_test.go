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
	"github.com/KafScale/platform/pkg/broker"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	"testing"
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
