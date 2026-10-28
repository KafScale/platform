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

func TestJoinGroupRecordsClientDisplayMetadata(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"service","allow":[{"action":"group_write","resource":"group","name":"readers"},{"action":"group_read","resource":"group","name":"readers"}]}]}`)
	h := newTestHandler(metadata.NewInMemoryStore(defaultMetadata()))
	t.Cleanup(h.coordinator.Stop)
	ctx := broker.ContextWithConnInfo(context.Background(), &broker.ConnContext{Principal: "service", RemoteAddr: "198.51.100.9:4321"})
	req := kmsg.NewPtrJoinGroupRequest()
	req.Version, req.Group, req.ProtocolType = 5, "readers", "consumer"
	subscription := kmsg.NewConsumerMemberMetadata()
	subscription.Topics = []string{"orders"}
	req.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: subscription.AppendTo(nil)}}
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyJoinGroup, APIVersion: 5, ClientID: kmsg.StringPtr("display-only")}
	data, err := h.Handle(ctx, header, req)
	if err != nil {
		t.Fatal(err)
	}
	joined := decodeKmsgResponse(t, 5, data, kmsg.NewPtrJoinGroupResponse)
	if joined.ErrorCode != 0 {
		t.Fatal(joined)
	}
	describe := kmsg.NewPtrDescribeGroupsRequest()
	describe.Version, describe.Groups = 5, []string{"readers"}
	header.APIKey, header.APIVersion = protocol.APIKeyDescribeGroups, 5
	data, err = h.Handle(ctx, header, describe)
	if err != nil {
		t.Fatal(err)
	}
	resp := decodeKmsgResponse(t, 5, data, kmsg.NewPtrDescribeGroupsResponse)
	if resp.Groups[0].ErrorCode != 0 || len(resp.Groups[0].Members) != 1 {
		t.Fatal(resp)
	}
	member := resp.Groups[0].Members[0]
	if member.ClientID != "display-only" || member.ClientHost != "198.51.100.9" || len(member.ProtocolMetadata) == 0 {
		t.Fatal(member)
	}
}
