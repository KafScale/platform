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

func TestOffsetFetchAllRespectsTopicAndGroupACLs(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"reader","allow":[{"action":"group_read","resource":"group","name":"g"},{"action":"fetch","resource":"topic","name":"orders"}]}]}`)
	store := metadata.NewInMemoryStore(defaultMetadata())
	for _, topic := range []string{"orders", "private"} {
		if err := store.CommitConsumerOffset(context.Background(), "g", topic, 0, 42, "secret-checkpoint"); err != nil {
			t.Fatal(err)
		}
	}
	h := newTestHandler(store)
	ctx := broker.ContextWithConnInfo(context.Background(), &broker.ConnContext{Principal: "reader"})
	// The header deliberately names another client; connection identity must win.
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyOffsetFetch, APIVersion: 5, ClientID: kmsg.StringPtr("spoofed")}
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Version, req.Group = 5, "g"
	data, err := h.Handle(ctx, header, req)
	if err != nil {
		t.Fatal(err)
	}
	all := decodeKmsgResponse(t, 5, data, kmsg.NewPtrOffsetFetchResponse)
	if all.ErrorCode != 0 || len(all.Topics) != 1 || all.Topics[0].Topic != "orders" || all.Topics[0].Partitions[0].Offset != 42 {
		t.Fatal(all)
	}
	req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "orders", Partitions: []int32{0}}, {Topic: "private", Partitions: []int32{0}}}
	data, err = h.Handle(ctx, header, req)
	if err != nil {
		t.Fatal(err)
	}
	explicit := decodeKmsgResponse(t, 5, data, kmsg.NewPtrOffsetFetchResponse)
	denied := explicit.Topics[1].Partitions[0]
	if explicit.Topics[0].Partitions[0].Offset != 42 || denied.ErrorCode != protocol.TOPIC_AUTHORIZATION_FAILED || denied.Offset != -1 || denied.Metadata != nil || denied.LeaderEpoch != -1 {
		t.Fatal(explicit)
	}
	req.Topics = []kmsg.OffsetFetchRequestTopic{}
	data, err = h.Handle(ctx, header, req)
	if err != nil {
		t.Fatal(err)
	}
	if empty := decodeKmsgResponse(t, 5, data, kmsg.NewPtrOffsetFetchResponse); len(empty.Topics) != 0 {
		t.Fatal(empty)
	}
	req.Topics = nil
	req.Group = "denied-group"
	data, err = h.Handle(ctx, header, req)
	if err != nil {
		t.Fatal(err)
	}
	if denied := decodeKmsgResponse(t, 5, data, kmsg.NewPtrOffsetFetchResponse); denied.ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED || len(denied.Topics) != 0 {
		t.Fatal(denied)
	}
}
