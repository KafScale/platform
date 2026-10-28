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
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	"reflect"
	"testing"
)

type groupOffsetFailureStore struct{ metadata.Store }

func (groupOffsetFailureStore) ListGroupConsumerOffsets(context.Context, string) ([]metadata.ConsumerOffset, error) {
	return nil, errCoordinatorStore
}

func TestOffsetFetchAllCommittedTopics(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	for _, offset := range []metadata.ConsumerOffset{{Group: "g", Topic: "a", Partition: 0, Offset: 42, Metadata: "checkpoint"}, {Group: "g", Topic: "a", Partition: 2, Offset: 17}, {Group: "g", Topic: "z", Partition: 1, Offset: 3}, {Group: "other", Topic: "private", Offset: 99}} {
		if err := store.CommitConsumerOffset(ctx, offset.Group, offset.Topic, offset.Partition, offset.Offset, offset.Metadata); err != nil {
			t.Fatal(err)
		}
	}
	c := newTestCoordinator(t, store)
	for _, version := range []int16{2, 3, 4, 5} {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Version, req.Group = version, "g"
		all, err := c.OffsetFetch(ctx, req)
		if err != nil || all.ErrorCode != 0 || len(all.Topics) != 2 {
			t.Fatal(all, err)
		}
		req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "a", Partitions: []int32{0, 2}}, {Topic: "z", Partitions: []int32{1}}}
		named, err := c.OffsetFetch(ctx, req)
		if err != nil || !reflect.DeepEqual(all, named) {
			t.Fatal("null and explicit results differ", all, named, err)
		}
		all.Version = version
		var decoded kmsg.OffsetFetchResponse
		decoded.Version = version
		if err := decoded.ReadFrom(all.AppendTo(nil)); err != nil || len(decoded.Topics) != 2 || decoded.Topics[0].Partitions[0].Offset != 42 || *decoded.Topics[0].Partitions[0].Metadata != "checkpoint" {
			t.Fatal(decoded, err)
		}
		req.Topics = []kmsg.OffsetFetchRequestTopic{}
		empty, err := c.OffsetFetch(ctx, req)
		if err != nil || len(empty.Topics) != 0 {
			t.Fatal("empty topics treated as null", empty, err)
		}
		req.Topics = nil
		req.Group = "missing"
		empty, err = c.OffsetFetch(ctx, req)
		if err != nil || empty.ErrorCode != 0 || len(empty.Topics) != 0 {
			t.Fatal(empty, err)
		}
	}
	for _, version := range []int16{0, 1} {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Version, req.Group = version, "g"
		resp, err := c.OffsetFetch(ctx, req)
		if err != nil || len(resp.Topics) != 0 {
			t.Fatal("legacy nil topics changed", resp, err)
		}
	}
	failed := newTestCoordinator(t, groupOffsetFailureStore{Store: store})
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Version, req.Group = 5, "g"
	if resp, err := failed.OffsetFetch(ctx, req); err != nil || resp.ErrorCode != protocol.UNKNOWN_SERVER_ERROR || len(resp.Topics) != 0 {
		t.Fatal(resp, err)
	}
}
