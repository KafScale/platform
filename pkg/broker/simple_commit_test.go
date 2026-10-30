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

func TestSimpleOffsetCommit(t *testing.T) {
	ctx := context.Background()
	etcd, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = etcd.Close() })
	old := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	if err := etcd.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "empty", State: groupStateEmptyStr, ProtocolType: "consumer", EmptySince: old}); err != nil {
		t.Fatal(err)
	}
	members := map[string]*metadatapb.GroupMember{"m1": {HeartbeatAt: time.Now().UTC().Format(time.RFC3339Nano), SessionTimeoutMs: 60000}}
	if err := etcd.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "active", State: groupStateStableStr, GenerationId: 4, Leader: "m1", ProtocolType: "consumer", Protocol: "range", Members: members}); err != nil {
		t.Fatal(err)
	}
	c := newTestCoordinator(t, etcd)
	commit := func(group, member string, generation int32) int16 {
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group, req.MemberID, req.Generation = group, member, generation
		topic := kmsg.NewOffsetCommitRequestTopic()
		topic.Topic = "orders"
		part := kmsg.NewOffsetCommitRequestTopicPartition()
		part.Offset = 11
		topic.Partitions = append(topic.Partitions, part)
		req.Topics = append(req.Topics, topic)
		resp, err := c.OffsetCommit(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		return resp.Topics[0].Partitions[0].ErrorCode
	}
	cases := []struct {
		group, member string
		generation    int32
		want          int16
		stored        bool
	}{
		{"absent", "", -1, protocol.NONE, true},
		{"empty", "", -1, protocol.NONE, true},
		{"active", "", -1, protocol.UNKNOWN_MEMBER_ID, false},
		{"active", "m1", -1, protocol.ILLEGAL_GENERATION, false},
		{"absent-member", "m1", -1, protocol.UNKNOWN_MEMBER_ID, false},
		{"absent-generation", "", 0, protocol.UNKNOWN_MEMBER_ID, false},
	}
	for _, tc := range cases {
		if code := commit(tc.group, tc.member, tc.generation); code != tc.want {
			t.Fatalf("%s %q %d returned %d, want %d", tc.group, tc.member, tc.generation, code, tc.want)
		}
		offset, _, err := etcd.FetchConsumerOffset(ctx, tc.group, "orders", 0)
		if err != nil || (offset == 11) != tc.stored {
			t.Fatalf("%s stored offset %d, %v", tc.group, offset, err)
		}
	}
	if group, _ := etcd.FetchConsumerGroup(ctx, "empty"); group == nil || group.EmptySince == old {
		t.Fatal("commit did not reset retention clock", group)
	}
}
