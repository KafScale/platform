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

type pausedCommitStore struct {
	*metadata.EtcdStore
	reached chan struct{}
	release chan struct{}
}

func (s pausedCommitStore) pause() {
	close(s.reached)
	<-s.release
}

func (s pausedCommitStore) CommitConsumerOffset(ctx context.Context, group, topic string, partition int32, offset int64, meta string) error {
	s.pause()
	return s.EtcdStore.CommitConsumerOffset(ctx, group, topic, partition, offset, meta)
}

func (s pausedCommitStore) CommitConsumerOffsetIfGroupExists(ctx context.Context, group, topic string, partition int32, offset int64, meta string) error {
	s.pause()
	return s.EtcdStore.CommitConsumerOffsetIfGroupExists(ctx, group, topic, partition, offset, meta)
}

func TestOffsetCommitAfterDeleteGroupsFails(t *testing.T) {
	ctx := context.Background()
	etcd, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = etcd.Close() })
	members := map[string]*metadatapb.GroupMember{"m1": {HeartbeatAt: time.Now().UTC().Format(time.RFC3339Nano), SessionTimeoutMs: 60000}}
	if err := etcd.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "g", State: groupStateStableStr, GenerationId: 4, Leader: "m1", ProtocolType: "consumer", Protocol: "range", Members: members}); err != nil {
		t.Fatal(err)
	}
	store := pausedCommitStore{EtcdStore: etcd, reached: make(chan struct{}), release: make(chan struct{})}
	c := newTestCoordinator(t, store)

	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group, commit.MemberID, commit.Generation = "g", "m1", 4
	topic := kmsg.NewOffsetCommitRequestTopic()
	topic.Topic = "orders"
	part := kmsg.NewOffsetCommitRequestTopicPartition()
	part.Offset = 42
	topic.Partitions = append(topic.Partitions, part)
	commit.Topics = append(commit.Topics, topic)
	done := make(chan *kmsg.OffsetCommitResponse, 1)
	go func() {
		resp, err := c.OffsetCommit(ctx, commit)
		if err != nil {
			t.Error(err)
		}
		done <- resp
	}()
	<-store.reached

	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Group, leave.MemberID = "g", "m1"
	if resp := c.LeaveGroup(ctx, leave); resp.ErrorCode != 0 {
		t.Fatal(resp)
	}
	del := kmsg.NewPtrDeleteGroupsRequest()
	del.Groups = []string{"g"}
	if resp, err := c.DeleteGroups(ctx, del); err != nil || resp.Groups[0].ErrorCode != 0 {
		t.Fatal(resp, err)
	}
	close(store.release)
	resp := <-done
	if code := resp.Topics[0].Partitions[0].ErrorCode; code != protocol.UNKNOWN_MEMBER_ID {
		t.Fatalf("late commit returned %d", code)
	}
	if group, err := etcd.FetchConsumerGroup(ctx, "g"); err != nil || group != nil {
		t.Fatal("group came back", group, err)
	}
	if offsets, err := etcd.ListGroupConsumerOffsets(ctx, "g"); err != nil || len(offsets) != 0 {
		t.Fatal("offset survived", offsets, err)
	}
}
