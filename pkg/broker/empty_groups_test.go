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
	"fmt"
	"testing"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type storeWithoutAtomicDeletion struct{ metadata.Store }

func TestDeleteGroupsWithLegacyStore(t *testing.T) {
	ctx := context.Background()
	for _, offsets := range []bool{false, true} {
		base := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
		if err := base.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "readers", State: groupStateEmptyStr, ProtocolType: "consumer"}); err != nil {
			t.Fatal(err)
		}
		if offsets {
			if err := base.CommitConsumerOffset(ctx, "readers", "orders", 0, 42, ""); err != nil {
				t.Fatal(err)
			}
		}
		legacy := storeWithoutAtomicDeletion{Store: base}
		if _, ok := any(legacy).(metadata.ConsumerGroupDeleter); ok {
			t.Fatal("Store interface requires atomic deletion")
		}
		c := newTestCoordinator(t, legacy)
		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"readers"}
		resp, err := c.DeleteGroups(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Groups[0].ErrorCode != 0 {
			t.Fatal(resp)
		}
		if group, _ := base.FetchConsumerGroup(ctx, "readers"); group != nil {
			t.Fatal("fallback deletion kept group")
		}
		if remaining, err := base.ListGroupConsumerOffsets(ctx, "readers"); err != nil || len(remaining) != 0 {
			t.Fatal("fallback deletion kept offsets", remaining, err)
		}
	}
}

type retryableDeleteStore struct {
	metadata.Store
	err error
}

func (s retryableDeleteStore) DeleteConsumerGroupAndOffsets(context.Context, string) error {
	return s.err
}

func TestDeleteGroupsRetryableStoreErrors(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		err  error
		code int16
	}{
		{metadata.ErrConsumerGroupChanged, protocol.COORDINATOR_NOT_AVAILABLE},
		{fmt.Errorf("txn: %w", context.DeadlineExceeded), protocol.COORDINATOR_NOT_AVAILABLE},
		{fmt.Errorf("txn: %w", context.Canceled), protocol.COORDINATOR_NOT_AVAILABLE},
		{errCoordinatorStore, protocol.UNKNOWN_SERVER_ERROR},
	} {
		base := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
		if err := base.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "readers", State: groupStateEmptyStr, ProtocolType: "consumer"}); err != nil {
			t.Fatal(err)
		}
		c := newTestCoordinator(t, retryableDeleteStore{Store: base, err: tc.err})
		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{"readers"}
		resp, err := c.DeleteGroups(ctx, req)
		if err != nil || resp.Groups[0].ErrorCode != tc.code {
			t.Fatal(tc.err, resp, err)
		}
	}
}
