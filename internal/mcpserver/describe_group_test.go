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

package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Hide optional capabilities to exercise the compatibility adapter for older stores.
type legacyGroupStore struct{ metadata.Store }

func TestDescribeGroupWithOnlyOffsets(t *testing.T) {
	for _, backend := range []string{"memory", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			var store metadata.Store = metadata.NewInMemoryStore(metadata.ClusterMetadata{})
			if backend == "etcd" {
				etcd, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t)})
				if err != nil {
					t.Fatal(err)
				}
				defer etcd.Close()
				store = etcd
			}
			for _, offset := range []metadata.ConsumerOffset{
				{Group: "g", Topic: "retired", Partition: 2, Offset: 42, Metadata: "checkpoint"},
				{Group: "g", Topic: "a", Partition: 1, Offset: 17, Metadata: "partition-one"},
				{Group: "g", Topic: "a", Partition: 0, Offset: 0},
				{Group: "g-other", Topic: "private", Offset: 99},
				{Group: "g/offsets/child", Topic: "private", Offset: 88},
				{Group: "zero", Topic: "a", Offset: 0},
			} {
				if err := store.CommitConsumerOffset(ctx, offset.Group, offset.Topic, offset.Partition, offset.Offset, offset.Metadata); err != nil {
					t.Fatal(err)
				}
			}
			for _, tc := range []struct {
				name  string
				store metadata.Store
			}{
				{"group-scoped", store}, {"legacy", legacyGroupStore{store}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					_, listed, err := listGroupsHandler(Options{Store: tc.store})(ctx, nil, emptyInput{})
					if err != nil {
						t.Fatal(err)
					}
					var summary *GroupSummary
					for i := range listed.Groups {
						if listed.Groups[i].GroupID == "g" {
							summary = &listed.Groups[i]
						}
					}
					if summary == nil {
						t.Fatal("offset-only group was not listed")
					}
					_, got, err := describeGroupHandler(Options{Store: tc.store})(ctx, nil, GroupInput{GroupID: "g"})
					if err != nil {
						t.Fatal(err)
					}
					if got.GroupID != summary.GroupID || got.State != "empty" || got.State != summary.State || got.ProtocolType != summary.ProtocolType || len(got.Members) != 0 {
						t.Fatalf("describe=%+v disagrees with list=%+v", got, summary)
					}
					want := []OffsetDetails{
						{Topic: "a", Partition: 0, Offset: 0},
						{Topic: "a", Partition: 1, Offset: 17, Metadata: "partition-one"},
						{Topic: "retired", Partition: 2, Offset: 42, Metadata: "checkpoint"},
					}
					if !reflect.DeepEqual(got.Offsets, want) {
						t.Fatalf("offsets=%+v, want %+v", got.Offsets, want)
					}
					if group, err := store.FetchConsumerGroup(ctx, "g"); err != nil || group != nil {
						t.Fatalf("read-only describe created group metadata: %+v, %v", group, err)
					}
					if _, _, err := describeGroupHandler(Options{Store: tc.store})(ctx, nil, GroupInput{GroupID: "missing"}); err == nil {
						t.Fatal("unrelated offsets made a missing group exist")
					}
					_, zero, err := describeGroupHandler(Options{Store: tc.store})(ctx, nil, GroupInput{GroupID: "zero"})
					if err != nil || zero.State != "empty" || !reflect.DeepEqual(zero.Offsets, []OffsetDetails{{Topic: "a", Offset: 0}}) {
						t.Fatalf("zero-valued commit treated as missing: %+v, %v", zero, err)
					}
				})
			}
		})
	}
}

type failedGroupReadStore struct {
	metadata.Store
	err error
}

func (s failedGroupReadStore) FetchConsumerGroup(context.Context, string) (*metadatapb.ConsumerGroup, error) {
	return nil, s.err
}

type failedOffsetReadStore struct {
	metadata.Store
	err error
}

func (s failedOffsetReadStore) ListGroupConsumerOffsets(context.Context, string) ([]metadata.ConsumerOffset, error) {
	return nil, s.err
}

func (s failedOffsetReadStore) ListConsumerOffsets(context.Context) ([]metadata.ConsumerOffset, error) {
	return nil, s.err
}

func TestDescribeGroupReadErrors(t *testing.T) {
	store := testStore()
	failure := errors.New("metadata unavailable")
	for _, tc := range []struct {
		name    string
		store   metadata.Store
		wantErr error
	}{
		{"no-store", nil, nil},
		{"group-read", failedGroupReadStore{store, failure}, failure},
		{"scoped-offset-read", failedOffsetReadStore{store, failure}, failure},
		{"legacy-offset-read", legacyGroupStore{failedOffsetReadStore{store, failure}}, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := describeGroupHandler(Options{Store: tc.store})(context.Background(), nil, GroupInput{GroupID: "g"})
			if err == nil || (tc.wantErr != nil && !errors.Is(err, tc.wantErr)) {
				t.Fatalf("error=%v, want %v", err, tc.wantErr)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := describeGroupHandler(Options{Store: store})(ctx, nil, GroupInput{GroupID: "g"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}

func TestDescribeGroupPreservesMetadata(t *testing.T) {
	ctx := context.Background()
	store := testStore()
	group := &metadatapb.ConsumerGroup{
		GroupId: "g", State: "stable", ProtocolType: "consumer", Protocol: "range", Leader: "m", GenerationId: 7,
		Members: map[string]*metadatapb.GroupMember{"m": {ClientId: "client", Assignments: []*metadatapb.Assignment{{Topic: "orders", Partitions: []int32{0}}}}},
	}
	if err := store.PutConsumerGroup(ctx, group); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitConsumerOffset(ctx, "g", "orders", 0, 23, "commit"); err != nil {
		t.Fatal(err)
	}
	_, got, err := describeGroupHandler(Options{Store: store})(ctx, nil, GroupInput{GroupID: "g"})
	if err != nil {
		t.Fatal(err)
	}
	want := toGroupDetails(group)
	want.Offsets = []OffsetDetails{{Topic: "orders", Partition: 0, Offset: 23, Metadata: "commit"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("description changed group metadata: got=%+v, want=%+v", got, want)
	}
	if err := store.PutConsumerGroup(ctx, &metadatapb.ConsumerGroup{GroupId: "empty", State: "empty"}); err != nil {
		t.Fatal(err)
	}
	_, got, err = describeGroupHandler(Options{Store: store})(ctx, nil, GroupInput{GroupID: "empty"})
	if err != nil || got.Offsets == nil || len(got.Offsets) != 0 || got.Members == nil {
		t.Fatalf("empty group without commits: %+v, %v", got, err)
	}
}

func TestDescribeOffsetOnlyGroupThroughMCP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := testStore()
	if err := store.CommitConsumerOffset(ctx, "g", "orders", 0, 42, "checkpoint"); err != nil {
		t.Fatal(err)
	}
	st, ct := mcp.NewInMemoryTransports()
	server, err := NewServer(Options{Store: store}).Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: toolDescribeGroup, Arguments: map[string]any{"group_id": "g"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("MCP returned tool error: %+v", result)
	}
	raw, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var got GroupDetails
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.GroupID != "g" || got.State != "empty" || len(got.Members) != 0 || !reflect.DeepEqual(got.Offsets, []OffsetDetails{{Topic: "orders", Partition: 0, Offset: 42, Metadata: "checkpoint"}}) {
		t.Fatalf("MCP structured response: %s", raw)
	}
}
