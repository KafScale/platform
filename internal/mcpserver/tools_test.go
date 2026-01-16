// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"testing"

	metadatapb "github.com/novatechflow/kafscale/pkg/gen/metadata"
	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/protocol"
)

func TestRequireStore(t *testing.T) {
	if _, err := requireStore(nil); err == nil {
		t.Fatalf("expected error for nil store")
	}
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	if _, err := requireStore(store); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSummarizeTopicsSorts(t *testing.T) {
	topics := []protocol.MetadataTopic{
		{Name: "b", Partitions: []protocol.MetadataPartition{{PartitionIndex: 0}}},
		{Name: "a", Partitions: []protocol.MetadataPartition{{PartitionIndex: 0}, {PartitionIndex: 1}}},
	}
	out := summarizeTopics(topics)
	if len(out) != 2 || out[0].Name != "a" || out[1].Name != "b" {
		t.Fatalf("unexpected summary order: %+v", out)
	}
	if out[0].PartitionCount != 2 {
		t.Fatalf("unexpected partition count: %+v", out[0])
	}
}

func TestCopyInt32Slice(t *testing.T) {
	input := []int32{1, 2, 3}
	out := copyInt32Slice(input)
	if len(out) != 3 || out[1] != 2 {
		t.Fatalf("unexpected copy: %+v", out)
	}
	out[0] = 9
	if input[0] == 9 {
		t.Fatalf("expected copy to be isolated")
	}
}

func TestToGroupDetailsSortsMembers(t *testing.T) {
	group := &metadatapb.ConsumerGroup{
		GroupId:      "group-1",
		State:        "stable",
		ProtocolType: "consumer",
		Protocol:     "range",
		Leader:       "member-b",
		GenerationId: 3,
		Members: map[string]*metadatapb.GroupMember{
			"member-b": {ClientId: "client-b", Assignments: []*metadatapb.Assignment{{Topic: "t1", Partitions: []int32{0}}}},
			"member-a": {ClientId: "client-a", Assignments: []*metadatapb.Assignment{{Topic: "t1", Partitions: []int32{1}}}},
		},
	}

	details := toGroupDetails(group)
	if len(details.Members) != 2 || details.Members[0].MemberID != "member-a" || details.Members[1].MemberID != "member-b" {
		t.Fatalf("unexpected member ordering: %+v", details.Members)
	}
}
