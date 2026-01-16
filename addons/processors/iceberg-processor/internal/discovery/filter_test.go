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

package discovery

import (
	"testing"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

func TestBuildTopicPartitionFilter(t *testing.T) {
	snapshot := metadata.ClusterMetadata{
		Topics: []protocol.MetadataTopic{
			{
				Name: "orders",
				Partitions: []protocol.MetadataPartition{
					{PartitionIndex: 0},
					{PartitionIndex: 1},
				},
			},
			{
				Name:      "bad-topic",
				ErrorCode: 3,
				Partitions: []protocol.MetadataPartition{
					{PartitionIndex: 0},
				},
			},
			{
				Name: "payments",
				Partitions: []protocol.MetadataPartition{
					{PartitionIndex: 2, ErrorCode: 2},
					{PartitionIndex: 3},
				},
			},
		},
	}

	filter := buildTopicPartitionFilter(snapshot)
	if !filterAllows(filter, "orders", 0) || !filterAllows(filter, "orders", 1) {
		t.Fatalf("expected orders partitions to be included")
	}
	if filterAllows(filter, "bad-topic", 0) {
		t.Fatalf("expected bad-topic to be excluded")
	}
	if filterAllows(filter, "payments", 2) {
		t.Fatalf("expected errored partition to be excluded")
	}
	if !filterAllows(filter, "payments", 3) {
		t.Fatalf("expected payments partition 3 to be included")
	}
}
