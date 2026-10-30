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
import "testing"

func TestRecreatedTopicSegmentsKeepStorageTopic(t *testing.T) {
	partition := int32(0)
	segments := manifestEntriesToSegments("prod/", []manifestEntry{
		{SegmentKey: "orders@1/0/segment-0.kfs", IndexKey: "orders@1/0/segment-0.index"},
		{Topic: "orders", Partition: &partition, SegmentKey: "orders@2/0/segment-0.kfs", IndexKey: "orders@2/0/segment-0.index"},
	})
	if len(segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(segments))
	}
	for i, want := range []string{"orders@1", "orders@2"} {
		if segments[i].Topic != "orders" || segments[i].CheckpointTopic() != want {
			t.Fatalf("segment %d: %+v", i, segments[i])
		}
	}
	key, _, ok := parseSegmentKey("", "orders@3/1/segment-9.kfs")
	if !ok || topicName(key.topic) != "orders" || key.topic != "orders@3" {
		t.Fatalf("unexpected key %+v", key)
	}
	if topicName("orders@x") != "orders@x" || topicName("@1") != "@1" || topicName("orders@") != "orders@" {
		t.Fatal("topicName stripped a non-generation suffix")
	}
	if (SegmentRef{Topic: "orders"}).CheckpointTopic() != "orders" {
		t.Fatal("expected topic as checkpoint without storage topic")
	}
}
