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

func TestRecreatedTopicSegmentKeepsStorageTopic(t *testing.T) {
	key, _, ok := parseSegmentKey("prod/", "prod/orders@2/0/segment-5.kfs")
	if !ok {
		t.Fatal("expected generation prefix to parse")
	}
	seg := newSegmentRef(key, &segmentEntry{kfsKey: "k", indexKey: "i"})
	if seg.Topic != "orders" || seg.StorageTopic != "orders@2" || seg.CheckpointTopic() != "orders@2" {
		t.Fatalf("unexpected segment %+v", seg)
	}
	filter := map[string]map[int32]struct{}{"orders": {0: {}}}
	if !filterAllows(filter, topicName(key.topic), key.partition) {
		t.Fatal("filter rejected re-created topic")
	}
	if (SegmentRef{Topic: "orders"}).CheckpointTopic() != "orders" {
		t.Fatal("expected topic as checkpoint without storage topic")
	}
}

func TestTopicName(t *testing.T) {
	cases := map[string]string{
		"orders":      "orders",
		"orders@1":    "orders",
		"orders@12":   "orders",
		"a@b@3":       "a@b",
		"orders@":     "orders@",
		"orders@x":    "orders@x",
		"@1":          "@1",
		"orders@1foo": "orders@1foo",
	}
	for in, want := range cases {
		if got := topicName(in); got != want {
			t.Fatalf("topicName(%q) = %q, want %q", in, got, want)
		}
	}
}
