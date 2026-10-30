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

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

var invalidTopicNames = []string{"col@1", "a/b", ".", "..", "bad name", strings.Repeat("a", 250)}

func createTopicCode(t *testing.T, ctx context.Context, h *handler, name string, validateOnly bool) int16 {
	t.Helper()
	req := &kmsg.CreateTopicsRequest{ValidateOnly: validateOnly, Topics: []kmsg.CreateTopicsRequestTopic{{Topic: name, NumPartitions: 1, ReplicationFactor: 1}}}
	payload, err := h.handleCreateTopics(ctx, &protocol.RequestHeader{CorrelationID: 1}, req)
	if err != nil {
		t.Fatal(err)
	}
	return decodeKmsgResponse(t, 0, payload, kmsg.NewPtrCreateTopicsResponse).Topics[0].ErrorCode
}

func topicExists(t *testing.T, ctx context.Context, store metadata.Store, name string) bool {
	t.Helper()
	meta, err := store.Metadata(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range meta.Topics {
		if *topic.Topic == name {
			return true
		}
	}
	return false
}

func TestCreateTopicsRejectsInvalidNames(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewInMemoryStore(defaultMetadata())
	h := newTestHandler(store)
	for _, name := range invalidTopicNames {
		for _, validateOnly := range []bool{true, false} {
			if code := createTopicCode(t, ctx, h, name, validateOnly); code != protocol.INVALID_TOPIC_EXCEPTION {
				t.Fatalf("create %q validateOnly=%v: got code %d", name, validateOnly, code)
			}
		}
		if topicExists(t, ctx, store, name) {
			t.Fatalf("topic %q was created", name)
		}
	}
	if code := createTopicCode(t, ctx, h, strings.Repeat("a", 249), false); code != protocol.NONE {
		t.Fatalf("249 character name: got code %d", code)
	}
}

func TestGenerationNameCannotBeCreatedAsTopic(t *testing.T) {
	ctx := context.Background()
	store := newEtcdTopicStore(t, ctx)
	h := newTestHandler(store)
	if code := createTopicCode(t, ctx, h, "orders@1", false); code != protocol.INVALID_TOPIC_EXCEPTION {
		t.Fatalf("create orders@1: got code %d", code)
	}
	produceOrders(t, ctx, h, 1)
	if err := h.deleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	offsets := produceOrders(t, ctx, h, 1)
	if offsets[0] != 0 {
		t.Fatalf("re-created topic started at offset %d", offsets[0])
	}
}

func TestAutoCreateSkipsInvalidNames(t *testing.T) {
	ctx := context.Background()
	store := metadata.NewInMemoryStore(defaultMetadata())
	h := newTestHandler(store)
	h.autoCreateTopics = true
	name := "a@1"
	request := kmsg.NewPtrMetadataRequest()
	request.Version = 4
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &name}}
	if _, err := h.Handle(ctx, &protocol.RequestHeader{APIKey: protocol.APIKeyMetadata, APIVersion: 4}, request); err != nil {
		t.Fatal(err)
	}
	produce := &kmsg.ProduceRequest{Acks: -1, TimeoutMillis: 1000, Topics: []kmsg.ProduceRequestTopic{{Topic: "a/b", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: testBatchBytes(0, 0, 1)}}}}}
	payload, err := h.handleProduce(ctx, &protocol.RequestHeader{CorrelationID: 1, APIVersion: 3}, produce)
	if err != nil {
		t.Fatal(err)
	}
	if code := decodeKmsgResponse(t, 3, payload, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0].ErrorCode; code != protocol.INVALID_TOPIC_EXCEPTION {
		t.Fatalf("produce a/b: got code %d", code)
	}
	for _, topic := range []string{"a@1", "a/b"} {
		if topicExists(t, ctx, store, topic) {
			t.Fatalf("topic %q was auto-created", topic)
		}
	}
}

func TestExistingInvalidTopicNameStillWorks(t *testing.T) {
	ctx := context.Background()
	meta := defaultMetadata()
	name := "legacy@1"
	meta.Topics = append(meta.Topics, protocol.MetadataTopic{Topic: &name, Partitions: []protocol.MetadataPartition{{Partition: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}})
	h := newTestHandler(metadata.NewInMemoryStore(meta))
	produce := &kmsg.ProduceRequest{Acks: -1, TimeoutMillis: 1000, Topics: []kmsg.ProduceRequestTopic{{Topic: name, Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: testBatchBytes(0, 0, 1)}}}}}
	payload, err := h.handleProduce(ctx, &protocol.RequestHeader{CorrelationID: 1, APIVersion: 3}, produce)
	if err != nil {
		t.Fatal(err)
	}
	if code := decodeKmsgResponse(t, 3, payload, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0].ErrorCode; code != protocol.NONE {
		t.Fatalf("produce to existing %s: got code %d", name, code)
	}
}
