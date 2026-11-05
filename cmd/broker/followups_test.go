// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"errors"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	"testing"
	"time"
)

func TestInvalidMissingTopicMetadataAndProduce(t *testing.T) {
	for _, auto := range []bool{false, true} {
		for _, name := range invalidTopicNames {
			h := newTestHandler(metadata.NewInMemoryStore(defaultMetadata()))
			h.autoCreateTopics = auto
			req := kmsg.NewPtrMetadataRequest()
			req.Topics = []kmsg.MetadataRequestTopic{{Topic: &name}}
			data, err := h.Handle(context.Background(), &protocol.RequestHeader{APIKey: protocol.APIKeyMetadata, APIVersion: 4}, req)
			if err != nil {
				t.Fatal(err)
			}
			if code := decodeKmsgResponse(t, 4, data, kmsg.NewPtrMetadataResponse).Topics[0].ErrorCode; code != protocol.INVALID_TOPIC_EXCEPTION {
				t.Fatalf("metadata %q auto=%v: %d", name, auto, code)
			}
			produce := &kmsg.ProduceRequest{Acks: -1, Topics: []kmsg.ProduceRequestTopic{{Topic: name, Partitions: []kmsg.ProduceRequestTopicPartition{{Records: testBatchBytes(0, 0, 1)}}}}}
			data, err = h.handleProduce(context.Background(), &protocol.RequestHeader{APIVersion: 3}, produce)
			if err != nil {
				t.Fatal(err)
			}
			if code := decodeKmsgResponse(t, 3, data, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0].ErrorCode; code != protocol.INVALID_TOPIC_EXCEPTION {
				t.Fatalf("produce %q auto=%v: %d", name, auto, code)
			}
		}
	}
}

type deadlineOffsetStore struct {
	metadata.Store
	deadline chan time.Time
}

func (s deadlineOffsetStore) NextOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, errors.New("produce has no deadline")
	}
	s.deadline <- deadline
	<-ctx.Done()
	return 0, ctx.Err()
}
func TestProduceHonorsTimeoutMillis(t *testing.T) {
	deadlines := make(chan time.Time, 1)
	h := newTestHandler(deadlineOffsetStore{metadata.NewInMemoryStore(defaultMetadata()), deadlines})
	req := &kmsg.ProduceRequest{Acks: -1, TimeoutMillis: 30, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Records: testBatchBytes(0, 0, 1)}}}}}
	start := time.Now()
	data, err := h.handleProduce(context.Background(), &protocol.RequestHeader{APIVersion: 3}, req)
	if err != nil {
		t.Fatal(err)
	}
	if code := decodeKmsgResponse(t, 3, data, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0].ErrorCode; code != protocol.REQUEST_TIMED_OUT {
		t.Fatalf("code %d", code)
	}
	select {
	case deadline := <-deadlines:
		if deadline.Sub(start) > 100*time.Millisecond {
			t.Fatal("deadline exceeds request timeout")
		}
	default:
		t.Fatal("produce deadline not set")
	}
	if time.Since(start) > time.Second {
		t.Fatal("produce exceeded timeout")
	}
}
