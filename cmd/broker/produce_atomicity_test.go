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
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/KafScale/platform/pkg/broker"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type produceFailureS3 struct {
	*storage.MemoryS3Client
	failSegmentAt int32
	failIndex     bool
	segments      atomic.Int32
}

func (s *produceFailureS3) UploadSegment(ctx context.Context, key string, body []byte) error {
	if s.segments.Add(1) == s.failSegmentAt {
		return errors.New("injected segment failure")
	}
	return s.MemoryS3Client.UploadSegment(ctx, key, body)
}

func (s *produceFailureS3) UploadIndex(ctx context.Context, key string, body []byte) error {
	if s.failIndex {
		return errors.New("injected index failure")
	}
	return s.MemoryS3Client.UploadIndex(ctx, key, body)
}

func TestProduceRecordSetAtomicity(t *testing.T) {
	for _, tc := range []struct {
		name          string
		failSegmentAt int32
		failIndex     bool
		wantFailure   bool
	}{
		{"segment-failure", 1, false, true},
		{"index-failure", 0, true, true},
		{"no-second-segment-write", 2, false, false},
	} {
		for _, syncFlush := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/sync=%t", tc.name, syncFlush), func(t *testing.T) {
				ctx := context.Background()
				store := metadata.NewInMemoryStore(defaultMetadata())
				s3 := &produceFailureS3{MemoryS3Client: storage.NewMemoryS3Client(), failSegmentAt: tc.failSegmentAt, failIndex: tc.failIndex}
				h := newHandler(store, s3, protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}, testLogger())
				t.Cleanup(h.coordinator.Stop)
				h.flushOnAck = syncFlush
				// The old append loop flushes each batch separately at this threshold.
				h.logConfig.Buffer = storage.WriteBufferConfig{MaxBatches: 1}
				req := kmsg.NewPtrProduceRequest()
				req.Version, req.Acks = 9, -1
				records := append(testBatchBytes(0, 1, 2), testBatchBytes(0, 2, 3)...)
				req.Topics = []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: records}}}}
				produce := func() kmsg.ProduceResponseTopicPartition {
					t.Helper()
					wire, err := h.handleProduce(ctx, &protocol.RequestHeader{APIVersion: 9}, req)
					if err != nil {
						t.Fatal(err)
					}
					return decodeKmsgResponse(t, 9, wire, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
				}
				response := produce()
				plog := h.logs["orders"][0]
				if tc.wantFailure {
					if response.ErrorCode == protocol.NONE {
						t.Fatal("expected produce failure")
					}
					if next, err := store.NextOffset(ctx, "orders", 0); err != nil || next != 0 {
						t.Fatalf("failed request advanced durable offset: %d, %v", next, err)
					}
					if next := plog.BufferedHighWatermark(); next != 0 {
						t.Fatalf("failed request reserved offsets: %d", next)
					}
					if _, err := plog.Read(ctx, 0, 1<<20); !errors.Is(err, storage.ErrOffsetOutOfRange) {
						t.Fatalf("failed request is readable: %v", err)
					}
					// Reopen from the same storage before retrying: no prefix may survive.
					restored := newHandler(store, s3, protocol.MetadataBroker{NodeID: 1}, testLogger())
					t.Cleanup(restored.coordinator.Stop)
					recovered, err := restored.getPartitionLog(ctx, "orders", 0)
					if err != nil || recovered.BufferedHighWatermark() != 0 {
						t.Fatalf("failed request survived recovery: %v", err)
					}
					s3.failSegmentAt, s3.failIndex = 0, false
					// The storage outage has recovered before the client retries.
					h.s3Health = broker.NewS3HealthMonitor(broker.S3HealthConfig{})
					response = produce()
				} else if count := s3.segments.Load(); count != 1 {
					t.Fatalf("record set split into %d segment writes", count)
				}
				if response.ErrorCode != protocol.NONE || response.BaseOffset != 0 {
					t.Fatalf("produce/retry: %+v", response)
				}
				if next, err := store.NextOffset(ctx, "orders", 0); err != nil || next != 5 {
					t.Fatalf("durable offset after retry: %d, %v", next, err)
				}
				got, err := plog.Read(ctx, 0, 1<<20)
				if err != nil || len(got) != len(records) || binary.BigEndian.Uint64(got[:8]) != 0 || binary.BigEndian.Uint64(got[70:78]) != 2 {
					t.Fatalf("retry duplicated or lost records: %x, %v", got, err)
				}
			})
		}
	}
}
