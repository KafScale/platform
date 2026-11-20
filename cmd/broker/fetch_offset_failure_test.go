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

package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/KafScale/platform/v2/pkg/storage"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type fetchOffsetStore struct {
	metadata.Store
	fault          error
	returnedOffset int64
	available      bool
}

func (s *fetchOffsetStore) Available() bool { return s.available }
func (s *fetchOffsetStore) NextOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	if s.fault != nil {
		return s.returnedOffset, s.fault
	}
	return s.Store.NextOffset(ctx, topic, partition)
}

func fetchOffsetFixture(t *testing.T, flush bool) (*handler, *fetchOffsetStore) {
	t.Helper()
	store := &fetchOffsetStore{Store: metadata.NewInMemoryStore(defaultMetadata()), available: true}
	h := newTestHandler(store)
	h.flushOnAck = flush
	if !flush {
		h.logConfig.Buffer = storage.WriteBufferConfig{MaxBytes: 1 << 30}
	}
	t.Cleanup(h.coordinator.Stop)
	for range 3 {
		if err := produceFetchWait(h, 0); err != nil {
			t.Fatal(err)
		}
	}
	return h, store
}

func fetchAtOffset(t *testing.T, h *handler, version int16, offset int64) kmsg.FetchResponseTopicPartition {
	t.Helper()
	meta, err := h.store.Metadata(context.Background(), []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	req := kmsg.NewPtrFetchRequest()
	req.Version = version
	req.Topics = []kmsg.FetchRequestTopic{{Topic: "orders", TopicID: meta.Topics[0].TopicID, Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 0, FetchOffset: offset, PartitionMaxBytes: 1 << 20}}}}
	data, err := h.Handle(context.Background(), &protocol.RequestHeader{APIKey: protocol.APIKeyFetch, APIVersion: version, CorrelationID: 342}, req)
	if err != nil {
		t.Fatal(err)
	}
	resp := decodeKmsgResponse(t, version, data, kmsg.NewPtrFetchResponse)
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("invalid fetch response: %+v", resp)
	}
	return resp.Topics[0].Partitions[0]
}

func TestFetchOffsetReadFailuresDoNotResetOffsets(t *testing.T) {
	for version := int16(4); version <= 13; version++ {
		for _, flush := range []bool{false, true} {
			t.Run(fmt.Sprintf("v%d/flush=%v", version, flush), func(t *testing.T) {
				h, store := fetchOffsetFixture(t, flush)
				for _, tc := range []struct {
					name        string
					err         error
					unavailable bool
					returned    int64
					want        int16
				}{
					{name: "deadline", err: context.DeadlineExceeded, want: protocol.REQUEST_TIMED_OUT},
					{name: "canceled", err: context.Canceled, want: protocol.REQUEST_TIMED_OUT},
					{name: "wrapped deadline", err: fmt.Errorf("read offset: %w", context.DeadlineExceeded), want: protocol.REQUEST_TIMED_OUT},
					{name: "wrapped cancellation", err: fmt.Errorf("read offset: %w", context.Canceled), want: protocol.REQUEST_TIMED_OUT},
					{name: "unavailable", err: errors.New("connection unavailable"), unavailable: true, want: protocol.REQUEST_TIMED_OUT},
					{name: "offset with error is unusable", err: context.DeadlineExceeded, returned: 99, want: protocol.REQUEST_TIMED_OUT},
					{name: "unexpected store error", err: errors.New("invalid persisted offset"), want: protocol.UNKNOWN_SERVER_ERROR},
				} {
					t.Run(tc.name, func(t *testing.T) {
						store.fault, store.returnedOffset, store.available = tc.err, tc.returned, !tc.unavailable
						for _, offset := range []int64{1, 0, 3, 4} {
							part := fetchAtOffset(t, h, version, offset)
							if part.ErrorCode != tc.want || len(part.RecordBatches) != 0 || part.HighWatermark != -1 || part.LastStableOffset != -1 {
								t.Fatalf("offset=%d: %+v, want error=%d and unknown watermark", offset, part, tc.want)
							}
							if tc.want == protocol.REQUEST_TIMED_OUT && !kerr.IsRetriable(kerr.ErrorForCode(part.ErrorCode)) {
								t.Fatal("timeout is not retriable")
							}
						}
					})
				}
				// Recovery must fetch the requested offset, with the real watermark,
				// rather than resetting to the beginning or skipping to the end.
				store.fault, store.available = nil, true
				for _, offset := range []int64{0, 1, 3, 4} {
					part := fetchAtOffset(t, h, version, offset)
					want := int16(protocol.NONE)
					if offset > 3 {
						want = protocol.OFFSET_OUT_OF_RANGE
					}
					if part.ErrorCode != want || part.HighWatermark != 3 || part.LastStableOffset != 3 {
						t.Fatalf("recovered offset=%d: %+v", offset, part)
					}
					if offset < 3 {
						if len(part.RecordBatches) < 8 || int64(binary.BigEndian.Uint64(part.RecordBatches[:8])) != offset {
							t.Fatalf("recovered fetch at %d returned wrong batch: %+v", offset, part)
						}
					} else if len(part.RecordBatches) != 0 {
						t.Fatal("tail fetch returned records")
					}
				}
				// Unknown-topic errors retain their protocol meaning rather than retrying.
				store.fault = fmt.Errorf("deleted: %w", metadata.ErrUnknownTopic)
				if part := fetchAtOffset(t, h, version, 1); part.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
					t.Fatal(part)
				}
			})
		}
	}
}

func TestFetchOffsetFailurePreservesOwnershipFence(t *testing.T) {
	h, store := fetchOffsetFixture(t, true)
	store.fault = context.DeadlineExceeded
	checks := 0
	h.logs["orders"][0] = storage.NewPartitionLogWithOptions(h.s3Namespace, "orders", 0, 3, h.s3, h.cache, storage.PartitionLogOptions{
		PartitionLogConfig: h.logConfig,
		CheckOwnership:     func() error { checks++; return metadata.ErrNotOwner },
	}, nil, nil, nil)
	part := fetchAtOffset(t, h, 11, 1)
	if part.ErrorCode != protocol.NOT_LEADER_OR_FOLLOWER || len(part.RecordBatches) != 0 || checks != 1 {
		t.Fatalf("lost owner: %+v, checks=%d", part, checks)
	}
}

func TestFetchOffsetFailureStopsLongPoll(t *testing.T) {
	h, store := fetchOffsetFixture(t, true)
	store.fault = context.DeadlineExceeded
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req := kmsg.NewPtrFetchRequest()
	req.MinBytes, req.MaxWaitMillis = 1, 30000
	req.Topics = []kmsg.FetchRequestTopic{{Topic: "orders", Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 0, FetchOffset: 1, PartitionMaxBytes: 1 << 20}}}}
	raw, err := h.handleFetch(ctx, &protocol.RequestHeader{APIVersion: 11}, req)
	if err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != nil {
		t.Fatal("Fetch kept polling after the offset read failed")
	}
	part := decodeKmsgResponse(t, 11, raw, kmsg.NewPtrFetchResponse).Topics[0].Partitions[0]
	if part.ErrorCode != protocol.REQUEST_TIMED_OUT || part.HighWatermark != -1 || len(part.RecordBatches) != 0 {
		t.Fatal(part)
	}
}
