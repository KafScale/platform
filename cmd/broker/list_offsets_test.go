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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type listOffsetsStore struct {
	metadata.Store
	next    func(context.Context, string, int32) (int64, error)
	creates atomic.Int64
}

func (s *listOffsetsStore) NextOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	if s.next != nil {
		return s.next(ctx, topic, partition)
	}
	return s.Store.NextOffset(ctx, topic, partition)
}

func (s *listOffsetsStore) CreateTopic(ctx context.Context, spec metadata.TopicSpec) (*protocol.MetadataTopic, error) {
	s.creates.Add(1)
	return s.Store.CreateTopic(ctx, spec)
}

func requestListOffset(t *testing.T, h *handler, version int16, topic string, partition int32, timestamp int64) kmsg.ListOffsetsResponseTopicPartition {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req := kmsg.NewPtrListOffsetsRequest()
	req.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: partition, Timestamp: timestamp, MaxNumOffsets: 1}}}}
	payload, err := h.handleListOffsets(ctx, &protocol.RequestHeader{CorrelationID: 175, APIVersion: version}, req)
	if err != nil {
		t.Fatal(err)
	}
	decoded := decodeKmsgResponse(t, version, payload, kmsg.NewPtrListOffsetsResponse)
	if len(decoded.Topics) != 1 || len(decoded.Topics[0].Partitions) != 1 {
		t.Fatalf("invalid response: %+v", decoded)
	}
	return decoded.Topics[0].Partitions[0]
}

func TestListOffsetsDoesNotAutoCreate(t *testing.T) {
	for version := int16(0); version <= 4; version++ {
		for _, autoCreate := range []bool{false, true} {
			for _, timestamps := range [][]int64{{-1, -2, 1234}, {-2, -1, 1234}} {
				t.Run(fmt.Sprintf("v%d_auto_%t_first_%d", version, autoCreate, timestamps[0]), func(t *testing.T) {
					memory := metadata.NewInMemoryStore(defaultMetadata())
					store := &listOffsetsStore{Store: memory}
					h := newTestHandler(store)
					h.autoCreateTopics = autoCreate
					for _, timestamp := range timestamps {
						for _, target := range []struct {
							topic     string
							partition int32
						}{{"missing", 0}, {"orders", 99}} {
							part := requestListOffset(t, h, version, target.topic, target.partition, timestamp)
							if part.Partition != target.partition || part.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
								t.Fatalf("timestamp %d: %+v", timestamp, part)
							}
							if version == 0 {
								if len(part.OldStyleOffsets) != 0 {
									t.Fatalf("error returned offsets: %+v", part)
								}
							} else if part.Offset != -1 || part.Timestamp != -1 {
								t.Fatalf("error returned a successful offset: %+v", part)
							}
						}
					}
					if store.creates.Load() != 0 {
						t.Fatal("ListOffsets attempted topic creation")
					}
					if _, err := memory.NextOffset(context.Background(), "missing", 0); !errors.Is(err, metadata.ErrUnknownTopic) {
						t.Fatalf("missing topic was created: %v", err)
					}
					req := kmsg.NewPtrCreateTopicsRequest()
					req.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: "missing", NumPartitions: 1, ReplicationFactor: 1}}
					payload, err := h.handleCreateTopics(context.Background(), &protocol.RequestHeader{CorrelationID: 176, APIVersion: 0}, req)
					if err != nil {
						t.Fatal(err)
					}
					created := decodeKmsgResponse(t, 0, payload, kmsg.NewPtrCreateTopicsResponse)
					if len(created.Topics) != 1 || created.Topics[0].ErrorCode != protocol.NONE {
						t.Fatalf("explicit creation after ListOffsets failed: %+v", created)
					}
				})
			}
		}
	}
}

func TestListOffsetsFailureMappingAndLogging(t *testing.T) {
	for _, version := range []int16{0, 4} {
		for _, timestamp := range []int64{-2, -1, 1234} {
			for _, failAt := range []int{1, 2} {
				if failAt == 2 && timestamp != -2 {
					continue
				}
				for _, tc := range []struct {
					name string
					err  error
					code int16
				}{
					{"unknown", fmt.Errorf("wrapped: %w", metadata.ErrUnknownTopic), protocol.UNKNOWN_TOPIC_OR_PARTITION},
					{"deadline", fmt.Errorf("wrapped: %w", context.DeadlineExceeded), protocol.REQUEST_TIMED_OUT},
					{"cancelled", fmt.Errorf("wrapped: %w", context.Canceled), protocol.REQUEST_TIMED_OUT},
					{"parse", errors.New("invalid stored next_offset"), protocol.UNKNOWN_SERVER_ERROR},
				} {
					t.Run(fmt.Sprintf("v%d_timestamp_%d_call_%d_%s", version, timestamp, failAt, tc.name), func(t *testing.T) {
						memory := metadata.NewInMemoryStore(defaultMetadata())
						calls := 0
						store := &listOffsetsStore{Store: memory, next: func(ctx context.Context, topic string, partition int32) (int64, error) {
							calls++
							if calls == failAt {
								return 0, tc.err
							}
							return memory.NextOffset(ctx, topic, partition)
						}}
						h := newTestHandler(store)
						var logs bytes.Buffer
						h.logger = slog.New(slog.NewJSONHandler(&logs, nil))
						part := requestListOffset(t, h, version, "orders", 0, timestamp)
						if part.ErrorCode != tc.code {
							t.Fatalf("error code=%d want=%d", part.ErrorCode, tc.code)
						}
						if version == 0 {
							if len(part.OldStyleOffsets) != 0 {
								t.Fatal("error contains offsets")
							}
						} else if part.Offset != -1 {
							t.Fatalf("error offset=%d", part.Offset)
						}
						if store.creates.Load() != 0 {
							t.Fatal("lookup failure triggered creation")
						}
						found := false
						for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
							if line == "" {
								continue
							}
							var record map[string]any
							if err := json.Unmarshal([]byte(line), &record); err != nil {
								t.Fatalf("invalid log %q: %v", line, err)
							}
							if record["msg"] != "list offsets failed" {
								continue
							}
							found = true
							if record["level"] != "WARN" || record["topic"] != "orders" || record["partition"] != float64(0) || record["timestamp"] != float64(timestamp) || record["error_code"] != float64(tc.code) || record["error"] != tc.err.Error() {
								t.Fatalf("incomplete diagnostic: %s", line)
							}
						}
						if !found {
							t.Fatalf("missing warning: %s", logs.String())
						}
					})
				}
			}
		}
	}
}

func TestListOffsetsDoesNotRecreateDeletedTopic(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached_%t", cached), func(t *testing.T) {
			memory := metadata.NewInMemoryStore(defaultMetadata())
			store := &listOffsetsStore{Store: memory}
			h := newTestHandler(store)
			h.autoCreateTopics = true
			if cached {
				if _, err := h.getPartitionLog(context.Background(), "orders", 0); err != nil {
					t.Fatal(err)
				}
				if err := memory.DeleteTopic(context.Background(), "orders"); err != nil {
					t.Fatal(err)
				}
			} else {
				calls := 0
				store.next = func(ctx context.Context, topic string, partition int32) (int64, error) {
					calls++
					offset, err := memory.NextOffset(ctx, topic, partition)
					if calls == 1 {
						if err := memory.DeleteTopic(ctx, topic); err != nil {
							t.Fatal(err)
						}
					}
					return offset, err
				}
			}
			part := requestListOffset(t, h, 4, "orders", 0, -2)
			if part.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
				t.Fatalf("deleted topic: %+v", part)
			}
			if store.creates.Load() != 0 {
				t.Fatal("deleted topic was recreated")
			}
		})
	}
}

func TestListOffsetsMixedPartitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	memory := metadata.NewInMemoryStore(defaultMetadata())
	if err := memory.UpdateOffsets(context.Background(), "orders", 0, 9); err != nil {
		t.Fatal(err)
	}
	store := &listOffsetsStore{Store: memory}
	h := newTestHandler(store)
	h.autoCreateTopics = true
	req := kmsg.NewPtrListOffsetsRequest()
	req.Topics = []kmsg.ListOffsetsRequestTopic{
		{Topic: "missing", Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: 0, Timestamp: -1}, {Partition: 0, Timestamp: -2}}},
		{Topic: "orders", Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: 0, Timestamp: -1}, {Partition: 99, Timestamp: -2}}},
	}
	payload, err := h.handleListOffsets(ctx, &protocol.RequestHeader{CorrelationID: 175, APIVersion: 4}, req)
	if err != nil {
		t.Fatal(err)
	}
	response := decodeKmsgResponse(t, 4, payload, kmsg.NewPtrListOffsetsResponse)
	if len(response.Topics) != 2 || len(response.Topics[0].Partitions) != 2 || len(response.Topics[1].Partitions) != 2 {
		t.Fatalf("invalid mixed response: %+v", response)
	}
	for _, part := range append(response.Topics[0].Partitions, response.Topics[1].Partitions[1]) {
		if part.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION || part.Offset != -1 {
			t.Fatalf("missing partition: %+v", part)
		}
	}
	valid := response.Topics[1].Partitions[0]
	if valid.ErrorCode != protocol.NONE || valid.Offset != 10 {
		t.Fatalf("valid partition affected by missing partitions: %+v", valid)
	}
	if store.creates.Load() != 0 {
		t.Fatal("mixed ListOffsets created a topic")
	}
}
