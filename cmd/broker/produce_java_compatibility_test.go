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
	"fmt"
	"testing"

	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka's Java decoder rejects null recordErrors even though some Go decoders
// accept it. Check the wire length directly to guard the failure from issue #28.
func TestProduceJavaRecordErrorsNonNullable(t *testing.T) {
	for _, version := range []int16{8, 9} {
		for _, tc := range []struct {
			name      string
			partition int32
			records   []byte
			wantCode  int16
		}{
			{"success", 0, testBatchBytes(0, 0, 1), protocol.NONE},
			{"unknown_partition", 99, testBatchBytes(0, 0, 1), protocol.UNKNOWN_TOPIC_OR_PARTITION},
			{"corrupt_batch", 0, []byte{1}, protocol.CORRUPT_MESSAGE},
		} {
			t.Run(fmt.Sprintf("v%d/%s", version, tc.name), func(t *testing.T) {
				h := newTestHandler(metadata.NewInMemoryStore(defaultMetadata()))
				req := &kmsg.ProduceRequest{
					Version: version, Acks: -1, TimeoutMillis: 1000,
					Topics: []kmsg.ProduceRequestTopic{{
						Topic: "orders",
						Partitions: []kmsg.ProduceRequestTopicPartition{{
							Partition: tc.partition, Records: tc.records,
						}},
					}},
				}
				wire, err := h.handleProduce(context.Background(), &protocol.RequestHeader{
					APIKey: protocol.APIKeyProduce, APIVersion: version, CorrelationID: 28,
				}, req)
				if err != nil {
					t.Fatal(err)
				}
				resp := decodeKmsgResponse(t, version, wire, kmsg.NewPtrProduceResponse)
				if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
					t.Fatalf("unexpected response: %+v", resp)
				}
				part := resp.Topics[0].Partitions[0]
				if part.ErrorCode != tc.wantCode {
					t.Fatalf("error code = %d, want %d", part.ErrorCode, tc.wantCode)
				}
				body, ok := protocol.SkipResponseHeader(protocol.APIKeyProduce, version, wire)
				if !ok {
					t.Fatal("invalid response header")
				}
				// Before recordErrors: topic array length, topic string, partition
				// array length, then partition/error/baseOffset/timestamp/logStartOffset.
				offset := 4 + 2 + len("orders") + 4 + 4 + 2 + 8 + 8 + 8
				want := []byte{0, 0, 0, 0} // empty non-null array in v8
				if version == 9 {
					offset = 1 + 1 + len("orders") + 1 + 4 + 2 + 8 + 8 + 8
					want = []byte{1} // compact empty array; zero would mean null
				}
				if len(body) < offset+len(want) || !bytes.Equal(body[offset:offset+len(want)], want) {
					t.Fatalf("recordErrors must encode as an empty non-null array: %x", body)
				}
			})
		}
	}
}
