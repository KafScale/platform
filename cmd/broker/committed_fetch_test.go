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
	"testing"

	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestReadCommittedExcludesBufferedTail(t *testing.T) {
	ctx := context.Background()
	h := newFlushDisabledHandler(metadata.NewInMemoryStore(defaultMetadata()))
	t.Cleanup(h.coordinator.Stop)
	produce := &kmsg.ProduceRequest{Version: 9, Acks: -1, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Records: testBatchBytes(0, 0, 1)}}}}}
	wire, err := h.Handle(ctx, &protocol.RequestHeader{APIKey: 0, APIVersion: 9}, produce)
	if err != nil {
		t.Fatal(err)
	}
	if p := decodeKmsgResponse(t, 9, wire, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]; p.ErrorCode != 0 {
		t.Fatal(p)
	}
	fetch := func(isolation int8, offset int64) kmsg.FetchResponseTopicPartition {
		t.Helper()
		req := kmsg.NewPtrFetchRequest()
		req.Version, req.IsolationLevel, req.MaxBytes = 4, isolation, 1<<20
		req.Topics = []kmsg.FetchRequestTopic{{Topic: "orders", Partitions: []kmsg.FetchRequestTopicPartition{{FetchOffset: offset, PartitionMaxBytes: 1 << 20}}}}
		wire, err := h.Handle(ctx, &protocol.RequestHeader{APIKey: 1, APIVersion: 4}, req)
		if err != nil {
			t.Fatal(err)
		}
		part := decodeKmsgResponse(t, 4, wire, kmsg.NewPtrFetchResponse).Topics[0].Partitions[0]
		if part.ErrorCode != 0 {
			t.Fatal(part)
		}
		return part
	}
	if p := fetch(0, 0); len(p.RecordBatches) == 0 || p.HighWatermark != 1 || p.LastStableOffset != 0 {
		t.Fatalf("uncommitted buffered read: %+v", p)
	}
	if p := fetch(1, 0); len(p.RecordBatches) != 0 || p.HighWatermark != 0 || p.LastStableOffset != 0 {
		t.Fatalf("committed exposed buffered tail: %+v", p)
	}
	if p := fetch(1, 1); len(p.RecordBatches) != 0 || p.LastStableOffset != 0 {
		t.Fatalf("position in volatile tail must wait: %+v", p)
	}
	log, err := h.getPartitionLog(ctx, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if p := fetch(1, 0); len(p.RecordBatches) == 0 || p.HighWatermark != 1 || p.LastStableOffset != 1 || len(p.AbortedTransactions) != 0 {
		t.Fatalf("committed did not advance after flush: %+v", p)
	}
}

func TestCommittedFetchStableBoundary(t *testing.T) {
	first, second := testBatchBytes(0, 1, 2), testBatchBytes(2, 0, 1)
	records := append(append([]byte(nil), first...), second...)
	for _, tc := range []struct {
		stable int64
		want   int
	}{{0, 0}, {1, 0}, {2, len(first)}, {3, len(records)}} {
		if got := limitFetchToStableOffset(records, tc.stable); len(got) != tc.want {
			t.Fatalf("stable %d: got %d bytes, want %d", tc.stable, len(got), tc.want)
		}
	}
}
