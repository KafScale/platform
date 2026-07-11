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
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
)

// newFlushDisabledHandler builds a handler whose per-acknowledgement flush is
// turned off and whose buffer thresholds never trip, so an acknowledged produce
// stays in the in-memory write buffer instead of being written to a segment.
// This is the KAFSCALE_PRODUCE_SYNC_FLUSH=false shape, reproduced without env.
func newFlushDisabledHandler(store metadata.Store) *handler {
	h := newTestHandler(store)
	h.flushOnAck = false
	// Thresholds that never trip: no size flush (1 GiB), no time flush.
	h.logConfig.Buffer = storage.WriteBufferConfig{
		MaxBytes:      1 << 30,
		FlushInterval: 0,
	}
	return h
}

// TestHandleFetchReadAfterAckFlushDisabled drives the REAL fetch path
// (handleFetch -> waitForFetchData -> high-watermark bound -> PartitionLog.Read),
// not a direct PartitionLog.Read call. It produces with acks=-1 under
// flushOnAck=false (nothing flushes), then fetches the just-acknowledged offset.
//
// A Kafka client reads up to the high-watermark the broker advertises. The fetch
// handler bounds the read by that watermark: FetchOffset == watermark returns
// empty, FetchOffset > watermark returns OFFSET_OUT_OF_RANGE, and only
// FetchOffset < watermark reaches PartitionLog.Read. The watermark comes from the
// metadata store and advances on flush. With flushOnAck=false and no threshold
// tripped, no flush fires, so the watermark stays at 0 and the acknowledged
// record is never requested through the real path. The storage-layer buffer
// fallback (PartitionLog.Read serving the buffer) is therefore not reachable
// end-to-end unless the broker also makes the buffered tail visible in the
// high-watermark it advertises on fetch.
//
// This test asserts the corrected end-to-end behavior: with flushOnAck=false the
// broker advertises a watermark that includes the buffered tail, so the
// acknowledged record is consumable. It fails when the watermark ignores the
// buffer.
func TestHandleFetchReadAfterAckFlushDisabled(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	h := newFlushDisabledHandler(store)

	const messages = 5
	for i := 0; i < messages; i++ {
		produceReq := &kmsg.ProduceRequest{
			Acks:          -1,
			TimeoutMillis: 1000,
			Topics: []kmsg.ProduceRequestTopic{
				{
					Topic: "orders",
					Partitions: []kmsg.ProduceRequestTopicPartition{
						{
							Partition: 0,
							Records:   testBatchBytes(0, 0, 1),
						},
					},
				},
			},
		}
		if _, err := h.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: int32(i + 1)}, produceReq); err != nil {
			t.Fatalf("handleProduce %d: %v", i, err)
		}
	}

	// Nothing should have flushed: the metadata-store offset (durable
	// high-watermark) is still 0. This is the precondition that makes the bug
	// observable: without the fix, the fetch path bounds reads at this 0.
	durable, err := store.NextOffset(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("NextOffset: %v", err)
	}
	if durable != 0 {
		t.Fatalf("precondition: expected durable high-watermark 0 (nothing flushed), got %d", durable)
	}

	fetchReq := &kmsg.FetchRequest{
		MaxWaitMillis: 50,
		Topics: []kmsg.FetchRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.FetchRequestTopicPartition{
					{
						Partition:         0,
						FetchOffset:       0,
						PartitionMaxBytes: 1 << 20,
					},
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := h.handleFetch(ctx, &protocol.RequestHeader{CorrelationID: 100, APIVersion: 11}, fetchReq)
	if err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	fetchResp := decodeKmsgResponse(t, 11, resp, kmsg.NewPtrFetchResponse)
	if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
		t.Fatalf("expected a topic partition in the fetch response")
	}
	p := fetchResp.Topics[0].Partitions[0]
	if p.ErrorCode != 0 {
		t.Fatalf("fetch returned error code %d for an acknowledged offset", p.ErrorCode)
	}
	if p.HighWatermark < int64(messages) {
		t.Fatalf("read-after-ack: high-watermark %d does not include the %d acknowledged-but-buffered records; "+
			"a consumer bounded by this watermark never requests them", p.HighWatermark, messages)
	}
	if len(p.RecordBatches) == 0 {
		t.Fatalf("read-after-ack: fetch at offset 0 returned no records for %d acknowledged-but-buffered messages", messages)
	}
}

// TestHandleFetchDefaultFlushOnAckNoBufferFallback backs the no-op-in-default
// claim. In the default configuration (KAFSCALE_PRODUCE_SYNC_FLUSH=true) every
// acknowledged produce is flushed before the ack, so the write buffer is empty
// at read time, the durable high-watermark already covers every acknowledged
// offset, and the flush-disabled visibility raise added for read-after-ack is a
// no-op (it only fires when flushOnAck is false). This asserts the buffer is
// empty after the acks and that the durable watermark already includes them, so
// the buffer fallback is never the source in the default path.
func TestHandleFetchDefaultFlushOnAckNoBufferFallback(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	h := newTestHandler(store) // flushOnAck defaults to true

	if !h.flushOnAck {
		t.Fatalf("precondition: expected default flushOnAck=true")
	}

	const messages = 5
	for i := 0; i < messages; i++ {
		produceReq := &kmsg.ProduceRequest{
			Acks:          -1,
			TimeoutMillis: 1000,
			Topics: []kmsg.ProduceRequestTopic{
				{
					Topic: "orders",
					Partitions: []kmsg.ProduceRequestTopicPartition{
						{
							Partition: 0,
							Records:   testBatchBytes(0, 0, 1),
						},
					},
				},
			},
		}
		if _, err := h.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: int32(i + 1)}, produceReq); err != nil {
			t.Fatalf("handleProduce %d: %v", i, err)
		}
	}

	// Every ack flushed: the durable high-watermark already covers all messages.
	durable, err := store.NextOffset(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("NextOffset: %v", err)
	}
	if durable != int64(messages) {
		t.Fatalf("default path: expected durable high-watermark %d (every ack flushed), got %d", messages, durable)
	}

	// The flush-disabled visibility raise is a no-op here: the buffered
	// high-watermark equals the durable one, which means there is no buffered
	// tail beyond what is already flushed, so the buffer fallback can never be
	// the source of a read in the default path. (The storage-layer assertion
	// that the buffer itself is empty after a flush lives in
	// pkg/storage: TestWriteBufferDrainEmptiesBuffer.)
	plog, err := h.getPartitionLog(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("getPartitionLog: %v", err)
	}
	if buffered := plog.BufferedHighWatermark(); buffered != durable {
		t.Fatalf("default path: buffered high-watermark %d should equal durable %d (visibility raise must be a no-op)", buffered, durable)
	}

	// And the records are consumable end-to-end (via segments, not the buffer).
	fetchReq := &kmsg.FetchRequest{
		MaxWaitMillis: 50,
		Topics: []kmsg.FetchRequestTopic{
			{
				Topic: "orders",
				Partitions: []kmsg.FetchRequestTopicPartition{
					{Partition: 0, FetchOffset: 0, PartitionMaxBytes: 1 << 20},
				},
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := h.handleFetch(ctx, &protocol.RequestHeader{CorrelationID: 200, APIVersion: 11}, fetchReq)
	if err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	fetchResp := decodeKmsgResponse(t, 11, resp, kmsg.NewPtrFetchResponse)
	if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
		t.Fatalf("expected a topic partition in the fetch response")
	}
	if got := fetchResp.Topics[0].Partitions[0]; len(got.RecordBatches) == 0 {
		t.Fatalf("default path: fetch returned no records (should be served from segments)")
	}
}
