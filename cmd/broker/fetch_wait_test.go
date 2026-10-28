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
	"fmt"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	"testing"
	"time"
)

func fetchWaitFixture(t *testing.T) (*handler, *kmsg.FetchRequest) {
	t.Helper()
	store := metadata.NewInMemoryStore(defaultMetadata())
	if err := store.CreatePartitions(context.Background(), "orders", 8); err != nil {
		t.Fatal(err)
	}
	h := newFlushDisabledHandler(store)
	req := kmsg.NewPtrFetchRequest()
	req.Version = 4
	req.MinBytes = 1
	req.MaxWaitMillis = 100
	req.Topics = []kmsg.FetchRequestTopic{{Topic: "orders"}}
	for i := int32(0); i < 8; i++ {
		req.Topics[0].Partitions = append(req.Topics[0].Partitions, kmsg.FetchRequestTopicPartition{Partition: i, FetchOffset: 0, PartitionMaxBytes: 1 << 20})
	}
	return h, req
}

func produceFetchWaitRecord(t *testing.T, h *handler, partition int32) {
	t.Helper()
	if err := produceFetchWait(h, partition); err != nil {
		t.Fatal(err)
	}
}

func produceFetchWait(h *handler, partition int32) error {
	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.Topics = []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: partition, Records: testBatchBytes(0, 0, 1)}}}}
	data, err := h.handleProduce(context.Background(), &protocol.RequestHeader{APIVersion: 3}, req)
	if err != nil {
		return err
	}
	resp := kmsg.NewPtrProduceResponse()
	resp.Version = 3
	if err := resp.ReadFrom(data[4:]); err != nil {
		return err
	}
	if resp.Topics[0].Partitions[0].ErrorCode != 0 {
		return fmt.Errorf("produce error %d", resp.Topics[0].Partitions[0].ErrorCode)
	}
	return nil
}

func timedFetch(t *testing.T, h *handler, ctx context.Context, req *kmsg.FetchRequest) (*kmsg.FetchResponse, time.Duration) {
	t.Helper()
	start := time.Now()
	data, err := h.handleFetch(ctx, &protocol.RequestHeader{APIVersion: 4}, req)
	if err != nil {
		t.Fatal(err)
	}
	return decodeKmsgResponse(t, 4, data, kmsg.NewPtrFetchResponse), time.Since(start)
}

func TestFetchWaitIsSharedAcrossPartitions(t *testing.T) {
	h, req := fetchWaitFixture(t)
	resp, elapsed := timedFetch(t, h, context.Background(), req)
	if elapsed < 80*time.Millisecond || elapsed > 450*time.Millisecond {
		t.Fatalf("8 empty partitions took %s for a 100ms request budget", elapsed)
	}
	if len(resp.Topics[0].Partitions) != 8 {
		t.Fatal(resp)
	}
	for _, part := range resp.Topics[0].Partitions {
		if part.ErrorCode != 0 || len(part.RecordBatches) != 0 {
			t.Fatal(part)
		}
	}
}

func TestFetchScansLastPartitionBeforeWaiting(t *testing.T) {
	h, req := fetchWaitFixture(t)
	produceFetchWaitRecord(t, h, 7)
	resp, elapsed := timedFetch(t, h, context.Background(), req)
	if elapsed > 450*time.Millisecond || len(resp.Topics[0].Partitions[7].RecordBatches) == 0 {
		t.Fatal("data in last partition stalled", elapsed, resp)
	}
}

func TestFetchWakesForDataOnAnyPartition(t *testing.T) {
	h, req := fetchWaitFixture(t)
	req.MaxWaitMillis = 1000
	ready := make(chan error, 1)
	go func() { time.Sleep(40 * time.Millisecond); ready <- produceFetchWait(h, 7) }()
	resp, elapsed := timedFetch(t, h, context.Background(), req)
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	if elapsed > 500*time.Millisecond || len(resp.Topics[0].Partitions[7].RecordBatches) == 0 {
		t.Fatal("late data was not observed", elapsed, resp)
	}
}

func TestFetchMinBytesAndCancellation(t *testing.T) {
	h, req := fetchWaitFixture(t)
	produceFetchWaitRecord(t, h, 7)
	// An available batch below MinBytes must wait for the request deadline.
	req.MinBytes = 1 << 20
	resp, elapsed := timedFetch(t, h, context.Background(), req)
	if elapsed < 80*time.Millisecond || elapsed > 450*time.Millisecond || len(resp.Topics[0].Partitions[7].RecordBatches) == 0 {
		t.Fatal(elapsed, resp)
	}
	h.fetchRate.mu.Lock()
	var counted int64
	for _, count := range h.fetchRate.buckets {
		counted += count
	}
	h.fetchRate.mu.Unlock()
	if counted != 1 {
		t.Fatal("polling counted records more than once", counted)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	req.MaxWaitMillis = 1000
	_, elapsed = timedFetch(t, h, ctx, req)
	if elapsed > 450*time.Millisecond {
		t.Fatal("cancellation ignored", elapsed)
	}
	req.MinBytes = 0
	_, elapsed = timedFetch(t, h, context.Background(), req)
	if elapsed > 450*time.Millisecond {
		t.Fatal("zero MinBytes waited", elapsed)
	}
}

func TestFetchMinBytesCombinesPartitions(t *testing.T) {
	h, req := fetchWaitFixture(t)
	produceFetchWaitRecord(t, h, 0)
	produceFetchWaitRecord(t, h, 7)
	req.MinBytes = int32(2 * len(testBatchBytes(0, 0, 1)))
	resp, elapsed := timedFetch(t, h, context.Background(), req)
	if elapsed > 450*time.Millisecond || len(resp.Topics[0].Partitions[0].RecordBatches) == 0 || len(resp.Topics[0].Partitions[7].RecordBatches) == 0 {
		t.Fatal(elapsed, resp)
	}
}
