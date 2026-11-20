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
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type incrementalFetchBackend struct {
	mu      sync.Mutex
	batch   []byte
	changed chan struct{}
	polls   chan *kmsg.FetchRequest
	addr    string
}

func newIncrementalFetchBackend(t *testing.T) *incrementalFetchBackend {
	b := &incrementalFetchBackend{changed: make(chan struct{}), polls: make(chan *kmsg.FetchRequest, 32)}
	b.addr = startFetchProgressBackend(t, func(ctx context.Context, _ net.Conn, req *kmsg.FetchRequest) (*kmsg.FetchResponse, error) {
		if req.MaxWaitMillis > 0 {
			select {
			case b.polls <- req:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		timer := time.NewTimer(time.Duration(req.MaxWaitMillis) * time.Millisecond)
		defer timer.Stop()
		for {
			b.mu.Lock()
			batch := b.batch
			changed := b.changed
			b.mu.Unlock()
			if len(batch) >= int(req.MinBytes) || req.MaxWaitMillis <= 0 {
				return b.response(req, batch), nil
			}
			select {
			case <-changed:
			case <-timer.C:
				b.mu.Lock()
				batch = b.batch
				b.mu.Unlock()
				return b.response(req, batch), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	})
	return b
}

func (b *incrementalFetchBackend) response(req *kmsg.FetchRequest, batch []byte) *kmsg.FetchResponse {
	response := progressFetchResponse(req, false)
	for i := range response.Topics {
		for j := range response.Topics[i].Partitions {
			part := &response.Topics[i].Partitions[j]
			part.RecordBatches = append([]byte(nil), batch...)
			if len(batch) > 0 {
				offset := req.Topics[i].Partitions[j].FetchOffset
				batches, err := lfsDecodeRecordBatches(part.RecordBatches)
				if err != nil {
					panic(err)
				} // setBytes constructs valid fixture batches.
				for _, recordBatch := range batches {
					binary.BigEndian.PutUint64(recordBatch.Raw[:8], uint64(offset))
					offset += int64(recordBatch.NumRecords)
				}
				part.HighWatermark, part.LastStableOffset = offset, offset
			}
		}
	}
	return response
}

func (b *incrementalFetchBackend) setBytes(t *testing.T, size int) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	additional := size - len(b.batch)
	if additional < 0 {
		t.Fatal("fixture must append records without replacing prior records")
	}
	if additional == 0 {
		return
	}
	var batch []byte
	for n := 0; n < additional; n++ {
		candidate := lfsBuildRecordBatch([]kmsg.Record{{Value: bytes.Repeat([]byte("x"), n)}})
		if len(candidate) == additional {
			batch = candidate
			break
		}
	}
	if batch == nil {
		t.Fatalf("cannot construct %d-byte record batch", additional)
	}
	b.batch = append(append([]byte(nil), b.batch...), batch...)
	close(b.changed)
	b.changed = make(chan struct{})
}

func awaitIncrementalPoll(t *testing.T, b *incrementalFetchBackend) *kmsg.FetchRequest {
	t.Helper()
	select {
	case req := <-b.polls:
		return req
	case <-time.After(time.Second):
		t.Fatal("backend did not start its long poll")
		return nil
	}
}

func startAggregateFetch(t *testing.T, a, b *incrementalFetchBackend, minimum, wait int32) (<-chan []byte, <-chan error) {
	t.Helper()
	p := fetchProgressProxy(a.addr, b.addr)
	pool := newConnPool(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	req, groups := progressFetchGroups(12, a.addr, b.addr)
	req.MinBytes, req.MaxWaitMillis = minimum, wait
	for _, sub := range groups {
		sub.MinBytes, sub.MaxWaitMillis = minimum, wait
	}
	response := make(chan []byte, 1)
	failure := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer pool.Close()
		wire, err := p.forwardFetch(ctx, &protocol.RequestHeader{APIKey: 1, APIVersion: 12}, req, nil, groups, pool)
		if err != nil {
			failure <- err
			return
		}
		response <- wire
	}()
	t.Cleanup(func() { cancel(); <-done })
	return response, failure
}

func awaitAggregateBytes(t *testing.T, response <-chan []byte, failure <-chan error, want int) {
	t.Helper()
	select {
	case err := <-failure:
		t.Fatal(err)
	case wire := <-response:
		decoded, err := parseFetchResponse(wire, 12)
		if err != nil {
			t.Fatal(err)
		}
		total, partitions := 0, map[int32]bool{}
		for _, topic := range decoded.Topics {
			for _, part := range topic.Partitions {
				if part.ErrorCode != 0 || partitions[part.Partition] {
					t.Fatalf("invalid partition response: %+v", part)
				}
				partitions[part.Partition] = true
				batches, err := lfsDecodeRecordBatches(part.RecordBatches)
				if err != nil {
					t.Fatal(err)
				}
				offset := int64(part.Partition) * 10 // progressFetchGroups starts at these offsets.
				for _, batch := range batches {
					if batch.FirstOffset != offset {
						t.Fatalf("partition %d returned offset %d, want %d", part.Partition, batch.FirstOffset, offset)
					}
					offset += int64(batch.NumRecords)
				}
				if part.HighWatermark != offset || part.LastStableOffset != offset {
					t.Fatal("watermarks do not match returned records")
				}
				total += len(part.RecordBatches)
			}
		}
		if total != want || len(partitions) != 2 {
			t.Fatalf("bytes=%d partitions=%d, want %d bytes in two partitions", total, len(partitions), want)
		}
	case <-time.After(time.Second):
		t.Fatal("aggregate MinBytes reached but Fetch still waited")
	}
}

func TestFetchAggregateMinBytesAfterProbe(t *testing.T) {
	a, b := newIncrementalFetchBackend(t), newIncrementalFetchBackend(t)
	response, failure := startAggregateFetch(t, a, b, 1000, 30000)
	awaitIncrementalPoll(t, a)
	awaitIncrementalPoll(t, b)
	a.setBytes(t, 600)
	b.setBytes(t, 400)
	awaitAggregateBytes(t, response, failure, 1000)
}

func TestFetchAggregateMinBytesTracksSuccessiveGrowth(t *testing.T) {
	a, b := newIncrementalFetchBackend(t), newIncrementalFetchBackend(t)
	response, failure := startAggregateFetch(t, a, b, 1000, 30000)
	for _, backend := range []*incrementalFetchBackend{a, b} {
		if req := awaitIncrementalPoll(t, backend); req.MinBytes != 1 {
			t.Fatalf("initial increment target=%d", req.MinBytes)
		}
	}
	a.setBytes(t, 600)
	if req := awaitIncrementalPoll(t, a); req.MinBytes != 601 {
		t.Fatalf("increment target=%d want 601", req.MinBytes)
	}
	select {
	case <-response:
		t.Fatal("returned only 600 bytes below aggregate minimum")
	case err := <-failure:
		t.Fatal(err)
	default:
	}
	a.setBytes(t, 800)
	if req := awaitIncrementalPoll(t, a); req.MinBytes != 801 {
		t.Fatalf("increment target=%d want 801", req.MinBytes)
	}
	select {
	case <-response:
		t.Fatal("returned only 800 bytes below aggregate minimum")
	case err := <-failure:
		t.Fatal(err)
	default:
	}
	b.setBytes(t, 200)
	awaitAggregateBytes(t, response, failure, 1000)
}

func TestFetchAggregateMinBytesIncludesPartialProbe(t *testing.T) {
	a, b := newIncrementalFetchBackend(t), newIncrementalFetchBackend(t)
	a.setBytes(t, 600)
	response, failure := startAggregateFetch(t, a, b, 1000, 30000)
	if req := awaitIncrementalPoll(t, a); req.MinBytes != 601 {
		t.Fatalf("partial probe increment target=%d", req.MinBytes)
	}
	if req := awaitIncrementalPoll(t, b); req.MinBytes != 1 {
		t.Fatalf("empty probe increment target=%d", req.MinBytes)
	}
	select {
	case <-response:
		t.Fatal("partial probe bypassed MinBytes")
	case err := <-failure:
		t.Fatal(err)
	default:
	}
	b.setBytes(t, 400)
	awaitAggregateBytes(t, response, failure, 1000)
}

func TestFetchAggregateMinBytesExpiresOnce(t *testing.T) {
	for _, available := range []int{0, 600} {
		t.Run(fmt.Sprint(available), func(t *testing.T) {
			a, b := newIncrementalFetchBackend(t), newIncrementalFetchBackend(t)
			a.setBytes(t, available)
			start := time.Now()
			response, failure := startAggregateFetch(t, a, b, 1000, 80)
			awaitAggregateBytes(t, response, failure, available)
			if time.Since(start) < 75*time.Millisecond {
				t.Fatal("fetch returned below minimum before its deadline")
			}
			if len(a.polls) > 2 || len(b.polls) > 2 {
				t.Fatal("unchanged records caused repeated immediate polling")
			}
		})
	}
}

func TestFetchFanOutConnectionErrorKeepsOriginalRoute(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := makeFetchRequestTyped(4, map[string][]int32{"orders": {0}})
	pool := newConnPool(time.Second)
	defer pool.Close()
	results := fetchProgressProxy().fanOutFetchOnce(ctx, &protocol.RequestHeader{APIKey: 1, APIVersion: 4}, map[string]*kmsg.FetchRequest{"missing-broker": req}, nil, nil, pool)
	if len(results) != 1 || results[0].route != "missing-broker" || results[0].target != "missing-broker" || results[0].subReq != req || results[0].err == nil || results[0].conn != nil {
		t.Fatalf("results=%+v", results)
	}
}
