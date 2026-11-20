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
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type fetchProgressHandler func(context.Context, net.Conn, *kmsg.FetchRequest) (*kmsg.FetchResponse, error)

func startFetchProgressBackend(t *testing.T, handle fetchProgressHandler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	var connections []net.Conn
	var workers sync.WaitGroup
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connections = append(connections, conn)
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer conn.Close()
				for {
					frame, err := protocol.ReadFrame(conn)
					if err != nil {
						return
					}
					header, request, err := protocol.ParseRequest(frame.Payload)
					if err != nil {
						t.Error(err)
						return
					}
					req, ok := request.(*kmsg.FetchRequest)
					if !ok {
						t.Errorf("request %T", request)
						return
					}
					response, err := handle(ctx, conn, req)
					if err != nil {
						return
					}
					if err := protocol.WriteFrame(conn, protocol.EncodeResponse(header.CorrelationID, header.APIVersion, response)); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		cancel()
		ln.Close()
		<-accepted
		mu.Lock()
		for _, conn := range connections {
			conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return ln.Addr().String()
}

func progressFetchResponse(req *kmsg.FetchRequest, records bool) *kmsg.FetchResponse {
	response := kmsg.NewPtrFetchResponse()
	for _, topic := range req.Topics {
		out := kmsg.FetchResponseTopic{Topic: topic.Topic, TopicID: topic.TopicID}
		for _, part := range topic.Partitions {
			result := kmsg.NewFetchResponseTopicPartition()
			result.Partition, result.HighWatermark, result.LastStableOffset = part.Partition, 128, 128
			if !records {
				result.HighWatermark = part.FetchOffset
				result.LastStableOffset = part.FetchOffset
			}
			if records {
				result.RecordBatches = lfsBuildRecordBatch([]kmsg.Record{{Value: []byte{byte(part.FetchOffset)}}})
				binary.BigEndian.PutUint64(result.RecordBatches[:8], uint64(part.FetchOffset))
			}
			out.Partitions = append(out.Partitions, result)
		}
		response.Topics = append(response.Topics, out)
	}
	return response
}

func fetchProgressProxy(backends ...string) *proxy {
	p := &proxy{backends: backends, backendRetries: 1, dialTimeout: time.Second, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	p.setReady(true)
	return p
}

func progressFetchGroups(version int16, ready, idle string) (*kmsg.FetchRequest, map[string]*kmsg.FetchRequest) {
	req := makeFetchRequestTyped(version, map[string][]int32{"orders": {0, 1}})
	req.MaxWaitMillis = 30000
	req.Topics[0].TopicID = [16]byte{1}
	groups := map[string]*kmsg.FetchRequest{}
	for i, addr := range []string{ready, idle} {
		sub := *req
		topic := req.Topics[0]
		topic.Partitions = []kmsg.FetchRequestTopicPartition{topic.Partitions[i]}
		sub.Topics = []kmsg.FetchRequestTopic{topic}
		groups[addr] = &sub
	}
	return req, groups
}

func TestFetchProgressDoesNotWaitForCaughtUpBroker(t *testing.T) {
	for _, version := range []int16{4, 12, 13} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			var readyConnections, idleConnections sync.Map
			ready := startFetchProgressBackend(t, func(_ context.Context, conn net.Conn, req *kmsg.FetchRequest) (*kmsg.FetchResponse, error) {
				readyConnections.Store(conn, true)
				return progressFetchResponse(req, true), nil
			})
			idle := startFetchProgressBackend(t, func(_ context.Context, conn net.Conn, req *kmsg.FetchRequest) (*kmsg.FetchResponse, error) {
				idleConnections.Store(conn, true)
				if req.MaxWaitMillis > 0 {
					_, err := protocol.ReadFrame(conn)
					return nil, err
				}
				return progressFetchResponse(req, false), nil
			})
			p := fetchProgressProxy(ready, idle)
			pool := newConnPool(time.Second)
			defer pool.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for offset := int64(0); offset < 128; offset++ {
				req, groups := progressFetchGroups(version, ready, idle)
				groups[ready].Topics[0].Partitions[0].FetchOffset = offset
				req.Topics[0].Partitions[0].FetchOffset = offset
				header := &protocol.RequestHeader{APIKey: protocol.APIKeyFetch, APIVersion: version, CorrelationID: int32(offset + 1)}
				wire, err := p.forwardFetch(ctx, header, req, nil, groups, pool)
				if err != nil {
					t.Fatalf("available records blocked behind idle broker: %v", err)
				}
				response, err := parseFetchResponse(wire, version)
				if err != nil {
					t.Fatal(err)
				}
				parts := map[int32]kmsg.FetchResponseTopicPartition{}
				for _, topic := range response.Topics {
					for _, part := range topic.Partitions {
						if _, exists := parts[part.Partition]; exists {
							t.Fatal("duplicate partition")
						}
						parts[part.Partition] = part
					}
				}
				if len(parts) != 2 || parts[0].ErrorCode != 0 || parts[1].ErrorCode != 0 || len(parts[0].RecordBatches) == 0 || progressRecordValue(t, parts[0].RecordBatches, offset) != byte(offset) || len(parts[1].RecordBatches) != 0 || parts[1].HighWatermark != 10 {
					t.Fatalf("offset=%d partitions=%+v", offset, parts)
				}
			}
			for _, connections := range []*sync.Map{&readyConnections, &idleConnections} {
				count := 0
				connections.Range(func(_, _ any) bool { count++; return true })
				if count != 1 {
					t.Fatalf("backlog draining reopened %d backend connections, want one", count)
				}
			}
		})
	}
}

func progressRecordValue(t *testing.T, wire []byte, offset int64) byte {
	t.Helper()
	batches, err := lfsDecodeRecordBatches(wire)
	if err != nil || len(batches) != 1 {
		t.Fatalf("batches=%d err=%v", len(batches), err)
	}
	if binary.BigEndian.Uint64(wire[:8]) != uint64(offset) {
		t.Fatal("record offset changed")
	}
	records, _, err := lfsDecodeBatchRecords(&batches[0], nil)
	if err != nil || len(records) != 1 || len(records[0].Value) != 1 {
		t.Fatalf("records=%v err=%v", records, err)
	}
	return records[0].Value[0]
}

func TestFetchProgressInterruptsLongPollAndReadsFreshMetadata(t *testing.T) {
	for _, version := range []int16{4, 12, 13} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			idleStarted := make(chan struct{})
			idleClosed := make(chan struct{})
			var once sync.Once
			var firstConn net.Conn
			var connMu sync.Mutex
			var zeroCalls atomic.Int32
			ready := startFetchProgressBackend(t, func(ctx context.Context, _ net.Conn, req *kmsg.FetchRequest) (*kmsg.FetchResponse, error) {
				if req.MaxWaitMillis == 0 {
					return progressFetchResponse(req, false), nil
				}
				select {
				case <-idleStarted:
					return progressFetchResponse(req, true), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			})
			idle := startFetchProgressBackend(t, func(_ context.Context, conn net.Conn, req *kmsg.FetchRequest) (*kmsg.FetchResponse, error) {
				if req.MaxWaitMillis > 0 {
					once.Do(func() { close(idleStarted) })
					_, err := protocol.ReadFrame(conn)
					close(idleClosed)
					return nil, err
				}
				call := zeroCalls.Add(1)
				if call == 1 {
					connMu.Lock()
					firstConn = conn
					connMu.Unlock()
					return progressFetchResponse(req, false), nil
				}
				connMu.Lock()
				sameConnection := conn == firstConn
				connMu.Unlock()
				if sameConnection {
					t.Error("interrupted socket was reused")
				}
				response := progressFetchResponse(req, true)
				response.Topics[0].Partitions[0].HighWatermark = 73
				response.Topics[0].Partitions[0].LastStableOffset = 73
				return response, nil
			})
			p := fetchProgressProxy(ready, idle)
			pool := newConnPool(time.Second)
			defer pool.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			request, groups := progressFetchGroups(version, ready, idle)
			header := &protocol.RequestHeader{APIKey: protocol.APIKeyFetch, APIVersion: version, CorrelationID: 44}
			wire, err := p.forwardFetch(ctx, header, request, nil, groups, pool)
			if err != nil {
				t.Fatal(err)
			}
			response, err := parseFetchResponse(wire, version)
			if err != nil {
				t.Fatal(err)
			}
			parts := map[int32]kmsg.FetchResponseTopicPartition{}
			for _, topic := range response.Topics {
				for _, part := range topic.Partitions {
					if _, ok := parts[part.Partition]; ok {
						t.Fatal("duplicate partition")
					}
					parts[part.Partition] = part
				}
			}
			if len(parts) != 2 || len(parts[0].RecordBatches) == 0 || len(parts[1].RecordBatches) == 0 || progressRecordValue(t, parts[1].RecordBatches, 10) != 10 || parts[1].HighWatermark != 73 || parts[1].ErrorCode != 0 {
				t.Fatalf("partitions=%+v", parts)
			}
			select {
			case <-idleClosed:
			case <-ctx.Done():
				t.Fatal("interrupted broker did not see EOF")
			}
			if zeroCalls.Load() != 2 || len(pool.conns) != 2 {
				t.Fatalf("idle probes=%d pooled=%d", zeroCalls.Load(), len(pool.conns))
			}
			if request.MaxWaitMillis != 30000 || groups[idle].MaxWaitMillis != 30000 {
				t.Fatal("original request mutated")
			}
		})
	}
}

func TestFetchProgressParentCancellationDoesNotRefetch(t *testing.T) {
	entered := make(chan struct{}, 2)
	var probes atomic.Int32
	handler := func(_ context.Context, conn net.Conn, req *kmsg.FetchRequest) (*kmsg.FetchResponse, error) {
		if req.MaxWaitMillis == 0 {
			probes.Add(1)
			return progressFetchResponse(req, false), nil
		}
		entered <- struct{}{}
		_, err := protocol.ReadFrame(conn)
		return nil, err
	}
	a, b := startFetchProgressBackend(t, handler), startFetchProgressBackend(t, handler)
	p := fetchProgressProxy(a, b)
	pool := newConnPool(time.Second)
	defer pool.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, groups := progressFetchGroups(4, a, b)
	done := make(chan error, 1)
	go func() {
		_, err := p.forwardFetch(ctx, &protocol.RequestHeader{APIKey: 1, APIVersion: 4}, req, nil, groups, pool)
		done <- err
	}()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("long polls not started")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fetch ignored cancellation")
	}
	if probes.Load() != 2 || len(pool.conns) != 0 {
		t.Fatalf("probes=%d pooled=%d", probes.Load(), len(pool.conns))
	}
}

func TestFetchResultsWaitPolicy(t *testing.T) {
	empty := &kmsg.FetchResponse{Topics: []kmsg.FetchResponseTopic{{Partitions: []kmsg.FetchResponseTopicPartition{{}}}}}
	records := &kmsg.FetchResponse{Topics: []kmsg.FetchResponseTopic{{Partitions: []kmsg.FetchResponseTopicPartition{{RecordBatches: []byte{1}}}}}}
	for _, tc := range []struct {
		name    string
		results []fetchFanOutResult
		minimum int32
		wait    bool
	}{
		{"empty", []fetchFanOutResult{{subResp: empty}}, 1, true},
		{"below minimum", []fetchFanOutResult{{subResp: records}}, 2, true},
		{"combined minimum", []fetchFanOutResult{{subResp: records}, {subResp: records}}, 2, false},
		{"transport error", []fetchFanOutResult{{err: io.EOF}}, 1, false},
		{"nil response", []fetchFanOutResult{{}}, 1, false},
		{"session error", []fetchFanOutResult{{subResp: &kmsg.FetchResponse{ErrorCode: 70}}}, 1, false},
		{"partition error", []fetchFanOutResult{{subResp: &kmsg.FetchResponse{Topics: []kmsg.FetchResponseTopic{{Partitions: []kmsg.FetchResponseTopicPartition{{ErrorCode: 6}}}}}}}, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := fetchResultsCanWait(tc.results, tc.minimum); got != tc.wait {
				t.Fatalf("wait=%v want %v", got, tc.wait)
			}
		})
	}
	if fetchResponseHasRecords(nil) || fetchResponseHasRecords(&kmsg.FetchResponse{ErrorCode: 70}) || fetchResponseHasRecords(empty) || !fetchResponseHasRecords(records) {
		t.Fatal("incorrect record detection")
	}
}

func TestFetchProgressRetriesWithoutWaitingAfterRecords(t *testing.T) {
	var calls atomic.Int32
	ready := startFetchProgressBackend(t, func(_ context.Context, _ net.Conn, req *kmsg.FetchRequest) (*kmsg.FetchResponse, error) {
		return progressFetchResponse(req, true), nil
	})
	idle := startFetchProgressBackend(t, func(_ context.Context, _ net.Conn, req *kmsg.FetchRequest) (*kmsg.FetchResponse, error) {
		if calls.Add(1) == 1 {
			return nil, io.EOF
		}
		if req.MaxWaitMillis != 0 {
			t.Error("retry restarted long polling despite retained records")
		}
		return progressFetchResponse(req, false), nil
	})
	p := fetchProgressProxy(idle)
	pool := newConnPool(time.Second)
	defer pool.Close()
	req, groups := progressFetchGroups(4, ready, idle)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wire, err := p.forwardFetch(ctx, &protocol.RequestHeader{APIKey: 1, APIVersion: 4}, req, nil, groups, pool)
	if err != nil {
		t.Fatal(err)
	}
	response, err := parseFetchResponse(wire, 4)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, topic := range response.Topics {
		for _, part := range topic.Partitions {
			count++
			if part.ErrorCode != 0 {
				t.Fatal(part.ErrorCode)
			}
		}
	}
	if count != 2 || calls.Load() != 2 {
		t.Fatalf("parts=%d calls=%d", count, calls.Load())
	}
}

func TestFetchProbeDoesNotResetExpiredWaitBudget(t *testing.T) {
	var calls atomic.Int32
	handler := func(ctx context.Context, _ net.Conn, req *kmsg.FetchRequest) (*kmsg.FetchResponse, error) {
		calls.Add(1)
		if req.MaxWaitMillis != 0 {
			t.Error("expired budget started a long poll")
		}
		timer := time.NewTimer(5 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return progressFetchResponse(req, false), nil
	}
	a, b := startFetchProgressBackend(t, handler), startFetchProgressBackend(t, handler)
	p := fetchProgressProxy(a, b)
	pool := newConnPool(time.Second)
	defer pool.Close()
	req, groups := progressFetchGroups(4, a, b)
	req.MaxWaitMillis = 1
	for _, sub := range groups {
		sub.MaxWaitMillis = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := p.forwardFetch(ctx, &protocol.RequestHeader{APIKey: 1, APIVersion: 4}, req, nil, groups, pool); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("requests=%d", calls.Load())
	}
}
