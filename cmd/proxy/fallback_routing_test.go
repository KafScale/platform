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
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	"io"
	"log/slog"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestCoalesceFallbackPreservesTopicsAndInputs(t *testing.T) {
	for _, acks := range []int16{0, -1} {
		known := &kmsg.ProduceRequest{Version: 9, Acks: acks, TimeoutMillis: 9000, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 7}}}}}
		fallback := &kmsg.ProduceRequest{Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0}}}, {Topic: "events", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 1}}}}}
		groups := map[string]*kmsg.ProduceRequest{"b": {}, "a": known, "": fallback}
		coalesceProduceFallback(groups)
		merged := groups["a"]
		if len(groups) != 2 || groups[""] != nil || merged.Acks != acks || merged.Version != 9 || merged.TimeoutMillis != 9000 || countPartitions(merged) != 3 || len(merged.Topics) != 2 {
			t.Fatal(groups)
		}
		merged.Topics[0].Partitions[0].Partition = 99
		if known.Topics[0].Partitions[0].Partition != 7 || len(known.Topics) != 1 || fallback.Topics[0].Partitions[0].Partition != 0 {
			t.Fatal("mutated original requests")
		}
	}
	idA, idB := [16]byte{1}, [16]byte{2}
	known := &kmsg.FetchRequest{Version: 13, MaxWaitMillis: 5000, MinBytes: 1, MaxBytes: 1 << 20, SessionID: 7, Topics: []kmsg.FetchRequestTopic{{TopicID: idA, Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 7}}}}}
	fallback := &kmsg.FetchRequest{Topics: []kmsg.FetchRequestTopic{{TopicID: idA, Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 0}}}, {TopicID: idB, Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 1}}}}}
	groups := map[string]*kmsg.FetchRequest{"a": known, "": fallback}
	coalesceFetchFallback(groups)
	merged := groups["a"]
	if len(groups) != 1 || merged.MaxWaitMillis != 5000 || merged.MinBytes != 1 || merged.MaxBytes != 1<<20 || merged.SessionID != 7 || len(merged.Topics) != 2 || len(merged.Topics[0].Partitions) != 2 || merged.Topics[1].TopicID != idB {
		t.Fatal(merged)
	}
	merged.Topics[0].Partitions[0].Partition = 99
	if known.Topics[0].Partitions[0].Partition != 7 || len(known.Topics[0].Partitions) != 1 {
		t.Fatal("mutated original fetch")
	}
	// A wholly unowned request keeps normal backend discovery.
	only := map[string]*kmsg.FetchRequest{"": fallback}
	coalesceFetchFallback(only)
	if !reflect.DeepEqual(only, map[string]*kmsg.FetchRequest{"": fallback}) {
		t.Fatal(only)
	}
	coalesceFetchFallback(nil)
	coalesceProduceFallback(nil)
}

type routedRequest struct {
	api      int16
	parts    int
	clientID string
}

func fallbackBackend(t *testing.T) (string, <-chan routedRequest) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var connections []net.Conn
	seen := make(chan routedRequest, 16)
	var workers sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
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
						return
					}
					result := routedRequest{api: header.APIKey, clientID: clientIDStr(header.ClientID)}
					var payload []byte
					switch req := request.(type) {
					case *kmsg.FetchRequest:
						resp := kmsg.NewPtrFetchResponse()
						for _, topic := range req.Topics {
							out := kmsg.FetchResponseTopic{Topic: topic.Topic, TopicID: topic.TopicID}
							for _, part := range topic.Partitions {
								result.parts++
								out.Partitions = append(out.Partitions, kmsg.FetchResponseTopicPartition{Partition: part.Partition, RecordBatches: []byte{1}})
							}
							resp.Topics = append(resp.Topics, out)
						}
						payload = protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp)
					case *kmsg.ProduceRequest:
						resp := kmsg.NewPtrProduceResponse()
						for _, topic := range req.Topics {
							out := kmsg.ProduceResponseTopic{Topic: topic.Topic}
							for _, part := range topic.Partitions {
								result.parts++
								out.Partitions = append(out.Partitions, kmsg.ProduceResponseTopicPartition{Partition: part.Partition, BaseOffset: 42})
							}
							resp.Topics = append(resp.Topics, out)
						}
						payload = protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp)
					}
					seen <- result
					if err := protocol.WriteFrame(conn, payload); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		<-done
		mu.Lock()
		for _, conn := range connections {
			conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return ln.Addr().String(), seen
}

func TestFanOutDoesNotExcludeSuccessfulBrokers(t *testing.T) {
	for _, api := range []int16{protocol.APIKeyFetch, protocol.APIKeyProduce} {
		for _, mixed := range []bool{true, false} {
			t.Run(fmt.Sprintf("api=%d/mixed=%t", api, mixed), func(t *testing.T) {
				addr, seen := fallbackBackend(t)
				p := &proxy{backends: []string{addr}, backendRetries: 1, dialTimeout: time.Second, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
				pool := newConnPool(time.Second)
				pool.dial = p.dialBackend
				defer pool.Close()
				header := &protocol.RequestHeader{APIKey: api, APIVersion: 4, ClientID: kmsg.StringPtr("original-client")}
				other := "127.0.0.1:0"
				expected := 2
				if mixed {
					other = ""
					expected = 1
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if api == protocol.APIKeyFetch {
					groups := map[string]*kmsg.FetchRequest{addr: {Version: 4, Topics: []kmsg.FetchRequestTopic{{Topic: "orders", Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 7}}}}}, other: {Version: 4, Topics: []kmsg.FetchRequestTopic{{Topic: "orders", Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 0}}}}}}
					results := p.fanOutFetch(ctx, header, groups, nil, map[string]bool{}, pool)
					if len(results) != expected {
						t.Fatal(results)
					}
					for _, result := range results {
						if result.err != nil {
							t.Fatal(result.err)
						}
						result.conn.Close()
					}
				} else {
					header.APIVersion = 3
					groups := map[string]*kmsg.ProduceRequest{addr: {Version: 3, Acks: -1, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 7}}}}}, other: {Version: 3, Acks: -1, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0}}}}}}
					results := p.fanOutProduce(ctx, header, groups, nil, map[string]bool{}, pool)
					if len(results) != expected {
						t.Fatal(results)
					}
					for _, result := range results {
						if result.err != nil {
							t.Fatal(result.err)
						}
						result.conn.Close()
					}
				}
				parts := 0
				for i := 0; i < expected; i++ {
					select {
					case request := <-seen:
						if request.clientID != "original-client" {
							t.Fatal(request)
						}
						parts += request.parts
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				if parts != 2 {
					t.Fatal("lost partitions", parts)
				}
			})
		}
	}
}
