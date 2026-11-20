// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/pkg/lfs"
	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestLFSListenerTracksUnconfirmedUploads(t *testing.T) {
	for _, tc := range []struct {
		name    string
		acks    int16
		failure string
		want    uint64
	}{
		{"connect", 1, "connect", 3},
		{"connect_no_acks", 0, "connect", 3},
		{"lost_ack", 1, "lost_ack", 3},
		{"wrong_correlation", 1, "wrong_correlation", 3},
		{"rejected_partition", 1, "reject", 1},
		{"missing_partition", 1, "missing", 1},
		{"success", 1, "", 0},
		{"success_no_acks", 0, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, storage := testLFSModule(t)
			p := lfsTestProxy(nil, time.Minute)
			p.lfs = m
			addr := closedBackendAddr(t)
			received := make(chan error, 1)
			if tc.failure != "connect" {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = listener.Close() })
				addr = listener.Addr().String()
				go func() {
					conn, err := listener.Accept()
					if err != nil {
						received <- err
						return
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
					frame, err := protocol.ReadFrame(conn)
					if err != nil {
						received <- err
						return
					}
					h, parsed, err := protocol.ParseRequest(frame.Payload)
					if err != nil {
						received <- err
						return
					}
					req, ok := parsed.(*kmsg.ProduceRequest)
					if !ok {
						received <- fmt.Errorf("not a produce: %T", parsed)
						return
					}
					response := &kmsg.ProduceResponse{Version: 9}
					for _, topic := range req.Topics {
						out := kmsg.ProduceResponseTopic{Topic: topic.Topic}
						for _, part := range topic.Partitions {
							batches, err := lfsDecodeRecordBatches(part.Records)
							if err != nil || len(batches) != 1 {
								received <- fmt.Errorf("invalid rewritten batches: %v", err)
								return
							}
							records, _, err := lfsDecodeBatchRecords(&batches[0], kgo.DefaultDecompressor())
							if err != nil || len(records) != 1 {
								received <- fmt.Errorf("invalid rewritten records: %v", err)
								return
							}
							if _, err := lfs.DecodeEnvelope(records[0].Value); err != nil {
								received <- err
								return
							}
							if tc.failure == "missing" && topic.Topic == "orders" && part.Partition == 7 {
								continue
							}
							result := kmsg.ProduceResponseTopicPartition{Partition: part.Partition, BaseOffset: 42}
							if tc.failure == "reject" && topic.Topic == "orders" && part.Partition == 7 {
								result.ErrorCode = protocol.REQUEST_TIMED_OUT
							}
							out.Partitions = append(out.Partitions, result)
						}
						response.Topics = append(response.Topics, out)
					}
					received <- nil
					if tc.acks != 0 && tc.failure != "lost_ack" {
						correlationID := h.CorrelationID
						if tc.failure == "wrong_correlation" {
							correlationID++
						}
						_ = protocol.WriteFrame(conn, protocol.EncodeResponse(correlationID, 9, response))
					}
				}()
			}
			p.backends = []string{addr}
			request := &kmsg.ProduceRequest{Version: 9, Acks: tc.acks, TimeoutMillis: 1000}
			for _, topic := range []string{"orders", "events"} {
				tr := kmsg.ProduceRequestTopic{Topic: topic}
				parts := []int32{0}
				if topic == "orders" {
					parts = append(parts, 7)
				}
				for _, part := range parts {
					tr.Partitions = append(tr.Partitions, kmsg.ProduceRequestTopicPartition{Partition: part, Records: buildTestBatch([]kmsg.Record{{Value: []byte("payload"), Headers: []kmsg.Header{{Key: "LFS_BLOB"}}}})})
				}
				request.Topics = append(request.Topics, tr)
			}
			h := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: 3}
			pool := newConnPool(time.Second)
			defer pool.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			response, err := p.handleProduceRouting(ctx, h, encodeProduceRequest(h, request), pool)
			if err != nil {
				t.Fatal(err)
			}
			if tc.acks == 0 && response != nil {
				t.Fatal("acks=0 returned a response")
			}
			if tc.failure == "wrong_correlation" {
				result, err := parseProduceResponse(response, 9)
				if err != nil {
					t.Fatal(err)
				}
				for _, topic := range result.Topics {
					for _, part := range topic.Partitions {
						if part.ErrorCode != protocol.REQUEST_TIMED_OUT {
							t.Fatal("unrelated response accepted as an acknowledgement")
						}
					}
				}
			}
			if tc.failure != "connect" {
				select {
				case err := <-received:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("backend never received rewritten produce")
				}
			}
			if got := atomic.LoadUint64(&m.metrics.orphans); got != tc.want {
				t.Fatalf("orphans=%d, want %d", got, tc.want)
			}
			if len(storage.objects) != 3 || len(storage.deleted) != 0 {
				t.Fatal("possibly published objects must be retained")
			}
		})
	}
}

// Capture real deadlines without depending on a scheduler beating a short
// timeout. Use the HTTP publication entry point to check its actual budget.
type lfsDeadlineConn struct {
	net.Conn
	readDeadline  time.Time
	writeDeadline time.Time
	reset         bool
}

func (c *lfsDeadlineConn) SetReadDeadline(d time.Time) error {
	c.readDeadline = d
	return c.Conn.SetReadDeadline(d)
}
func (c *lfsDeadlineConn) SetWriteDeadline(d time.Time) error {
	c.writeDeadline = d
	return c.Conn.SetWriteDeadline(d)
}
func (c *lfsDeadlineConn) SetDeadline(d time.Time) error {
	c.reset = d.IsZero()
	return c.Conn.SetDeadline(d)
}

func TestLFSHTTPProduceResponseBudget(t *testing.T) {
	m, _ := testLFSModule(t)
	m.dialTimeout = 50 * time.Millisecond
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() {
		defer server.Close()
		if _, err := protocol.ReadFrame(server); err != nil {
			return
		}
		response := &kmsg.ProduceResponse{Version: 9, Topics: []kmsg.ProduceResponseTopic{{Topic: "orders", Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0, BaseOffset: 7}}}}}
		_ = protocol.WriteFrame(server, protocol.EncodeResponse(3, 9, response))
	}()
	conn := &lfsDeadlineConn{Conn: client}
	before := time.Now()
	offset, err := m.produceHTTPRecord(context.Background(), conn, []byte("request"), "orders", 0, 3)
	if err != nil || offset != 7 {
		t.Fatal(offset, err)
	}
	if conn.readDeadline.Before(before.Add(15*time.Second + m.dialTimeout)) {
		t.Fatalf("read budget cut short: %v", conn.readDeadline.Sub(before))
	}
	if conn.writeDeadline.After(time.Now().Add(m.dialTimeout)) || !conn.reset {
		t.Fatal("write deadline or reset missing")
	}
}

func TestLFSForwardWaitsBeyondDialTimeout(t *testing.T) {
	m, _ := testLFSModule(t)
	m.dialTimeout = time.Second
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() {
		defer server.Close()
		if _, err := protocol.ReadFrame(server); err != nil {
			return
		}
		time.Sleep(1100 * time.Millisecond)
		_ = protocol.WriteFrame(server, []byte("ack"))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := m.forwardToBackend(ctx, client, []byte("produce"), 2*time.Second)
	if err != nil || string(response) != "ack" {
		t.Fatalf("late acknowledgement: %q %v", response, err)
	}
}

func TestLFSForwardStopsOnCancellation(t *testing.T) {
	for _, phase := range []string{"write", "read", "deadline", "already_cancelled"} {
		t.Run(phase, func(t *testing.T) {
			m, _ := testLFSModule(t)
			m.dialTimeout = time.Second
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if phase == "deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 50*time.Millisecond)
				defer stop()
			}
			if phase == "already_cancelled" {
				cancel()
			}
			if phase == "read" || phase == "deadline" {
				go func() {
					if _, err := protocol.ReadFrame(server); err == nil && phase == "read" {
						cancel()
					}
				}()
			} else if phase == "write" {
				time.AfterFunc(50*time.Millisecond, cancel)
			}
			done := make(chan error, 1)
			go func() { _, err := m.forwardToBackend(ctx, client, []byte("produce"), 15*time.Second); done <- err }()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancelled exchange succeeded")
				}
				if phase == "already_cancelled" && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("exchange did not stop on cancellation")
			}
		})
	}
}
