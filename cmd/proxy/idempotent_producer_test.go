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
	"crypto/sha256"
	"fmt"
	"hash/crc32"
	"net"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func idempotentProxyRequest(t *testing.T, p *proxy, req kmsg.Request) kmsg.Response {
	t.Helper()
	server, client := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer client.Close()
	client.SetDeadline(time.Now().Add(5 * time.Second))
	done := make(chan struct{})
	go func() { defer close(done); p.handleConnection(ctx, server) }()
	defer func() { client.Close(); <-done }()
	if _, err := client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, req, 28)); err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.ReadFrame(client)
	if err != nil {
		t.Fatal(err)
	}
	body, ok := protocol.SkipResponseHeader(req.Key(), req.GetVersion(), frame.Payload)
	if !ok {
		t.Fatal("bad response header")
	}
	resp := req.ResponseKind()
	resp.SetVersion(req.GetVersion())
	if err := resp.ReadFrom(body); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestProxyInitProducerIDForwardingAndUnavailable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			func() {
				defer conn.Close()
				frame, err := protocol.ReadFrame(conn)
				if err != nil {
					return
				}
				header, request, err := protocol.ParseRequest(frame.Payload)
				if err != nil {
					t.Error(err)
					return
				}
				req, ok := request.(*kmsg.InitProducerIDRequest)
				if !ok || req.TransactionalID != nil || req.ProducerID != -1 && req.Version >= 3 {
					t.Errorf("unexpected forwarded request: %+v", request)
					return
				}
				resp := kmsg.NewPtrInitProducerIDResponse()
				resp.ProducerID, resp.ProducerEpoch = 1234, 0
				if err := protocol.WriteFrame(conn, protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp)); err != nil {
					t.Error(err)
				}
			}()
		}
	}()
	defer func() { listener.Close(); <-done }()
	for version := int16(0); version <= 4; version++ {
		req := kmsg.NewPtrInitProducerIDRequest()
		req.Version = version
		p := lfsTestProxy(nil, time.Minute)
		p.backends = []string{listener.Addr().String()}
		p.setReady(true)
		resp := idempotentProxyRequest(t, p, req).(*kmsg.InitProducerIDResponse)
		if resp.ErrorCode != 0 || resp.ProducerID != 1234 || resp.ProducerEpoch != 0 {
			t.Fatal(resp)
		}
		p.setReady(false)
		resp = idempotentProxyRequest(t, p, req).(*kmsg.InitProducerIDResponse)
		if resp.ErrorCode != protocol.REQUEST_TIMED_OUT || resp.ProducerID != -1 || resp.ProducerEpoch != -1 {
			t.Fatal(resp)
		}
		p.setReady(true)
		p.backends = []string{closedBackendAddr(t)}
		resp = idempotentProxyRequest(t, p, req).(*kmsg.InitProducerIDResponse)
		if resp.ErrorCode != protocol.REQUEST_TIMED_OUT || resp.ProducerID != -1 || resp.ProducerEpoch != -1 {
			t.Fatal(resp)
		}
	}
}

func TestProxyRejectsTransactionsBeforeBackendAccess(t *testing.T) {
	p := lfsTestProxy(nil, time.Minute) // no backend or metadata is available
	requests := []kmsg.Request{
		&kmsg.InitProducerIDRequest{Version: 4, TransactionalID: kmsg.StringPtr("tx")},
		&kmsg.FindCoordinatorRequest{Version: 3, CoordinatorType: 1, CoordinatorKey: "tx"},
		&kmsg.AddOffsetsToTxnRequest{Version: 3},
		&kmsg.EndTxnRequest{Version: 3},
	}
	for _, req := range requests {
		resp := idempotentProxyRequest(t, p, req)
		var code int16
		switch r := resp.(type) {
		case *kmsg.InitProducerIDResponse:
			code = r.ErrorCode
		case *kmsg.FindCoordinatorResponse:
			code = r.ErrorCode
			if r.NodeID != -1 {
				t.Fatal(r)
			}
		case *kmsg.AddOffsetsToTxnResponse:
			code = r.ErrorCode
		case *kmsg.EndTxnResponse:
			code = r.ErrorCode
		}
		if code != protocol.UNSUPPORTED_VERSION {
			t.Fatal(resp)
		}
	}
}

func TestProxyAdvertisesIdempotenceWithoutTransactions(t *testing.T) {
	found := false
	for _, api := range generateProxyApiVersions() {
		if api.MinVersion < 0 || api.MaxVersion < api.MinVersion {
			t.Fatalf("invalid API version range: %+v", api)
		}
		if api.ApiKey == protocol.APIKeyInitProducerID {
			found = true
			if api.MinVersion != 0 || api.MaxVersion != 4 {
				t.Fatal(api)
			}
		}
		if api.ApiKey >= 24 && api.ApiKey <= 28 || api.ApiKey >= 65 && api.ApiKey <= 67 {
			t.Fatalf("transaction API advertised: %+v", api)
		}
	}
	if !found {
		t.Fatal("InitProducerId missing")
	}
}

func TestLFSRewritePreservesIdempotentBatchIdentity(t *testing.T) {
	m, _ := testLFSModule(t)
	value := []byte("idempotent LFS payload")
	records := []kmsg.Record{{Key: []byte("key"), Value: value, Headers: []kmsg.Header{{Key: "LFS_BLOB", Value: []byte(fmt.Sprintf("%x", sha256.Sum256(value)))}}}}
	var batch kmsg.RecordBatch
	if err := batch.ReadFrom(buildTestBatch(records)); err != nil {
		t.Fatal(err)
	}
	batch.ProducerID, batch.ProducerEpoch, batch.FirstSequence = 1234, 2, 9
	encoded := batch.AppendTo(nil)
	batch.CRC = int32(crc32.Checksum(encoded[21:], lfsCRC32cTable))
	req := &kmsg.ProduceRequest{Version: 9, Acks: -1, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Records: batch.AppendTo(nil)}}}}}
	result, err := m.rewriteProduceRecords(context.Background(), &protocol.RequestHeader{APIKey: 0, APIVersion: 9}, req)
	if err != nil || !result.modified {
		t.Fatalf("rewrite: %+v %v", result, err)
	}
	rewritten, err := lfsDecodeRecordBatches(req.Topics[0].Partitions[0].Records)
	if err != nil || len(rewritten) != 1 {
		t.Fatalf("decode: %v", err)
	}
	got := rewritten[0]
	if got.ProducerID != batch.ProducerID || got.ProducerEpoch != batch.ProducerEpoch || got.FirstSequence != batch.FirstSequence || got.NumRecords != batch.NumRecords || got.LastOffsetDelta != batch.LastOffsetDelta {
		t.Fatalf("rewrite changed producer identity: %+v", got.RecordBatch)
	}
}
