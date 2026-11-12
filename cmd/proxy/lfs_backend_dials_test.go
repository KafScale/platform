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
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func dialCountingBackend(t *testing.T, topic string) (string, *atomic.Int32) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var dials atomic.Int32
	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			dials.Add(1)
			go func(c net.Conn) {
				defer c.Close()
				for {
					frame, err := protocol.ReadFrame(c)
					if err != nil {
						return
					}
					header, _, _ := protocol.ParseRequestHeader(frame.Payload)
					response := kmsg.NewPtrProduceResponse()
					response.Version = 9
					response.Topics = []kmsg.ProduceResponseTopic{{Topic: topic, Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0, BaseOffset: 7}}}}
					if err := protocol.WriteFrame(c, protocol.EncodeResponse(header.CorrelationID, 9, response)); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return listener.Addr().String(), &dials
}

func dialCountingModule(t *testing.T) (*lfsModule, *atomic.Int32) {
	t.Helper()
	addr, dials := dialCountingBackend(t, "orders")
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.backends = []string{addr}
	m.backendRetries = 1
	m.backendBackoff = time.Millisecond
	m.dialTimeout = time.Second
	return m, dials
}

func TestLFSHTTPProduceDialsBrokerOnce(t *testing.T) {
	m, dials := dialCountingModule(t)
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, lfsProduceHTTPRequest("orders"))
	if rr.Code != http.StatusOK {
		t.Fatalf("produce: %d %s", rr.Code, rr.Body.String())
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("broker connections for one produce: %d, want 1", got)
	}
}

func TestLFSHTTPMultipartDialsBrokerOnce(t *testing.T) {
	m, dials := dialCountingModule(t)
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewBufferString(`{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)))
	if rr.Code != http.StatusOK {
		t.Fatalf("init: %d %s", rr.Code, rr.Body.String())
	}
	var init lfsUploadInitResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &init); err != nil {
		t.Fatal(err)
	}
	rr = httptest.NewRecorder()
	m.handleHTTPUploadPart(rr, httptest.NewRequest(http.MethodPut, "/", bytes.NewReader([]byte("payload"))), "part", init.UploadID, 1)
	if rr.Code != http.StatusOK {
		t.Fatalf("part: %d %s", rr.Code, rr.Body.String())
	}
	rr = httptest.NewRecorder()
	m.handleHTTPUploadComplete(rr, httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"parts":[{"part_number":1,"etag":"test-etag"}]}`)), "complete", init.UploadID)
	if rr.Code != http.StatusOK {
		t.Fatalf("complete: %d %s", rr.Code, rr.Body.String())
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("broker connections for one multipart upload: %d, want 1", got)
	}
}

func TestLFSConnectBackendReturnsOnCancelDuringBackoff(t *testing.T) {
	m := testHTTPModule(t)
	m.backends = []string{closedBackendAddr(t)}
	m.backendRetries = 5
	m.backendBackoff = 2 * time.Second
	m.dialTimeout = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	_, _, err := m.connectBackend(ctx)
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error %v, want context.Canceled", err)
	}
	if elapsed > time.Second {
		t.Errorf("connectBackend returned %s after the context was cancelled", elapsed)
	}
}
