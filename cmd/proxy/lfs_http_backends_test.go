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
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func closedBackendAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return addr
}

func ackingBackend(t *testing.T, topic string) (string, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := listener.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		frame, err := protocol.ReadFrame(c)
		if err != nil {
			return
		}
		header, _, _ := protocol.ParseRequestHeader(frame.Payload)
		response := kmsg.NewPtrProduceResponse()
		response.Version = 9
		response.Topics = []kmsg.ProduceResponseTopic{{Topic: topic, Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0, BaseOffset: 7}}}}
		_ = protocol.WriteFrame(c, protocol.EncodeResponse(header.CorrelationID, 9, response))
	}()
	return listener.Addr().String(), done
}

func lfsProduceHTTPRequest(topic string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", bytes.NewReader([]byte("payload")))
	req.Header.Set(lfsHeaderTopic, topic)
	req.Header.Set("Content-Type", "application/octet-stream")
	return req
}

func TestLFSHTTPProduceWithoutBackendSkipsUpload(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.backendRetries = 1
	m.backendBackoff = time.Millisecond
	fs3 := m.s3Uploader.api.(*fakeS3)
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, lfsProduceHTTPRequest("orders"))
	if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "backend_unavailable") {
		t.Fatalf("got %d %s", rr.Code, rr.Body.String())
	}
	if len(fs3.objects) != 0 || len(fs3.deleted) != 0 {
		t.Fatalf("object uploaded without a backend: objects=%v deleted=%v", fs3.objects, fs3.deleted)
	}
}

func TestLFSHTTPProduceDeletesUploadWhenBackendDialFails(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.backendRetries = 1
	m.backendBackoff = time.Millisecond
	m.dialTimeout = time.Second
	m.backends = []string{closedBackendAddr(t)}
	fs3 := m.s3Uploader.api.(*fakeS3)
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, lfsProduceHTTPRequest("orders"))
	if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "backend_unavailable") {
		t.Fatalf("got %d %s", rr.Code, rr.Body.String())
	}
	if len(fs3.objects) != 0 || len(fs3.deleted) != 1 || !strings.HasPrefix(fs3.deleted[0], "test-ns/@lfs/orders/") {
		t.Fatalf("upload left behind: objects=%v deleted=%v", fs3.objects, fs3.deleted)
	}
}

func TestLFSHTTPUploadInitWithoutBackendStartsNoUpload(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.backendRetries = 1
	m.backendBackoff = time.Millisecond
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewBufferString(`{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)))
	if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "backend_unavailable") {
		t.Fatalf("got %d %s", rr.Code, rr.Body.String())
	}
	if len(m.uploadSessions) != 0 {
		t.Fatalf("upload session started without a backend")
	}
}

func TestLFSHTTPMultipartDeletesUploadWhenBackendDialFails(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.backendRetries = 1
	m.backendBackoff = time.Millisecond
	m.dialTimeout = time.Second
	m.backends = []string{closedBackendAddr(t)}
	fs3 := m.s3Uploader.api.(*fakeS3)
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewBufferString(`{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)))
	if rr.Code != http.StatusOK {
		t.Fatalf("init: %d %s", rr.Code, rr.Body.String())
	}
	var session *uploadSession
	for _, s := range m.uploadSessions {
		session = s
	}
	rr = httptest.NewRecorder()
	m.handleHTTPUploadPart(rr, httptest.NewRequest(http.MethodPut, "/", bytes.NewReader([]byte("payload"))), "part", session.ID, 1)
	if rr.Code != http.StatusOK {
		t.Fatalf("part: %d %s", rr.Code, rr.Body.String())
	}
	fs3.objects[session.S3Key] = []byte("payload")
	rr = httptest.NewRecorder()
	m.handleHTTPUploadComplete(rr, httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"parts":[{"part_number":1,"etag":"test-etag"}]}`)), "complete", session.ID)
	if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "backend_unavailable") {
		t.Fatalf("got %d %s", rr.Code, rr.Body.String())
	}
	if len(fs3.objects) != 0 || len(fs3.deleted) != 1 || fs3.deleted[0] != session.S3Key {
		t.Fatalf("upload left behind: objects=%v deleted=%v", fs3.objects, fs3.deleted)
	}
}

func TestLFSHTTPProduceStaticBackendStillWorks(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.backendRetries = 1
	m.dialTimeout = time.Second
	addr, done := ackingBackend(t, "orders")
	m.backends = []string{addr}
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, lfsProduceHTTPRequest("orders"))
	<-done
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d %s", rr.Code, rr.Body.String())
	}
}

func brokerAt(t *testing.T, nodeID int32, addr string) protocol.MetadataBroker {
	t.Helper()
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.MetadataBroker{NodeID: nodeID, Host: host, Port: int32(port)}
}

func TestLFSHTTPProduceUsesDiscoveredBrokersWithoutBackends(t *testing.T) {
	addr, done := ackingBackend(t, "orders")
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{Brokers: []protocol.MetadataBroker{brokerAt(t, 1, addr)}})
	p := &proxy{store: store, backendRetries: 1, dialTimeout: time.Second, cacheTTL: time.Minute, brokerAddrs: make(map[string]string), topicNames: make(map[[16]byte]string), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.backendRetries = 1
	m.dialTimeout = time.Second
	p.attachLFSModule(m)
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, lfsProduceHTTPRequest("orders"))
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d %s", rr.Code, rr.Body.String())
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("discovered broker got no produce request")
	}
}

func TestLFSHTTPProduceExplicitBackendsWinOverDiscovery(t *testing.T) {
	explicit, done := ackingBackend(t, "orders")
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{Brokers: []protocol.MetadataBroker{brokerAt(t, 1, closedBackendAddr(t))}})
	p := &proxy{store: store, backendRetries: 1, dialTimeout: time.Second, cacheTTL: time.Minute, brokerAddrs: make(map[string]string), topicNames: make(map[[16]byte]string), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.backendRetries = 1
	m.dialTimeout = time.Second
	m.backends = []string{explicit}
	p.attachLFSModule(m)
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, lfsProduceHTTPRequest("orders"))
	<-done
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d %s", rr.Code, rr.Body.String())
	}
}
