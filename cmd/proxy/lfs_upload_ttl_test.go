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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ttlExpiringBody ages the backend cache past its TTL while the body is read,
// like an upload that outlasts the rest of the TTL during an etcd outage.
type ttlExpiringBody struct {
	p    *proxy
	data []byte
	done bool
}

func (b *ttlExpiringBody) Read(out []byte) (int, error) {
	if b.done {
		return 0, io.EOF
	}
	ageBackendCache(b.p)
	b.done = true
	return copy(out, b.data), nil
}

func produceWithTTLExpiringDuringUpload(t *testing.T, p *proxy) (*httptest.ResponseRecorder, *fakeS3) {
	t.Helper()
	m, fs3 := lfsTestModuleFor(t, p)
	req := lfsProduceHTTPRequest("orders")
	req.Body = io.NopCloser(&ttlExpiringBody{p: p, data: []byte("payload")})
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, req)
	return rr, fs3
}

func brokerGotProduce(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-time.After(time.Second):
		return false
	}
}

func TestLFSHTTPProduceChecksTTLAgainAfterUpload(t *testing.T) {
	p, done := etcdOutageProxy(t)
	p.touchHealthy()
	rr, fs3 := produceWithTTLExpiringDuringUpload(t, p)
	if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "backend_unavailable") {
		t.Errorf("got %d %s, want 503 backend_unavailable", rr.Code, rr.Body.String())
	}
	if brokerGotProduce(done) {
		t.Error("produce reached the broker after the TTL ran out during the upload")
	}
	if fs3.puts != 1 || len(fs3.deleted) != 1 {
		t.Errorf("uploaded object not deleted: puts=%d deleted=%v", fs3.puts, fs3.deleted)
	}
}

func TestLFSHTTPProduceStaticBackendsIgnoreTTLAfterUpload(t *testing.T) {
	p, done := etcdOutageProxy(t)
	p.backends = p.cachedBackendsSnapshot()
	p.touchHealthy()
	rr, fs3 := produceWithTTLExpiringDuringUpload(t, p)
	if rr.Code != http.StatusOK {
		t.Fatalf("static backends: got %d %s", rr.Code, rr.Body.String())
	}
	if !brokerGotProduce(done) {
		t.Error("produce did not reach the static broker")
	}
	if len(fs3.deleted) != 0 {
		t.Errorf("deleted %v with static backends", fs3.deleted)
	}
}

// The multipart completion connects to a broker right before its produce, so
// the TTL is checked there already. This guards that it stays that way.
func TestLFSHTTPMultipartCompleteChecksTTL(t *testing.T) {
	p, done := etcdOutageProxy(t)
	p.touchHealthy()
	m, fs3 := lfsTestModuleFor(t, p)
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, lfsUploadInitHTTPRequest("orders"))
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
	ageBackendCache(p)
	rr = httptest.NewRecorder()
	m.handleHTTPUploadComplete(rr, httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"parts":[{"part_number":1,"etag":"test-etag"}]}`)), "complete", session.ID)
	if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "backend_unavailable") {
		t.Errorf("got %d %s, want 503 backend_unavailable", rr.Code, rr.Body.String())
	}
	if brokerGotProduce(done) {
		t.Error("produce reached the broker after the TTL ran out")
	}
	if len(fs3.deleted) != 1 || fs3.deleted[0] != session.S3Key {
		t.Errorf("uploaded object not deleted: deleted=%v", fs3.deleted)
	}
}
