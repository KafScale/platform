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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// partitionBroker acks a produce only while it owns the partition and answers
// NOT_LEADER_OR_FOLLOWER otherwise, like a KafScale broker without the lease.
type partitionBroker struct {
	addr     string
	owns     atomic.Bool
	produced atomic.Int32
	refused  atomic.Int32
}

func startPartitionBroker(t *testing.T) *partitionBroker {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &partitionBroker{addr: listener.Addr().String()}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
			go b.serve(c)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
	})
	return b
}

func (b *partitionBroker) serve(c net.Conn) {
	defer c.Close()
	for {
		frame, err := protocol.ReadFrame(c)
		if err != nil {
			return
		}
		header, parsed, err := protocol.ParseRequest(frame.Payload)
		if err != nil {
			return
		}
		req, ok := parsed.(*kmsg.ProduceRequest)
		if !ok {
			return
		}
		code := int16(0)
		if b.owns.Load() {
			b.produced.Add(1)
		} else {
			b.refused.Add(1)
			code = protocol.NOT_LEADER_OR_FOLLOWER
		}
		resp := kmsg.NewPtrProduceResponse()
		for _, topic := range req.Topics {
			rt := kmsg.ProduceResponseTopic{Topic: topic.Topic}
			for _, part := range topic.Partitions {
				rt.Partitions = append(rt.Partitions, kmsg.ProduceResponseTopicPartition{Partition: part.Partition, ErrorCode: code, BaseOffset: 7})
			}
			resp.Topics = append(resp.Topics, rt)
		}
		if err := protocol.WriteFrame(c, protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp)); err != nil {
			return
		}
	}
}

type partitionRoutingFixture struct {
	p       *proxy
	m       *lfsModule
	fs3     *fakeS3
	brokers [2]*partitionBroker
	cli     *clientv3.Client
}

// newPartitionRoutingFixture lists brokers 0 and 1 in etcd, points the route
// for orders/0 at routeTo and lets broker owner (or none with -1) ack.
func newPartitionRoutingFixture(t *testing.T, routeTo, owner int) *partitionRoutingFixture {
	t.Helper()
	ctx := context.Background()
	endpoints := testutil.StartEmbeddedEtcd(t)
	f := &partitionRoutingFixture{}
	for i := range f.brokers {
		f.brokers[i] = startPartitionBroker(t)
		f.brokers[i].owns.Store(i == owner)
	}
	putProxyTestSnapshot(t, endpoints, metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{brokerAt(t, 0, f.brokers[0].addr), brokerAt(t, 1, f.brokers[1].addr)},
		Topics:  []protocol.MetadataTopic{{Topic: kmsg.StringPtr("orders"), Partitions: []protocol.MetadataPartition{{Partition: 0, Leader: int32(routeTo)}}}},
	})
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	f.cli = cli
	if _, err := cli.Put(ctx, metadata.PartitionLeasePrefix()+"/orders/0", strconv.Itoa(routeTo)); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	f.p = lfsTestProxy(store, time.Minute)
	if f.p.router, err = metadata.NewPartitionRouter(ctx, store.EtcdClient(), f.p.logger); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.p.router.Stop)
	f.p.refreshMetadataCache(ctx)
	if f.p.router.LookupOwner("orders", 0) != strconv.Itoa(routeTo) {
		t.Fatal("partition route not loaded")
	}
	f.m, f.fs3 = lfsTestModuleFor(t, f.p)
	return f
}

// moveOwner hands the partition to broker to, in the broker and in etcd.
func (f *partitionRoutingFixture) moveOwner(t *testing.T, to int) {
	t.Helper()
	for i, b := range f.brokers {
		b.owns.Store(i == to)
	}
	if _, err := f.cli.Put(context.Background(), metadata.PartitionLeasePrefix()+"/orders/0", strconv.Itoa(to)); err != nil {
		t.Fatal(err)
	}
}

type onReadBody struct {
	data   []byte
	onRead func()
	done   bool
}

func (b *onReadBody) Read(out []byte) (int, error) {
	if b.done {
		return 0, io.EOF
	}
	b.onRead()
	b.done = true
	return copy(out, b.data), nil
}

func (f *partitionRoutingFixture) produce(t *testing.T, duringUpload func()) *httptest.ResponseRecorder {
	t.Helper()
	req := lfsProduceHTTPRequest("orders")
	if duringUpload != nil {
		req.Body = io.NopCloser(&onReadBody{data: []byte("payload"), onRead: duringUpload})
	}
	rr := httptest.NewRecorder()
	f.m.handleHTTPProduce(rr, req)
	return rr
}

func (f *partitionRoutingFixture) multipart(t *testing.T, beforeComplete func()) (*httptest.ResponseRecorder, string) {
	t.Helper()
	rr := httptest.NewRecorder()
	f.m.handleHTTPUploadInit(rr, lfsUploadInitHTTPRequest("orders"))
	if rr.Code != http.StatusOK {
		t.Fatalf("init: %d %s", rr.Code, rr.Body.String())
	}
	var session *uploadSession
	for _, s := range f.m.uploadSessions {
		session = s
	}
	rr = httptest.NewRecorder()
	f.m.handleHTTPUploadPart(rr, httptest.NewRequest(http.MethodPut, "/", bytes.NewReader([]byte("payload"))), "part", session.ID, 1)
	if rr.Code != http.StatusOK {
		t.Fatalf("part: %d %s", rr.Code, rr.Body.String())
	}
	f.fs3.objects[session.S3Key] = []byte("payload")
	if beforeComplete != nil {
		beforeComplete()
	}
	rr = httptest.NewRecorder()
	f.m.handleHTTPUploadComplete(rr, httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"parts":[{"part_number":1,"etag":"test-etag"}]}`)), "complete", session.ID)
	return rr, session.S3Key
}

type partitionRoutingCase struct {
	name             string
	routeTo, owner   int
	moveTo           int
	wantStatus       int
	wantProduced     [2]int32
	wantRefused      [2]int32
	wantObjectsKept  bool
	wantErrorCodeStr string
}

func partitionRoutingCases() []partitionRoutingCase {
	return []partitionRoutingCase{
		{name: "owner on first try", routeTo: 0, owner: 0, moveTo: -1, wantStatus: http.StatusOK, wantProduced: [2]int32{1, 0}, wantObjectsKept: true},
		{name: "stale route then owner", routeTo: 1, owner: 0, moveTo: -1, wantStatus: http.StatusOK, wantProduced: [2]int32{1, 0}, wantRefused: [2]int32{0, 1}, wantObjectsKept: true},
		{name: "owner moves during the request", routeTo: 1, owner: 1, moveTo: 0, wantStatus: http.StatusOK, wantProduced: [2]int32{1, 0}, wantRefused: [2]int32{0, 1}, wantObjectsKept: true},
		{name: "no broker leads the partition", routeTo: 0, owner: -1, moveTo: -1, wantStatus: http.StatusServiceUnavailable, wantRefused: [2]int32{1, 1}, wantErrorCodeStr: "backend_unavailable"},
	}
}

func (tc partitionRoutingCase) check(t *testing.T, f *partitionRoutingFixture, rr *httptest.ResponseRecorder, deleted []string) {
	t.Helper()
	if rr.Code != tc.wantStatus || (tc.wantErrorCodeStr != "" && !strings.Contains(rr.Body.String(), tc.wantErrorCodeStr)) {
		t.Errorf("got %d %s, want %d %s", rr.Code, rr.Body.String(), tc.wantStatus, tc.wantErrorCodeStr)
	}
	for i, b := range f.brokers {
		if got := b.produced.Load(); got != tc.wantProduced[i] {
			t.Errorf("broker %d acked %d produce(s), want %d", i, got, tc.wantProduced[i])
		}
		if got := b.refused.Load(); got != tc.wantRefused[i] {
			t.Errorf("broker %d refused %d produce(s), want %d", i, got, tc.wantRefused[i])
		}
	}
	if tc.wantObjectsKept && len(deleted) != 0 {
		t.Errorf("deleted %v after a successful produce", deleted)
	}
	if !tc.wantObjectsKept && len(deleted) != 1 {
		t.Errorf("object not deleted after no broker took the produce: deleted=%v", deleted)
	}
}

func TestLFSHTTPProduceRoutesToPartitionOwner(t *testing.T) {
	for _, tc := range partitionRoutingCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := newPartitionRoutingFixture(t, tc.routeTo, tc.owner)
			var during func()
			if tc.moveTo >= 0 {
				during = func() { f.moveOwner(t, tc.moveTo) }
			}
			rr := f.produce(t, during)
			tc.check(t, f, rr, f.fs3.deleted)
		})
	}
}

func TestLFSHTTPMultipartCompleteRoutesToPartitionOwner(t *testing.T) {
	for _, tc := range partitionRoutingCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := newPartitionRoutingFixture(t, tc.routeTo, tc.owner)
			var before func()
			if tc.moveTo >= 0 {
				// Init left an idle connection to the old owner. Once the route
				// has moved, completion must not reuse it.
				tc.wantRefused = [2]int32{}
				before = func() {
					f.moveOwner(t, tc.moveTo)
					deadline := time.Now().Add(5 * time.Second)
					for f.p.router.LookupOwner("orders", 0) != strconv.Itoa(tc.moveTo) {
						if time.Now().After(deadline) {
							t.Fatal("route did not move")
						}
						time.Sleep(10 * time.Millisecond)
					}
				}
			}
			rr, key := f.multipart(t, before)
			tc.check(t, f, rr, f.fs3.deleted)
			if !tc.wantObjectsKept && (len(f.fs3.deleted) != 1 || f.fs3.deleted[0] != key) {
				t.Errorf("multipart object %s not deleted: %v", key, f.fs3.deleted)
			}
		})
	}
}
