// Copyright 2026 KafScale team.
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
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func deleteRoutingBackend(t *testing.T, respond func(*protocol.RequestHeader, *kmsg.DeleteGroupsRequest) *kmsg.DeleteGroupsResponse) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var connections []net.Conn
	var workers sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
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
					header, parsed, err := protocol.ParseRequest(frame.Payload)
					if err != nil {
						return
					}
					req, ok := parsed.(*kmsg.DeleteGroupsRequest)
					if !ok {
						return
					}
					resp := respond(header, req)
					if resp == nil {
						return
					}
					if err := protocol.WriteFrame(conn, protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp)); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		<-done
		mu.Lock()
		for _, conn := range connections {
			conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return listener.Addr().String()
}

func TestDeleteGroupsRoutesAndRetriesOnlyRejectedGroups(t *testing.T) {
	ctx := context.Background()
	cli, err := clientv3.New(clientv3.Config{Endpoints: testutil.StartEmbeddedEtcd(t), DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	// Deliberately stale route: group a is actually owned by broker 1.
	for group, owner := range map[string]string{"a": "2", "b": "2", "active": "1"} {
		if _, err := cli.Put(ctx, metadata.GroupLeasePrefix()+"/"+group, owner); err != nil {
			t.Fatal(err)
		}
	}
	for _, version := range []int16{0, 1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			var mu sync.Mutex
			successes := map[string]int{}
			wrongEnvelope := false
			backend := func(owner string) string {
				return deleteRoutingBackend(t, func(header *protocol.RequestHeader, req *kmsg.DeleteGroupsRequest) *kmsg.DeleteGroupsResponse {
					mu.Lock()
					defer mu.Unlock()
					if len(req.Groups) != 1 || header.ClientID == nil || *header.ClientID != "admin" || header.CorrelationID != 71 || req.Version != version {
						wrongEnvelope = true
						return nil
					}
					id := req.Groups[0]
					if id == "broken" {
						return nil
					}
					code := int16(protocol.NOT_COORDINATOR)
					switch {
					case id == "a" && owner == "1", id == "b" && owner == "2":
						successes[id]++
						code = protocol.NONE
					case id == "active":
						code = protocol.NON_EMPTY_GROUP
					case id == "missing":
						code = protocol.GROUP_ID_NOT_FOUND
					case id == "denied":
						code = protocol.GROUP_AUTHORIZATION_FAILED
					}
					resp := kmsg.NewPtrDeleteGroupsResponse()
					resp.Groups = []kmsg.DeleteGroupsResponseGroup{{Group: id, ErrorCode: code}}
					return resp
				})
			}
			a, b := backend("1"), backend("2")
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			router, err := metadata.NewGroupRouter(ctx, cli, logger)
			if err != nil {
				t.Fatal(err)
			}
			defer router.Stop()
			p := &proxy{backends: []string{a, b}, backendRetries: 1, dialTimeout: time.Second, groupRouter: router, brokerAddrs: map[string]string{"1": a, "2": b}, logger: logger}
			pool := newConnPool(time.Second)
			defer pool.Close()
			req := kmsg.NewPtrDeleteGroupsRequest()
			req.Version, req.Groups = version, []string{"a", "b", "active", "missing", "denied", "broken"}
			formatter := kmsg.NewRequestFormatter(kmsg.FormatterClientID("admin"))
			payload := formatter.AppendRequest(nil, req, 71)[4:]
			header, _, err := protocol.ParseRequest(payload)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := p.handleGroupRouting(ctx, header, payload, pool)
			if err != nil {
				t.Fatal(err)
			}
			body, ok := protocol.SkipResponseHeader(header.APIKey, version, wire)
			if !ok {
				t.Fatal("response header")
			}
			resp := kmsg.NewPtrDeleteGroupsResponse()
			resp.Version = version
			if err := resp.ReadFrom(body); err != nil {
				t.Fatal(err)
			}
			if len(resp.Groups) != len(req.Groups) {
				t.Fatal(resp)
			}
			for i, code := range []int16{0, 0, protocol.NON_EMPTY_GROUP, protocol.GROUP_ID_NOT_FOUND, protocol.GROUP_AUTHORIZATION_FAILED, protocol.REQUEST_TIMED_OUT} {
				if resp.Groups[i].Group != req.Groups[i] || resp.Groups[i].ErrorCode != code {
					t.Fatal(resp)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if wrongEnvelope || successes["a"] != 1 || successes["b"] != 1 {
				t.Fatal("replayed completed groups or changed request identity", successes, wrongEnvelope)
			}
		})
	}
}
