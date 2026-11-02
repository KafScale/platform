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

//go:build e2e

package e2e

import (
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) { return *(v.(*[]byte)), nil }

func (rawCodec) Unmarshal(b []byte, v any) error {
	*(v.(*[]byte)) = append([]byte(nil), b...)
	return nil
}

func (rawCodec) Name() string { return "proto" }

type etcdProxy struct {
	addr       string
	holdCount  atomic.Bool
	failCount  atomic.Bool
	failRanges atomic.Bool
	countSeen  atomic.Int64
}

func countOnlyRange(b []byte) bool { return strings.Contains(string(b), "\x48\x01") }

func startEtcdProxy(t *testing.T, upstream string) *etcdProxy {
	t.Helper()
	up, err := grpc.NewClient(strings.TrimPrefix(upstream, "http://"),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})))
	if err != nil {
		t.Fatal(err)
	}
	p := &etcdProxy{}
	handler := func(_ any, ss grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(ss)
		md, _ := metadata.FromIncomingContext(ss.Context())
		ctx := metadata.NewOutgoingContext(ss.Context(), md)
		var first []byte
		if err := ss.RecvMsg(&first); err != nil {
			return err
		}
		if method == "/etcdserverpb.KV/Range" {
			if countOnlyRange(first) {
				p.countSeen.Add(1)
				if p.failCount.Load() {
					return status.Error(codes.ResourceExhausted, "test proxy: count range refused")
				}
				for p.holdCount.Load() {
					time.Sleep(50 * time.Millisecond)
				}
			}
			if p.failRanges.Load() {
				return status.Error(codes.Unavailable, "test proxy: range refused")
			}
		}
		cs, err := up.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, method)
		if err != nil {
			return err
		}
		if err := cs.SendMsg(&first); err != nil {
			return err
		}
		go func() {
			for {
				var b []byte
				if ss.RecvMsg(&b) != nil {
					_ = cs.CloseSend()
					return
				}
				if cs.SendMsg(&b) != nil {
					return
				}
			}
		}()
		for {
			var b []byte
			if err := cs.RecvMsg(&b); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			if err := ss.SendMsg(&b); err != nil {
				return err
			}
		}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.UnknownServiceHandler(handler), grpc.ForceServerCodec(rawCodec{}))
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = up.Close()
	})
	p.addr = "http://" + l.Addr().String()
	return p
}

func waitUntil(t *testing.T, what string, timeout time.Duration, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}
