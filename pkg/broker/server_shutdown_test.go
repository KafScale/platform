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

package broker

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func startShutdownTestServer(t *testing.T, h Handler, hooks ...ConnContextFunc) (*Server, net.Conn, context.CancelFunc, <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := &Server{Addr: addr, Handler: h}
	if len(hooks) > 0 {
		s.ConnContextFunc = hooks[0]
	}
	done := make(chan error, 1)
	go func() { done <- s.ListenAndServe(ctx) }()
	var conn net.Conn
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		conn, err = net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if err := protocol.WriteFrame(conn, buildApiVersionsRequest()); err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.ReadFrame(conn); err != nil {
		t.Fatal(err)
	}
	return s, conn, cancel, done
}

func assertServerDrained(t *testing.T, s *Server, conn net.Conn, served <-chan error) {
	t.Helper()
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener did not stop")
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := protocol.ReadFrame(conn); !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("shutdown kept an old coordinator connection alive: %v", err)
	}
	done := make(chan struct{})
	go func() { s.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not drain idle connections")
	}
}

func TestServerShutdownClosesIdleConnections(t *testing.T) {
	s, conn, cancel, done := startShutdownTestServer(t, &testHandler{})
	cancel()
	assertServerDrained(t, s, conn, done)
}

func TestServerShutdownInterruptsProxyAdmission(t *testing.T) {
	for size := 0; size < 5; size++ {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			server, client := net.Pipe()
			t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan struct{})
			s := &Server{Handler: &testHandler{}, ConnContextFunc: func(conn net.Conn) (net.Conn, *ConnContext, error) {
				close(started)
				wrapped, _, err := ReadProxyProtocol(conn)
				return wrapped, nil, err
			}}
			s.wg.Add(1)
			go func() { defer s.wg.Done(); s.handleConnectionUntil(ctx, server) }()
			<-started
			if size > 0 {
				_ = client.SetWriteDeadline(time.Now().Add(time.Second))
				if _, err := client.Write([]byte("PROXY")[:size]); err != nil {
					t.Fatal(err)
				}
			}
			cancel()
			done := make(chan struct{})
			go func() { s.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("shutdown blocked on incomplete PROXY header")
			}
		})
	}
}

type shutdownDrainHandler struct {
	started, finish chan struct{}
	ctxErr          chan error
}

func (h *shutdownDrainHandler) Handle(ctx context.Context, header *protocol.RequestHeader, req kmsg.Request) ([]byte, error) {
	if _, ok := req.(*kmsg.MetadataRequest); ok {
		close(h.started)
		<-h.finish
		h.ctxErr <- ctx.Err()
	}
	return (&testHandler{}).Handle(ctx, header, req)
}

func TestServerShutdownDrainsInFlightResponse(t *testing.T) {
	h := &shutdownDrainHandler{started: make(chan struct{}), finish: make(chan struct{}), ctxErr: make(chan error, 1)}
	s, conn, cancel, done := startShutdownTestServer(t, h, func(conn net.Conn) (net.Conn, *ConnContext, error) {
		return conn, nil, nil
	})
	if err := protocol.WriteFrame(conn, buildMetadataRequest()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	if err := protocol.WriteFrame(conn, buildApiVersionsRequest()); err != nil {
		t.Fatal(err)
	}
	cancel()
	// The completed in-flight request must still produce a response.
	close(h.finish)
	if _, err := protocol.ReadFrame(conn); err != nil {
		t.Fatalf("in-flight response lost: %v", err)
	}
	if err := <-h.ctxErr; err != nil {
		t.Fatalf("in-flight handler canceled: %v", err)
	}
	assertServerDrained(t, s, conn, done)
}
