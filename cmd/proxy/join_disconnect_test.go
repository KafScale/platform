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
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProxyDisconnectCancelsPendingJoin(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	entered := make(chan struct{})
	backendClosed := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			backendClosed <- err
			return
		}
		defer conn.Close()
		if _, err := protocol.ReadFrame(conn); err != nil {
			backendClosed <- err
			return
		}
		close(entered)
		var b [1]byte
		_, err = conn.Read(b[:])
		backendClosed <- err
	}()
	p := &proxy{backends: []string{listener.Addr().String()}, backendRetries: 1, dialTimeout: time.Second, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	p.setReady(true)
	server, client := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() { defer close(done); p.handleConnection(context.Background(), server) }()
	request := kmsg.NewPtrJoinGroupRequest()
	request.Version, request.Group, request.ProtocolType = 4, "readers", "consumer"
	payload := kmsg.NewRequestFormatter().AppendRequest(nil, request, 1)
	if _, err := client.Write(payload); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("JoinGroup did not reach backend")
	}
	client.Close()
	select {
	case err := <-backendClosed:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("backend did not receive EOF: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("proxy kept the disconnected client's JoinGroup alive")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("proxy handler did not exit")
	}
}

func TestForwardCancellationDoesNotCloseReusedConnection(t *testing.T) {
	backend, client := net.Pipe()
	defer backend.Close()
	defer client.Close()
	p := &proxy{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 2 {
			frame, err := protocol.ReadFrame(backend)
			if err != nil {
				return
			}
			if err := protocol.WriteFrame(backend, frame.Payload); err != nil {
				return
			}
		}
	}()
	if _, err := p.forwardToBackend(ctx, client, "backend", []byte("first")); err != nil {
		t.Fatal(err)
	}
	cancel()
	// The cancellation callback from the completed request must be detached
	// before the pool can reuse this connection for another request.
	if got, err := p.forwardToBackend(context.Background(), client, "backend", []byte("second")); err != nil || string(got) != "second" {
		t.Fatalf("completed request closed a reused connection: %q, %v", got, err)
	}
	<-done
}
