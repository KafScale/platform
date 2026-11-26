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

	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProxyResponseWriteDeadline(t *testing.T) {
	for _, mode := range []string{"normal", "not-ready", "backend-error"} {
		t.Run(mode, func(t *testing.T) {
			server, client := net.Pipe()
			defer func() { _ = client.Close() }()
			p := &proxy{responseWriteTimeout: 25 * time.Millisecond, backendRetries: 1, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			var request kmsg.Request
			switch mode {
			case "normal":
				p.apiVersions = make([]kmsg.ApiVersionsResponseApiKey, 100000)
				request = kmsg.NewPtrApiVersionsRequest()
			case "not-ready":
				request = kmsg.NewPtrProduceRequest()
			case "backend-error":
				p.setReady(true)
				p.store = metadata.NewInMemoryStore(metadata.ClusterMetadata{})
				request = kmsg.NewPtrListGroupsRequest()
			}
			done := make(chan struct{})
			go func() { defer close(done); p.handleConnection(context.Background(), server) }()
			if _, err := client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, request, 1)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("proxy response write never timed out")
			}
			var buffer [1]byte
			if _, err := client.Read(buffer[:]); !errors.Is(err, io.EOF) {
				t.Fatalf("timed-out client socket was not closed: %v", err)
			}
		})
	}
}

func TestProxyFetchEOFDrainsQueuedProduce(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	entered := make(chan struct{})
	stored := make(chan error, 1)
	go func() {
		for index := range 2 {
			conn, err := listener.Accept()
			if err != nil {
				stored <- err
				return
			}
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			frame, err := protocol.ReadFrame(conn)
			if err != nil {
				_ = conn.Close()
				stored <- err
				return
			}
			header, request, err := protocol.ParseRequest(frame.Payload)
			if err != nil {
				_ = conn.Close()
				stored <- err
				return
			}
			if index == 0 {
				if header.APIKey != protocol.APIKeyFetch {
					_ = conn.Close()
					stored <- errors.New("expected Fetch first")
					return
				}
				close(entered)
				var b [1]byte
				_, err = conn.Read(b[:])
				_ = conn.Close()
				if !errors.Is(err, io.EOF) {
					stored <- err
					return
				}
			} else {
				produce, ok := request.(*kmsg.ProduceRequest)
				if !ok || produce.Acks != 0 || len(produce.Topics) != 1 {
					_ = conn.Close()
					stored <- errors.New("queued acks=0 Produce did not arrive")
					return
				}
				_ = conn.Close()
				stored <- nil
			}
		}
	}()
	p := &proxy{backends: []string{listener.Addr().String()}, dialTimeout: time.Second, backendRetries: 6, backendBackoff: time.Hour, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	p.setReady(true)
	server, client := net.Pipe()
	defer func() { _ = client.Close() }()
	done := make(chan struct{})
	go func() { defer close(done); p.handleConnection(context.Background(), server) }()
	fetch := kmsg.NewPtrFetchRequest()
	fetch.Version, fetch.ReplicaID, fetch.MinBytes, fetch.MaxWaitMillis = 4, -1, 1, 30000
	fetch.Topics = []kmsg.FetchRequestTopic{{Topic: "orders", Partitions: []kmsg.FetchRequestTopicPartition{{PartitionMaxBytes: 1 << 20}}}}
	if _, err := client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, fetch, 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Fetch did not reach backend")
	}
	produce := kmsg.NewPtrProduceRequest()
	produce.Version, produce.Acks = 7, 0
	produce.Topics = []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Records: []byte{1}}}}}
	if _, err := client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, produce, 2)); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	select {
	case err := <-stored:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("disconnected Fetch or retry backoff blocked queued Produce")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("proxy did not drain after Fetch cancellation")
	}
}

func TestBackendRetryStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &proxy{store: metadata.NewInMemoryStore(metadata.ClusterMetadata{}), backendRetries: 6, backendBackoff: time.Hour}
	done := make(chan error, 1)
	go func() { _, _, err := p.connectBackendExcluding(ctx, nil); done <- err }()
	time.AfterFunc(20*time.Millisecond, cancel)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled request remained in backend retry sleep")
	}
}

func TestDisconnectedAcknowledgedProduceStopsBackendWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	entered := make(chan struct{})
	closed := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			closed <- err
			return
		}
		defer func() { _ = conn.Close() }()
		stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
		defer stop()
		if _, err := protocol.ReadFrame(conn); err != nil {
			closed <- err
			return
		}
		close(entered)
		var b [1]byte
		_, err = conn.Read(b[:])
		closed <- err
	}()
	p := &proxy{backends: []string{listener.Addr().String()}, dialTimeout: time.Second, backendRetries: 1, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	p.setReady(true)
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()
	done := make(chan struct{})
	go func() { defer close(done); p.handleConnection(ctx, server) }()
	req := kmsg.NewPtrProduceRequest()
	req.Version, req.Acks = 3, 1
	req.Topics = []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Records: []byte{1}}}}}
	if _, err := client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Produce did not reach backend")
	}
	_ = client.Close()
	select {
	case err := <-closed:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("backend disconnect: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("disconnected Produce kept waiting on its backend")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("proxy did not stop after Produce client disconnected")
	}
}
