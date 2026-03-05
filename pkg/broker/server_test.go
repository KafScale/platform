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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"syscall"

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type testHandler struct{}

func (h *testHandler) Handle(ctx context.Context, header *protocol.RequestHeader, req kmsg.Request) ([]byte, error) {
	switch req.(type) {
	case *kmsg.ApiVersionsRequest:
		resp := kmsg.NewPtrApiVersionsResponse()
		resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
			{ApiKey: protocol.APIKeyApiVersion, MinVersion: 0, MaxVersion: 0},
		}
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	case *kmsg.MetadataRequest:
		resp := kmsg.NewPtrMetadataResponse()
		resp.Brokers = []kmsg.MetadataResponseBroker{
			{NodeID: 1, Host: "localhost", Port: 9092},
		}
		resp.ControllerID = 1
		topic := kmsg.NewMetadataResponseTopic()
		topic.Topic = kmsg.StringPtr("orders")
		resp.Topics = []kmsg.MetadataResponseTopic{topic}
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	default:
		return nil, errors.New("unsupported api")
	}
}

func buildApiVersionsRequest() []byte {
	var buf bytes.Buffer
	writeInt16(&buf, protocol.APIKeyApiVersion)
	writeInt16(&buf, 0) // version
	writeInt32(&buf, 42)
	writeNullableString(&buf, nil)
	return buf.Bytes()
}

func buildMetadataRequest() []byte {
	var buf bytes.Buffer
	writeInt16(&buf, protocol.APIKeyMetadata)
	writeInt16(&buf, 0)
	writeInt32(&buf, 5)
	client := "tester"
	writeNullableString(&buf, &client)
	writeInt32(&buf, 1) // topic array length
	writeString(&buf, "orders")
	return buf.Bytes()
}

func writeInt16(buf *bytes.Buffer, v int16) {
	_ = binary.Write(buf, binary.BigEndian, v)
}

func writeInt32(buf *bytes.Buffer, v int32) {
	_ = binary.Write(buf, binary.BigEndian, v)
}

func writeString(buf *bytes.Buffer, s string) {
	writeInt16(buf, int16(len(s)))
	buf.WriteString(s)
}

func writeNullableString(buf *bytes.Buffer, s *string) {
	if s == nil {
		writeInt16(buf, -1)
		return
	}
	writeString(buf, *s)
}

func TestServerHandleConnection_ApiVersions(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	s := &Server{Handler: &testHandler{}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleConnection(serverConn)
	}()

	requestBytes := buildApiVersionsRequest()
	if err := protocol.WriteFrame(clientConn, requestBytes); err != nil {
		t.Fatalf("WriteFrame client: %v", err)
	}

	resp, err := protocol.ReadFrame(clientConn)
	if err != nil {
		t.Fatalf("ReadFrame client: %v", err)
	}

	reader := bytes.NewReader(resp.Payload)
	var corr int32
	if err := binary.Read(reader, binary.BigEndian, &corr); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	if corr != 42 {
		t.Fatalf("expected correlation id 42 got %d", corr)
	}

	_ = clientConn.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("server handleConnection did not exit")
	}
}

func TestServerHandleConnection_Metadata(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	s := &Server{Handler: &testHandler{}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleConnection(serverConn)
	}()

	if err := protocol.WriteFrame(clientConn, buildMetadataRequest()); err != nil {
		t.Fatalf("WriteFrame client: %v", err)
	}

	resp, err := protocol.ReadFrame(clientConn)
	if err != nil {
		t.Fatalf("ReadFrame client: %v", err)
	}

	reader := bytes.NewReader(resp.Payload)
	var corr int32
	if err := binary.Read(reader, binary.BigEndian, &corr); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	if corr != 5 {
		t.Fatalf("expected correlation id 5 got %d", corr)
	}

	_ = clientConn.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("server handleConnection did not exit")
	}
}

func TestServerListenAndServe_Shutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &Server{
		Addr:    "127.0.0.1:0",
		Handler: &testHandler{},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.ListenAndServe(ctx)
	}()

	// Allow listener to start
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			if errors.Is(err, syscall.EPERM) {
				t.Skip("binding sockets not permitted in sandbox")
			}
			t.Fatalf("ListenAndServe error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("server did not exit after cancel")
	}
}

func TestServerListenAndServeNoHandler(t *testing.T) {
	s := &Server{Addr: "127.0.0.1:0"}
	err := s.ListenAndServe(context.Background())
	if err == nil || err.Error() != "broker.Server requires a Handler" {
		t.Fatalf("expected handler required error, got: %v", err)
	}
}

func TestServerWait(t *testing.T) {
	s := &Server{Handler: &testHandler{}}
	// No goroutines → Wait returns immediately
	done := make(chan struct{})
	go func() {
		s.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait should return immediately with no connections")
	}
}

func TestServerListenAddress(t *testing.T) {
	s := &Server{Addr: "127.0.0.1:9999", Handler: &testHandler{}}
	// Before listening, returns configured addr
	if got := s.ListenAddress(); got != "127.0.0.1:9999" {
		t.Fatalf("expected configured addr, got %q", got)
	}
}

func TestServerHandleConnection_ConnContext(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	s := &Server{
		Handler: &testHandler{},
		ConnContextFunc: func(conn net.Conn) (net.Conn, *ConnContext, error) {
			return conn, &ConnContext{Principal: "test-user", RemoteAddr: "1.2.3.4:5678"}, nil
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleConnection(serverConn)
	}()

	if err := protocol.WriteFrame(clientConn, buildApiVersionsRequest()); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	resp, err := protocol.ReadFrame(clientConn)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}

	reader := bytes.NewReader(resp.Payload)
	var corr int32
	if err := binary.Read(reader, binary.BigEndian, &corr); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	if corr != 42 {
		t.Fatalf("expected correlation 42, got %d", corr)
	}

	_ = clientConn.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleConnection did not exit")
	}
}

func TestServerHandleConnection_ConnContextError(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	s := &Server{
		Handler: &testHandler{},
		ConnContextFunc: func(conn net.Conn) (net.Conn, *ConnContext, error) {
			return nil, nil, errors.New("auth failed")
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleConnection(serverConn)
	}()

	select {
	case <-done:
		// Connection should close immediately due to error
	case <-time.After(time.Second):
		t.Fatal("handleConnection should exit immediately on ConnContext error")
	}
}

func TestServerHandleConnection_BadFrame(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	s := &Server{Handler: &testHandler{}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleConnection(serverConn)
	}()

	// Write invalid data (too short for a frame header)
	_, _ = clientConn.Write([]byte{0, 0, 0, 2, 0xff, 0xff})
	_ = clientConn.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleConnection should exit on bad parse")
	}
}

type errorHandler struct{}

func (h *errorHandler) Handle(ctx context.Context, header *protocol.RequestHeader, req kmsg.Request) ([]byte, error) {
	return nil, errors.New("handler error")
}

func TestServerHandleConnection_HandlerError(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	s := &Server{Handler: &errorHandler{}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleConnection(serverConn)
	}()

	if err := protocol.WriteFrame(clientConn, buildApiVersionsRequest()); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	// Handler error should send an error response instead of closing the
	// connection so the client can recover gracefully.
	frame, err := protocol.ReadFrame(clientConn)
	if err != nil {
		t.Fatalf("expected error response frame, got read error: %v", err)
	}
	if len(frame.Payload) == 0 {
		t.Fatal("expected non-empty error response payload")
	}
}

type nilHandler struct{}

func (h *nilHandler) Handle(ctx context.Context, header *protocol.RequestHeader, req kmsg.Request) ([]byte, error) {
	return nil, nil
}

func TestServerHandleConnection_NilResponse(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	s := &Server{Handler: &nilHandler{}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleConnection(serverConn)
	}()

	if err := protocol.WriteFrame(clientConn, buildApiVersionsRequest()); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	// Handler returns nil → server continues loop, send another then close
	if err := protocol.WriteFrame(clientConn, buildApiVersionsRequest()); err != nil {
		t.Fatalf("WriteFrame 2: %v", err)
	}
	_ = clientConn.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleConnection should exit after client close")
	}
}
