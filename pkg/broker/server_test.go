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

	"github.com/novatechflow/kafscale/pkg/protocol"
)

type testHandler struct{}

func (h *testHandler) Handle(ctx context.Context, header *protocol.RequestHeader, req protocol.Request) ([]byte, error) {
	switch req.(type) {
	case *protocol.ApiVersionsRequest:
		return protocol.EncodeApiVersionsResponse(&protocol.ApiVersionsResponse{
			CorrelationID: header.CorrelationID,
			Versions: []protocol.ApiVersion{
				{APIKey: protocol.APIKeyApiVersion, MinVersion: 0, MaxVersion: 0},
			},
		}, header.APIVersion)
	case *protocol.MetadataRequest:
		return protocol.EncodeMetadataResponse(&protocol.MetadataResponse{
			CorrelationID: header.CorrelationID,
			Brokers: []protocol.MetadataBroker{
				{NodeID: 1, Host: "localhost", Port: 9092},
			},
			ControllerID: 1,
			Topics: []protocol.MetadataTopic{
				{Name: "orders"},
			},
		}, header.APIVersion)
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
	defer clientConn.Close()

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

	clientConn.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("server handleConnection did not exit")
	}
}

func TestServerHandleConnection_Metadata(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

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

	clientConn.Close()
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
