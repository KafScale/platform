// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"io"
	"net"
	"testing"
)

func TestProxyProtocolV1Unknown(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()

	payload := []byte("PROXY UNKNOWN\r\nping")
	go func() {
		_, _ = peer.Write(payload)
	}()

	wrapped, info, err := ReadProxyProtocol(conn)
	if err != nil {
		t.Fatalf("ReadProxyProtocol: %v", err)
	}
	if info == nil || !info.Local {
		t.Fatalf("expected local proxy info, got %+v", info)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(wrapped, buf); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if !bytes.Equal(buf, []byte("ping")) {
		t.Fatalf("unexpected payload %q", string(buf))
	}
}

func TestProxyProtocolV2Local(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()

	header := append([]byte{}, proxyV2Signature...)
	header = append(header, 0x20)       // v2 + LOCAL
	header = append(header, 0x00)       // UNSPEC
	header = append(header, 0x00, 0x00) // length 0
	payload := append(header, []byte("pong")...)

	go func() {
		_, _ = peer.Write(payload)
	}()

	wrapped, info, err := ReadProxyProtocol(conn)
	if err != nil {
		t.Fatalf("ReadProxyProtocol: %v", err)
	}
	if info == nil || !info.Local {
		t.Fatalf("expected local proxy info, got %+v", info)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(wrapped, buf); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if !bytes.Equal(buf, []byte("pong")) {
		t.Fatalf("unexpected payload %q", string(buf))
	}
}
