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
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
)

func TestProxyProtocolV1Unknown(t *testing.T) {
	conn, peer := net.Pipe()
	defer func() { _ = conn.Close() }()
	defer func() { _ = peer.Close() }()

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
	defer func() { _ = conn.Close() }()
	defer func() { _ = peer.Close() }()

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

func TestProxyProtocolV1Full(t *testing.T) {
	conn, peer := net.Pipe()
	defer func() { _ = conn.Close() }()
	defer func() { _ = peer.Close() }()

	header := "PROXY TCP4 192.168.1.1 192.168.1.2 12345 80\r\ndata"
	go func() {
		_, _ = peer.Write([]byte(header))
	}()

	wrapped, info, err := ReadProxyProtocol(conn)
	if err != nil {
		t.Fatalf("ReadProxyProtocol: %v", err)
	}
	if info == nil || info.Local {
		t.Fatalf("expected non-local proxy info, got %+v", info)
	}
	if info.SourceIP != "192.168.1.1" || info.DestIP != "192.168.1.2" {
		t.Fatalf("unexpected IPs: %+v", info)
	}
	if info.SourcePort != 12345 || info.DestPort != 80 {
		t.Fatalf("unexpected ports: %+v", info)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(wrapped, buf); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if string(buf) != "data" {
		t.Fatalf("unexpected trailing data: %q", buf)
	}
}

func TestProxyProtocolV2IPv4(t *testing.T) {
	conn, peer := net.Pipe()
	defer func() { _ = conn.Close() }()
	defer func() { _ = peer.Close() }()

	header := make([]byte, 16)
	copy(header[:12], proxyV2Signature)
	header[12] = 0x21 // v2 + PROXY
	header[13] = 0x11 // AF_INET + STREAM
	// IPv4 addresses: src=10.0.0.1, dst=10.0.0.2, src_port=1234, dst_port=5678
	payload := make([]byte, 12)
	copy(payload[0:4], net.ParseIP("10.0.0.1").To4())
	copy(payload[4:8], net.ParseIP("10.0.0.2").To4())
	binary.BigEndian.PutUint16(payload[8:10], 1234)
	binary.BigEndian.PutUint16(payload[10:12], 5678)
	binary.BigEndian.PutUint16(header[14:16], uint16(len(payload)))
	fullHeader := append(header, payload...)
	fullHeader = append(fullHeader, []byte("rest")...)

	go func() {
		_, _ = peer.Write(fullHeader)
	}()

	wrapped, info, err := ReadProxyProtocol(conn)
	if err != nil {
		t.Fatalf("ReadProxyProtocol: %v", err)
	}
	if info == nil || info.Local {
		t.Fatalf("expected non-local proxy info, got %+v", info)
	}
	if info.SourceIP != "10.0.0.1" || info.DestIP != "10.0.0.2" {
		t.Fatalf("unexpected IPs: src=%s dst=%s", info.SourceIP, info.DestIP)
	}
	if info.SourcePort != 1234 || info.DestPort != 5678 {
		t.Fatalf("unexpected ports: src=%d dst=%d", info.SourcePort, info.DestPort)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(wrapped, buf); err != nil {
		t.Fatalf("read trailing: %v", err)
	}
	if string(buf) != "rest" {
		t.Fatalf("unexpected trailing: %q", buf)
	}
}

func TestProxyProtocolV2IPv6(t *testing.T) {
	conn, peer := net.Pipe()
	defer func() { _ = conn.Close() }()
	defer func() { _ = peer.Close() }()

	header := make([]byte, 16)
	copy(header[:12], proxyV2Signature)
	header[12] = 0x21 // v2 + PROXY
	header[13] = 0x22 // routes to parseProxyV2Inet6 (low nibble 0x2)
	// IPv6 addresses: src=::1, dst=::2, src_port=8080, dst_port=9090
	payload := make([]byte, 36)
	srcIP := net.ParseIP("::1")
	dstIP := net.ParseIP("::2")
	copy(payload[0:16], srcIP.To16())
	copy(payload[16:32], dstIP.To16())
	binary.BigEndian.PutUint16(payload[32:34], 8080)
	binary.BigEndian.PutUint16(payload[34:36], 9090)
	binary.BigEndian.PutUint16(header[14:16], uint16(len(payload)))
	fullData := append(header, payload...)

	go func() {
		_, _ = peer.Write(fullData)
	}()

	_, info, err := ReadProxyProtocol(conn)
	if err != nil {
		t.Fatalf("ReadProxyProtocol: %v", err)
	}
	if info == nil || info.Local {
		t.Fatalf("expected non-local proxy info, got %+v", info)
	}
	if info.SourcePort != 8080 || info.DestPort != 9090 {
		t.Fatalf("unexpected ports: src=%d dst=%d", info.SourcePort, info.DestPort)
	}
}

func TestProxyProtocolNone(t *testing.T) {
	conn, peer := net.Pipe()
	defer func() { _ = conn.Close() }()
	defer func() { _ = peer.Close() }()

	go func() {
		_, _ = peer.Write([]byte("not a proxy header"))
	}()

	wrapped, info, err := ReadProxyProtocol(conn)
	if err != nil {
		t.Fatalf("ReadProxyProtocol: %v", err)
	}
	if info != nil {
		t.Fatalf("expected nil info for non-proxy data, got %+v", info)
	}
	buf := make([]byte, 3)
	if _, err := io.ReadFull(wrapped, buf); err != nil {
		t.Fatalf("read data: %v", err)
	}
	if string(buf) != "not" {
		t.Fatalf("unexpected data: %q", buf)
	}
}

func TestAtoiOrZero(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"0", 0},
		{"123", 123},
		{"80", 80},
		{"", 0},
		{"abc", 0},
		{"12x3", 0},
	}
	for _, tc := range tests {
		if got := atoiOrZero(tc.input); got != tc.want {
			t.Errorf("atoiOrZero(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestWrapConnWithReaderNil(t *testing.T) {
	conn, _ := net.Pipe()
	defer func() { _ = conn.Close() }()
	wrapped := wrapConnWithReader(conn, nil)
	if wrapped != conn {
		t.Fatal("expected original conn when reader is nil")
	}
}

func TestParseProxyV2InetTooShort(t *testing.T) {
	_, err := parseProxyV2Inet([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for short payload")
	}
}

func TestParseProxyV2Inet6TooShort(t *testing.T) {
	_, err := parseProxyV2Inet6(make([]byte, 10))
	if err == nil {
		t.Fatal("expected error for short payload")
	}
}

func TestReadProxyV1LineTooLong(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 300)
	br := bufio.NewReader(bytes.NewReader(data))
	_, err := readProxyV1Line(br, 256)
	if err == nil {
		t.Fatal("expected error for line too long")
	}
}

func TestParseProxyV1Malformed(t *testing.T) {
	// Too few fields
	data := "PROXY TCP4 1.2.3.4\r\n"
	br := bufio.NewReader(bytes.NewBufferString(data))
	_, err := parseProxyV1(br)
	if err == nil {
		t.Fatal("expected error for malformed v1 header")
	}
}
