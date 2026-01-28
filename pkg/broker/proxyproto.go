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
	"fmt"
	"io"
	"net"
)

var proxyV2Signature = []byte{'\r', '\n', '\r', '\n', 0x00, '\r', '\n', 'Q', 'U', 'I', 'T', '\n'}

// ProxyInfo captures parsed proxy protocol metadata.
type ProxyInfo struct {
	SourceAddr string
	DestAddr   string
	SourceIP   string
	DestIP     string
	SourcePort int
	DestPort   int
}

// ReadProxyProtocol consumes a PROXY protocol header (v1 or v2) if present.
// It returns a wrapped connection that preserves buffered bytes.
func ReadProxyProtocol(conn net.Conn) (net.Conn, *ProxyInfo, error) {
	br := bufio.NewReader(conn)
	info, err := parseProxyHeader(br)
	return wrapConnWithReader(conn, br), info, err
}

func parseProxyHeader(br *bufio.Reader) (*ProxyInfo, error) {
	peek, err := br.Peek(5)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	if bytes.Equal(peek, []byte("PROXY")) {
		return parseProxyV1(br)
	}
	if bytes.HasPrefix(peek, []byte{'\r', '\n', '\r', '\n', 0x00}) {
		sig, err := br.Peek(len(proxyV2Signature))
		if err != nil {
			return nil, err
		}
		if bytes.Equal(sig, proxyV2Signature) {
			return parseProxyV2(br)
		}
	}
	return nil, nil
}

func parseProxyV1(br *bufio.Reader) (*ProxyInfo, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	parts := bytes.Fields([]byte(line))
	if len(parts) < 6 {
		return nil, fmt.Errorf("proxy v1 header malformed")
	}
	srcIP := string(parts[2])
	dstIP := string(parts[3])
	srcPort := string(parts[4])
	dstPort := string(parts[5])
	return &ProxyInfo{
		SourceAddr: net.JoinHostPort(srcIP, srcPort),
		DestAddr:   net.JoinHostPort(dstIP, dstPort),
		SourceIP:   srcIP,
		DestIP:     dstIP,
		SourcePort: atoiOrZero(srcPort),
		DestPort:   atoiOrZero(dstPort),
	}, nil
}

func parseProxyV2(br *bufio.Reader) (*ProxyInfo, error) {
	header := make([]byte, 16)
	if _, err := io.ReadFull(br, header); err != nil {
		return nil, err
	}
	if !bytes.Equal(header[:12], proxyV2Signature) {
		return nil, fmt.Errorf("proxy v2 signature mismatch")
	}
	length := int(binary.BigEndian.Uint16(header[14:16]))
	payload := make([]byte, length)
	if _, err := io.ReadFull(br, payload); err != nil {
		return nil, err
	}
	family := header[13] & 0x0f
	switch family {
	case 0x1:
		return parseProxyV2Inet(payload)
	case 0x2:
		return parseProxyV2Inet6(payload)
	default:
		return nil, nil
	}
}

func parseProxyV2Inet(payload []byte) (*ProxyInfo, error) {
	if len(payload) < 12 {
		return nil, fmt.Errorf("proxy v2 inet payload too short")
	}
	srcIP := net.IP(payload[0:4]).String()
	dstIP := net.IP(payload[4:8]).String()
	srcPort := int(binary.BigEndian.Uint16(payload[8:10]))
	dstPort := int(binary.BigEndian.Uint16(payload[10:12]))
	return &ProxyInfo{
		SourceAddr: net.JoinHostPort(srcIP, fmt.Sprintf("%d", srcPort)),
		DestAddr:   net.JoinHostPort(dstIP, fmt.Sprintf("%d", dstPort)),
		SourceIP:   srcIP,
		DestIP:     dstIP,
		SourcePort: srcPort,
		DestPort:   dstPort,
	}, nil
}

func parseProxyV2Inet6(payload []byte) (*ProxyInfo, error) {
	if len(payload) < 36 {
		return nil, fmt.Errorf("proxy v2 inet6 payload too short")
	}
	srcIP := net.IP(payload[0:16]).String()
	dstIP := net.IP(payload[16:32]).String()
	srcPort := int(binary.BigEndian.Uint16(payload[32:34]))
	dstPort := int(binary.BigEndian.Uint16(payload[34:36]))
	return &ProxyInfo{
		SourceAddr: net.JoinHostPort(srcIP, fmt.Sprintf("%d", srcPort)),
		DestAddr:   net.JoinHostPort(dstIP, fmt.Sprintf("%d", dstPort)),
		SourceIP:   srcIP,
		DestIP:     dstIP,
		SourcePort: srcPort,
		DestPort:   dstPort,
	}, nil
}

type connWithReader struct {
	net.Conn
	reader *bufio.Reader
}

func wrapConnWithReader(conn net.Conn, reader *bufio.Reader) net.Conn {
	if reader == nil {
		return conn
	}
	return &connWithReader{Conn: conn, reader: reader}
}

func (c *connWithReader) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func atoiOrZero(value string) int {
	var out int
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch < '0' || ch > '9' {
			return 0
		}
		out = out*10 + int(ch-'0')
	}
	return out
}
