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

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type handshakePolicy struct {
	timeout      time.Duration
	queueTimeout time.Duration
	perIP        int
	mu           sync.Mutex
	pending      map[string]int
}

func handshakeSetting(name string, fallback int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > 60000 {
		return 0, fmt.Errorf("invalid %s: expected 1..60000", name)
	}
	return value, nil
}

func loadHandshakePolicy() (*handshakePolicy, int, error) {
	names := []string{"KAFSCALE_PROXY_TLS_MAX_HANDSHAKES", "KAFSCALE_PROXY_TLS_MAX_HANDSHAKES_PER_IP", "KAFSCALE_PROXY_TLS_HANDSHAKE_TIMEOUT_MS", "KAFSCALE_PROXY_TLS_HANDSHAKE_QUEUE_MS"}
	defaults := []int{64, 8, 1000, 100}
	for i, name := range names {
		value, err := handshakeSetting(name, defaults[i])
		if err != nil {
			return nil, 0, err
		}
		defaults[i] = value
	}
	return &handshakePolicy{perIP: defaults[1], timeout: time.Duration(defaults[2]) * time.Millisecond, queueTimeout: time.Duration(defaults[3]) * time.Millisecond, pending: make(map[string]int)}, defaults[0], nil
}

type bufferedTLSConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedTLSConn) Read(b []byte) (int, error) { return c.reader.Read(b) }

type authenticationFailure struct {
	reason string
	err    error
}

func (e *authenticationFailure) Error() string { return e.reason + ": " + e.err.Error() }
func (e *authenticationFailure) Unwrap() error { return e.err }

func (s *proxySecurity) authFailure(ctx context.Context, reason string, err error) (net.Conn, context.Context, error) {
	s.failures.WithLabelValues(reason).Inc()
	return nil, ctx, &authenticationFailure{reason: reason, err: err}
}

func (p *handshakePolicy) acquireIP(conn net.Conn) (func(), error) {
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		host = conn.RemoteAddr().String()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending[host] >= p.perIP {
		return nil, errors.New("per-IP handshake capacity reached")
	}
	p.pending[host]++
	return func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.pending[host]--
		if p.pending[host] == 0 {
			delete(p.pending, host)
		}
	}, nil
}

// Idle sockets are bounded by the per-IP limit and a deadline before they can
// occupy a cryptographic handshake slot. Preserve peeked bytes for crypto/tls.
func clientHello(conn net.Conn) (net.Conn, error) {
	reader := bufio.NewReaderSize(conn, 18437)
	header, err := reader.Peek(6)
	if err != nil {
		return nil, err
	}
	length := int(header[3])<<8 | int(header[4])
	if header[0] != 22 || header[1] != 3 || header[5] != 1 || length < 1 || length > 18432 {
		return nil, errors.New("TLS ClientHello required")
	}
	if _, err := reader.Peek(length + 5); err != nil {
		return nil, err
	}
	return &bufferedTLSConn{Conn: conn, reader: reader}, nil
}
