// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/KafScale/platform/pkg/protocol"
)

const (
	apiKeySaslHandshake    int16 = 17
	apiKeySaslAuthenticate int16 = 36
)

func (p *lfsProxy) wrapBackendTLS(ctx context.Context, conn net.Conn, addr string) (net.Conn, error) {
	if p.backendTLSConfig == nil {
		return conn, nil
	}
	cfg := p.backendTLSConfig.Clone()
	if cfg.ServerName == "" {
		if host, _, err := net.SplitHostPort(addr); err == nil {
			cfg.ServerName = host
		}
	}
	tlsConn := tls.Client(conn, cfg)
	deadline := time.Now().Add(p.dialTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok {
		deadline = ctxDeadline
	}
	_ = tlsConn.SetDeadline(deadline)
	if err := tlsConn.Handshake(); err != nil {
		return nil, err
	}
	_ = tlsConn.SetDeadline(time.Time{})
	return tlsConn, nil
}

func (p *lfsProxy) performBackendSASL(ctx context.Context, conn net.Conn) error {
	mech := strings.TrimSpace(p.backendSASLMechanism)
	if mech == "" {
		return nil
	}
	if strings.ToUpper(mech) != "PLAIN" {
		return fmt.Errorf("unsupported SASL mechanism %q", mech)
	}
	if p.backendSASLUsername == "" {
		return errors.New("backend SASL username required")
	}

	// 1) Handshake
	correlationID := int32(1)
	handshakeReq, err := encodeSaslHandshakeRequest(&protocol.RequestHeader{
		APIKey:        apiKeySaslHandshake,
		APIVersion:    1,
		CorrelationID: correlationID,
	}, mech)
	if err != nil {
		return err
	}
	if err := protocol.WriteFrame(conn, handshakeReq); err != nil {
		return err
	}
	if err := readSaslResponse(conn); err != nil {
		return fmt.Errorf("sasl handshake failed: %w", err)
	}

	// 2) Authenticate
	authBytes := buildSaslPlainAuthBytes(p.backendSASLUsername, p.backendSASLPassword)
	authReq, err := encodeSaslAuthenticateRequest(&protocol.RequestHeader{
		APIKey:        apiKeySaslAuthenticate,
		APIVersion:    1,
		CorrelationID: correlationID + 1,
	}, authBytes)
	if err != nil {
		return err
	}
	if err := protocol.WriteFrame(conn, authReq); err != nil {
		return err
	}
	if err := readSaslResponse(conn); err != nil {
		return fmt.Errorf("sasl authenticate failed: %w", err)
	}

	return nil
}
