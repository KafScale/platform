// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package proxy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgproto3/v2"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
	kafsql "github.com/kafscale/platform/addons/processors/sql-processor/internal/sql"
)

type Server struct {
	cfg config.ProxyConfig
	log *log.Logger

	rrCounter uint32
	dialer    func(ctx context.Context, addr string) (net.Conn, error)
}

func New(cfg config.ProxyConfig, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	srv := &Server{cfg: cfg, log: logger}
	srv.dialer = func(ctx context.Context, addr string) (net.Conn, error) {
		dialer := net.Dialer{Timeout: 5 * time.Second}
		return dialer.DialContext(ctx, "tcp", addr)
	}
	return srv
}

func (s *Server) Run(ctx context.Context) error {
	if s.cfg.Listen == "" {
		return errors.New("proxy listen address is required")
	}
	if len(s.cfg.Upstreams) == 0 {
		return errors.New("proxy upstreams are required")
	}

	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("proxy listen: %w", err)
	}
	defer ln.Close()

	s.log.Printf("kafsql_proxy_listening addr=%s upstreams=%d", s.cfg.Listen, len(s.cfg.Upstreams))

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = ln.Close()
		case <-done:
		}
	}()
	defer close(done)

	maxConns := s.cfg.MaxConnections
	if maxConns <= 0 {
		maxConns = 200
	}
	sem := make(chan struct{}, maxConns)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				continue
			}
			return fmt.Errorf("proxy accept: %w", err)
		}
		sem <- struct{}{}
		go func(c net.Conn) {
			defer func() { <-sem }()
			if err := s.handleConn(ctx, c); err != nil {
				s.log.Printf("kafsql_proxy_conn_error err=%v", err)
			}
		}(conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) error {
	defer conn.Close()
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	startup, err := s.receiveStartup(backend, conn)
	if err != nil {
		return err
	}
	if startup == nil {
		return nil
	}

	upstreamConn, err := s.dialUpstream(ctx)
	if err != nil {
		return err
	}
	defer upstreamConn.Close()

	if err := sendStartup(upstreamConn, startup); err != nil {
		return err
	}

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(upstreamConn), upstreamConn)
	if err := relayUntilReady(frontend, conn); err != nil {
		return err
	}

	acl := ACL{Allow: s.cfg.ACL.Allow, Deny: s.cfg.ACL.Deny}
	cache := newQueryCache(time.Duration(s.cfg.CacheTTLSeconds)*time.Second, s.cfg.CacheMaxEntries)
	for {
		msg, err := backend.Receive()
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case *pgproto3.Terminate:
			return nil
		case *pgproto3.Query:
			start := time.Now()
			trimmed := trimQuery(m.String)
			key := cacheKey(trimmed)
			decision, hit := cache.get(key)
			if !hit {
				allowed, reason, topics, showTopics := authorizeQuery(acl, trimmed)
				decision = cacheDecision{
					created:    time.Now(),
					allowed:    allowed,
					reason:     reason,
					topics:     topics,
					showTopics: showTopics,
				}
				cache.set(key, decision)
			}
			allowed := decision.allowed
			reason := decision.reason
			topics := decision.topics
			showTopics := decision.showTopics
			if !allowed {
				s.log.Printf("kafsql_proxy_audit decision=deny remote=%s topics=%s show_topics=%t reason=%q query=%q",
					conn.RemoteAddr().String(),
					strings.Join(topics, ","),
					showTopics,
					reason,
					trimmed,
				)
				if err := sendError(backend, reason); err != nil {
					return err
				}
				continue
			}
			if err := frontend.Send(m); err != nil {
				return err
			}
			if err := relayUntilReady(frontend, conn); err != nil {
				return err
			}
			s.log.Printf("kafsql_proxy_audit decision=allow remote=%s topics=%s show_topics=%t duration_ms=%d query=%q",
				conn.RemoteAddr().String(),
				strings.Join(topics, ","),
				showTopics,
				time.Since(start).Milliseconds(),
				trimmed,
			)
		default:
			if err := sendError(backend, "extended protocol is not supported"); err != nil {
				return err
			}
		}
	}
}

func (s *Server) receiveStartup(backend *pgproto3.Backend, conn net.Conn) (*pgproto3.StartupMessage, error) {
	for {
		msg, err := backend.ReceiveStartupMessage()
		if err != nil {
			return nil, err
		}
		switch m := msg.(type) {
		case *pgproto3.SSLRequest:
			if _, err := conn.Write([]byte("N")); err != nil {
				return nil, err
			}
			continue
		case *pgproto3.StartupMessage:
			return m, nil
		case *pgproto3.CancelRequest:
			return nil, nil
		default:
			return nil, fmt.Errorf("unsupported startup message: %T", msg)
		}
	}
}

func (s *Server) dialUpstream(ctx context.Context) (net.Conn, error) {
	idx := atomic.AddUint32(&s.rrCounter, 1)
	addr := s.cfg.Upstreams[int(idx)%len(s.cfg.Upstreams)]
	conn, err := s.dialer(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("dial upstream %s: %w", addr, err)
	}
	return conn, nil
}

func sendStartup(conn net.Conn, startup *pgproto3.StartupMessage) error {
	buf, err := startup.Encode(nil)
	if err != nil {
		return err
	}
	_, err = conn.Write(buf)
	return err
}

func relayUntilReady(frontend *pgproto3.Frontend, client net.Conn) error {
	for {
		msg, err := frontend.Receive()
		if err != nil {
			return err
		}
		buf, err := msg.Encode(nil)
		if err != nil {
			return err
		}
		if _, err := client.Write(buf); err != nil {
			return err
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			return nil
		}
	}
}

func sendError(backend *pgproto3.Backend, message string) error {
	if err := backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Message:  message,
	}); err != nil {
		return err
	}
	return backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

func authorizeQuery(acl ACL, query string) (bool, string, []string, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	if trimmed == "" {
		return true, "", nil, false
	}
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "set ") || strings.HasPrefix(lower, "reset ") {
		return true, "", nil, false
	}
	if len(acl.Allow) == 0 && len(acl.Deny) == 0 {
		return true, "", nil, false
	}
	parsed, err := kafsql.Parse(trimmed)
	if err != nil {
		return false, "proxy cannot authorize query", nil, false
	}
	topics, showTopics := queryTopics(parsed)
	if showTopics && !acl.AllowShowTopics() {
		return false, "show topics is not allowed by proxy ACL", topics, showTopics
	}
	for _, topic := range topics {
		if !acl.Allows(topic) {
			return false, fmt.Sprintf("access denied to topic %q", topic), topics, showTopics
		}
	}
	return true, "", topics, showTopics
}

func queryTopics(parsed kafsql.Query) ([]string, bool) {
	switch parsed.Type {
	case kafsql.QueryShowTopics:
		return nil, true
	case kafsql.QueryShowPartitions, kafsql.QueryDescribe:
		return []string{parsed.Topic}, false
	case kafsql.QueryExplain:
		if parsed.Explain != nil {
			return queryTopics(*parsed.Explain)
		}
		return nil, false
	case kafsql.QuerySelect:
		topics := []string{parsed.Topic}
		if parsed.JoinTopic != "" {
			topics = append(topics, parsed.JoinTopic)
		}
		return topics, false
	default:
		return nil, false
	}
}

func trimQuery(query string) string {
	trimmed := strings.TrimSpace(query)
	if len(trimmed) > 512 {
		return trimmed[:512] + "..."
	}
	return trimmed
}
