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
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/KafScale/platform/pkg/acl"
	"github.com/KafScale/platform/pkg/authn"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type proxySecurity struct {
	tls         authn.TLSOptions
	keyFile     string
	registry    *prometheus.Registry
	connections prometheus.Counter
	failures    *prometheus.CounterVec
	duration    prometheus.Histogram
	handshakes  chan struct{}
	lfsACL      *acl.Authorizer
	admission   *handshakePolicy
}

func loadProxySecurity() (*proxySecurity, error) {
	options := authn.TLSOptions{CertFile: os.Getenv("KAFSCALE_PROXY_TLS_CERT_FILE"), KeyFile: os.Getenv("KAFSCALE_PROXY_TLS_KEY_FILE"), ClientCAFile: os.Getenv("KAFSCALE_PROXY_TLS_CLIENT_CA_FILE"), PrincipalsFile: os.Getenv("KAFSCALE_PROXY_TLS_PRINCIPALS_FILE")}
	keyFile := os.Getenv("KAFSCALE_PROXY_IDENTITY_KEY_FILE")
	if options.CertFile == "" && options.KeyFile == "" && options.ClientCAFile == "" && options.PrincipalsFile == "" && keyFile == "" {
		return nil, nil
	}
	if _, _, err := options.Config(); err != nil {
		return nil, err
	}
	if (options.ClientCAFile != "") != (keyFile != "") {
		return nil, errors.New("mTLS and backend identity key must be configured together")
	}
	if keyFile != "" {
		if _, err := authn.ReadKey(keyFile); err != nil {
			return nil, err
		}
	}
	if keyFile != "" && envBoolDefault("KAFSCALE_PROXY_LFS_ENABLED", false) && strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_HTTP_ADDR")) != "" && strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_HTTP_API_KEY")) == "" {
		return nil, errors.New("authenticated proxy requires an API key for the LFS HTTP endpoint")
	}
	lfsACL, err := loadLFSAuthorizer(keyFile != "")
	if err != nil {
		return nil, err
	}
	admission, limit, err := loadHandshakePolicy()
	if err != nil {
		return nil, err
	}
	registry := prometheus.NewRegistry()
	s := &proxySecurity{tls: options, keyFile: keyFile, registry: registry, lfsACL: lfsACL, handshakes: make(chan struct{}, limit), admission: admission,
		connections: prometheus.NewCounter(prometheus.CounterOpts{Name: "kafscale_proxy_connections_total", Help: "Accepted TCP connections."}),
		failures:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "kafscale_proxy_authentication_failures_total", Help: "Rejected TLS or certificate identities."}, []string{"reason"}),
		duration:    prometheus.NewHistogram(prometheus.HistogramOpts{Name: "kafscale_proxy_handshake_seconds", Help: "TLS handshake and identity mapping duration."}),
	}
	registry.MustRegister(s.connections, s.failures, s.duration)
	return s, nil
}

func (s *proxySecurity) authenticate(ctx context.Context, conn net.Conn) (net.Conn, context.Context, error) {
	s.connections.Inc()
	started := time.Now()
	defer func() { s.duration.Observe(time.Since(started).Seconds()) }()
	handshakeCtx, cancel := context.WithTimeout(ctx, s.admission.timeout)
	defer cancel()
	deadline, _ := handshakeCtx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return s.authFailure(ctx, "deadline", err)
	}
	release, err := s.admission.acquireIP(conn)
	if err != nil {
		return s.authFailure(ctx, "ip_limit", err)
	}
	defer release()
	buffered, err := clientHello(conn)
	if err != nil {
		return s.authFailure(ctx, "client_hello", err)
	}
	queue := time.NewTimer(s.admission.queueTimeout)
	defer queue.Stop()
	select {
	case s.handshakes <- struct{}{}:
		defer func() { <-s.handshakes }()
	case <-handshakeCtx.Done():
		return s.authFailure(ctx, "timeout", handshakeCtx.Err())
	case <-queue.C:
		return s.authFailure(ctx, "capacity", errors.New("TLS handshake queue timeout"))
	}
	secured, principal, err := s.tls.Authenticate(handshakeCtx, buffered)
	if err != nil {
		return s.authFailure(ctx, "tls", err)
	}
	return secured, authn.WithPrincipal(ctx, principal), nil
}

func (s *proxySecurity) metricsHandler() http.Handler {
	return promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{})
}

func (p *proxy) dialBackend(ctx context.Context, addr string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: p.dialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if p.security != nil && p.security.keyFile != "" {
		principal := authn.Principal(ctx)
		if principal == "" {
			conn.Close()
			return nil, errors.New("authenticated backend principal required")
		}
		key, keyErr := authn.ReadKey(p.security.keyFile)
		if keyErr == nil {
			keyErr = authn.SendIdentity(conn, key, principal)
		}
		if keyErr != nil {
			conn.Close()
			return nil, keyErr
		}
	}
	return conn, nil
}

// authenticatedMetadata obtains broker-authorized metadata before rewriting
// broker addresses. Reading etcd directly here would bypass metadata ACLs.
func (p *proxy) authenticatedMetadata(ctx context.Context, header *protocol.RequestHeader, payload []byte, pool *connPool) (encoded []byte, err error) {
	phase := errMetadataBackend
	defer func() {
		if err != nil {
			err = errors.Join(phase, err)
		}
	}()
	var conn net.Conn
	var addr string
	reused := false
	if pool == nil {
		conn, addr, err = p.connectBackend(ctx)
	} else {
		var backends []string
		if p.store == nil && len(p.backends) == 0 {
			return nil, errors.New("no metadata backends")
		}
		backends, err = p.currentBackends(ctx)
		if err != nil || len(backends) == 0 {
			cached := p.cachedBackendsSnapshot()
			if len(cached) == 0 || !p.cacheFresh() {
				if err != nil {
					return nil, err
				}
				return nil, errors.New("no metadata backends available")
			}
			backends = cached
			err = nil
		}
		for _, candidate := range backends {
			if _, ok := pool.conns[candidate]; ok {
				addr = candidate
				break
			}
		}
		if addr == "" {
			addr = backends[int(atomic.AddUint32(&p.rr, 1))%len(backends)]
		}
		_, reused = pool.conns[addr]
		conn, err = pool.Borrow(ctx, addr)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if conn == nil {
			return
		}
		if err != nil || pool == nil {
			conn.Close()
		} else {
			pool.Return(addr, conn)
		}
	}()
	response, err := p.forwardToBackend(ctx, conn, addr, payload)
	if err != nil && reused {
		conn.Close()
		conn, err = p.dialBackend(ctx, addr)
		if err != nil {
			return nil, err
		}
		response, err = p.forwardToBackend(ctx, conn, addr, payload)
	}
	if err != nil {
		return nil, err
	}
	phase = errMetadataResponse
	offset := 4
	if header.APIVersion >= 9 {
		offset++
	}
	if len(response) < offset {
		return nil, errors.New("short metadata response")
	}
	decoded := kmsg.NewPtrMetadataResponse()
	decoded.Version = header.APIVersion
	if err := decoded.ReadFrom(response[offset:]); err != nil {
		return nil, err
	}
	decoded.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 0, Host: p.advertisedHost, Port: p.advertisedPort}}
	decoded.ControllerID = 0
	for i := range decoded.Topics {
		for j := range decoded.Topics[i].Partitions {
			part := &decoded.Topics[i].Partitions[j]
			part.Leader = 0
			part.Replicas = []int32{0}
			part.ISR = []int32{0}
			part.OfflineReplicas = nil
		}
	}
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, decoded), nil
}

func (s *proxySecurity) warnSharedIP(logger *slog.Logger) {
	logger.Warn("TLS per-IP limits use socket source addresses; source-rewriting load balancers make clients share this quota", "max_pending_per_ip", s.admission.perIP, "recommendation", "preserve client source IPs or tune KAFSCALE_PROXY_TLS_MAX_HANDSHAKES_PER_IP")
}
