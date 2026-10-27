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
	"net"
	"net/http"
	"os"
	"strings"
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
	failures    prometheus.Counter
	duration    prometheus.Histogram
	handshakes  chan struct{}
	lfsACL      *acl.Authorizer
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
	registry := prometheus.NewRegistry()
	s := &proxySecurity{tls: options, keyFile: keyFile, registry: registry, lfsACL: lfsACL, handshakes: make(chan struct{}, 64),
		connections: prometheus.NewCounter(prometheus.CounterOpts{Name: "kafscale_proxy_connections_total", Help: "Accepted TCP connections."}),
		failures:    prometheus.NewCounter(prometheus.CounterOpts{Name: "kafscale_proxy_authentication_failures_total", Help: "Rejected TLS or certificate identities."}),
		duration:    prometheus.NewHistogram(prometheus.HistogramOpts{Name: "kafscale_proxy_handshake_seconds", Help: "TLS handshake and identity mapping duration."}),
	}
	registry.MustRegister(s.connections, s.failures, s.duration)
	return s, nil
}

func (s *proxySecurity) authenticate(ctx context.Context, conn net.Conn) (net.Conn, context.Context, error) {
	s.connections.Inc()
	select {
	case s.handshakes <- struct{}{}:
		defer func() { <-s.handshakes }()
	default:
		s.failures.Inc()
		return nil, ctx, errors.New("TLS handshake capacity reached")
	}
	start := time.Now()
	secured, principal, err := s.tls.Authenticate(ctx, conn)
	s.duration.Observe(time.Since(start).Seconds())
	if err != nil {
		s.failures.Inc()
		return nil, ctx, err
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
func (p *proxy) authenticatedMetadata(ctx context.Context, header *protocol.RequestHeader, payload []byte) ([]byte, error) {
	conn, addr, err := p.connectBackend(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	response, err := p.forwardToBackend(ctx, conn, addr, payload)
	if err != nil {
		return nil, err
	}
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
