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
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/authn"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func proxySecurityEnv(t *testing.T, p *testPKI) string {
	t.Helper()
	for k, v := range map[string]string{"KAFSCALE_PROXY_TLS_CERT_FILE": p.options.CertFile, "KAFSCALE_PROXY_TLS_KEY_FILE": p.options.KeyFile, "KAFSCALE_PROXY_TLS_CLIENT_CA_FILE": p.options.ClientCAFile, "KAFSCALE_PROXY_TLS_PRINCIPALS_FILE": p.options.PrincipalsFile} {
		t.Setenv(k, v)
	}
	path := filepath.Join(t.TempDir(), "identity")
	writeTestFile(t, path, bytes.Repeat([]byte{29}, 32))
	t.Setenv("KAFSCALE_PROXY_IDENTITY_KEY_FILE", path)
	return path
}
func TestLoadProxySecurity(t *testing.T) {
	for _, k := range []string{"KAFSCALE_PROXY_TLS_CERT_FILE", "KAFSCALE_PROXY_TLS_KEY_FILE", "KAFSCALE_PROXY_TLS_CLIENT_CA_FILE", "KAFSCALE_PROXY_TLS_PRINCIPALS_FILE", "KAFSCALE_PROXY_IDENTITY_KEY_FILE"} {
		t.Setenv(k, "")
	}
	if s, err := loadProxySecurity(); err != nil || s != nil {
		t.Fatal(s, err)
	}
	p := newTestPKI(t)
	path := proxySecurityEnv(t, p)
	if _, err := loadProxySecurity(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ k, v string }{{"KAFSCALE_PROXY_TLS_CERT_FILE", "/missing"}, {"KAFSCALE_PROXY_IDENTITY_KEY_FILE", "/missing"}, {"KAFSCALE_PROXY_IDENTITY_KEY_FILE", ""}} {
		t.Run(tc.k+tc.v, func(t *testing.T) {
			t.Setenv(tc.k, tc.v)
			if _, err := loadProxySecurity(); err == nil {
				t.Fatal("invalid security configuration accepted")
			}
		})
	}
	t.Setenv("KAFSCALE_PROXY_TLS_CLIENT_CA_FILE", "")
	t.Setenv("KAFSCALE_PROXY_TLS_PRINCIPALS_FILE", "")
	if _, err := loadProxySecurity(); err == nil {
		t.Fatal("identity forwarding without client auth accepted")
	}
	t.Setenv("KAFSCALE_PROXY_IDENTITY_KEY_FILE", "")
	if _, err := loadProxySecurity(); err != nil {
		t.Fatal("TLS only", err)
	}
	_ = path
}

// securityBackend emulates the broker's mandatory identity handshake and records
// the fixed connection principal alongside each Kafka request's spoofable ID.
func securityBackend(t *testing.T, key []byte, requests chan<- string, response func(*protocol.RequestHeader, []byte) []byte) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	acceptDone := make(chan struct{})
	t.Cleanup(func() { ln.Close(); <-acceptDone; wg.Wait() })
	go func() {
		defer close(acceptDone)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				principal, err := authn.ReceiveIdentity(c, key)
				if err != nil {
					return
				}
				for {
					frame, err := protocol.ReadFrame(c)
					if err != nil {
						return
					}
					h, _, err := protocol.ParseRequestHeader(frame.Payload)
					if err != nil {
						return
					}
					if requests != nil {
						requests <- principal + ":" + *h.ClientID
					}
					reply := response(h, frame.Payload)
					if reply == nil {
						return
					}
					if err := protocol.WriteFrame(c, reply); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}
func TestProxyMTLSIdentityIsolation(t *testing.T) {
	pki := newTestPKI(t)
	path := proxySecurityEnv(t, pki)
	security, err := loadProxySecurity()
	if err != nil {
		t.Fatal(err)
	}
	key, _ := authn.ReadKey(path)
	requests := make(chan string, 8)
	addr := securityBackend(t, key, requests, func(h *protocol.RequestHeader, _ []byte) []byte {
		r := kmsg.NewPtrListGroupsResponse()
		return protocol.EncodeResponse(h.CorrelationID, h.APIVersion, r)
	})
	p := &proxy{security: security, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), backends: []string{addr}, dialTimeout: time.Second, backendRetries: 1}
	p.setReady(true)
	var wg sync.WaitGroup
	for _, identity := range []string{"a", "b"} {
		cert, _, _ := pki.certificate(t, "spiffe://test/"+identity, false, false)
		wg.Add(1)
		go func(cert tls.Certificate) {
			defer wg.Done()
			server, raw := net.Pipe()
			defer raw.Close()
			done := make(chan struct{})
			go func() { defer close(done); p.handleConnection(context.Background(), server) }()
			client := tls.Client(raw, &tls.Config{RootCAs: pki.roots, ServerName: "localhost", Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
			if err := client.Handshake(); err != nil {
				t.Error(err)
				return
			}
			for i := 0; i < 2; i++ {
				request := kmsg.NewPtrListGroupsRequest()
				request.Version = 0
				payload := kmsg.NewRequestFormatter(kmsg.FormatterClientID("pretend-admin")).AppendRequest(nil, request, int32(i))
				if _, err := client.Write(payload); err != nil {
					t.Error(err)
					return
				}
				if _, err := protocol.ReadFrame(client); err != nil {
					t.Error(err)
					return
				}
			}
			raw.Close()
			<-done
		}(cert)
	}
	wg.Wait()
	counts := map[string]int{}
	for i := 0; i < 4; i++ {
		counts[<-requests]++
	}
	if counts["service-a:pretend-admin"] != 2 || counts["service-b:pretend-admin"] != 2 {
		t.Fatal(counts)
	}
	server, raw := net.Pipe()
	done := make(chan struct{})
	go func() { p.handleConnection(context.Background(), server); close(done) }()
	raw.Close()
	<-done
	recorder := httptest.NewRecorder()
	security.metricsHandler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	for _, expected := range []string{"kafscale_proxy_connections_total 3", `kafscale_proxy_authentication_failures_total{reason="deadline"} 1`, "kafscale_proxy_handshake_seconds_count 3"} {
		if !strings.Contains(body, expected) {
			t.Fatal(body)
		}
	}
}
func TestProxyBackendIdentityFailures(t *testing.T) {
	pki := newTestPKI(t)
	path := proxySecurityEnv(t, pki)
	s, err := loadProxySecurity()
	if err != nil {
		t.Fatal(err)
	}
	key, _ := authn.ReadKey(path)
	seen := make(chan string, 2)
	addr := securityBackend(t, key, seen, func(h *protocol.RequestHeader, _ []byte) []byte { return nil })
	p := &proxy{security: s, dialTimeout: time.Second}
	if c, err := p.dialBackend(context.Background(), addr); err == nil {
		c.Close()
		t.Fatal("missing backend principal accepted")
	}
	c, err := p.dialBackend(authn.WithPrincipal(context.Background(), authn.ProbePrincipal), addr)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	// Client identity is not needed for the internal readiness connection.
	ctx := authn.WithPrincipal(context.Background(), "service-a")
	c, err = p.dialBackend(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if c, err = p.dialBackend(ctx, addr); err == nil {
		c.Close()
		t.Fatal("missing key accepted")
	}
	writeTestFile(t, path, bytes.Repeat([]byte{9}, 32))
	if c, err = p.dialBackend(ctx, addr); err == nil {
		c.Close()
		t.Fatal("wrong key accepted")
	}
	if _, err = p.dialBackend(ctx, "127.0.0.1:0"); err == nil {
		t.Fatal("dial failure ignored")
	}
}
func TestAuthenticatedMetadata(t *testing.T) {
	pki := newTestPKI(t)
	path := proxySecurityEnv(t, pki)
	s, _ := loadProxySecurity()
	key, _ := authn.ReadKey(path)
	name := "allowed"
	req := kmsg.NewPtrMetadataRequest()
	req.Version = 9
	request := kmsg.NewRequestFormatter(kmsg.FormatterClientID("spoof")).AppendRequest(nil, req, 7)[4:]
	header, _, _ := protocol.ParseRequestHeader(request)
	for _, tc := range []string{"ok", "short", "invalid", "closed"} {
		t.Run(tc, func(t *testing.T) {
			addr := securityBackend(t, key, nil, func(h *protocol.RequestHeader, _ []byte) []byte {
				switch tc {
				case "short":
					return []byte{1}
				case "invalid":
					return []byte{0, 0, 0, 7, 0, 255}
				case "closed":
					return nil
				}
				r := kmsg.NewPtrMetadataResponse()
				r.Version = 9
				topic := kmsg.NewMetadataResponseTopic()
				topic.Topic = &name
				part := kmsg.NewMetadataResponseTopicPartition()
				part.Leader = 4
				part.Replicas = []int32{4}
				topic.Partitions = []kmsg.MetadataResponseTopicPartition{part}
				r.Topics = []kmsg.MetadataResponseTopic{topic}
				return protocol.EncodeResponse(7, 9, r)
			})
			p := &proxy{security: s, backends: []string{addr}, backendRetries: 1, dialTimeout: time.Second, advertisedHost: "entry", advertisedPort: 9092}
			payload, err := p.brokerMetadata(authn.WithPrincipal(context.Background(), "service-a"), header, request, nil)
			if tc != "ok" {
				if err == nil {
					t.Fatal("invalid response accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			r := kmsg.NewPtrMetadataResponse()
			r.Version = 9
			if err := r.ReadFrom(payload[5:]); err != nil {
				t.Fatal(err)
			}
			if r.Brokers[0].Host != "entry" || r.Topics[0].Partitions[0].Leader != 0 {
				t.Fatal(r)
			}
		})
	}
	p := &proxy{backends: []string{"127.0.0.1:0"}, backendRetries: 1, dialTimeout: time.Millisecond}
	if _, err := p.brokerMetadata(context.Background(), header, request, nil); err == nil {
		t.Fatal("missing backend accepted")
	}
}

func TestProxyHandshakeLimit(t *testing.T) {
	pki := newTestPKI(t)
	proxySecurityEnv(t, pki)
	s, err := loadProxySecurity()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < cap(s.handshakes); i++ {
		s.handshakes <- struct{}{}
	}
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	if _, _, err := s.authenticate(context.Background(), server); err == nil {
		t.Fatal("handshake limit ignored")
	}
}
func TestProxyRequiresLFSHTTPAuthentication(t *testing.T) {
	pki := newTestPKI(t)
	proxySecurityEnv(t, pki)
	t.Setenv("KAFSCALE_PROXY_LFS_ENABLED", "true")
	t.Setenv("KAFSCALE_LFS_PROXY_HTTP_ADDR", ":8080")
	t.Setenv("KAFSCALE_LFS_PROXY_HTTP_API_KEY", "")
	if _, err := loadProxySecurity(); err == nil {
		t.Fatal("unauthenticated LFS HTTP listener accepted")
	}
	t.Setenv("KAFSCALE_LFS_PROXY_HTTP_API_KEY", "test-only-api-key")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny"}`)
	if _, err := loadProxySecurity(); err != nil {
		t.Fatal(err)
	}
}

func TestLFSAuthorizerConfiguration(t *testing.T) {
	t.Setenv("KAFSCALE_PROXY_LFS_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", "")
	t.Setenv("KAFSCALE_ACL_FILE", "")
	if _, err := loadLFSAuthorizer(true); err == nil {
		t.Fatal("missing ACL accepted")
	}
	t.Setenv("KAFSCALE_ACL_FILE", "/missing")
	if _, err := loadLFSAuthorizer(true); err == nil {
		t.Fatal("missing ACL file accepted")
	}
	path := filepath.Join(t.TempDir(), "acl.json")
	t.Setenv("KAFSCALE_ACL_FILE", path)
	writeTestFile(t, path, []byte("invalid"))
	if _, err := loadLFSAuthorizer(true); err == nil {
		t.Fatal("invalid ACL accepted")
	}
	writeTestFile(t, path, []byte(`{"default_policy":"deny"}`))
	if _, err := loadLFSAuthorizer(true); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KAFSCALE_PROXY_LFS_ENABLED", "false")
	if a, err := loadLFSAuthorizer(true); err != nil || a != nil {
		t.Fatal(a, err)
	}
	// Exercise the startup error propagation with valid TLS but missing LFS ACLs.
	pki := newTestPKI(t)
	proxySecurityEnv(t, pki)
	t.Setenv("KAFSCALE_PROXY_LFS_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_FILE", "/missing")
	if _, err := loadProxySecurity(); err == nil {
		t.Fatal("missing LFS ACL did not fail startup")
	}
}

func TestLFSDoesNotUploadUnauthorizedRecords(t *testing.T) {
	t.Setenv("KAFSCALE_PROXY_LFS_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_FILE", "")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"service-a","allow":[{"action":"produce","resource":"topic","name":"allowed"}]}]}`)
	authorizer, err := loadLFSAuthorizer(true)
	if err != nil {
		t.Fatal(err)
	}
	module, store := testLFSModule(t)
	module.authorizer = authorizer
	payload := []byte("large record")
	sum := sha256.Sum256(payload)
	batch := buildTestBatch([]kmsg.Record{{Value: payload, Headers: []kmsg.Header{{Key: "LFS_BLOB", Value: []byte(hex.EncodeToString(sum[:]))}}}})
	request := kmsg.NewPtrProduceRequest()
	request.Topics = []kmsg.ProduceRequestTopic{{Topic: "allowed", Partitions: []kmsg.ProduceRequestTopicPartition{{Records: append([]byte(nil), batch...)}}}, {Topic: "denied", Partitions: []kmsg.ProduceRequestTopicPartition{{Records: append([]byte(nil), batch...)}}}}
	spoof := "admin"
	result, err := module.rewriteProduceRecords(authn.WithPrincipal(context.Background(), "service-a"), &protocol.RequestHeader{ClientID: &spoof}, request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.modified || len(store.objects) != 1 {
		t.Fatal("unexpected uploads", len(store.objects))
	}
	if !bytes.Equal(batch, request.Topics[1].Partitions[0].Records) {
		t.Fatal("unauthorized record was rewritten")
	}
}

func TestLFSACLPrecedenceMatchesBroker(t *testing.T) {
	t.Setenv("KAFSCALE_PROXY_LFS_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_FILE", "/missing")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny"}`)
	if _, err := loadLFSAuthorizer(true); err != nil {
		t.Fatal("inline ACL should take precedence over file", err)
	}
	t.Setenv("KAFSCALE_ACL_JSON", "invalid")
	if _, err := loadLFSAuthorizer(true); err == nil {
		t.Fatal("invalid inline ACL accepted")
	}
}

func TestAuthenticatedReadinessUsesReservedPrincipal(t *testing.T) {
	pki := newTestPKI(t)
	path := proxySecurityEnv(t, pki)
	security, err := loadProxySecurity()
	if err != nil {
		t.Fatal(err)
	}
	key, _ := authn.ReadKey(path)
	seen := make(chan string, 1)
	addr := securityBackend(t, key, seen, func(h *protocol.RequestHeader, _ []byte) []byte {
		response := kmsg.NewPtrFetchResponse()
		response.Version = h.APIVersion
		return protocol.EncodeResponse(h.CorrelationID, h.APIVersion, response)
	})
	proxy := &proxy{security: security, backends: []string{addr}, backendRetries: 1, dialTimeout: time.Second}
	if err := proxy.fetchProbe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := <-seen; got != authn.ProbePrincipal+":kafscale-proxy-readyz" {
		t.Fatal("readiness principal", got)
	}
}

type brokenMetadataStore struct{ metadata.Store }

func (brokenMetadataStore) Metadata(context.Context, []string) (*metadata.ClusterMetadata, error) {
	return nil, errors.New("metadata unavailable")
}

func TestAuthenticatedMetadataReusesClientPool(t *testing.T) {
	pki := newTestPKI(t)
	path := proxySecurityEnv(t, pki)
	s, err := loadProxySecurity()
	if err != nil {
		t.Fatal(err)
	}
	key, _ := authn.ReadKey(path)
	seen := make(chan string, 2)
	addr := securityBackend(t, key, seen, func(header *protocol.RequestHeader, _ []byte) []byte {
		r := kmsg.NewPtrMetadataResponse()
		r.Version = 9
		return protocol.EncodeResponse(header.CorrelationID, 9, r)
	})
	p := &proxy{security: s, backends: []string{addr}, dialTimeout: time.Second}
	pool := newConnPool(time.Second)
	pool.dial = p.dialBackend
	defer pool.Close()
	req := kmsg.NewPtrMetadataRequest()
	req.Version = 9
	data := kmsg.NewRequestFormatter(kmsg.FormatterClientID("client")).AppendRequest(nil, req, 1)[4:]
	header, _, _ := protocol.ParseRequestHeader(data)
	ctx := authn.WithPrincipal(context.Background(), "service-a")
	if _, err := p.brokerMetadata(ctx, header, data, pool); err != nil {
		t.Fatal(err)
	}
	first := pool.conns[addr]
	p.backends = []string{"127.0.0.1:0", addr}
	if _, err := p.brokerMetadata(ctx, header, data, pool); err != nil {
		t.Fatal(err)
	}
	if pool.conns[addr] != first {
		t.Fatal("metadata opened a new backend connection")
	}
	for i := 0; i < 2; i++ {
		if got := <-seen; got != "service-a:client" {
			t.Fatal(got)
		}
	}
	p.backends = nil
	p.store = brokenMetadataStore{}
	p.cacheTTL = time.Minute
	p.setCachedBackends([]string{addr})
	p.touchHealthy()
	if _, err := p.brokerMetadata(ctx, header, data, pool); err != nil {
		t.Fatal("fresh cached backend unavailable during metadata outage", err)
	}
	if got := <-seen; got != "service-a:client" {
		t.Fatal(got)
	}

	for _, p := range []*proxy{{}, {store: metadata.NewInMemoryStore(metadata.ClusterMetadata{})}, {store: brokenMetadataStore{}}, {backends: []string{"127.0.0.1:0"}, dialTimeout: time.Millisecond}} {
		if _, err := p.brokerMetadata(ctx, header, data, pool); err == nil {
			t.Fatal("missing backend accepted")
		}
	}
}

func TestSharedIPWarningAtStartup(t *testing.T) {
	pki := newTestPKI(t)
	proxySecurityEnv(t, pki)
	security, err := loadProxySecurity()
	if err != nil {
		t.Fatal(err)
	}
	var buffer bytes.Buffer
	security.warnSharedIP(slog.New(slog.NewJSONHandler(&buffer, nil)))
	for _, expected := range []string{"source-rewriting", "max_pending_per_ip", "8", "KAFSCALE_PROXY_TLS_MAX_HANDSHAKES_PER_IP"} {
		if !strings.Contains(buffer.String(), expected) {
			t.Fatal(buffer.String())
		}
	}
}

func TestMetadataRetriesStaleAuthenticatedConnection(t *testing.T) {
	for _, kind := range []string{"write", "read", "redial_failure", "second_failure"} {
		t.Run(kind, func(t *testing.T) {
			pki := newTestPKI(t)
			path := proxySecurityEnv(t, pki)
			security, err := loadProxySecurity()
			if err != nil {
				t.Fatal(err)
			}
			key, _ := authn.ReadKey(path)
			seen := make(chan string, 1)
			addr := securityBackend(t, key, seen, func(h *protocol.RequestHeader, _ []byte) []byte {
				if kind == "second_failure" {
					return nil
				}
				r := kmsg.NewPtrMetadataResponse()
				r.Version = 9
				return protocol.EncodeResponse(h.CorrelationID, 9, r)
			})
			if kind == "redial_failure" {
				addr = "127.0.0.1:0"
			}
			p := &proxy{security: security, backends: []string{addr}, dialTimeout: time.Second}
			pool := newConnPool(time.Second)
			pool.dial = p.dialBackend
			defer pool.Close()
			oldServer, oldClient := net.Pipe()
			if kind == "read" {
				go func() { defer oldServer.Close(); _, _ = protocol.ReadFrame(oldServer) }()
			} else {
				oldServer.Close()
			}
			pool.Return(addr, oldClient)
			request := kmsg.NewPtrMetadataRequest()
			request.Version = 9
			data := kmsg.NewRequestFormatter(kmsg.FormatterClientID("spoof")).AppendRequest(nil, request, 1)[4:]
			header, _, _ := protocol.ParseRequestHeader(data)
			_, err = p.brokerMetadata(authn.WithPrincipal(context.Background(), "service-a"), header, data, pool)
			if kind == "redial_failure" || kind == "second_failure" {
				if err == nil || len(pool.conns) != 0 {
					t.Fatal("failed retry was pooled", err)
				}
				return
			}
			if err != nil {
				t.Fatal("stale connection dropped metadata request", err)
			}
			if pool.conns[addr] == oldClient {
				t.Fatal("stale connection returned to pool")
			}
			if principal := <-seen; principal != "service-a:spoof" {
				t.Fatal("retry lost authenticated principal", principal)
			}
		})
	}
}
