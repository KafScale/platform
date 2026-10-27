// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

type admissionConn struct {
	net.Conn
	host          string
	deadlineError bool
}

func (c *admissionConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP(c.host), Port: 1234}
}
func (c *admissionConn) SetDeadline(d time.Time) error {
	if c.deadlineError {
		return errors.New("deadline failed")
	}
	return c.Conn.SetDeadline(d)
}

func TestHandshakeSettings(t *testing.T) {
	for _, name := range []string{"KAFSCALE_PROXY_TLS_MAX_HANDSHAKES", "KAFSCALE_PROXY_TLS_MAX_HANDSHAKES_PER_IP", "KAFSCALE_PROXY_TLS_HANDSHAKE_TIMEOUT_MS", "KAFSCALE_PROXY_TLS_HANDSHAKE_QUEUE_MS"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "invalid")
			if _, _, err := loadHandshakePolicy(); err == nil {
				t.Fatal("invalid setting accepted")
			}
		})
	}
	t.Setenv("KAFSCALE_PROXY_TLS_MAX_HANDSHAKES", "2")
	policy, limit, err := loadHandshakePolicy()
	if err != nil || limit != 2 || policy.timeout != time.Second {
		t.Fatal(policy, limit, err)
	}
	pki := newTestPKI(t)
	proxySecurityEnv(t, pki)
	t.Setenv("KAFSCALE_PROXY_TLS_MAX_HANDSHAKES", "0")
	if _, err := loadProxySecurity(); err == nil {
		t.Fatal("bad handshake configuration accepted")
	}
}

func TestClientHelloAdmission(t *testing.T) {
	for _, data := range [][]byte{{}, {22, 3, 3, 0, 4}, {23, 3, 3, 0, 4, 1}, {22, 3, 3, 0, 0, 1}, {22, 3, 3, 255, 255, 1}, {22, 3, 3, 0, 4, 1}} {
		server, client := net.Pipe()
		go func() { client.Write(data); client.Close() }()
		if _, err := clientHello(server); err == nil {
			t.Fatal("invalid hello accepted")
		}
		server.Close()
	}
	hello := []byte{22, 3, 3, 0, 4, 1, 0, 0, 0}
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	go func() { client.Write(hello) }()
	buffered, err := clientHello(server)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, len(hello))
	if _, err := io.ReadFull(buffered, data); err != nil || !bytes.Equal(hello, data) {
		t.Fatal("lost TLS bytes", err)
	}
}

func TestHandshakeQueueAndIPLimits(t *testing.T) {
	pki := newTestPKI(t)
	proxySecurityEnv(t, pki)
	for _, reason := range []string{"capacity", "timeout", "ip_limit", "deadline", "tls"} {
		t.Run(reason, func(t *testing.T) {
			s, err := loadProxySecurity()
			if err != nil {
				t.Fatal(err)
			}
			s.admission.queueTimeout = 20 * time.Millisecond
			s.admission.timeout = 100 * time.Millisecond
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			conn := &admissionConn{Conn: server, host: "127.0.0.2"}
			if reason == "deadline" {
				conn.deadlineError = true
			}
			if reason == "ip_limit" {
				s.admission.perIP = 1
				release, err := s.admission.acquireIP(conn)
				if err != nil {
					t.Fatal(err)
				}
				defer release()
			}
			if reason == "capacity" || reason == "timeout" {
				for i := 0; i < cap(s.handshakes); i++ {
					s.handshakes <- struct{}{}
				}
			}
			ctx := context.Background()
			if reason == "timeout" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 5*time.Millisecond)
				defer cancel()
			}
			go func() { client.Write([]byte{22, 3, 3, 0, 4, 1, 0, 0, 0}) }()
			_, _, err = s.authenticate(ctx, conn)
			var failure *authenticationFailure
			if !errors.As(err, &failure) || failure.reason != reason {
				t.Fatalf("%s: %v", reason, err)
			}
			if errors.Unwrap(failure) == nil {
				t.Fatal("lost error reason")
			}
		})
	}
}

func TestIdlePeersDoNotOccupyTLSSlots(t *testing.T) {
	pki := newTestPKI(t)
	proxySecurityEnv(t, pki)
	s, err := loadProxySecurity()
	if err != nil {
		t.Fatal(err)
	}
	s.admission.timeout = 300 * time.Millisecond
	pending := make(chan error, 64)
	for i := 0; i < 64; i++ {
		server, client := net.Pipe()
		t.Cleanup(func() { client.Close() })
		go func(ip string) {
			defer server.Close()
			_, _, err := s.authenticate(context.Background(), &admissionConn{Conn: server, host: ip})
			pending <- err
		}(fmt.Sprintf("127.0.0.%d", i+2))
	}
	for deadline := time.Now().Add(time.Second); ; {
		s.admission.mu.Lock()
		count := len(s.admission.pending)
		s.admission.mu.Unlock()
		if count == 64 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("idle peers were not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	if len(s.handshakes) != 0 {
		t.Fatal("idle peers consumed cryptographic slots")
	}
	server, raw := net.Pipe()
	defer raw.Close()
	done := make(chan error, 1)
	finish := make(chan struct{})
	go func() {
		defer server.Close()
		_, _, err := s.authenticate(context.Background(), &admissionConn{Conn: server, host: "127.0.0.1"})
		done <- err
		<-finish
	}()
	defer close(finish)
	cert, _, _ := pki.certificate(t, "spiffe://test/a", false, false)
	secured := tls.Client(raw, &tls.Config{RootCAs: pki.roots, ServerName: "localhost", Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
	if err := secured.Handshake(); err != nil {
		t.Fatal("idle peers blocked a legitimate handshake", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		if err := <-pending; err == nil {
			t.Fatal("idle peer authenticated")
		}
	}
}

func TestHandshakeWaitsForAvailableSlot(t *testing.T) {
	pki := newTestPKI(t)
	proxySecurityEnv(t, pki)
	s, err := loadProxySecurity()
	if err != nil {
		t.Fatal(err)
	}
	s.handshakes = make(chan struct{}, 1)
	s.handshakes <- struct{}{}
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	done := make(chan error, 1)
	go func() { _, _, err := s.authenticate(context.Background(), server); done <- err }()
	if _, err := client.Write([]byte{22, 3, 3, 0, 4, 1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatal("connection did not wait for a slot", err)
	default:
	}
	<-s.handshakes
	go io.Copy(io.Discard, client)
	var failure *authenticationFailure
	if err := <-done; !errors.As(err, &failure) || failure.reason != "tls" {
		t.Fatal("queued connection never reached TLS", err)
	}
}

type helloBytes struct {
	net.Conn
	reader io.Reader
}

func (c *helloBytes) Read(data []byte) (int, error) { return c.reader.Read(data) }
func FuzzClientHelloAdmission(f *testing.F) {
	f.Add([]byte{22, 3, 3, 0, 1, 1})
	f.Add([]byte{22, 3, 3, 0, 4, 1, 0, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) { _, _ = clientHello(&helloBytes{reader: bytes.NewReader(data)}) })
}
