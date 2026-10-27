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

package authn

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testPKI struct {
	options TLSOptions
	ca      *x509.Certificate
	key     ed25519.PrivateKey
	roots   *x509.CertPool
	serial  int64
}

func newTestPKI(t testing.TB) *testPKI {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, public, private)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	dir := t.TempDir()
	p := &testPKI{ca: ca, key: private, roots: roots, serial: 1, options: TLSOptions{CertFile: filepath.Join(dir, "cert.pem"), KeyFile: filepath.Join(dir, "key.pem"), ClientCAFile: filepath.Join(dir, "ca.pem"), PrincipalsFile: filepath.Join(dir, "principals.json")}}
	writeTestFile(t, p.options.ClientCAFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	writeTestFile(t, p.options.PrincipalsFile, []byte(`{"spiffe://test/a":"service-a","spiffe://test/b":"service-b"}`))
	p.rotateServer(t)
	return p
}
func writeTestFile(t testing.TB, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}
func (p *testPKI) certificate(t testing.TB, identity string, expired bool, server bool) (tls.Certificate, []byte, []byte) {
	t.Helper()
	p.serial++
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(p.serial), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	if expired {
		cert.NotAfter = time.Now().Add(-time.Minute)
	}
	if server {
		cert.DNSNames = []string{"localhost"}
		cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	} else {
		uri, _ := url.Parse(identity)
		cert.URIs = []*url.URL{uri}
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, p.ca, public, p.key)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return pair, certPEM, keyPEM
}
func (p *testPKI) rotateServer(t testing.TB) {
	_, cert, key := p.certificate(t, "", false, true)
	writeTestFile(t, p.options.CertFile, cert)
	writeTestFile(t, p.options.KeyFile, key)
}
func (p *testPKI) authenticate(t testing.TB, certificate *tls.Certificate, options TLSOptions, wrap func(net.Conn) net.Conn) (string, error) {
	t.Helper()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	type result struct {
		principal string
		err       error
	}
	done := make(chan result, 1)
	go func() {
		conn := net.Conn(server)
		if wrap != nil {
			conn = wrap(conn)
		}
		_, principal, err := options.Authenticate(context.Background(), conn)
		done <- result{principal, err}
		server.Close()
	}()
	cfg := &tls.Config{RootCAs: p.roots, ServerName: "localhost", MinVersion: tls.VersionTLS13}
	if certificate != nil {
		cfg.Certificates = []tls.Certificate{*certificate}
	}
	secured := tls.Client(client, cfg)
	_ = secured.HandshakeContext(context.Background())
	r := <-done
	return r.principal, r.err
}
func TestTLSAuthenticationAndRotation(t *testing.T) {
	p := newTestPKI(t)
	a, _, _ := p.certificate(t, "spiffe://test/a", false, false)
	principal, err := p.authenticate(t, &a, p.options, nil)
	if err != nil || principal != "service-a" {
		t.Fatalf("%q %v", principal, err)
	}
	p.rotateServer(t)
	principal, err = p.authenticate(t, &a, p.options, nil)
	if err != nil || principal != "service-a" {
		t.Fatalf("rotation: %q %v", principal, err)
	}
	writeTestFile(t, p.options.PrincipalsFile, []byte(`{"spiffe://test/a":"rotated"}`))
	principal, err = p.authenticate(t, &a, p.options, nil)
	if err != nil || principal != "rotated" {
		t.Fatalf("mapping rotation: %q %v", principal, err)
	}
	writeTestFile(t, p.options.PrincipalsFile, []byte(`{"spiffe://test/b":"service-b"}`))
	if _, err = p.authenticate(t, &a, p.options, nil); err == nil {
		t.Fatal("removed mapping accepted")
	}
	for _, tc := range []struct {
		name, uri string
		expired   bool
	}{{"unmapped", "spiffe://test/unknown", false}, {"expired", "spiffe://test/a", true}} {
		t.Run(tc.name, func(t *testing.T) {
			cert, _, _ := p.certificate(t, tc.uri, tc.expired, false)
			if _, err := p.authenticate(t, &cert, p.options, nil); err == nil {
				t.Fatal("accepted")
			}
		})
	}
	if _, err = p.authenticate(t, nil, p.options, nil); err == nil {
		t.Fatal("missing certificate accepted")
	}
	other := newTestPKI(t)
	bad, _, _ := other.certificate(t, "spiffe://test/b", false, false)
	if _, err = p.authenticate(t, &bad, p.options, nil); err == nil {
		t.Fatal("untrusted certificate accepted")
	}
	writeTestFile(t, p.options.ClientCAFile, []byte("invalid"))
	if _, err = p.authenticate(t, &a, p.options, nil); err == nil {
		t.Fatal("invalid trust rotation accepted")
	}
}
func TestTLSOptionsErrors(t *testing.T) {
	p := newTestPKI(t)
	tests := []struct {
		name   string
		change func(*TLSOptions)
	}{
		{"missing cert", func(o *TLSOptions) { o.CertFile = "/missing" }},
		{"missing key", func(o *TLSOptions) { o.KeyFile = "/missing" }},
		{"missing CA", func(o *TLSOptions) { o.ClientCAFile = "/missing" }},
		{"CA without mapping", func(o *TLSOptions) { o.PrincipalsFile = "" }},
		{"mapping without CA", func(o *TLSOptions) { o.ClientCAFile = "" }},
		{"missing map", func(o *TLSOptions) { o.PrincipalsFile = "/missing" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := p.options
			tc.change(&o)
			if _, _, err := o.Config(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	for _, data := range []string{"{", "{}", "null", `{"":"a"}`, `{"a":""}`, `{"a":"two words"}`, `{"a":"__kafscale_proxy_probe"}`} {
		writeTestFile(t, p.options.PrincipalsFile, []byte(data))
		if _, _, err := p.options.Config(); err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
	o := p.options
	o.ClientCAFile = ""
	o.PrincipalsFile = ""
	cfg, m, err := o.Config()
	if err != nil || m != nil || !cfg.SessionTicketsDisabled {
		t.Fatalf("TLS config: %v", err)
	}
	if principal, err := p.authenticate(t, nil, o, nil); err != nil || principal != "" {
		t.Fatalf("TLS only: %q %v", principal, err)
	}
}
func TestCertificatePrincipal(t *testing.T) {
	a, _ := url.Parse("spiffe://test/a")
	b, _ := url.Parse("spiffe://test/b")
	c, _ := url.Parse("spiffe://test/c")
	cert := &x509.Certificate{URIs: []*url.URL{a, b, c}}
	if _, err := CertificatePrincipal(cert, map[string]string{a.String(): "a", b.String(): "b"}); err == nil {
		t.Fatal("ambiguous identity accepted")
	}
	if got, err := CertificatePrincipal(cert, map[string]string{a.String(): "same", b.String(): "same"}); err != nil || got != "same" {
		t.Fatal(got, err)
	}
	if _, err := CertificatePrincipal(&x509.Certificate{}, nil); err == nil {
		t.Fatal("empty identity accepted")
	}
}

type deadlineConn struct {
	net.Conn
	calls, failAt int
}

func (c *deadlineConn) SetDeadline(at time.Time) error {
	c.calls++
	if c.calls == c.failAt {
		return errors.New("deadline failed")
	}
	return c.Conn.SetDeadline(at)
}
func TestTLSDeadlineFailures(t *testing.T) {
	p := newTestPKI(t)
	cert, _, _ := p.certificate(t, "spiffe://test/a", false, false)
	for _, n := range []int{1, 2} {
		_, err := p.authenticate(t, &cert, p.options, func(c net.Conn) net.Conn { return &deadlineConn{Conn: c, failAt: n} })
		if err == nil {
			t.Fatal("deadline failure ignored")
		}
	}
}

func BenchmarkTLSHandshake(b *testing.B) {
	for _, mtls := range []bool{false, true} {
		name := "tls"
		if mtls {
			name = "mtls"
		}
		b.Run(name, func(b *testing.B) {
			p := newTestPKI(b)
			cert, _, _ := p.certificate(b, "spiffe://test/a", false, false)
			options := p.options
			if !mtls {
				options.ClientCAFile = ""
				options.PrincipalsFile = ""
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := p.authenticate(b, &cert, options, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMTLSReconnectStorm(b *testing.B) {
	p := newTestPKI(b)
	certificate, _, _ := p.certificate(b, "spiffe://test/a", false, false)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := p.authenticate(b, &certificate, p.options, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMTLSSteadyRecords(b *testing.B) {
	p := newTestPKI(b)
	certificate, _, _ := p.certificate(b, "spiffe://test/a", false, false)
	server, raw := net.Pipe()
	done := make(chan error, 1)
	payload := make([]byte, 64<<10)
	go func() {
		defer server.Close()
		secured, _, err := p.options.Authenticate(context.Background(), server)
		if err != nil {
			done <- err
			return
		}
		buffer := make([]byte, len(payload))
		for {
			if _, err := io.ReadFull(secured, buffer); err != nil {
				done <- nil
				return
			}
			if err := writeBytes(secured, buffer); err != nil {
				done <- err
				return
			}
		}
	}()
	client := tls.Client(raw, &tls.Config{RootCAs: p.roots, ServerName: "localhost", MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}})
	if err := client.Handshake(); err != nil {
		b.Fatal(err)
	}
	response := make([]byte, len(payload))
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := writeBytes(client, payload); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(client, response); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	raw.Close()
	if err := <-done; err != nil {
		b.Fatal(err)
	}
}
