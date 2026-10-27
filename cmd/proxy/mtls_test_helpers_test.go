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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"github.com/KafScale/platform/pkg/authn"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type TLSOptions = authn.TLSOptions

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
