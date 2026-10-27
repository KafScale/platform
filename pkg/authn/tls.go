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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"os"
	"time"
)

// TLSOptions uses projected Secret files. Files are reloaded for every new
// connection; existing connections retain their authenticated principal.
// Tickets are disabled so resumption cannot bypass rotated trust or mappings.
type TLSOptions struct {
	CertFile       string
	KeyFile        string
	ClientCAFile   string
	PrincipalsFile string
}

func (o TLSOptions) Config() (*tls.Config, map[string]string, error) {
	cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
	if err != nil {
		return nil, nil, err
	}
	config := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, SessionTicketsDisabled: true}
	if o.ClientCAFile == "" && o.PrincipalsFile == "" {
		return config, nil, nil
	}
	if o.ClientCAFile == "" || o.PrincipalsFile == "" {
		return nil, nil, errors.New("mTLS requires client CA and principal mapping files")
	}
	ca, err := os.ReadFile(o.ClientCAFile)
	if err != nil {
		return nil, nil, err
	}
	config.ClientCAs = x509.NewCertPool()
	if !config.ClientCAs.AppendCertsFromPEM(ca) {
		return nil, nil, errors.New("client CA contains no certificates")
	}
	config.ClientAuth = tls.RequireAndVerifyClientCert
	data, err := os.ReadFile(o.PrincipalsFile)
	if err != nil {
		return nil, nil, err
	}
	var principals map[string]string
	if err := json.Unmarshal(data, &principals); err != nil {
		return nil, nil, err
	}
	if len(principals) == 0 {
		return nil, nil, errors.New("empty principal mapping")
	}
	for identity, principal := range principals {
		if identity == "" || !validPrincipal(principal) || (principal == ProbePrincipal || principal == LFSPrincipal) {
			return nil, nil, errors.New("invalid principal mapping")
		}
	}
	return config, principals, nil
}

// CertificatePrincipal requires exactly one mapped URI SAN identity. There is
// no implicit CN, subject DN, client.id, or network address fallback.
func CertificatePrincipal(cert *x509.Certificate, principals map[string]string) (string, error) {
	var principal string
	for _, uri := range cert.URIs {
		if mapped := principals[uri.String()]; mapped != "" {
			if principal != "" && principal != mapped {
				return "", errors.New("ambiguous certificate identity")
			}
			principal = mapped
		}
	}
	if principal == "" {
		return "", errors.New("unmapped certificate identity")
	}
	return principal, nil
}

func (o TLSOptions) Authenticate(ctx context.Context, conn net.Conn) (net.Conn, string, error) {
	config, principals, err := o.Config()
	if err != nil {
		return nil, "", err
	}
	var principal string
	if principals != nil {
		config.VerifyConnection = func(state tls.ConnectionState) error {
			var err error
			principal, err = CertificatePrincipal(state.PeerCertificates[0], principals)
			return err
		}
	}
	deadline := time.Now().Add(handshakeTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, "", err
	}
	secured := tls.Server(conn, config)
	handshakeCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()
	if err := secured.HandshakeContext(handshakeCtx); err != nil {
		return nil, "", err
	}
	if err := secured.SetDeadline(time.Time{}); err != nil {
		return nil, "", err
	}
	return secured, principal, nil
}
