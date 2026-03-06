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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"
)

func buildBackendTLSConfig() (*tls.Config, error) {
	enabled := envBoolDefault("KAFSCALE_LFS_PROXY_BACKEND_TLS_ENABLED", false)
	if !enabled {
		return nil, nil
	}
	caFile := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_BACKEND_TLS_CA_FILE"))
	certFile := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_BACKEND_TLS_CERT_FILE"))
	keyFile := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_BACKEND_TLS_KEY_FILE"))
	serverName := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_BACKEND_TLS_SERVER_NAME"))
	insecureSkip := envBoolDefault("KAFSCALE_LFS_PROXY_BACKEND_TLS_INSECURE_SKIP_VERIFY", false)

	var rootCAs *x509.CertPool
	if caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		rootCAs = x509.NewCertPool()
		if !rootCAs.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("failed to parse backend TLS CA file")
		}
	}

	var certs []tls.Certificate
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, errors.New("backend TLS cert and key must both be set")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		certs = append(certs, cert)
	}

	return &tls.Config{
		RootCAs:            rootCAs,
		Certificates:       certs,
		ServerName:         serverName,
		InsecureSkipVerify: insecureSkip,
		MinVersion:         tls.VersionTLS12,
	}, nil
}
