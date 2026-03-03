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

func buildHTTPServerTLSConfig() (*tls.Config, string, string, error) {
	enabled := envBoolDefault("KAFSCALE_LFS_PROXY_HTTP_TLS_ENABLED", false)
	if !enabled {
		return nil, "", "", nil
	}
	certFile := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_HTTP_TLS_CERT_FILE"))
	keyFile := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_HTTP_TLS_KEY_FILE"))
	clientCA := strings.TrimSpace(os.Getenv("KAFSCALE_LFS_PROXY_HTTP_TLS_CLIENT_CA_FILE"))
	requireClient := envBoolDefault("KAFSCALE_LFS_PROXY_HTTP_TLS_REQUIRE_CLIENT_CERT", false)

	if certFile == "" || keyFile == "" {
		return nil, "", "", errors.New("http TLS cert and key must be set when enabled")
	}

	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if clientCA != "" {
		caPEM, err := os.ReadFile(clientCA)
		if err != nil {
			return nil, "", "", err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, "", "", errors.New("failed to parse http TLS client CA file")
		}
		cfg.ClientCAs = pool
		if requireClient {
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			cfg.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}

	return cfg, certFile, keyFile, nil
}
