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

import "testing"

func TestBuildHTTPServerTLSConfigDisabled(t *testing.T) {
	t.Setenv("KAFSCALE_LFS_PROXY_HTTP_TLS_ENABLED", "false")
	cfg, certFile, keyFile, err := buildHTTPServerTLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil || certFile != "" || keyFile != "" {
		t.Fatalf("expected empty TLS config when disabled")
	}
}

func TestBuildHTTPServerTLSConfigMissingCert(t *testing.T) {
	t.Setenv("KAFSCALE_LFS_PROXY_HTTP_TLS_ENABLED", "true")
	t.Setenv("KAFSCALE_LFS_PROXY_HTTP_TLS_CERT_FILE", "")
	t.Setenv("KAFSCALE_LFS_PROXY_HTTP_TLS_KEY_FILE", "")
	_, _, _, err := buildHTTPServerTLSConfig()
	if err == nil {
		t.Fatal("expected error when cert/key missing")
	}
}
