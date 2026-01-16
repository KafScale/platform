// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package console

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

func TestConsoleStatusEndpoint(t *testing.T) {
	mux, err := NewMux(ServerOptions{
		Auth: AuthConfig{
			Username: "demo",
			Password: "secret",
		},
	})
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	srv := newIPv4Server(t, mux)
	defer srv.Close()

	client := srv.Client()
	cookie := loginForTest(t, client, srv.URL, "demo", "secret")
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/ui/api/status", nil)
	req.AddCookie(cookie)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET status: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "\"cluster\"") {
		t.Fatalf("missing cluster field in response: %s", body)
	}
}

func TestStatusFromMetadataInjectsBrokerRuntime(t *testing.T) {
	meta := &metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "broker-1"},
		},
		Topics: []protocol.MetadataTopic{
			{Name: "orders"},
		},
	}
	snap := &MetricsSnapshot{
		BrokerRuntime: map[string]BrokerRuntime{
			"broker-1": {
				CPUPercent: 42.7,
				MemBytes:   512 * 1024 * 1024,
			},
		},
	}
	resp := statusFromMetadata(meta, snap)
	if len(resp.Brokers.Nodes) != 1 {
		t.Fatalf("expected 1 broker got %d", len(resp.Brokers.Nodes))
	}
	if resp.Brokers.Nodes[0].CPU != 43 {
		t.Fatalf("expected cpu 43 got %d", resp.Brokers.Nodes[0].CPU)
	}
	if resp.Brokers.Nodes[0].Memory != 512 {
		t.Fatalf("expected mem 512 got %d", resp.Brokers.Nodes[0].Memory)
	}
}

func TestMetricsStream(t *testing.T) {
	mux, err := NewMux(ServerOptions{
		Auth: AuthConfig{
			Username: "demo",
			Password: "secret",
		},
	})
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	srv := newIPv4Server(t, mux)
	defer srv.Close()

	client := srv.Client()
	cookie := loginForTest(t, client, srv.URL, "demo", "secret")
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/ui/api/metrics", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	req.AddCookie(cookie)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("metrics stream: %v", err)
	}
	defer resp.Body.Close()

	buf := make([]byte, 64)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	if !strings.Contains(string(buf), "data:") {
		t.Fatalf("expected sse data, got %s", buf)
	}
}

func newIPv4Server(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("skipping console HTTP test: %v", err)
		}
		t.Fatalf("listen: %v", err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = ln
	server.Start()
	return server
}

func TestConsoleAuthDisabled(t *testing.T) {
	mux, err := NewMux(ServerOptions{})
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	srv := newIPv4Server(t, mux)
	defer srv.Close()

	client := srv.Client()
	resp, err := client.Get(srv.URL + "/ui/api/auth/config")
	if err != nil {
		t.Fatalf("auth config: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("auth config status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "\"enabled\":false") {
		t.Fatalf("expected auth disabled response: %s", body)
	}

	sessionResp, err := client.Get(srv.URL + "/ui/api/auth/session")
	if err != nil {
		t.Fatalf("auth session: %v", err)
	}
	defer sessionResp.Body.Close()
	if sessionResp.StatusCode != http.StatusOK {
		t.Fatalf("auth session status: %d", sessionResp.StatusCode)
	}

	statusResp, err := client.Get(srv.URL + "/ui/api/status")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	defer statusResp.Body.Close()
	if statusResp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", statusResp.StatusCode)
	}

	loginResp, err := client.Post(srv.URL+"/ui/api/auth/login", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("auth login: %v", err)
	}
	defer loginResp.Body.Close()
	if loginResp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected login status 503, got %d", loginResp.StatusCode)
	}
}

func TestConsoleLoginFlow(t *testing.T) {
	mux, err := NewMux(ServerOptions{
		Auth: AuthConfig{
			Username: "demo",
			Password: "secret",
		},
	})
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	srv := newIPv4Server(t, mux)
	defer srv.Close()

	client := srv.Client()
	cookie := loginForTest(t, client, srv.URL, "demo", "secret")

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/ui/api/auth/session", nil)
	req.AddCookie(cookie)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("auth session: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("auth session status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "\"authenticated\":true") {
		t.Fatalf("expected authenticated session: %s", body)
	}
}

func loginForTest(t *testing.T, client *http.Client, baseURL, username, password string) *http.Cookie {
	t.Helper()
	payload := strings.NewReader(`{"username":"` + username + `","password":"` + password + `"}`)
	resp, err := client.Post(baseURL+"/ui/api/auth/login", "application/json", payload)
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("login status %d: %s", resp.StatusCode, body)
	}
	for _, cookie := range resp.Cookies() {
		if cookie.Name == sessionCookieName {
			return cookie
		}
	}
	t.Fatalf("missing session cookie")
	return nil
}
