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
