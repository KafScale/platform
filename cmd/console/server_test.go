package main

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
	mux, err := newConsoleMux()
	if err != nil {
		t.Fatalf("newConsoleMux: %v", err)
	}
	srv := newIPv4Server(t, mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/ui/api/status")
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
	mux, err := newConsoleMux()
	if err != nil {
		t.Fatalf("newConsoleMux: %v", err)
	}
	srv := newIPv4Server(t, mux)
	defer srv.Close()

	client := srv.Client()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/ui/api/metrics", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
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
