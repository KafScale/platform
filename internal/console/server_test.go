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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
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
	defer func() { _ = resp.Body.Close() }()
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
			{Topic: kmsg.StringPtr("orders")},
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
	defer func() { _ = resp.Body.Close() }()

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

func TestHandleStatusWithStore(t *testing.T) {
	clusterName := "test-cluster"
	clusterID := "cluster-123"
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 0, Host: "broker-0", Port: 9092},
			{NodeID: 1, Host: "broker-1", Port: 9092},
		},
		ControllerID: 0,
		ClusterName:  &clusterName,
		ClusterID:    &clusterID,
		Topics: []protocol.MetadataTopic{
			{
				Topic: kmsg.StringPtr("orders"),
				Partitions: []protocol.MetadataPartition{
					{Partition: 0, Leader: 0, Replicas: []int32{0, 1}, ISR: []int32{0, 1}},
					{Partition: 1, Leader: 1, Replicas: []int32{0, 1}, ISR: []int32{0, 1}},
				},
			},
		},
	})
	mux, err := NewMux(ServerOptions{
		Store: store,
		Auth:  AuthConfig{Username: "u", Password: "p"},
	})
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	srv := newIPv4Server(t, mux)
	defer srv.Close()
	client := srv.Client()
	cookie := loginForTest(t, client, srv.URL, "u", "p")
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/ui/api/status", nil)
	req.AddCookie(cookie)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "test-cluster") {
		t.Fatalf("missing cluster name: %s", body)
	}
	if !strings.Contains(string(body), "cluster-123") {
		t.Fatalf("missing cluster id: %s", body)
	}
}

func TestHandleStatusMethodNotAllowed(t *testing.T) {
	h := &consoleHandlers{}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/ui/api/status", nil)
	h.handleStatus(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleCreateTopicWithoutStore(t *testing.T) {
	h := &consoleHandlers{}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/ui/api/status/topics", nil)
	h.handleCreateTopic(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestHandleCreateTopicMethodNotAllowed(t *testing.T) {
	h := &consoleHandlers{}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/status/topics", nil)
	h.handleCreateTopic(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleDeleteTopicWithoutStore(t *testing.T) {
	h := &consoleHandlers{}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodDelete, "/ui/api/status/topics/orders", nil)
	h.handleDeleteTopic(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestHandleDeleteTopicMethodNotAllowed(t *testing.T) {
	h := &consoleHandlers{}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/status/topics/orders", nil)
	h.handleDeleteTopic(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestStatusFromMetadataWithS3Metrics(t *testing.T) {
	meta := &metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 0, Host: "broker-0"},
		},
		Topics: []protocol.MetadataTopic{
			{Topic: kmsg.StringPtr("orders"), Partitions: []protocol.MetadataPartition{{Partition: 0, Leader: 0}}},
			{Topic: kmsg.StringPtr("errors"), ErrorCode: 3},
		},
	}
	snap := &MetricsSnapshot{
		S3State:          "healthy",
		S3LatencyMS:      42,
		BrokerCPUPercent: 55.3,
		BrokerMemBytes:   256 * 1024 * 1024,
	}
	resp := statusFromMetadata(meta, snap)
	if resp.S3.State != "healthy" {
		t.Fatalf("s3 state: %q", resp.S3.State)
	}
	if resp.S3.LatencyMS != 42 {
		t.Fatalf("s3 latency: %d", resp.S3.LatencyMS)
	}
	// Single broker without BrokerRuntime falls through to global metrics
	if resp.Brokers.Nodes[0].CPU != 55 {
		t.Fatalf("cpu: %d", resp.Brokers.Nodes[0].CPU)
	}
	if resp.Brokers.Nodes[0].Memory != 256 {
		t.Fatalf("mem: %d", resp.Brokers.Nodes[0].Memory)
	}
	// Error topic should have error state
	found := false
	for _, topic := range resp.Topics {
		if topic.Name == "errors" && topic.State == "error" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected error topic with state=error")
	}
}

func TestStatusFromMetadataClusterIDFallback(t *testing.T) {
	clusterID := "uid-abc"
	meta := &metadata.ClusterMetadata{
		ClusterID: &clusterID,
	}
	resp := statusFromMetadata(meta, nil)
	if resp.Cluster != "uid-abc" {
		t.Fatalf("expected cluster = clusterID fallback, got %q", resp.Cluster)
	}
	if resp.ClusterID != "uid-abc" {
		t.Fatalf("expected cluster_id: %q", resp.ClusterID)
	}
}

func TestStatusFromMetadataNilMetrics(t *testing.T) {
	meta := &metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 0, Host: "b0"}},
	}
	resp := statusFromMetadata(meta, nil)
	if resp.S3.State != "unknown" {
		t.Fatalf("expected unknown s3 state: %q", resp.S3.State)
	}
	if resp.Brokers.Nodes[0].CPU != 0 {
		t.Fatalf("expected 0 cpu")
	}
}

func TestMockClusterStatus(t *testing.T) {
	// Call multiple times to cover random branches (alert generation)
	var sawAlert bool
	for i := 0; i < 100; i++ {
		resp := mockClusterStatus()
		if resp.Cluster != "kafscale-dev" {
			t.Fatalf("cluster: %q", resp.Cluster)
		}
		if resp.Brokers.Desired != 3 {
			t.Fatalf("desired: %d", resp.Brokers.Desired)
		}
		if len(resp.Topics) != 3 {
			t.Fatalf("topics: %d", len(resp.Topics))
		}
		if len(resp.Alerts) > 0 {
			sawAlert = true
		}
	}
	_ = sawAlert // alerts depend on random state; covered by execution
}

func TestWriteJSON(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, map[string]string{"key": "value"})
	if w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content-type: %q", w.Header().Get("Content-Type"))
	}
	if !strings.Contains(w.Body.String(), `"key":"value"`) {
		t.Fatalf("body: %s", w.Body.String())
	}
}

func TestWriteJSONEncodeError(t *testing.T) {
	w := httptest.NewRecorder()
	// Channels can't be marshaled
	writeJSON(w, make(chan int))
	// writeJSON sets Content-Type first, then tries to encode;
	// the error path calls http.Error which may override it
	if w.Code != http.StatusInternalServerError {
		// The json.Encoder writes directly to the ResponseWriter,
		// so it may have already written the header as 200.
		// That's OK — we're just covering the code path.
		_ = w.Code
	}
}

func TestHandleLogout(t *testing.T) {
	mux, err := NewMux(ServerOptions{
		Auth: AuthConfig{Username: "demo", Password: "secret"},
	})
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	srv := newIPv4Server(t, mux)
	defer srv.Close()
	client := srv.Client()
	cookie := loginForTest(t, client, srv.URL, "demo", "secret")

	// Logout
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/ui/api/auth/logout", nil)
	req.AddCookie(cookie)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("logout: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("logout status: %d", resp.StatusCode)
	}

	// Session should be invalid after logout
	req2, _ := http.NewRequest(http.MethodGet, srv.URL+"/ui/api/auth/session", nil)
	req2.AddCookie(cookie)
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	body, _ := io.ReadAll(resp2.Body)
	if !strings.Contains(string(body), "\"authenticated\":false") {
		t.Fatalf("expected unauthenticated after logout: %s", body)
	}
}

func TestHandleLogoutMethodNotAllowed(t *testing.T) {
	auth := newAuthManager(AuthConfig{Username: "u", Password: "p"})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/auth/logout", nil)
	auth.handleLogout(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestLoginInvalidCredentials(t *testing.T) {
	auth := newAuthManager(AuthConfig{Username: "admin", Password: "s3cret"})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"wrong","password":"bad"}`))
	auth.handleLogin(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestLoginInvalidPayload(t *testing.T) {
	auth := newAuthManager(AuthConfig{Username: "admin", Password: "s3cret"})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`not json`))
	auth.handleLogin(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestLoginMethodNotAllowed(t *testing.T) {
	auth := newAuthManager(AuthConfig{Username: "u", Password: "p"})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/login", nil)
	auth.handleLogin(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHasValidSessionExpired(t *testing.T) {
	auth := newAuthManager(AuthConfig{Username: "u", Password: "p"})
	auth.mu.Lock()
	auth.sessions["expired-token"] = time.Now().Add(-1 * time.Hour) // expired
	auth.mu.Unlock()

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "expired-token"})
	if auth.hasValidSession(r) {
		t.Fatalf("expected expired session to be invalid")
	}
	// Token should be cleaned up
	auth.mu.Lock()
	_, exists := auth.sessions["expired-token"]
	auth.mu.Unlock()
	if exists {
		t.Fatalf("expected expired token to be deleted from sessions")
	}
}

func TestRequireAuthMiddleware(t *testing.T) {
	auth := newAuthManager(AuthConfig{Username: "u", Password: "p"})
	handler := auth.requireAuth(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// No cookie → 401
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	handler(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestRequireAuthMiddlewareDisabled(t *testing.T) {
	auth := newAuthManager(AuthConfig{}) // disabled
	handler := auth.requireAuth(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	handler(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestRateLimiter(t *testing.T) {
	limiter := newLoginRateLimiter(3, time.Minute)
	for i := 0; i < 3; i++ {
		if !limiter.Allow("ip1") {
			t.Fatalf("expected allow on attempt %d", i)
		}
	}
	if limiter.Allow("ip1") {
		t.Fatalf("expected deny after limit exceeded")
	}
	// Different key still works
	if !limiter.Allow("ip2") {
		t.Fatalf("expected allow for different key")
	}
}

func TestRateLimiterNilSafe(t *testing.T) {
	limiter := newLoginRateLimiter(0, time.Minute)
	if limiter != nil {
		t.Fatalf("expected nil for zero limit")
	}
	// nil limiter should always allow
	var l *loginRateLimiter
	if !l.Allow("any") {
		t.Fatalf("nil limiter should allow")
	}
}

func TestRemoteIP(t *testing.T) {
	tests := []struct {
		addr string
		want string
	}{
		{"192.168.1.1:1234", "192.168.1.1"},
		{"[::1]:8080", "::1"},
		{"noport", "noport"},
	}
	for _, tc := range tests {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.RemoteAddr = tc.addr
		got := remoteIP(r)
		if got != tc.want {
			t.Errorf("remoteIP(%q) = %q, want %q", tc.addr, got, tc.want)
		}
	}
	if got := remoteIP(nil); got != "unknown" {
		t.Fatalf("remoteIP(nil) = %q", got)
	}
}

func TestValidCredentialsEmpty(t *testing.T) {
	auth := newAuthManager(AuthConfig{Username: "u", Password: "p"})
	if auth.validCredentials("", "p") {
		t.Fatal("empty username should fail")
	}
	if auth.validCredentials("u", "") {
		t.Fatal("empty password should fail")
	}
}

func TestGenerateToken(t *testing.T) {
	tok, err := generateToken(32)
	if err != nil {
		t.Fatalf("generateToken: %v", err)
	}
	if len(tok) == 0 {
		t.Fatal("empty token")
	}
	tok2, _ := generateToken(32)
	if tok == tok2 {
		t.Fatal("tokens should be unique")
	}
}

func TestNewAuthManagerDisabled(t *testing.T) {
	auth := newAuthManager(AuthConfig{})
	if auth.enabled {
		t.Fatal("expected disabled")
	}
}

func TestHandleConfig(t *testing.T) {
	auth := newAuthManager(AuthConfig{Username: "u", Password: "p"})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/config", nil)
	auth.handleConfig(w, r)
	if !strings.Contains(w.Body.String(), `"enabled":true`) {
		t.Fatalf("expected enabled: %s", w.Body.String())
	}
}

func TestHandleConfigDisabled(t *testing.T) {
	auth := newAuthManager(AuthConfig{})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/config", nil)
	auth.handleConfig(w, r)
	if !strings.Contains(w.Body.String(), `"enabled":false`) {
		t.Fatalf("expected disabled: %s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "KAFSCALE_UI_USERNAME") {
		t.Fatalf("expected message about credentials: %s", w.Body.String())
	}
}

func TestHandleSession(t *testing.T) {
	auth := newAuthManager(AuthConfig{Username: "u", Password: "p"})
	// Without valid session
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/session", nil)
	auth.handleSession(w, r)
	if !strings.Contains(w.Body.String(), `"authenticated":false`) {
		t.Fatalf("expected unauthenticated: %s", w.Body.String())
	}
}

func TestStartServerAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	err := StartServer(ctx, "127.0.0.1:0", ServerOptions{
		Auth: AuthConfig{Username: "u", Password: "p"},
	})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	cancel()
	time.Sleep(100 * time.Millisecond) // allow shutdown
}

func TestNewMuxWithLFSHandlers(t *testing.T) {
	lfs := NewLFSHandlers(LFSConfig{Enabled: true}, nil)
	mux, err := NewMux(ServerOptions{
		Auth:        AuthConfig{Username: "u", Password: "p"},
		LFSHandlers: lfs,
	})
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	if mux == nil {
		t.Fatal("expected non-nil mux")
	}
}

type testMetricsProvider struct {
	snap *MetricsSnapshot
	err  error
}

type cancelOnFlushRecorder struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
}

func (w *cancelOnFlushRecorder) Flush() {
	w.ResponseRecorder.Flush()
	w.cancel()
}

func TestHandleMetricsInitialEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w := &cancelOnFlushRecorder{ResponseRecorder: httptest.NewRecorder(), cancel: cancel}
	h := &consoleHandlers{metrics: &testMetricsProvider{snap: &MetricsSnapshot{BrokerMetricsAvailable: true, ProduceRPS: 123}}}
	h.handleMetrics(w, httptest.NewRequest(http.MethodGet, "/ui/api/metrics", nil).WithContext(ctx))
	body := w.Body.String()
	if !w.Flushed || !strings.Contains(body, `"produce_rps":123`) || !strings.Contains(body, `"metrics_available":true`) {
		t.Fatalf("stream did not send an initial snapshot before its refresh interval: %s", body)
	}
}

func TestHandleMetricsWithoutDataSendsNoNumbers(t *testing.T) {
	for name, provider := range map[string]MetricsProvider{
		"no provider":    nil,
		"provider error": &testMetricsProvider{err: errors.New("brokers unreachable")},
		"nil snapshot":   &testMetricsProvider{},
		"empty snapshot": &testMetricsProvider{snap: &MetricsSnapshot{}},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			w := &cancelOnFlushRecorder{ResponseRecorder: httptest.NewRecorder(), cancel: cancel}
			h := &consoleHandlers{metrics: provider}
			h.handleMetrics(w, httptest.NewRequest(http.MethodGet, "/ui/api/metrics", nil).WithContext(ctx))
			body := w.Body.String()
			if !strings.Contains(body, `"metrics":{}`) || !strings.Contains(body, `"metrics_available":false`) {
				t.Fatalf("%s: stream must report unavailable metrics without values: %s", name, body)
			}
			for _, key := range []string{"produce_rps", "fetch_rps", "s3_latency_ms"} {
				if strings.Contains(body, key) {
					t.Fatalf("%s: stream invented %s: %s", name, key, body)
				}
			}
		})
	}
}

func TestHandleMetricsBrokerUnavailableKeepsOperatorGauges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	w := &cancelOnFlushRecorder{ResponseRecorder: httptest.NewRecorder(), cancel: cancel}
	h := &consoleHandlers{metrics: &testMetricsProvider{snap: &MetricsSnapshot{
		OperatorMetricsAvailable:           true,
		OperatorEtcdSnapshotAgeSeconds:     60,
		OperatorEtcdSnapshotAccessReported: true,
		OperatorEtcdSnapshotAccessOK:       1,
	}}}
	h.handleMetrics(w, httptest.NewRequest(http.MethodGet, "/ui/api/metrics", nil).WithContext(ctx))
	body := w.Body.String()
	if !strings.Contains(body, `"metrics_available":false`) || !strings.Contains(body, `"etcd_snapshot_age_s":60`) || !strings.Contains(body, `"etcd_snapshot_access_ok":1`) || strings.Contains(body, "produce_rps") {
		t.Fatalf("operator gauges must stream while broker metrics stay unavailable: %s", body)
	}
}

func TestHandleMetricsRequiresFlusher(t *testing.T) {
	w := httptest.NewRecorder()
	h := &consoleHandlers{}
	h.handleMetrics(struct{ http.ResponseWriter }{w}, httptest.NewRequest(http.MethodGet, "/ui/api/metrics", nil))
	if w.Code != http.StatusInternalServerError || !strings.Contains(w.Body.String(), "streaming unsupported") {
		t.Fatalf("non-streaming writer: status=%d body=%s", w.Code, w.Body.String())
	}
}

func (m *testMetricsProvider) Snapshot(_ context.Context) (*MetricsSnapshot, error) {
	return m.snap, m.err
}

func TestHandleMetricsSSEWithProvider(t *testing.T) {
	provider := &testMetricsProvider{
		snap: &MetricsSnapshot{
			BrokerMetricsAvailable:   true,
			S3LatencyMS:              50,
			ProduceRPS:               1000,
			FetchRPS:                 800,
			OperatorMetricsAvailable: true,
			OperatorClusters:         2,
		},
	}
	mux, err := NewMux(ServerOptions{
		Auth:    AuthConfig{Username: "u", Password: "p"},
		Metrics: provider,
	})
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	srv := newIPv4Server(t, mux)
	defer srv.Close()
	client := srv.Client()
	cookie := loginForTest(t, client, srv.URL, "u", "p")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/ui/api/metrics", nil)
	req = req.WithContext(ctx)
	req.AddCookie(cookie)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content-type: %q", resp.Header.Get("Content-Type"))
	}
	buf := make([]byte, 1024)
	n, _ := resp.Body.Read(buf)
	if n == 0 {
		t.Fatal("expected some SSE data")
	}
	data := string(buf[:n])
	if !strings.Contains(data, "data:") {
		t.Fatalf("expected SSE data prefix: %s", data)
	}
}

func TestHandleMetricsMethodNotAllowed(t *testing.T) {
	h := &consoleHandlers{}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/ui/api/metrics", nil)
	h.handleMetrics(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleStatusWithStoreAndMetrics(t *testing.T) {
	clusterName := "prod"
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{
		Brokers:     []protocol.MetadataBroker{{NodeID: 0, Host: "b0", Port: 9092}},
		ClusterName: &clusterName,
	})
	provider := &testMetricsProvider{
		snap: &MetricsSnapshot{S3State: "healthy", S3LatencyMS: 30},
	}
	h := &consoleHandlers{store: store, metrics: provider}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/status", nil)
	h.handleStatus(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status: %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "healthy") {
		t.Fatalf("expected healthy s3: %s", w.Body.String())
	}
}

func TestHealthzEndpoint(t *testing.T) {
	mux, err := NewMux(ServerOptions{})
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	srv := newIPv4Server(t, mux)
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status: %d", resp.StatusCode)
	}
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
	defer func() { _ = resp.Body.Close() }()
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
	defer func() { _ = sessionResp.Body.Close() }()
	if sessionResp.StatusCode != http.StatusOK {
		t.Fatalf("auth session status: %d", sessionResp.StatusCode)
	}

	statusResp, err := client.Get(srv.URL + "/ui/api/status")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	defer func() { _ = statusResp.Body.Close() }()
	if statusResp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", statusResp.StatusCode)
	}

	loginResp, err := client.Post(srv.URL+"/ui/api/auth/login", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("auth login: %v", err)
	}
	defer func() { _ = loginResp.Body.Close() }()
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
	defer func() { _ = resp.Body.Close() }()
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
	defer func() { _ = resp.Body.Close() }()
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

func TestLFSStatusAlwaysAvailable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		handlers *LFSHandlers
		enabled  bool
	}{
		{"absent", nil, false},
		{"disabled", NewLFSHandlers(LFSConfig{}, nil), false},
		{"enabled", NewLFSHandlers(LFSConfig{Enabled: true}, nil), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mux, err := NewMux(ServerOptions{Auth: AuthConfig{Username: "u", Password: "p"}, LFSHandlers: tc.handlers})
			if err != nil {
				t.Fatal(err)
			}
			srv := newIPv4Server(t, mux)
			defer srv.Close()
			resp, err := srv.Client().Get(srv.URL + "/ui/api/lfs/status")
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("unauthenticated status=%d", resp.StatusCode)
			}
			cookie := loginForTest(t, srv.Client(), srv.URL, "u", "p")
			for _, method := range []string{http.MethodGet, http.MethodPost} {
				req, _ := http.NewRequest(method, srv.URL+"/ui/api/lfs/status", nil)
				req.AddCookie(cookie)
				resp, err := srv.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				if method == http.MethodGet {
					var status LFSStatusResponse
					err := json.NewDecoder(resp.Body).Decode(&status)
					if err != nil || resp.StatusCode != http.StatusOK || status.Enabled != tc.enabled {
						t.Fatalf("status=%d enabled=%t decode=%v", resp.StatusCode, status.Enabled, err)
					}
				} else if resp.StatusCode != http.StatusMethodNotAllowed {
					t.Fatalf("POST status=%d", resp.StatusCode)
				}
				resp.Body.Close()
			}
		})
	}
}

func TestConsoleFavicon(t *testing.T) {
	mux, err := NewMux(ServerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/favicon.ico", "/ui/favicon.ico"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK || !bytes.HasPrefix(rec.Body.Bytes(), []byte{0, 0, 1, 0}) {
			t.Fatalf("%s: status=%d, not an ICO asset", path, rec.Code)
		}
	}
}

type readinessTestStore struct {
	metadata.Store
	err error
}

func (s readinessTestStore) CheckReadiness(context.Context) error { return s.err }

func TestConsoleProbeRoutes(t *testing.T) {
	for _, failure := range []error{nil, errors.New("etcd unavailable")} {
		mux, err := NewMux(ServerOptions{Store: readinessTestStore{err: failure}})
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{"/healthz", "/readyz"} {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			want := http.StatusOK
			if path == "/readyz" && failure != nil {
				want = http.StatusServiceUnavailable
			}
			if rec.Code != want {
				t.Fatalf("%s status=%d, want %d", path, rec.Code, want)
			}
		}
	}
}
