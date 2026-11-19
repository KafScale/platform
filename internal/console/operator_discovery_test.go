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
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOperatorDiscoveryScrapesLeaderAfterStandbyConnection(t *testing.T) {
	var standbyCalls, leaderCalls atomic.Int32
	standby := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		standbyCalls.Add(1)
		if r.Host != "operators.test:8080" {
			t.Errorf("Host = %s", r.Host)
		}
		_, _ = io.WriteString(w, standbyWithThresholdMetrics)
	}))
	defer standby.Close()
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaderCalls.Add(1)
		if r.Host != "operators.test:8080" {
			t.Errorf("Host = %s", r.Host)
		}
		_, _ = io.WriteString(w, leaderWithThresholdMetrics)
	}))
	defer leader.Close()
	provider := NewDiscoveredCompositeMetricsProvider(nil, "http://operators.test:8080/metrics").(*compositeMetricsProvider)
	provider.resolveOperator = func(context.Context, string) ([]string, error) {
		return []string{"127.0.0.1", "127.0.0.2", "127.0.0.1"}, nil
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		server := standby
		if address == "127.0.0.2:8080" {
			server = leader
		}
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}}
	defer transport.CloseIdleConnections()
	provider.client.Transport = transport
	// Pin the first persistent connection to the standby, as in the QA report.
	if _, err := fetchOperatorSnapshotHost(context.Background(), provider.client, "http://127.0.0.1:8080/metrics", "operators.test:8080"); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		snap, err := provider.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !snap.OperatorMetricsAvailable || snap.OperatorEtcdSnapshotAgeSeconds != 120 || snap.OperatorEtcdSnapshotAccessOK != 1 {
			t.Fatalf("leader snapshot = %+v", snap)
		}
	}
	if standbyCalls.Load() != 4 || leaderCalls.Load() != 3 {
		t.Fatalf("scrapes: standby=%d leader=%d", standbyCalls.Load(), leaderCalls.Load())
	}
}

func TestOperatorDiscoveryRefreshesAddressesAndKeepsCache(t *testing.T) {
	provider := NewDiscoveredCompositeMetricsProvider(nil, "http://operators.test/metrics").(*compositeMetricsProvider)
	addresses := []string{"192.0.2.1", "192.0.2.2"}
	provider.resolveOperator = func(context.Context, string) ([]string, error) { return addresses, nil }
	clock := time.Unix(1700001100, 0)
	provider.now = func() time.Time { return clock }
	bodies := map[string]string{"192.0.2.1:80": standbyWithThresholdMetrics, "192.0.2.2:80": leaderWithThresholdMetrics}
	provider.client.Transport = metricsRoundTripper(func(r *http.Request) (*http.Response, error) {
		body, ok := bodies[r.URL.Host]
		if !ok {
			t.Errorf("unexpected endpoint %s", r.URL.Host)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})
	if snap, _ := provider.Snapshot(context.Background()); !snap.OperatorMetricsAvailable {
		t.Fatal("leader unavailable")
	}
	// The leader is replaced. A standby-only scrape uses, but does not renew, the cache.
	addresses = []string{"192.0.2.1"}
	clock = clock.Add(time.Minute)
	if snap, _ := provider.Snapshot(context.Background()); !snap.OperatorMetricsAvailable || snap.OperatorEtcdSnapshotAgeSeconds != 180 {
		t.Fatalf("cached = %+v", snap)
	}
	clock = clock.Add(3 * time.Minute)
	if snap, _ := provider.Snapshot(context.Background()); snap.OperatorMetricsAvailable {
		t.Fatal("cache must expire")
	}
	addresses = []string{"192.0.2.3", "192.0.2.1"}
	bodies["192.0.2.3:80"] = strings.ReplaceAll(leaderWithThresholdMetrics, "1700001000", "1700002000")
	if snap, _ := provider.Snapshot(context.Background()); !snap.OperatorMetricsAvailable || snap.OperatorEtcdSnapshotLastScheduleTS != 1700002000 {
		t.Fatalf("new leader = %+v", snap)
	}
}

func TestOperatorDiscoveryFailuresAndURLHandling(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint string
		addresses      []string
		dnsErr         error
		status         int
		body           string
		transportErr   bool
		wantHost       string
		wantAvailable  bool
	}{
		{name: "DNS error", endpoint: "http://operators.test/metrics", dnsErr: errors.New("DNS unavailable")},
		{name: "no endpoints", endpoint: "http://operators.test/metrics"},
		{name: "invalid URL", endpoint: "http://%xx"},
		{name: "missing host", endpoint: "http:/metrics"},
		{name: "invalid scheme", endpoint: "ftp://operators.test/metrics"},
		{name: "HTTP failure", endpoint: "http://operators.test/metrics", addresses: []string{"192.0.2.1"}, status: 503},
		{name: "transport failure", endpoint: "http://operators.test/metrics", addresses: []string{"192.0.2.1"}, transportErr: true},
		{name: "standby only", endpoint: "http://operators.test/metrics", addresses: []string{"192.0.2.1"}, status: 200, body: standbyWithThresholdMetrics},
		{name: "IPv6", endpoint: "http://operators.test/metrics?format=prom", addresses: []string{"2001:db8::1"}, status: 200, body: leaderWithThresholdMetrics, wantHost: "[2001:db8::1]:80", wantAvailable: true},
		{name: "HTTPS default port", endpoint: "https://operators.test/metrics", addresses: []string{"192.0.2.1"}, status: 200, body: leaderWithThresholdMetrics, wantHost: "192.0.2.1:443", wantAvailable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := NewDiscoveredCompositeMetricsProvider(nil, tc.endpoint).(*compositeMetricsProvider)
			provider.resolveOperator = func(context.Context, string) ([]string, error) { return tc.addresses, tc.dnsErr }
			provider.client.Transport = metricsRoundTripper(func(r *http.Request) (*http.Response, error) {
				if tc.wantHost != "" && r.URL.Host != tc.wantHost {
					t.Errorf("endpoint host=%s, want %s", r.URL.Host, tc.wantHost)
				}
				if r.URL.RawQuery != "" && r.URL.RawQuery != "format=prom" {
					t.Errorf("query=%s", r.URL.RawQuery)
				}
				if tc.transportErr {
					return nil, errors.New("pod gone")
				}
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body)), Header: make(http.Header)}, nil
			})
			snap, err := provider.Snapshot(context.Background())
			if err != nil || snap.OperatorMetricsAvailable != tc.wantAvailable {
				t.Fatalf("snapshot=%+v err=%v", snap, err)
			}
		})
	}
}

func TestOperatorDiscoveryConcurrentScrapesRespectDeadline(t *testing.T) {
	provider := NewDiscoveredCompositeMetricsProvider(nil, "http://operators.test/metrics").(*compositeMetricsProvider)
	provider.client.Timeout = 100 * time.Millisecond
	provider.resolveOperator = func(context.Context, string) ([]string, error) { return []string{"192.0.2.1", "192.0.2.2"}, nil }
	// Neither request can finish until both have started. Serial scraping fails.
	bothStarted := make(chan struct{})
	var started atomic.Int32
	provider.client.Transport = metricsRoundTripper(func(r *http.Request) (*http.Response, error) {
		if started.Add(1) == 2 {
			close(bothStarted)
		}
		select {
		case <-bothStarted:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		if r.URL.Hostname() == "192.0.2.1" {
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(leaderWithThresholdMetrics)), Header: make(http.Header)}, nil
	})
	snap, err := provider.Snapshot(context.Background())
	if err != nil || !snap.OperatorMetricsAvailable || started.Load() != 2 {
		t.Fatalf("snapshot=%+v err=%v started=%d", snap, err, started.Load())
	}
}

func TestOperatorDiscoveryVerifiesServiceTLSHostname(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS.ServerName != "example.com" {
			t.Errorf("SNI=%s", r.TLS.ServerName)
		}
		_, _ = io.WriteString(w, leaderWithThresholdMetrics)
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL)
	provider := NewDiscoveredCompositeMetricsProvider(nil, "https://example.com:"+endpoint.Port()+"/metrics").(*compositeMetricsProvider)
	provider.resolveOperator = func(context.Context, string) ([]string, error) { return []string{"127.0.0.1"}, nil }
	transport := provider.client.Transport.(*http.Transport)
	defer transport.CloseIdleConnections()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	transport.TLSClientConfig.RootCAs = roots
	transport.Proxy = nil
	snap, err := provider.Snapshot(context.Background())
	if err != nil || !snap.OperatorMetricsAvailable {
		t.Fatalf("snapshot=%+v err=%v", snap, err)
	}
	transport.CloseIdleConnections()
	transport.TLSClientConfig.ServerName = "wrong.example"
	if snap, _ := provider.Snapshot(context.Background()); snap.OperatorMetricsAvailable {
		t.Fatal("invalid certificate must fail closed")
	}
}

func TestOperatorDiscoveryInitializesTLSWithoutMutatingDefault(t *testing.T) {
	previous := http.DefaultTransport
	base := &http.Transport{Protocols: new(http.Protocols)}
	base.Protocols.SetHTTP1(true)
	http.DefaultTransport = base
	defer func() { http.DefaultTransport = previous }()
	provider := NewDiscoveredCompositeMetricsProvider(nil, "https://operators.test/metrics").(*compositeMetricsProvider)
	transport := provider.client.Transport.(*http.Transport)
	defer transport.CloseIdleConnections()
	if transport == base || transport.TLSClientConfig.ServerName != "operators.test" || transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("discovery must clone the transport and verify the Service hostname")
	}
	if base.TLSClientConfig != nil {
		t.Fatal("discovery changed the default transport TLS configuration")
	}
}

func TestOperatorSampleSelection(t *testing.T) {
	reported := operatorSnapshot{EtcdSnapshotReported: true, EtcdSnapshotLastScheduleTS: 10, EtcdSnapshotLastSuccessTS: 5, EtcdSnapshotAccessReported: true, EtcdSnapshotAccessOK: 1}
	cases := []struct {
		name   string
		change func(*operatorSnapshot)
		want   bool
	}{
		{"reported leader", func(s *operatorSnapshot) { s.EtcdSnapshotReported = false }, false},
		{"new schedule", func(s *operatorSnapshot) { s.EtcdSnapshotLastScheduleTS++ }, true},
		{"old schedule", func(s *operatorSnapshot) { s.EtcdSnapshotLastScheduleTS-- }, false},
		{"new success", func(s *operatorSnapshot) { s.EtcdSnapshotLastSuccessTS++ }, true},
		{"old success", func(s *operatorSnapshot) { s.EtcdSnapshotLastSuccessTS-- }, false},
		{"stale", func(s *operatorSnapshot) { s.EtcdSnapshotStale = 1 }, true},
		{"failed access", func(s *operatorSnapshot) { s.EtcdSnapshotAccessOK = 0 }, true},
		{"no access report", func(s *operatorSnapshot) { s.EtcdSnapshotAccessReported = false }, false},
		{"same", func(s *operatorSnapshot) {}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := reported
			tc.change(&candidate)
			if got := operatorSampleNewer(&candidate, &reported); got != tc.want {
				t.Fatalf("selection=%v want %v", got, tc.want)
			}
		})
	}
}
