// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

//go:build e2e

package e2e

import (
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func brokerAddrs(t *testing.T) (string, string, string) {
	t.Helper()
	if addr := strings.TrimSpace(os.Getenv("KAFSCALE_E2E_BROKER_ADDR")); addr != "" {
		metrics := envOrDefault("KAFSCALE_E2E_METRICS_ADDR", "127.0.0.1:39093")
		control := envOrDefault("KAFSCALE_E2E_CONTROL_ADDR", "127.0.0.1:39094")
		return addr, metrics, control
	}
	addresses := newBrokerAddresses(t)
	return addresses.kafka, addresses.metrics, addresses.control
}

// Selections are advisory until the subprocess binds. Keep selections distinct
// within this test process; startBrokerAttempts handles external bind races.
var localAddressSelections = struct {
	sync.Mutex
	used map[string]bool
}{used: make(map[string]bool)}

func allocateLocalAddresses(t *testing.T, count int) []string {
	t.Helper()
	localAddressSelections.Lock()
	defer localAddressSelections.Unlock()
	var listeners []net.Listener
	defer func() {
		for _, listener := range listeners {
			listener.Close()
		}
	}()
	addresses := make([]string, 0, count)
	for attempt := 0; len(addresses) < count && attempt < 100*count; attempt++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("allocate address: %v", err)
		}
		addr := listener.Addr().String()
		if localAddressSelections.used[addr] {
			listener.Close()
			continue
		}
		localAddressSelections.used[addr] = true
		t.Cleanup(func() {
			localAddressSelections.Lock()
			delete(localAddressSelections.used, addr)
			localAddressSelections.Unlock()
		})
		listeners = append(listeners, listener)
		addresses = append(addresses, addr)
	}
	if len(addresses) != count {
		t.Fatal("could not allocate distinct local addresses")
	}
	return addresses
}

func freeAddr(t *testing.T) string {
	t.Helper()
	return allocateLocalAddresses(t, 1)[0]
}

func pickFreePort(t *testing.T) string {
	t.Helper()
	_, port, err := net.SplitHostPort(freeAddr(t))
	if err != nil {
		t.Fatal(err)
	}
	return port
}

// minioEndpoint returns the MinIO endpoint from environment or the default localhost:9000.
func minioEndpoint() string {
	if val := strings.TrimSpace(os.Getenv("KAFSCALE_S3_ENDPOINT")); val != "" {
		return val
	}
	return "http://127.0.0.1:9000"
}

// minioAvailable checks if MinIO is reachable at the configured endpoint.
// Tests that require MinIO should call requireMinIO(t) at the start.
func minioAvailable() bool {
	endpoint := minioEndpoint()
	// Extract host:port from http://host:port
	addr := strings.TrimPrefix(endpoint, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// requireMinIO skips the test if MinIO is not available.
// Use this at the start of tests that require a real MinIO instance.
func requireMinIO(t *testing.T) {
	t.Helper()
	if !minioAvailable() {
		t.Skipf("MinIO not available at %s; run 'make ensure-minio' first or use 'make test-produce-consume'", minioEndpoint())
	}
}
