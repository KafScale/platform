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
	"testing"
)

func brokerAddrs(t *testing.T) (string, string, string) {
	t.Helper()
	if addr := strings.TrimSpace(os.Getenv("KAFSCALE_E2E_BROKER_ADDR")); addr != "" {
		metrics := envOrDefault("KAFSCALE_E2E_METRICS_ADDR", "127.0.0.1:39093")
		control := envOrDefault("KAFSCALE_E2E_CONTROL_ADDR", "127.0.0.1:39094")
		return addr, metrics, control
	}
	brokerPort := pickFreePort(t)
	metricsPort := pickFreePort(t)
	controlPort := pickFreePort(t)
	return "127.0.0.1:" + brokerPort, "127.0.0.1:" + metricsPort, "127.0.0.1:" + controlPort
}

func pickFreePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick free port: %v", err)
	}
	defer l.Close()
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatalf("split free port: %v", err)
	}
	return port
}
