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

package main

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParseCSVAddrs(t *testing.T) {
	addrs := parseCSVAddrs("  a:1, b:2 ,,c:3, ")
	if len(addrs) != 3 {
		t.Fatalf("expected 3 addrs got %d", len(addrs))
	}
	if strings.Join(addrs, "|") != "a:1|b:2|c:3" {
		t.Fatalf("unexpected addrs: %v", addrs)
	}
}

func TestProbeAddrs(t *testing.T) {
	calls := make(map[string]int)
	dial := func(addr string, _ time.Duration) error {
		calls[addr]++
		if addr == "bad:1" {
			return errors.New("dial failed")
		}
		return nil
	}
	if err := probeAddrs([]string{"good:1"}, 10*time.Millisecond, 2, 1*time.Millisecond, dial); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := probeAddrs([]string{"bad:1"}, 1*time.Millisecond, 2, 1*time.Millisecond, dial); err == nil {
		t.Fatalf("expected error")
	}
	if calls["bad:1"] < 2 {
		t.Fatalf("expected retries, got %d", calls["bad:1"])
	}
}

func TestParseEnvInts(t *testing.T) {
	t.Setenv("E2E_INT", "12")
	t.Setenv("E2E_INT32", "34")
	t.Setenv("E2E_INT64", "56")
	if got := parseEnvInt("E2E_INT", 1); got != 12 {
		t.Fatalf("expected 12 got %d", got)
	}
	if got := parseEnvInt32("E2E_INT32", -1); got != 34 {
		t.Fatalf("expected 34 got %d", got)
	}
	if got := parseEnvInt64("E2E_INT64", -1); got != 56 {
		t.Fatalf("expected 56 got %d", got)
	}
	t.Setenv("E2E_INT", "bad")
	t.Setenv("E2E_INT32", "bad")
	t.Setenv("E2E_INT64", "bad")
	if got := parseEnvInt("E2E_INT", 7); got != 7 {
		t.Fatalf("expected fallback got %d", got)
	}
	if got := parseEnvInt32("E2E_INT32", 9); got != 9 {
		t.Fatalf("expected fallback got %d", got)
	}
	if got := parseEnvInt64("E2E_INT64", 11); got != 11 {
		t.Fatalf("expected fallback got %d", got)
	}
}
