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

package main

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/KafScale/platform/pkg/acl"
)

type authMetrics struct {
	deniedTotal uint64
	mu          sync.Mutex
	byKey       map[string]uint64
}

func newAuthMetrics() *authMetrics {
	return &authMetrics{byKey: make(map[string]uint64)}
}

func (m *authMetrics) RecordDenied(action acl.Action, resource acl.Resource) {
	if m == nil {
		return
	}
	atomic.AddUint64(&m.deniedTotal, 1)
	key := fmt.Sprintf("%s|%s", action, resource)
	m.mu.Lock()
	m.byKey[key]++
	m.mu.Unlock()
}

func (m *authMetrics) writePrometheus(w io.Writer) {
	if m == nil {
		return
	}
	total := atomic.LoadUint64(&m.deniedTotal)
	fmt.Fprintln(w, "# HELP kafscale_authz_denied_total Authorization denials across broker APIs.")
	fmt.Fprintln(w, "# TYPE kafscale_authz_denied_total counter")
	fmt.Fprintf(w, "kafscale_authz_denied_total %d\n", total)

	m.mu.Lock()
	defer m.mu.Unlock()
	for key, count := range m.byKey {
		action, resource := splitAuthMetricKey(key)
		fmt.Fprintf(w, "kafscale_authz_denied_total{action=%q,resource=%q} %d\n", action, resource, count)
	}
}

func splitAuthMetricKey(key string) (string, string) {
	for i := 0; i < len(key); i++ {
		if key[i] == '|' {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}
