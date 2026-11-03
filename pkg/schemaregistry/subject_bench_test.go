// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package schemaregistry

import (
	"fmt"
	"testing"
)

func benchSubject(b *testing.B, versions int, withRef bool) (*harness, func(int) map[string]any) {
	h := newHarness(b, nil, Config{})
	h.ok("PUT", "/config", map[string]string{"compatibility": "NONE"}, nil)
	var refs []map[string]any
	field := ""
	if withRef {
		h.register("addr", schema(address))
		refs = []map[string]any{{"name": "example.Address", "subject": "addr", "version": 1}}
		field = `,{"name":"home","type":"example.Address"}`
	}
	body := func(i int) map[string]any {
		return map[string]any{
			"schema":     fmt.Sprintf(`{"type":"record","name":"P","namespace":"example","fields":[{"name":"f%d","type":"int"}%s]}`, i, field),
			"references": refs,
		}
	}
	for i := range versions {
		h.register("big", body(i))
	}
	return h, body
}

func benchmarkSubjectMiss(b *testing.B, withRef, lookup bool) {
	h, body := benchSubject(b, 1000, withRef)
	path := "/subjects/big/versions"
	if lookup {
		path = "/subjects/big"
	}
	n := 1_000_000
	b.ResetTimer()
	for b.Loop() {
		n++
		h.do("POST", path, body(n))
	}
}

func BenchmarkRegisterNewVersion(b *testing.B)        { benchmarkSubjectMiss(b, false, false) }
func BenchmarkRegisterNewVersionWithRef(b *testing.B) { benchmarkSubjectMiss(b, true, false) }
func BenchmarkLookupMiss(b *testing.B)                { benchmarkSubjectMiss(b, false, true) }
func BenchmarkLookupMissWithRef(b *testing.B)         { benchmarkSubjectMiss(b, true, true) }
