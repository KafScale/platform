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

package schemaregistry

import (
	"encoding/json"
	"os"
	"testing"
)

func TestCanonicalMatchesConfluent(t *testing.T) {
	data, err := os.ReadFile("testdata/cp-canonical.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Schema    string   `json:"schema"`
		Normalize bool     `json:"normalize"`
		Refs      []string `json:"refs"`
		Want      string   `json:"want"`
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for i, c := range cases {
		var refs []resolvedReference
		for _, r := range c.Refs {
			refs = append(refs, resolvedReference{Text: r})
		}
		got, err := canonicalAvro(c.Schema, refs, c.Normalize)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if got != c.Want {
			t.Errorf("case %d normalize=%v\n got %s\nwant %s", i, c.Normalize, got, c.Want)
		}
	}
}

func TestJavaDouble(t *testing.T) {
	for in, want := range map[float64]string{1000: "1000.0", 0.1: "0.1", 1.5e10: "1.5E10", 0.0001: "1.0E-4", -2.5: "-2.5", 1e7: "1.0E7", 0.001: "0.001"} {
		if got := javaDouble(in); got != want {
			t.Errorf("javaDouble(%v) = %s, want %s", in, got, want)
		}
	}
}
