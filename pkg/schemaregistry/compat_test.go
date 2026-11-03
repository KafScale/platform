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
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

func compatRecord(fields ...string) string {
	return `{"type":"record","name":"R","fields":[` + strings.Join(fields, ",") + `]}`
}

var compatSequences = func() map[string][]string {
	a := `{"name":"a","type":"int"}`
	ad := `{"name":"a","type":"int","default":0}`
	as := `{"name":"a","type":"string","default":""}`
	al := `{"name":"a","type":"long"}`
	b := `{"name":"b","type":"string","default":"x"}`
	c := `{"name":"c","type":"int"}`
	return map[string][]string{
		"bt":                 {compatRecord(a), compatRecord(), compatRecord(as)},
		"ft":                 {compatRecord(a), compatRecord(ad), compatRecord()},
		"pair_add_nodefault": {compatRecord(a), compatRecord(a, c)},
		"pair_remove":        {compatRecord(a, b), compatRecord(b)},
		"pair_promote":       {compatRecord(a), compatRecord(al)},
		"pair_add_default":   {compatRecord(a), compatRecord(a, b)},
		"pair_demote":        {compatRecord(al), compatRecord(a)},
	}
}()

func (h *harness) compatStep(kind, method, path string, body any) []any {
	status, data := h.do(method, path, body)
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		h.t.Fatalf("%s %s: %s", method, path, data)
	}
	var code any
	if c, ok := out["error_code"]; ok {
		code = c
	}
	_, isCompat := out["is_compatible"]
	switch {
	case status == 200 && isCompat:
		return []any{kind, float64(status), nil, out["is_compatible"]}
	case status == 200:
		_, hasID := out["id"]
		return []any{kind, float64(status), nil, hasID}
	}
	return []any{kind, float64(status), code, nil}
}

func TestCompatibilityLevelsMatchConfluent(t *testing.T) {
	data, err := os.ReadFile("testdata/cp-compat.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Level string  `json:"level"`
		Case  string  `json:"case"`
		Steps [][]any `json:"steps"`
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, nil, Config{})
	for _, c := range cases {
		subject := fmt.Sprintf("%s-%s", c.Level, c.Case)
		h.ok("PUT", "/config/"+subject, map[string]string{"compatibility": c.Level}, nil)
		var got [][]any
		if c.Case == "softdel-bt" {
			seq := compatSequences["bt"]
			h.do("POST", "/subjects/"+subject+"/versions", schema(seq[0]))
			h.do("POST", "/subjects/"+subject+"/versions", schema(seq[1]))
			h.do("DELETE", "/subjects/"+subject+"/versions/1", nil)
			got = append(got, h.compatStep("register", "POST", "/subjects/"+subject+"/versions", schema(seq[2])))
		} else {
			seq := compatSequences[c.Case]
			for i, s := range seq {
				if i == len(seq)-1 {
					got = append(got,
						h.compatStep("compat-latest", "POST", "/compatibility/subjects/"+subject+"/versions/latest", schema(s)),
						h.compatStep("compat-all", "POST", "/compatibility/subjects/"+subject+"/versions", schema(s)),
						h.compatStep("compat-v1", "POST", "/compatibility/subjects/"+subject+"/versions/1", schema(s)))
				}
				got = append(got, h.compatStep("register", "POST", "/subjects/"+subject+"/versions", schema(s)))
			}
		}
		if !reflect.DeepEqual(got, c.Steps) {
			t.Errorf("%s %s\n got %v\nwant %v", c.Level, c.Case, got, c.Steps)
		}
	}
}

func TestCompatibilityLevelConfig(t *testing.T) {
	h := newHarness(t, nil, Config{})
	var out map[string]string
	h.ok("PUT", "/config/s", map[string]string{"compatibility": "full_transitive"}, &out)
	if out["compatibility"] != "FULL_TRANSITIVE" {
		t.Fatalf("put %v", out)
	}
	h.ok("GET", "/config/s", nil, &out)
	if out["compatibilityLevel"] != "FULL_TRANSITIVE" {
		t.Fatalf("get %v", out)
	}
	h.register("s", schema(compatRecord(`{"name":"a","type":"int"}`)))
	status, body := h.do("POST", "/subjects/s/versions", schema(compatRecord()))
	if status != 409 || !strings.Contains(string(body), `{validateFields: 'false', compatibility: 'FULL_TRANSITIVE'}`) {
		t.Fatalf("register %d %s", status, body)
	}
}
