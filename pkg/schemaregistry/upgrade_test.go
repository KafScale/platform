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
	"context"
	"encoding/json"
	"os"
	"testing"
)

func TestSchemasStoredByEarlierBuildKeepTheirIDs(t *testing.T) {
	data, err := os.ReadFile("testdata/main-format.json")
	if err != nil {
		t.Fatal(err)
	}
	var keys map[string]string
	if err := json.Unmarshal(data, &keys); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, nil, Config{})
	for k, v := range keys {
		if _, err := h.cli.Put(context.Background(), k, v); err != nil {
			t.Fatal(err)
		}
	}
	m1 := `{"type":"record","name":"M1","namespace":"x","fields":[{"name":"b","type":{"type":"record","name":"B","namespace":"","fields":[{"name":"c","type":{"type":"record","name":"C","fields":[{"name":"d","type":"int"}]}},{"name":"e","type":"x.M1"}]}},{"name":"f","type":"C"}]}`
	for _, c := range []struct {
		subject, schema, query string
		id                     int64
	}{
		{"sub-raw", `{"type":"record","name":"D0","fields":[{"name":"f","type":"double","default":5e-324}]}`, "", 1},
		{"sub-norm", `{"type":"record","name":"D1","fields":[{"name":"f","type":"double","default":1e-323}]}`, "?normalize=true", 2},
		{"m1-raw", m1, "", 3},
		{"m1-norm", m1, "?normalize=true", 4},
	} {
		var got struct {
			ID      int64 `json:"id"`
			Version int   `json:"version"`
		}
		h.ok("POST", "/subjects/"+c.subject+c.query, map[string]any{"schema": c.schema}, &got)
		if got.ID != c.id || got.Version != 1 {
			t.Fatalf("%s lookup gave id %d version %d, want id %d version 1", c.subject, got.ID, got.Version, c.id)
		}
		h.ok("POST", "/subjects/"+c.subject+"/versions"+c.query, map[string]any{"schema": c.schema}, &got)
		if got.ID != c.id {
			t.Fatalf("%s register gave id %d, want %d", c.subject, got.ID, c.id)
		}
		if _, body := h.do("GET", "/subjects/"+c.subject+"/versions", nil); string(body) != "[1]" {
			t.Fatalf("%s versions %s", c.subject, body)
		}
	}
}

func TestNewSchemasRecordTheirNormForm(t *testing.T) {
	h := newHarness(t, nil, Config{})
	id := h.register("user", schema(userV1))
	resp, err := h.cli.Get(context.Background(), idKey(id))
	if err != nil || len(resp.Kvs) == 0 {
		t.Fatalf("id %d: %v %v", id, resp, err)
	}
	var rec schemaRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		t.Fatal(err)
	}
	if rec.NormForm != currentNormForm {
		t.Fatalf("normForm %d, want %d", rec.NormForm, currentNormForm)
	}
	s, err := h.reg.schemaByID(context.Background(), id)
	if err != nil || s == nil || s.olderNorm {
		t.Fatalf("schema %+v err %v", s, err)
	}
}
