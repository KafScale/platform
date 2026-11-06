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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func srGet(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return fmt.Sprintf("%d %s", resp.StatusCode, data)
	}
	if m, ok := v.(map[string]any); ok {
		delete(m, "ts")
		if code, ok := m["error_code"]; ok {
			return fmt.Sprintf("%d error %v", resp.StatusCode, code)
		}
	}
	norm, _ := json.Marshal(v)
	return fmt.Sprintf("%d %s", resp.StatusCode, norm)
}

func seedConfluentForImport(t *testing.T, cpURL string) {
	t.Helper()
	must := func(method, path string, body any) {
		t.Helper()
		if code := srCall(t, method, cpURL+path, body, nil); code != http.StatusOK {
			t.Fatalf("%s %s: HTTP %d", method, path, code)
		}
	}
	register := func(subject, schema string, refs ...map[string]any) {
		t.Helper()
		body := map[string]any{"schema": schema}
		if len(refs) > 0 {
			body["references"] = refs
		}
		must(http.MethodPost, "/subjects/"+subject+"/versions", body)
	}
	user1 := `{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}`
	must(http.MethodPut, "/config/imp-user", map[string]string{"compatibility": "NONE"})
	register("imp-user", user1)
	register("imp-addr", `{"type":"record","name":"Address","namespace":"example","fields":[{"name":"city","type":"string"},{"name":"lat","type":"double","default":1.0E-5}]}`)
	register("imp-person", `{"type":"record","name":"Person","namespace":"example","fields":[{"name":"home","type":"example.Address"}]}`,
		map[string]any{"name": "example.Address", "subject": "imp-addr", "version": 1})
	register("imp-user", `{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"age","type":"int","default":0}]}`)
	register("imp-user", `{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"tag","type":"string","default":""}]}`)
	must(http.MethodDelete, "/subjects/imp-user/versions/2", nil)
	register("imp-gone", `{"type":"record","name":"Gone","namespace":"","fields":[{"name":"x","type":"float","default":100}]}`)
	must(http.MethodDelete, "/subjects/imp-gone", nil)
	register("imp-hard", `{"type":"record","name":"Hard","fields":[{"name":"a","type":"long"}]}`)
	register("imp-hard", `{"type":"record","name":"Hard","fields":[{"name":"a","type":"long"},{"name":"b","type":"string","default":""}]}`)
	must(http.MethodDelete, "/subjects/imp-hard/versions/1", nil)
	must(http.MethodDelete, "/subjects/imp-hard/versions/1?permanent=true", nil)
	register("imp-shared", user1)

	must(http.MethodPut, "/config/imp-json", map[string]string{"compatibility": "NONE"})
	must(http.MethodPost, "/subjects/imp-json-leaf/versions", map[string]any{"schemaType": "JSON", "schema": `{"type":"string","minLength":1}`})
	must(http.MethodPost, "/subjects/imp-json/versions", map[string]any{"schemaType": "JSON", "schema": `{"type":"object","properties":{"name":{"$ref":"leaf.json"}}}`, "references": []map[string]any{{"name": "leaf.json", "subject": "imp-json-leaf", "version": 1}}})
	must(http.MethodPost, "/subjects/imp-json/versions", map[string]any{"schemaType": "JSON", "schema": `{"type":"object","properties":{"name":{"type":"string"}},"title":"next"}`})
	must(http.MethodDelete, "/subjects/imp-json/versions/2", nil)
	must(http.MethodPut, "/config/imp-user", map[string]string{"compatibility": "FULL_TRANSITIVE"})
	must(http.MethodPut, "/config", map[string]string{"compatibility": "FORWARD"})
	must(http.MethodPut, "/mode/imp-addr", map[string]string{"mode": "READONLY"})
}

func dumpForImport(t *testing.T, url string) map[string]string {
	t.Helper()
	out := map[string]string{}
	var subjects []string
	srCall(t, http.MethodGet, url+"/subjects?deleted=true", nil, &subjects)
	slices.Sort(subjects)
	out["subjects?deleted=true"] = fmt.Sprint(subjects)
	out["subjects"] = srGet(t, url+"/subjects")
	out["config"] = srGet(t, url+"/config")
	ids := map[int]bool{}
	for _, s := range subjects {
		if strings.HasPrefix(s, "after-") {
			continue
		}
		var versions []int
		srCall(t, http.MethodGet, url+"/subjects/"+s+"/versions?deleted=true", nil, &versions)
		out[s+" versions?deleted=true"] = fmt.Sprint(versions)
		out[s+" versions"] = srGet(t, url+"/subjects/"+s+"/versions")
		out[s+" latest"] = srGet(t, url+"/subjects/"+s+"/versions/latest")
		out[s+" config"] = srGet(t, url+"/config/"+s+"?defaultToGlobal=false")
		out[s+" mode"] = srGet(t, url+"/mode/"+s)
		for _, v := range versions {
			path := fmt.Sprintf("/subjects/%s/versions/%d?deleted=true", s, v)
			var rec struct {
				ID int `json:"id"`
			}
			srCall(t, http.MethodGet, url+path, nil, &rec)
			ids[rec.ID] = true
			out[path] = srGet(t, url+path)
			out[path+" schema"] = srGet(t, fmt.Sprintf("%s/subjects/%s/versions/%d/schema?deleted=true", url, s, v))
		}
	}
	for id := range ids {
		out[fmt.Sprintf("id %d", id)] = srGet(t, fmt.Sprintf("%s/schemas/ids/%d", url, id))
		out[fmt.Sprintf("id %d versions", id)] = srGet(t, fmt.Sprintf("%s/schemas/ids/%d/versions", url, id))
	}
	return out
}

func (h *srHarness) importFromConfluent(t *testing.T, cpURL, url string) {
	t.Helper()
	seedConfluentForImport(t, cpURL)
	cli := filepath.Join(h.work, "kafscale-cli")
	h.run(nil, "go", "build", "-o", cli, filepath.Join(repoRoot(t), "cmd", "kafscale-cli"))
	dry := h.run(nil, cli, "schema", "import", "--from", cpURL, "--to", url, "--dry-run")
	t.Logf("dry run:\n%s", dry)
	if !strings.Contains(dry, "dry run: imported 12, skipped 0, soft-deleted 3, settings changed 4, conflicts 0") {
		t.Fatalf("dry run summary:\n%s", dry)
	}
	first := h.run(nil, cli, "schema", "import", "--from", cpURL, "--to", url)
	t.Logf("import:\n%s", first)
	if !strings.Contains(first, "imported 12, skipped 0, soft-deleted 3, settings changed 4, conflicts 0") {
		t.Fatalf("import summary:\n%s", first)
	}
	again := h.run(nil, cli, "schema", "import", "--from", cpURL, "--to", url)
	if !strings.Contains(again, "imported 0, skipped 12, soft-deleted 0, settings changed 0, conflicts 0") {
		t.Fatalf("rerun summary:\n%s", again)
	}
	src, dst := dumpForImport(t, cpURL), dumpForImport(t, url)
	keys := make([]string, 0, len(src))
	for k := range src {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		if src[k] != dst[k] {
			t.Errorf("%s\n confluent %s\n kafscale  %s", k, src[k], dst[k])
		}
	}
	if len(src) != len(dst) {
		t.Errorf("confluent has %d entries, kafscale %d", len(src), len(dst))
	}
	t.Logf("compared %d endpoint responses between Confluent and KafScale after import", len(src))
	srCall(t, http.MethodPut, url+"/mode", map[string]string{"mode": "READWRITE"}, nil)
	srCall(t, http.MethodPut, url+"/config", map[string]string{"compatibility": "BACKWARD"}, nil)
}

// Compare the cross-subject descriptor case with a live Confluent registry,
// then re-import its real export without allocating any new schema IDs.
func (h *srHarness) protobufCrossSubjectImport(t *testing.T, cpURL, url string) {
	t.Helper()
	type registered struct {
		ID int `json:"id"`
	}
	register := func(base, subject, text string, refs []map[string]any) int {
		t.Helper()
		var out registered
		body := map[string]any{"schemaType": "PROTOBUF", "schema": text}
		if len(refs) > 0 {
			body["references"] = refs
		}
		if code := srCall(t, http.MethodPost, base+"/subjects/"+subject+"/versions", body, &out); code != http.StatusOK {
			t.Fatalf("register %s: HTTP %d", subject, code)
		}
		return out.ID
	}
	common := `syntax = "proto3"; package parity.common; message Money { string currency = 1; }`
	text := `syntax = "proto3"; package parity; import "parity/common.proto"; message Order { parity.common.Money total = 1; }`
	register(cpURL, "parity-common", common, nil)
	refs := []map[string]any{{"name": "parity/common.proto", "subject": "parity-common", "version": 1}}
	first := register(cpURL, "parity-text", text, refs)
	var serialized struct {
		Schema string `json:"schema"`
	}
	if code := srCall(t, http.MethodGet, fmt.Sprintf("%s/schemas/ids/%d?format=serialized", cpURL, first), nil, &serialized); code != http.StatusOK {
		t.Fatal(code)
	}
	if second := register(cpURL, "parity-descriptor", serialized.Schema, refs); second != first {
		t.Fatalf("Confluent cross-subject IDs %d and %d", first, second)
	}
	cli := filepath.Join(h.work, "kafscale-cli")
	imported := h.run(nil, cli, "schema", "import", "--from", cpURL, "--to", url, "--force")
	if !strings.Contains(imported, "conflicts 0") {
		t.Fatalf("real Protobuf export import: %s", imported)
	}
	var stored registered
	srCall(t, http.MethodGet, url+"/subjects/parity-text/versions/1", nil, &stored)
	if stored.ID != first {
		t.Fatalf("import changed Protobuf ID: %d, want %d", stored.ID, first)
	}
	srCall(t, http.MethodPut, url+"/mode", map[string]string{"mode": "READWRITE"}, nil)
	// Exercise the fixed path against a schema imported from Confluent, rather
	// than one generated locally by KafScale's descriptor printer.
	if got := register(url, "parity-descriptor-again", serialized.Schema, refs); got != first {
		t.Fatalf("KafScale cross-subject ID %d, want Confluent ID %d", got, first)
	}
	before := srGet(t, url+"/schemas")
	repeated := h.run(nil, cli, "schema", "import", "--from", cpURL, "--to", url, "--force")
	if !strings.Contains(repeated, "imported 0, skipped") || !strings.Contains(repeated, "conflicts 0") {
		t.Fatalf("real export re-import changed IDs: %s", repeated)
	}
	if after := srGet(t, url+"/schemas"); after != before {
		t.Fatalf("re-import changed registered schemas\nbefore: %s\nafter: %s", before, after)
	}
	// Keep the original migration scenario's subsequent registrations writable.
	srCall(t, http.MethodPut, url+"/mode", map[string]string{"mode": "READWRITE"}, nil)
	srCall(t, http.MethodPut, url+"/config", map[string]string{"compatibility": "BACKWARD"}, nil)
}
