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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/schemaregistry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	impAddress = `{"type":"record","name":"Address","namespace":"example","fields":[{"name":"city","type":"string"}]}`
	impPerson  = `{"type":"record","name":"Person","namespace":"example","fields":[{"name":"home","type":"example.Address"}]}`
	impUser1   = `{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}`
	impUser2   = `{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"age","type":"int","default":0}]}`
	impUser3   = `{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"tag","type":"string","default":""}]}`
	impGone    = `{"type":"record","name":"Gone","fields":[{"name":"x","type":"double","default":1e-5}]}`
)

type testRegistry struct {
	t   *testing.T
	url string
	srv *httptest.Server
}

func startTestRegistry(t *testing.T, wrap func(http.Handler) http.Handler) *testRegistry {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: testutil.StartEmbeddedEtcd(t), DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	reg, err := schemaregistry.New(schemaregistry.Config{Client: cli, InlineLimit: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	var h http.Handler = schemaregistry.NewServer(reg, 5*time.Second)
	if wrap != nil {
		h = wrap(h)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &testRegistry{t: t, url: srv.URL, srv: srv}
}

func (r *testRegistry) call(method, path string, body any) (int, []byte) {
	r.t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			r.t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, r.url+path, reader)
	if err != nil {
		r.t.Fatal(err)
	}
	req.Header.Set("Content-Type", srContentType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

func (r *testRegistry) must(method, path string, body any) []byte {
	r.t.Helper()
	status, data := r.call(method, path, body)
	if status != http.StatusOK {
		r.t.Fatalf("%s %s: %d %s", method, path, status, data)
	}
	return data
}

func (r *testRegistry) put(subject string, id, version int, schema string, refs ...srReference) {
	r.t.Helper()
	body := map[string]any{"schema": schema, "id": id, "version": version}
	if len(refs) > 0 {
		body["references"] = refs
	}
	r.must("POST", "/subjects/"+subject+"/versions", body)
}

func seedSource(t *testing.T, wrap ...func(http.Handler) http.Handler) *testRegistry {
	var w func(http.Handler) http.Handler
	if len(wrap) > 0 {
		w = wrap[0]
	}
	src := startTestRegistry(t, w)
	src.must("PUT", "/mode", map[string]string{"mode": "IMPORT"})
	src.put("user", 3, 1, impUser1)
	src.put("addr", 10, 1, impAddress)
	src.put("person", 5, 1, impPerson, srReference{Name: "example.Address", Subject: "addr", Version: 1})
	src.put("user", 11, 2, impUser2)
	src.put("user", 12, 3, impUser3)
	src.put("shared", 3, 1, impUser1)
	src.put("gone", 20, 1, impGone)
	src.must("DELETE", "/subjects/user/versions/2", nil)
	src.must("DELETE", "/subjects/gone", nil)
	src.must("PUT", "/mode", map[string]string{"mode": "READWRITE"})
	src.must("PUT", "/config", map[string]string{"compatibility": "FORWARD"})
	src.must("PUT", "/config/user", map[string]string{"compatibility": "FULL_TRANSITIVE"})
	src.must("PUT", "/mode/addr", map[string]string{"mode": "READONLY"})
	return src
}

func registryDump(t *testing.T, r *testRegistry, filter *regexp.Regexp) map[string]string {
	t.Helper()
	out := map[string]string{}
	get := func(path string) string {
		status, data := r.call("GET", path, nil)
		return fmt.Sprintf("%d %s", status, data)
	}
	var subjects []string
	if err := json.Unmarshal(r.must("GET", "/subjects?deleted=true", nil), &subjects); err != nil {
		t.Fatal(err)
	}
	ids := map[int64]bool{}
	for _, s := range subjects {
		if filter != nil && !filter.MatchString(s) {
			continue
		}
		var versions []int
		if err := json.Unmarshal(r.must("GET", "/subjects/"+s+"/versions?deleted=true", nil), &versions); err != nil {
			t.Fatal(err)
		}
		out[s+" versions"] = get("/subjects/" + s + "/versions")
		out[s+" all versions"] = fmt.Sprint(versions)
		out[s+" config"] = get("/config/" + s + "?defaultToGlobal=false")
		out[s+" mode"] = get("/mode/" + s)
		for _, v := range versions {
			path := fmt.Sprintf("/subjects/%s/versions/%d?deleted=true", s, v)
			var rec srVersion
			if err := json.Unmarshal(r.must("GET", path, nil), &rec); err != nil {
				t.Fatal(err)
			}
			ids[rec.ID] = true
			out[path] = get(path)
		}
	}
	for id := range ids {
		out[fmt.Sprintf("id %d", id)] = get(fmt.Sprintf("/schemas/ids/%d", id))
		out[fmt.Sprintf("id %d versions", id)] = get(fmt.Sprintf("/schemas/ids/%d/versions", id))
	}
	out["config"] = get("/config")
	return out
}

func assertSameRegistry(t *testing.T, src, dst *testRegistry, filter *regexp.Regexp) {
	t.Helper()
	a, b := registryDump(t, src, filter), registryDump(t, dst, filter)
	if len(a) != len(b) {
		t.Errorf("source has %d entries, target %d", len(a), len(b))
	}
	for k, v := range a {
		if b[k] != v {
			t.Errorf("%s\n source %s\n target %s", k, v, b[k])
		}
	}
}

func runImport(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := run(context.Background(), append([]string{"schema", "import"}, args...), &out, io.Discard)
	return out.String(), err
}

func TestSchemaImportCopiesEverything(t *testing.T) {
	src := seedSource(t)
	dst := startTestRegistry(t, nil)
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if !strings.Contains(out, "imported 7, skipped 0, soft-deleted 2, settings changed 3, conflicts 0") {
		t.Fatalf("summary\n%s", out)
	}
	assertSameRegistry(t, src, dst, nil)
	if got := string(dst.must("GET", "/mode", nil)); !strings.Contains(got, "IMPORT") {
		t.Fatalf("target mode %s", got)
	}
	out, err = runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil || !strings.Contains(out, "imported 0, skipped 7, soft-deleted 0, settings changed 0, conflicts 0") {
		t.Fatalf("rerun %v\n%s", err, out)
	}
	assertSameRegistry(t, src, dst, nil)
}

func TestSchemaImportDryRunWritesNothing(t *testing.T) {
	src := seedSource(t)
	var writes atomic.Int64
	dst := startTestRegistry(t, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				writes.Add(1)
			}
			h.ServeHTTP(w, r)
		})
	})
	out, err := runImport(t, "--from", src.url, "--to", dst.url, "--dry-run")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if writes.Load() != 0 {
		t.Fatalf("dry run sent %d writes", writes.Load())
	}
	for _, want := range []string{"would import subject person version 1 id 5", "would soft-delete subject gone version 1", "would set config of user to FULL_TRANSITIVE", "dry run: imported 7, skipped 0, soft-deleted 2, settings changed 3, conflicts 0"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q\n%s", want, out)
		}
	}
}

func TestSchemaImportImportsReferencesFirst(t *testing.T) {
	src := seedSource(t)
	var order []string
	var mu sync.Mutex
	dst := startTestRegistry(t, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				mu.Lock()
				order = append(order, r.URL.Path)
				mu.Unlock()
			}
			h.ServeHTTP(w, r)
		})
	})
	if out, err := runImport(t, "--from", src.url, "--to", dst.url); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	want := []string{"/subjects/shared/versions", "/subjects/user/versions", "/subjects/addr/versions", "/subjects/person/versions", "/subjects/user/versions", "/subjects/user/versions", "/subjects/gone/versions"}
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("import order %v, want %v", order, want)
	}
}

func TestSchemaImportSubjectFilterPullsInReferences(t *testing.T) {
	src := seedSource(t)
	dst := startTestRegistry(t, nil)
	out, err := runImport(t, "--from", src.url, "--to", dst.url, "--subjects", "^person$")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	var subjects []string
	if err := json.Unmarshal(dst.must("GET", "/subjects?deleted=true", nil), &subjects); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(subjects) != "[addr person]" {
		t.Fatalf("subjects %v\n%s", subjects, out)
	}
	if strings.Contains(out, "mode of addr") {
		t.Fatalf("settings of a dependency subject were copied\n%s", out)
	}
	assertSameRegistry(t, src, dst, regexp.MustCompile("^person$"))
}

func TestSchemaImportReportsConflicts(t *testing.T) {
	src := seedSource(t)
	dst := startTestRegistry(t, nil)
	dst.must("PUT", "/mode", map[string]string{"mode": "IMPORT"})
	dst.put("user", 30, 3, `{"type":"record","name":"Other","fields":[]}`)
	dst.put("elsewhere", 20, 1, `{"type":"record","name":"Taken","fields":[]}`)
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err == nil || !strings.Contains(err.Error(), "2 conflicts") {
		t.Fatalf("err %v\n%s", err, out)
	}
	for _, want := range []string{
		"conflict: subject user version 3 is id 12 in the source and id 30 with different content in the target",
		"conflict: schema id 20 already holds different content in the target",
		"imported 5, skipped 0, soft-deleted 1, settings changed 3, conflicts 2",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q\n%s", want, out)
		}
	}
}

func TestSchemaImportResumesAfterFailure(t *testing.T) {
	src := seedSource(t)
	var posts atomic.Int64
	dst := startTestRegistry(t, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && posts.Add(1) == 4 {
				http.Error(w, `{"error_code":40000,"message":"injected"}`, http.StatusBadRequest)
				return
			}
			h.ServeHTTP(w, r)
		})
	})
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("first run %v\n%s", err, out)
	}
	out, err = runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil || !strings.Contains(out, "imported 4, skipped 3") {
		t.Fatalf("second run %v\n%s", err, out)
	}
	assertSameRegistry(t, src, dst, nil)
}

func TestSchemaImportNeedsForceForNonEmptyTarget(t *testing.T) {
	src := seedSource(t)
	dst := startTestRegistry(t, nil)
	dst.must("POST", "/subjects/local/versions", map[string]string{"schema": `{"type":"record","name":"Local","fields":[]}`})
	dst.must("PUT", "/mode/addr", map[string]string{"mode": "READONLY"})
	dst.must("PUT", "/mode/shared", map[string]string{"mode": "READONLY"})
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("err %v\n%s", err, out)
	}
	if out, err = runImport(t, "--from", src.url, "--to", dst.url, "--force"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	assertSameRegistry(t, src, dst, regexp.MustCompile("^(user|addr|person|gone)$"))
	if got := string(dst.must("GET", "/mode/shared", nil)); !strings.Contains(got, "READONLY") {
		t.Fatalf("shared mode %s", got)
	}
	if got := string(dst.must("GET", "/subjects/shared/versions/1", nil)); !strings.Contains(got, `"id":3`) {
		t.Fatalf("shared %s", got)
	}
	src.must("DELETE", "/mode/addr", nil)
	src.must("PUT", "/mode?force=true", map[string]string{"mode": "IMPORT"})
	src.put("addr", 40, 2, `{"type":"record","name":"Address","namespace":"example","fields":[{"name":"city","type":"string"},{"name":"zip","type":"string","default":""}]}`)
	src.must("PUT", "/mode", map[string]string{"mode": "READWRITE"})
	src.must("PUT", "/mode/addr", map[string]string{"mode": "READONLY"})
	if out, err = runImport(t, "--from", src.url, "--to", dst.url); err != nil || !strings.Contains(out, "imported 1, skipped 7") {
		t.Fatalf("incremental %v\n%s", err, out)
	}
	if got := string(dst.must("GET", "/mode/addr", nil)); !strings.Contains(got, "READONLY") {
		t.Fatalf("addr mode %s", got)
	}
}

func TestSchemaImportSendsAuth(t *testing.T) {
	var seen sync.Map
	wrap := func(name string) func(http.Handler) http.Handler {
		return func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen.Store(name+" "+r.Header.Get("Authorization"), true)
				h.ServeHTTP(w, r)
			})
		}
	}
	src := startTestRegistry(t, wrap("src"))
	dst := startTestRegistry(t, wrap("dst"))
	if out, err := runImport(t, "--from", src.url, "--to", dst.url, "--auth", "u:p:w", "--to-auth", "bearer:tok"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	for _, want := range []string{"src Basic dTpwOnc=", "dst Bearer tok"} {
		if _, ok := seen.Load(want); !ok {
			t.Fatalf("missing header %q", want)
		}
	}
	if _, err := runImport(t, "--from", src.url, "--auth", "token"); err == nil {
		t.Fatal("auth without a colon accepted")
	}
	if _, err := runImport(t); err == nil {
		t.Fatal("missing --from accepted")
	}
	if _, err := runImport(t, "--from", "ftp://x"); err == nil {
		t.Fatal("bad URL accepted")
	}
	if _, err := runImport(t, "--from", src.url, "--subjects", "("); err == nil {
		t.Fatal("bad regex accepted")
	}
}

func withExtraSubjects(extra map[string]srVersion) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && r.URL.Path == "/subjects" {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, r)
				var subjects []string
				if err := json.Unmarshal(rec.Body.Bytes(), &subjects); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				for s := range extra {
					subjects = append(subjects, s)
				}
				_ = json.NewEncoder(w).Encode(subjects)
				return
			}
			for s, v := range extra {
				prefix := "/subjects/" + s + "/versions"
				if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, prefix) {
					continue
				}
				if r.URL.Path == prefix {
					_ = json.NewEncoder(w).Encode([]int{v.Version})
					return
				}
				v.Subject = s
				_ = json.NewEncoder(w).Encode(v)
				return
			}
			h.ServeHTTP(w, r)
		})
	}
}

func TestSchemaImportSkipsNonDefaultContexts(t *testing.T) {
	for _, dry := range []bool{true, false} {
		src := seedSource(t, withExtraSubjects(map[string]srVersion{
			":.ctx1:imp-ctx": {Version: 1, ID: 3, Schema: `{"type":"record","name":"Ctx","fields":[]}`},
		}))
		dst := startTestRegistry(t, nil)
		args := []string{"--from", src.url, "--to", dst.url}
		if dry {
			args = append(args, "--dry-run")
		}
		out, err := runImport(t, args...)
		if err == nil || !strings.Contains(err.Error(), "skipped 1 subjects in non-default contexts") {
			t.Fatalf("dry %v: err %v\n%s", dry, err, out)
		}
		for _, want := range []string{
			"skipped: subject :.ctx1:imp-ctx is in context .ctx1",
			"imported 7, skipped 0, soft-deleted 2, settings changed 3, conflicts 0, skipped contexts 1",
		} {
			if !strings.Contains(out, want) {
				t.Fatalf("dry %v: missing %q\n%s", dry, want, out)
			}
		}
		if dry {
			continue
		}
		if strings.Contains(string(dst.must("GET", "/subjects?deleted=true", nil)), "ctx") {
			t.Fatalf("context subject reached the target")
		}
	}
}

func TestSchemaImportKeepsDefaultContextPrefix(t *testing.T) {
	for subject, want := range map[string]string{":.:plain": "", ":.ctx1:x": ".ctx1", "plain": "", ":.ctx1": "", ":x:y": ""} {
		if got := subjectContext(subject); got != want {
			t.Fatalf("subjectContext(%q) = %q, want %q", subject, got, want)
		}
	}
}

func TestSchemaImportDryRunFindsSourceIDClash(t *testing.T) {
	src := seedSource(t, withExtraSubjects(map[string]srVersion{
		"clash": {Version: 1, ID: 3, Schema: `{"type":"record","name":"Clash","fields":[]}`},
	}))
	dst := startTestRegistry(t, nil)
	out, err := runImport(t, "--from", src.url, "--to", dst.url, "--dry-run")
	if err == nil || !strings.Contains(err.Error(), "2 conflicts") {
		t.Fatalf("err %v\n%s", err, out)
	}
	if !strings.Contains(out, "conflict: schema id 3 is used by subject") || !strings.Contains(out, "conflicts 2") {
		t.Fatalf("clash not reported\n%s", out)
	}
}

func TestSchemaImportFailedRunPrintsSummary(t *testing.T) {
	src := seedSource(t)
	var posts atomic.Int64
	dst := startTestRegistry(t, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && posts.Add(1) == 4 {
				http.Error(w, `{"error_code":42201,"message":"injected"}`, http.StatusUnprocessableEntity)
				return
			}
			h.ServeHTTP(w, r)
		})
	})
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("err %v\n%s", err, out)
	}
	for _, want := range []string{
		"imported 3, skipped 0, soft-deleted 0, settings changed 0, conflicts 0",
		"the run stopped early; rerun the same command to resume it",
		"target is still in IMPORT mode",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q\n%s", want, out)
		}
	}
}

func TestSchemaImportFailedPlanPrintsSummary(t *testing.T) {
	src := seedSource(t, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && r.URL.Path == "/subjects" {
				http.Error(w, `{"error_code":50001,"message":"listing failed"}`, http.StatusInternalServerError)
				return
			}
			h.ServeHTTP(w, r)
		})
	})
	dst := startTestRegistry(t, nil)
	dst.must("PUT", "/mode", map[string]string{"mode": "IMPORT"})
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err == nil || !strings.Contains(err.Error(), "list source subjects") {
		t.Fatalf("err %v\n%s", err, out)
	}
	for _, want := range []string{
		"imported 0, skipped 0, soft-deleted 0, settings changed 0, conflicts 0, skipped contexts 0",
		"the run stopped early; rerun the same command to resume it",
		"target is still in IMPORT mode",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q\n%s", want, out)
		}
	}
}

func TestSchemaImportSkipsMissingSourceModeEndpoint(t *testing.T) {
	var blockMode atomic.Bool
	src := seedSource(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if blockMode.Load() && strings.HasPrefix(r.URL.Path, "/mode") {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte("RESTEASY003210: Could not find resource for full path: " + r.URL.String()))
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	blockMode.Store(true)

	dst := startTestRegistry(t, nil)
	out, err := runImport(t, "--from", src.url, "--to", dst.url, "--dry-run")
	if err != nil {
		t.Fatalf("dry-run against a source that 404s on /mode: %v\n%s", err, out)
	}
	if strings.Count(out, "source has no /mode/{subject} endpoint; subject modes not copied") != 1 {
		t.Fatalf("dry-run should print the missing mode notice once\n%s", out)
	}
	if strings.Contains(out, "would set mode of") {
		t.Fatalf("dry-run should not copy subject modes when /mode 404s\n%s", out)
	}

	out, err = runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("import against a source that 404s on /mode: %v\n%s", err, out)
	}
	if strings.Count(out, "source has no /mode/{subject} endpoint; subject modes not copied") != 1 {
		t.Fatalf("import should print the missing mode notice once\n%s", out)
	}
	if status, data := dst.call("GET", "/subjects/user/versions/1", nil); status != http.StatusOK {
		t.Fatalf("imported schema missing: %d %s", status, data)
	}
	if status, data := dst.call("GET", "/config/user?defaultToGlobal=false", nil); status != http.StatusOK || !strings.Contains(string(data), "FULL_TRANSITIVE") {
		t.Fatalf("subject config not copied: %d %s", status, data)
	}
	if status, data := dst.call("GET", "/mode/addr?defaultToGlobal=false", nil); status != http.StatusNotFound {
		t.Fatalf("subject mode should be unset when the source has no mode endpoint: %d %s", status, data)
	}
	if strings.Contains(out, "set mode of") {
		t.Fatalf("import should not copy subject modes when /mode 404s\n%s", out)
	}
}

func TestSchemaImportRerunAcceptsNormalizedTargetSchema(t *testing.T) {
	address := `{
  "type": "record",
  "name": "Address",
  "namespace": "example",
  "fields": [
    {"name": "city", "type": "string"}
  ]
}`
	person := `{
  "type": "record",
  "name": "Person",
  "namespace": "example",
  "fields": [
    {"name": "home", "type": "example.Address"}
  ]
}`
	src := startRawSchemaSource(t, map[string][]rawSchemaVersion{
		"addr": {{ID: 1, Version: 1, Schema: address}},
		"person": {{
			ID:      2,
			Version: 1,
			Schema:  person,
			Refs:    []srReference{{Name: "example.Address", Subject: "addr", Version: 1}},
		}},
	})
	dst := startTestRegistry(t, nil)
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("first import: %v\n%s", err, out)
	}
	if strings.Contains(out, "conflict:") {
		t.Fatalf("first import reported a conflict\n%s", out)
	}
	out, err = runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("second import: %v\n%s", err, out)
	}
	if strings.Contains(out, "conflict:") {
		t.Fatalf("second import reported a false conflict\n%s", out)
	}
	if !strings.Contains(out, "imported 0, skipped 2") {
		t.Fatalf("expected second import to skip both versions\n%s", out)
	}
}

func TestSchemaImportSkipsSubjectConfigWhenSourceIgnoresDefaultToGlobal(t *testing.T) {
	src := seedSource(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/config") {
				w.Header().Set("Content-Type", srContentType)
				_, _ = w.Write([]byte(`{"compatibilityLevel":"NONE"}`))
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	dst := startTestRegistry(t, nil)
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("import: %v\n%s", err, out)
	}
	if strings.Contains(out, "set config of") || strings.Contains(out, "would set config of") {
		t.Fatalf("subject configs equal to the source global should not be pinned\n%s", out)
	}
	if status, data := dst.call("GET", "/config", nil); status != http.StatusOK || !strings.Contains(string(data), "NONE") {
		t.Fatalf("global config not copied: %d %s", status, data)
	}
	if status, data := dst.call("GET", "/config/user?defaultToGlobal=false", nil); status != http.StatusNotFound {
		t.Fatalf("subject user should not be pinned to NONE: %d %s", status, data)
	}
}

func TestSchemaImportCopiesExplicitSubjectConfigEqualToGlobal(t *testing.T) {
	src := startTestRegistry(t, nil)
	src.must("PUT", "/mode", map[string]string{"mode": "IMPORT"})
	src.put("user", 1, 1, impUser1)
	src.put("addr", 2, 1, impAddress)
	src.must("PUT", "/mode", map[string]string{"mode": "READWRITE"})
	src.must("PUT", "/config", map[string]string{"compatibility": "FORWARD"})
	src.must("PUT", "/config/user", map[string]string{"compatibility": "FORWARD"})
	dst := startTestRegistry(t, nil)
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("import: %v\n%s", err, out)
	}
	if status, data := dst.call("GET", "/config/user?defaultToGlobal=false", nil); status != http.StatusOK || !strings.Contains(string(data), "FORWARD") {
		t.Fatalf("explicit subject pin equal to global should be copied: %d %s", status, data)
	}
	if status, data := dst.call("GET", "/config/addr?defaultToGlobal=false", nil); status != http.StatusNotFound {
		t.Fatalf("unset subject config should stay unset: %d %s", status, data)
	}
}

func TestSchemaImportTargetModeHTTP404IsError(t *testing.T) {
	src := seedSource(t)
	dst := startTestRegistry(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/mode/") {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte("no mode endpoint"))
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err == nil || !strings.Contains(err.Error(), "read target mode") {
		t.Fatalf("expected a target mode read error, got %v\n%s", err, out)
	}
}

type rawSchemaVersion struct {
	ID         int64
	Version    int
	Schema     string
	Refs       []srReference
	Deleted    bool
	Unreadable bool
}

func startRawSchemaSource(t *testing.T, subjects map[string][]rawSchemaVersion) *testRegistry {
	t.Helper()
	names := make([]string, 0, len(subjects))
	bySubject := map[string]map[int]rawSchemaVersion{}
	for name, versions := range subjects {
		names = append(names, name)
		bySubject[name] = map[int]rawSchemaVersion{}
		for _, v := range versions {
			bySubject[name][v.Version] = v
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", srContentType)
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/subjects/") && strings.Contains(r.URL.Path, "/versions/"):
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			if len(parts) < 4 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			subject, ver := parts[1], 0
			if _, err := fmt.Sscanf(parts[3], "%d", &ver); err != nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			v, ok := bySubject[subject][ver]
			if !ok || v.Unreadable {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(fmt.Sprintf(`{"error_code":40401,"message":"No artifact with ID '%s' in group 'null' was found."}`, subject)))
				return
			}
			body := map[string]any{"subject": subject, "version": v.Version, "id": v.ID, "schemaType": "AVRO", "schema": v.Schema}
			if len(v.Refs) > 0 {
				body["references"] = v.Refs
			}
			_ = json.NewEncoder(w).Encode(body)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/subjects/") && strings.HasSuffix(r.URL.Path, "/versions"):
			subject := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/subjects/"), "/versions")
			versions := bySubject[subject]
			nums := make([]int, 0, len(versions))
			deleted := r.URL.Query().Get("deleted") == "true"
			for n, v := range versions {
				if deleted || !v.Deleted {
					nums = append(nums, n)
				}
			}
			if !deleted && len(nums) == 0 && len(versions) > 0 {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(fmt.Sprintf(`{"error_code":40401,"message":"Subject '%s' not found."}`, subject)))
				return
			}
			_ = json.NewEncoder(w).Encode(nums)
		case r.Method == http.MethodGet && (r.URL.Path == "/subjects" || r.URL.Path == "/subjects/"):
			_ = json.NewEncoder(w).Encode(names)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/config"):
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error_code":40408,"message":"subject compatibility not found"}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/mode"):
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error_code":40409,"message":"subject mode not found"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error_code":40400,"message":"not found"}`))
		}
	}))
	t.Cleanup(srv.Close)
	return &testRegistry{t: t, url: srv.URL, srv: srv}
}

func TestSchemaImportSharedPrettySchemaByID(t *testing.T) {
	pretty := `{
  "type": "record",
  "name": "W",
  "fields": [ {"name": "a", "type": "string"} ]
}`
	src := startRawSchemaSource(t, map[string][]rawSchemaVersion{
		"ws":  {{ID: 3, Version: 1, Schema: pretty}},
		"ws2": {{ID: 3, Version: 1, Schema: pretty}},
	})
	dst := startTestRegistry(t, nil)
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("first import: %v\n%s", err, out)
	}
	if strings.Contains(out, "conflict:") {
		t.Fatalf("first import reported a conflict\n%s", out)
	}
	if !strings.Contains(out, "imported 2") {
		t.Fatalf("expected both shared-id subjects to import\n%s", out)
	}
	out, err = runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("second import: %v\n%s", err, out)
	}
	if strings.Contains(out, "conflict:") {
		t.Fatalf("second import reported a false conflict\n%s", out)
	}
	if !strings.Contains(out, "imported 0, skipped 2") {
		t.Fatalf("expected second import to skip both versions\n%s", out)
	}
}

func TestSchemaImportCopiesAllPinnedSubjectConfigs(t *testing.T) {
	src := startTestRegistry(t, nil)
	src.must("PUT", "/mode", map[string]string{"mode": "IMPORT"})
	src.put("user", 1, 1, impUser1)
	src.put("addr", 2, 1, impAddress)
	src.must("PUT", "/mode", map[string]string{"mode": "READWRITE"})
	src.must("PUT", "/config", map[string]string{"compatibility": "BACKWARD"})
	src.must("PUT", "/config/user", map[string]string{"compatibility": "BACKWARD"})
	src.must("PUT", "/config/addr", map[string]string{"compatibility": "BACKWARD"})
	dst := startTestRegistry(t, nil)
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("import: %v\n%s", err, out)
	}
	for _, s := range []string{"user", "addr"} {
		if status, data := dst.call("GET", "/config/"+s+"?defaultToGlobal=false", nil); status != http.StatusOK || !strings.Contains(string(data), "BACKWARD") {
			t.Fatalf("pinned subject %s was not copied: %d %s\n%s", s, status, data, out)
		}
	}
}

func TestSchemaImportLookupDifferentIDConflicts(t *testing.T) {
	pretty := `{
  "type": "record",
  "name": "W",
  "fields": [ {"name": "a", "type": "string"} ]
}`
	src := startRawSchemaSource(t, map[string][]rawSchemaVersion{
		"ws": {{ID: 3, Version: 1, Schema: pretty}},
	})
	var intercept atomic.Bool
	dst := startTestRegistry(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if intercept.Load() && r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/subjects/") && !strings.Contains(r.URL.Path, "/versions") {
				w.Header().Set("Content-Type", srContentType)
				_, _ = w.Write([]byte(`{"id":99,"version":1}`))
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("first import: %v\n%s", err, out)
	}
	intercept.Store(true)
	out, err = runImport(t, "--from", src.url, "--to", dst.url)
	if err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("expected a conflict when lookup returns a different id, got %v\n%s", err, out)
	}
	if !strings.Contains(out, "conflict:") {
		t.Fatalf("missing conflict message\n%s", out)
	}
}

func TestSchemaImportLookupErrorStopsRun(t *testing.T) {
	pretty := `{
  "type": "record",
  "name": "W",
  "fields": [ {"name": "a", "type": "string"} ]
}`
	src := startRawSchemaSource(t, map[string][]rawSchemaVersion{
		"ws": {{ID: 3, Version: 1, Schema: pretty}},
	})
	var intercept atomic.Bool
	dst := startTestRegistry(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if intercept.Load() && r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/subjects/") && !strings.Contains(r.URL.Path, "/versions") {
				http.Error(w, "lookup failed", http.StatusInternalServerError)
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("first import: %v\n%s", err, out)
	}
	intercept.Store(true)
	out, err = runImport(t, "--from", src.url, "--to", dst.url)
	if err == nil || !strings.Contains(err.Error(), "compare source subject") {
		t.Fatalf("expected a lookup error to stop the run, got %v\n%s", err, out)
	}
	if strings.Contains(out, "conflict:") {
		t.Fatalf("lookup error was reported as a content conflict\n%s", out)
	}
}

func TestSchemaImportLookupAcceptsLatestMatchingVersion(t *testing.T) {
	pretty := `{
  "type": "record",
  "name": "W",
  "fields": [ {"name": "a", "type": "string"} ]
}`
	src := startRawSchemaSource(t, map[string][]rawSchemaVersion{
		"ws": {{ID: 3, Version: 1, Schema: pretty}},
	})
	dst := startTestRegistry(t, nil)
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("first import: %v\n%s", err, out)
	}
	dst.must("PUT", "/mode?force=true", map[string]string{"mode": "IMPORT"})
	dst.put("ws", 3, 3, pretty)
	dst.must("PUT", "/mode", map[string]string{"mode": "READWRITE"})
	out, err = runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("second import: %v\n%s", err, out)
	}
	if strings.Contains(out, "conflict:") {
		t.Fatalf("lookup of the latest matching version should not conflict with v1\n%s", out)
	}
	if !strings.Contains(out, "imported 0, skipped 1") {
		t.Fatalf("expected v1 to be skipped\n%s", out)
	}
}

func TestSchemaImportDryRunRerunWithNormalizedText(t *testing.T) {
	pretty := `{
  "type": "record",
  "name": "W",
  "fields": [ {"name": "a", "type": "string"} ]
}`
	src := startRawSchemaSource(t, map[string][]rawSchemaVersion{
		"ws": {{ID: 3, Version: 1, Schema: pretty}},
	})
	dst := startTestRegistry(t, nil)
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("first import: %v\n%s", err, out)
	}
	out, err = runImport(t, "--from", src.url, "--to", dst.url, "--dry-run")
	if err != nil {
		t.Fatalf("dry-run rerun: %v\n%s", err, out)
	}
	if strings.Contains(out, "conflict:") {
		t.Fatalf("dry-run rerun reported a false conflict\n%s", out)
	}
	if !strings.Contains(out, "imported 0, skipped 1") {
		t.Fatalf("expected dry-run rerun to skip the normalized schema\n%s", out)
	}
}

func TestSchemaImportSourceMode40401IsError(t *testing.T) {
	src := seedSource(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/mode/") {
				w.Header().Set("Content-Type", srContentType)
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error_code":40401,"message":"Subject not found."}`))
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	dst := startTestRegistry(t, nil)
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err == nil || !strings.Contains(err.Error(), "read source mode") {
		t.Fatalf("expected a source mode read error for 40401, got %v\n%s", err, out)
	}
	if strings.Contains(out, "source has no /mode/{subject} endpoint") {
		t.Fatalf("a Confluent 40401 should not be treated as a missing mode endpoint\n%s", out)
	}
}

func TestSchemaImportSkipsUnreadableSoftDeletedVersion(t *testing.T) {
	pretty := `{
  "type": "record",
  "name": "W",
  "fields": [ {"name": "a", "type": "string"} ]
}`
	src := startRawSchemaSource(t, map[string][]rawSchemaVersion{
		"ws":   {{ID: 3, Version: 1, Schema: pretty}},
		"gone": {{ID: 7, Version: 1, Schema: `{"type":"string"}`, Unreadable: true, Deleted: true}},
	})
	dst := startTestRegistry(t, nil)
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err != nil {
		t.Fatalf("import: %v\n%s", err, out)
	}
	want := "warning: source subject gone version 1 is soft deleted and the source does not return it; skipped"
	if strings.Count(out, want) != 1 {
		t.Fatalf("missing or repeated unreadable soft-delete warning\n%s", out)
	}
	if !strings.Contains(out, "imported 1, skipped 1") {
		t.Fatalf("expected the unreadable version to be counted as skipped\n%s", out)
	}
	if status, data := dst.call("GET", "/subjects/ws/versions/1", nil); status != http.StatusOK {
		t.Fatalf("live schema should still be imported: %d %s", status, data)
	}
	if status, data := dst.call("GET", "/subjects/gone/versions/1?deleted=true", nil); status != http.StatusNotFound {
		t.Fatalf("unreadable soft-deleted schema should not be imported: %d %s", status, data)
	}
}

func TestSchemaImportLiveVersion40401IsError(t *testing.T) {
	src := startRawSchemaSource(t, map[string][]rawSchemaVersion{
		"ws": {{ID: 3, Version: 1, Schema: `{"type":"string"}`, Unreadable: true}},
	})
	dst := startTestRegistry(t, nil)
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err == nil || !strings.Contains(err.Error(), "read source subject ws version 1") {
		t.Fatalf("expected a live version 40401 to stop the run, got %v\n%s", err, out)
	}
	if strings.Contains(out, "is soft deleted and the source does not return it") {
		t.Fatalf("a live version 40401 should not be skipped with a warning\n%s", out)
	}
}

func TestSchemaImportSameOnTargetByIDVersionsErrorStopsRun(t *testing.T) {
	pretty := `{
  "type": "record",
  "name": "W",
  "fields": [ {"name": "a", "type": "string"} ]
}`
	src := startRawSchemaSource(t, map[string][]rawSchemaVersion{
		"ws":  {{ID: 3, Version: 1, Schema: pretty}},
		"ws2": {{ID: 3, Version: 1, Schema: pretty}},
	})
	dst := startTestRegistry(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/schemas/ids/") && strings.HasSuffix(r.URL.Path, "/versions") {
				http.Error(w, "versions failed", http.StatusInternalServerError)
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err == nil || !strings.Contains(err.Error(), "list target versions of schema id 3") {
		t.Fatalf("expected a versions lookup error to stop the run, got %v\n%s", err, out)
	}
	if strings.Contains(out, "conflict:") {
		t.Fatalf("versions lookup error was reported as a content conflict\n%s", out)
	}
}

func TestSchemaImportSameOnTargetByIDEmptyHoldersConflicts(t *testing.T) {
	pretty := `{
  "type": "record",
  "name": "W",
  "fields": [ {"name": "a", "type": "string"} ]
}`
	src := startRawSchemaSource(t, map[string][]rawSchemaVersion{
		"ws":  {{ID: 3, Version: 1, Schema: pretty}},
		"ws2": {{ID: 3, Version: 1, Schema: pretty}},
	})
	dst := startTestRegistry(t, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/schemas/ids/") && strings.HasSuffix(r.URL.Path, "/versions") {
				w.Header().Set("Content-Type", srContentType)
				_, _ = w.Write([]byte(`[]`))
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	out, err := runImport(t, "--from", src.url, "--to", dst.url)
	if err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("expected a conflict when the id has no holders, got %v\n%s", err, out)
	}
	if !strings.Contains(out, "conflict: schema id 3 already holds different content") {
		t.Fatalf("missing by-id conflict message\n%s", out)
	}
}
