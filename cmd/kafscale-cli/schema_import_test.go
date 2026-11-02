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
