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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/storage"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	userV1  = `{"type":"record","name":"User","namespace":"example","fields":[{"name":"name","type":"string"}]}`
	userV2  = `{"type":"record","name":"User","namespace":"example","fields":[{"name":"name","type":"string"},{"name":"age","type":"int","default":0}]}`
	userV3  = `{"type":"record","name":"User","namespace":"example","fields":[{"name":"name","type":"string"},{"name":"email","type":"string"}]}`
	address = `{"type":"record","name":"Address","namespace":"example","fields":[{"name":"city","type":"string"}]}`
	person  = `{"type":"record","name":"Person","namespace":"example","fields":[{"name":"home","type":"example.Address"}]}`
)

type harness struct {
	t   testing.TB
	cli *clientv3.Client
	reg *Registry
	srv *httptest.Server
}

func newClient(t testing.TB, endpoints []string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func newHarness(t testing.TB, endpoints []string, cfg Config) *harness {
	t.Helper()
	if endpoints == nil {
		endpoints = testutil.StartEmbeddedEtcd(t)
	}
	cfg.Client = newClient(t, endpoints)
	if cfg.InlineLimit == 0 {
		cfg.InlineLimit = DefaultInlineLimit
	}
	reg, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewServer(reg, 5*time.Second))
	t.Cleanup(srv.Close)
	return &harness{t: t, cli: cfg.Client, reg: reg, srv: srv}
}

func (h *harness) do(method, path string, body any) (int, []byte) {
	h.t.Helper()
	var reader io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		reader = strings.NewReader(b)
	default:
		data, err := json.Marshal(b)
		if err != nil {
			h.t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, h.srv.URL+path, reader)
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	return resp.StatusCode, data
}

func (h *harness) ok(method, path string, body any, out any) {
	h.t.Helper()
	status, data := h.do(method, path, body)
	if status != http.StatusOK {
		h.t.Fatalf("%s %s: status %d: %s", method, path, status, data)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			h.t.Fatalf("%s %s: decode %s: %v", method, path, data, err)
		}
	}
}

func (h *harness) fail(method, path string, body any, status, code int) {
	h.t.Helper()
	gotStatus, data := h.do(method, path, body)
	var e errorBody
	if err := json.Unmarshal(data, &e); err != nil {
		h.t.Fatalf("%s %s: decode error body %s: %v", method, path, data, err)
	}
	if gotStatus != status || e.ErrorCode != code {
		h.t.Fatalf("%s %s: got status %d code %d (%s), want %d %d", method, path, gotStatus, e.ErrorCode, e.Message, status, code)
	}
}

func (h *harness) register(subject string, body any) int64 {
	h.t.Helper()
	var out struct{ ID int64 }
	h.ok("POST", "/subjects/"+subject+"/versions", body, &out)
	return out.ID
}

func schema(text string) map[string]any {
	return map[string]any{"schema": text}
}

func TestRegisterAndReadEndpoints(t *testing.T) {
	h := newHarness(t, nil, Config{})
	var root map[string]any
	h.ok("GET", "/", nil, &root)
	var types []string
	h.ok("GET", "/schemas/types", nil, &types)
	if !slices.Equal(types, []string{"AVRO", "JSON", "PROTOBUF"}) {
		t.Fatalf("types %v", types)
	}
	id1 := h.register("users-value", schema(userV1))
	id2 := h.register("users-value", schema(userV2))
	if id1 != 1 || id2 != 2 {
		t.Fatalf("ids %d %d", id1, id2)
	}
	var byID map[string]any
	h.ok("GET", "/schemas/ids/1", nil, &byID)
	if byID["schema"] != userV1 || byID["schemaType"] != "AVRO" || byID["references"] != nil || byID["subject"] != "users-value" || byID["version"] != 1.0 || byID["deleted"] != false {
		t.Fatalf("by id %v", byID)
	}
	if _, data := h.do("GET", "/schemas/ids/2/schema", nil); string(data) != userV2 {
		t.Fatalf("raw schema %s", data)
	}
	var idVersions []SubjectVersion
	h.ok("GET", "/schemas/ids/2/versions", nil, &idVersions)
	if len(idVersions) != 1 || idVersions[0] != (SubjectVersion{"users-value", 2}) {
		t.Fatalf("id versions %v", idVersions)
	}
	var subjects []string
	h.ok("GET", "/subjects", nil, &subjects)
	if len(subjects) != 1 || subjects[0] != "users-value" {
		t.Fatalf("subjects %v", subjects)
	}
	var versions []int
	h.ok("GET", "/subjects/users-value/versions", nil, &versions)
	if fmt.Sprint(versions) != "[1 2]" {
		t.Fatalf("versions %v", versions)
	}
	for _, v := range []string{"latest", "-1", "2"} {
		var got versionBody
		h.ok("GET", "/subjects/users-value/versions/"+v, nil, &got)
		if got.Subject != "users-value" || got.Version != 2 || got.ID != 2 || got.Schema != userV2 {
			t.Fatalf("version %s: %+v", v, got)
		}
	}
	if _, data := h.do("GET", "/subjects/users-value/versions/1/schema", nil); string(data) != userV1 {
		t.Fatalf("raw version schema %s", data)
	}
	var found versionBody
	h.ok("POST", "/subjects/users-value", schema(" "+userV1+"\n"), &found)
	if found.ID != 1 || found.Version != 1 {
		t.Fatalf("lookup %+v", found)
	}
	slashed := "team%2Fusers"
	h.register(slashed, schema(userV1))
	var got versionBody
	h.ok("GET", "/subjects/"+slashed+"/versions/1", nil, &got)
	if got.Subject != "team/users" || got.ID != 1 {
		t.Fatalf("slashed subject %+v", got)
	}
	status, data := h.do("GET", "/metrics", nil)
	if status != 200 || !strings.Contains(string(data), `kafscale_schema_registry_requests_total{code="200",route="POST /subjects/{subject}/versions"}`) || !strings.Contains(string(data), "kafscale_schema_registry_txn_conflicts_total") {
		t.Fatalf("metrics %d %s", status, data)
	}
	if status, _ := h.do("GET", "/healthz", nil); status != 200 {
		t.Fatalf("healthz %d", status)
	}
	if status, _ := h.do("GET", "/readyz", nil); status != 200 {
		t.Fatalf("readyz %d", status)
	}
}

func TestDedupe(t *testing.T) {
	h := newHarness(t, nil, Config{})
	a := h.register("a", schema(userV1))
	b := h.register("b", schema("{ \"type\" : \"record\", \"name\":\"User\",\"namespace\":\"example\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}] }"))
	again := h.register("a", schema(userV1))
	if a != 1 || b != 1 || again != 1 {
		t.Fatalf("ids %d %d %d", a, b, again)
	}
	var versions []int
	h.ok("GET", "/subjects/a/versions", nil, &versions)
	if fmt.Sprint(versions) != "[1]" {
		t.Fatalf("versions %v", versions)
	}
	var idVersions []SubjectVersion
	h.ok("GET", "/schemas/ids/1/versions", nil, &idVersions)
	if fmt.Sprint(idVersions) != "[{a 1} {b 1}]" {
		t.Fatalf("id versions %v", idVersions)
	}
	h.ok("PUT", "/config", map[string]string{"compatibility": "NONE"}, nil)
	h.register("a", schema(userV3))
	if id := h.register("a", schema(userV1)); id != 1 {
		t.Fatalf("re-register %d", id)
	}
	h.ok("GET", "/subjects/a/versions", nil, &versions)
	if fmt.Sprint(versions) != "[1 2]" {
		t.Fatalf("versions after re-register %v", versions)
	}
	h.ok("DELETE", "/subjects/a/versions/1", nil, nil)
	if id := h.register("a", schema(userV1)); id != 1 {
		t.Fatalf("re-register after delete %d", id)
	}
	h.ok("GET", "/subjects/a/versions", nil, &versions)
	if fmt.Sprint(versions) != "[2 3]" {
		t.Fatalf("versions after delete and re-register %v", versions)
	}
}

func TestErrorCodes(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	h.fail("GET", "/subjects/missing/versions", nil, 404, 40401)
	h.fail("GET", "/subjects/missing/versions/1", nil, 404, 40401)
	h.fail("POST", "/subjects/missing", schema(userV1), 404, 40401)
	h.fail("DELETE", "/subjects/missing", nil, 404, 40401)
	h.fail("GET", "/subjects/s/versions/7", nil, 404, 40402)
	h.fail("GET", "/schemas/ids/99", nil, 404, 40403)
	h.fail("GET", "/schemas/ids/abc", nil, 404, 404)
	h.fail("GET", "/schemas/ids/0", nil, 404, 40403)
	h.fail("GET", "/schemas/ids/99/schema", nil, 404, 40403)
	h.fail("GET", "/schemas/ids/99/versions", nil, 404, 40403)
	h.fail("POST", "/subjects/s", schema(userV2), 404, 40403)
	h.fail("POST", "/subjects/s/versions", schema(`{"type":"nope"}`), 422, 42201)
	h.fail("POST", "/subjects/s/versions", schema(""), 422, 42201)
	h.fail("POST", "/subjects/s/versions", map[string]any{"schema": userV1, "schemaType": "PROTOBUF"}, 422, 42201)
	h.fail("POST", "/subjects/s/versions", map[string]any{"schema": person, "references": []Reference{{Name: "example.Address", Subject: "addr", Version: 1}}}, 422, 42201)
	h.fail("POST", "/subjects/s/versions", map[string]any{"schema": person, "references": []Reference{{Name: "example.Address"}}}, 422, 42201)
	for _, v := range []string{"0", "abc", "-2", "2147483648"} {
		h.fail("GET", "/subjects/s/versions/"+v, nil, 422, 42202)
		h.fail("DELETE", "/subjects/s/versions/"+v, nil, 422, 42202)
		h.fail("GET", "/subjects/s/versions/"+v+"/schema", nil, 422, 42202)
		h.fail("POST", "/compatibility/subjects/s/versions/"+v, schema(userV1), 422, 42202)
	}
	h.fail("PUT", "/config", map[string]string{"compatibility": "FULLY"}, 422, 42203)
	h.fail("PUT", "/config/s", map[string]string{"compatibility": "bogus"}, 422, 42203)
	h.fail("PUT", "/mode", map[string]string{"mode": "bogus"}, 422, 42204)
	h.fail("POST", "/subjects/s/versions", map[string]any{"schema": userV2, "id": 5}, 422, 42205)
	h.fail("POST", "/subjects/s/versions", map[string]any{"schema": userV2, "version": 5}, 422, 42205)
	h.fail("PUT", "/mode", map[string]string{"mode": "IMPORT"}, 422, 42205)
	h.fail("PUT", "/mode/s", map[string]string{"mode": "IMPORT"}, 422, 42205)
	h.ok("PUT", "/mode/s", map[string]string{"mode": "READONLY"}, nil)
	h.fail("POST", "/subjects/s/versions", schema(userV2), 422, 42205)
	h.fail("DELETE", "/subjects/s", nil, 422, 42205)
	h.fail("DELETE", "/subjects/s/versions/1", nil, 422, 42205)
	h.ok("DELETE", "/mode/s", nil, nil)
	h.fail("POST", "/subjects/s/versions", schema(userV3), 409, 40901)
	h.register("s", schema(userV2))
	h.fail("DELETE", "/subjects/s?permanent=true", nil, 404, 40405)
	h.fail("DELETE", "/subjects/s/versions/1?permanent=true", nil, 404, 40407)
	h.ok("DELETE", "/subjects/s/versions/1", nil, nil)
	h.fail("DELETE", "/subjects/s/versions/1", nil, 404, 40406)
	h.ok("DELETE", "/subjects/s", nil, nil)
	h.fail("DELETE", "/subjects/s", nil, 404, 40404)
	h.fail("GET", "/config/s", nil, 404, 40408)
	h.fail("DELETE", "/config/s", nil, 404, 40401)
	h.fail("GET", "/mode/s", nil, 404, 40409)
	h.fail("DELETE", "/mode/s", nil, 404, 40401)
	h.register("addr", schema(address))
	h.register("person", map[string]any{"schema": person, "references": []Reference{{Name: "example.Address", Subject: "addr", Version: 1}}})
	h.fail("DELETE", "/subjects/addr", nil, 422, 42206)
	h.fail("DELETE", "/subjects/addr/versions/1", nil, 422, 42206)
	h.fail("POST", "/subjects/s/versions", "{not json", 400, 400)
	h.fail("PUT", "/config", "{", 400, 400)
	h.fail("PUT", "/mode", "{", 400, 400)
	h.fail("POST", "/subjects/s", "{", 400, 400)
	h.fail("POST", "/compatibility/subjects/addr/versions/1", "{", 400, 400)
	h.fail("POST", "/subjects/s/versions", map[string]any{"schema": userV1, "id": -1}, 422, 422)
	h.fail("GET", "/subjects/bad%0Aname/versions", nil, 422, 422)
	h.fail("GET", "/nope", nil, 404, 404)
	if err := h.cli.Close(); err != nil {
		t.Fatal(err)
	}
	h.fail("GET", "/subjects", nil, 500, 50001)
	h.fail("GET", "/schemas/ids/1000", nil, 500, 50001)
	if status, _ := h.do("GET", "/readyz", nil); status != 503 {
		t.Fatalf("readyz with closed client %d", status)
	}
}

func TestRegisterTimesOutWith50002(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	cli := newClient(t, endpoints)
	reg, err := New(Config{Client: cli, InlineLimit: DefaultInlineLimit})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Put(context.Background(), idKey(1), "occupied"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, _, err = registerIDs(reg, ctx, "s", RegisterRequest{Schema: userV1})
	var regErr *Error
	if !asError(err, &regErr) || regErr.Code != 50002 || regErr.Status != 500 {
		t.Fatalf("got %v, want 50002", err)
	}
	if reg.Conflicts() == 0 {
		t.Fatal("expected retries")
	}
	expired, cancelExpired := context.WithCancel(context.Background())
	cancelExpired()
	if _, err := reg.SchemaByID(expired, 1); !asError(err, &regErr) || regErr.Code != 50002 {
		t.Fatalf("expired context got %v", err)
	}
}

func asError(err error, target **Error) bool {
	e, ok := err.(*Error)
	if ok {
		*target = e
	}
	return ok
}

func TestSoftAndHardDelete(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	h.register("s", schema(userV2))
	var deleted int
	h.ok("DELETE", "/subjects/s/versions/latest", nil, &deleted)
	if deleted != 2 {
		t.Fatalf("deleted %d", deleted)
	}
	var versions []int
	h.ok("GET", "/subjects/s/versions", nil, &versions)
	if fmt.Sprint(versions) != "[1]" {
		t.Fatalf("versions %v", versions)
	}
	h.ok("GET", "/subjects/s/versions?deleted=true", nil, &versions)
	if fmt.Sprint(versions) != "[1 2]" {
		t.Fatalf("deleted versions %v", versions)
	}
	h.fail("GET", "/subjects/s/versions/2", nil, 404, 40402)
	var got versionBody
	h.ok("GET", "/subjects/s/versions/2?deleted=true", nil, &got)
	if got.ID != 2 {
		t.Fatalf("deleted version %+v", got)
	}
	var idVersions []SubjectVersion
	h.ok("GET", "/schemas/ids/2/versions", nil, &idVersions)
	if len(idVersions) != 0 {
		t.Fatalf("id versions %v", idVersions)
	}
	h.ok("GET", "/schemas/ids/2/versions?deleted=true", nil, &idVersions)
	if len(idVersions) != 1 {
		t.Fatalf("deleted id versions %v", idVersions)
	}
	h.fail("POST", "/subjects/s", schema(userV2), 404, 40403)
	h.ok("POST", "/subjects/s?deleted=true", schema(userV2), &got)
	if got.Version != 2 {
		t.Fatalf("lookup deleted %+v", got)
	}
	h.ok("DELETE", "/subjects/s/versions/2?permanent=true", nil, &deleted)
	h.ok("GET", "/subjects/s/versions?deleted=true", nil, &versions)
	if fmt.Sprint(versions) != "[1]" {
		t.Fatalf("versions after hard version delete %v", versions)
	}
	var list []int
	h.ok("DELETE", "/subjects/s", nil, &list)
	if fmt.Sprint(list) != "[1]" {
		t.Fatalf("soft deleted %v", list)
	}
	var subjects []string
	h.ok("GET", "/subjects", nil, &subjects)
	if len(subjects) != 0 {
		t.Fatalf("subjects %v", subjects)
	}
	h.ok("GET", "/subjects?deleted=true", nil, &subjects)
	if fmt.Sprint(subjects) != "[s]" {
		t.Fatalf("deleted subjects %v", subjects)
	}
	h.ok("DELETE", "/subjects/s?permanent=true", nil, &list)
	if fmt.Sprint(list) != "[1]" {
		t.Fatalf("hard deleted %v", list)
	}
	h.fail("GET", "/subjects/s/versions?deleted=true", nil, 404, 40401)
	for _, id := range []string{"1", "2"} {
		var byID map[string]any
		h.ok("GET", "/schemas/ids/"+id, nil, &byID)
	}
	h.ok("GET", "/schemas/ids/1/versions?deleted=true", nil, &idVersions)
	if len(idVersions) != 0 {
		t.Fatalf("index after hard delete %v", idVersions)
	}
	if id := h.register("s", schema(userV1)); id != 1 {
		t.Fatalf("same schema after hard delete got id %d", id)
	}
	if id := h.register("other", schema(userV3)); id != 3 {
		t.Fatalf("new schema after hard delete got id %d, ids must not be reused", id)
	}
	h.ok("GET", "/subjects/s/versions", nil, &versions)
	if fmt.Sprint(versions) != "[1]" {
		t.Fatalf("versions restart after hard delete %v", versions)
	}
}

func TestReferences(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("addr", schema(address))
	refs := []Reference{{Name: "example.Address", Subject: "addr", Version: 1}}
	id := h.register("person", map[string]any{"schema": person, "references": refs})
	var byID schemaBody
	h.ok("GET", "/schemas/ids/"+fmt.Sprint(id), nil, &byID)
	if len(byID.References) != 1 || byID.References[0] != refs[0] {
		t.Fatalf("references %+v", byID)
	}
	var got versionBody
	h.ok("GET", "/subjects/person/versions/1", nil, &got)
	if len(got.References) != 1 {
		t.Fatalf("version references %+v", got)
	}
	h.ok("POST", "/subjects/person", map[string]any{"schema": person, "references": refs}, &got)
	if got.ID != id {
		t.Fatalf("lookup with references %+v", got)
	}
	h.fail("POST", "/subjects/person", schema(person), 422, 42201)
	team := `{"type":"record","name":"Team","namespace":"example","fields":[{"name":"lead","type":"example.Person"}]}`
	teamID := h.register("team", map[string]any{"schema": team, "references": []Reference{{Name: "example.Person", Subject: "person", Version: 1}}})
	if teamID != id+1 {
		t.Fatalf("team id %d", teamID)
	}
	person2 := `{"type":"record","name":"Person","namespace":"example","fields":[{"name":"home","type":"example.Address"},{"name":"nick","type":"string","default":""}]}`
	h.register("person", map[string]any{"schema": person2, "references": refs})
	var compat map[string]bool
	h.ok("POST", "/compatibility/subjects/team/versions/latest", map[string]any{"schema": team, "references": []Reference{{Name: "example.Person", Subject: "person", Version: 2}}}, &compat)
	if !compat["is_compatible"] {
		t.Fatalf("nested reference compatibility %v", compat)
	}
	h.fail("DELETE", "/subjects/person/versions/1", nil, 422, 42206)
	h.ok("DELETE", "/subjects/team", nil, nil)
	h.fail("DELETE", "/subjects/person/versions/1", nil, 422, 42206)
	h.ok("DELETE", "/subjects/team?permanent=true", nil, nil)
	h.ok("DELETE", "/subjects/person", nil, nil)
	h.fail("DELETE", "/subjects/addr", nil, 422, 42206)
	h.ok("DELETE", "/subjects/person?permanent=true", nil, nil)
	h.ok("DELETE", "/subjects/addr", nil, nil)
	h.fail("POST", "/subjects/again/versions", map[string]any{"schema": person, "references": refs}, 422, 42201)
	h.ok("DELETE", "/subjects/addr?permanent=true", nil, nil)
}

func TestImportMode(t *testing.T) {
	h := newHarness(t, nil, Config{})
	var mode map[string]string
	h.ok("PUT", "/mode", map[string]string{"mode": "IMPORT"}, &mode)
	if mode["mode"] != "IMPORT" {
		t.Fatalf("mode %v", mode)
	}
	h.fail("POST", "/subjects/s/versions", schema(userV1), 422, 42205)
	imported := map[string]any{"schema": userV1, "id": 100, "version": 3}
	if id := h.register("s", imported); id != 100 {
		t.Fatalf("import id %d", id)
	}
	if id := h.register("s", imported); id != 100 {
		t.Fatalf("re-import id %d", id)
	}
	if id := h.register("s", map[string]any{"schema": userV1, "id": 100}); id != 100 {
		t.Fatalf("re-import without version %d", id)
	}
	if _, data := h.do("GET", "/schemas/ids/100/schema", nil); string(data) != userV1 {
		t.Fatalf("imported text %s", data)
	}
	h.fail("POST", "/subjects/s/versions", map[string]any{"schema": userV2, "id": 100, "version": 4}, 422, 42205)
	if id := h.register("s", map[string]any{"schema": " " + userV1, "id": 100, "version": 4}); id != 100 {
		t.Fatalf("import of the same canonical form %d", id)
	}
	h.fail("POST", "/subjects/s/versions", map[string]any{"schema": userV2, "id": 101, "version": 3}, 422, 42205)
	if id := h.register("s", map[string]any{"schema": userV3, "id": 50, "version": 1}); id != 50 {
		t.Fatalf("incompatible import id %d", id)
	}
	if id := h.register("s", map[string]any{"schema": userV2, "id": 101}); id != 101 {
		t.Fatalf("import without version %d", id)
	}
	var versions []int
	h.ok("GET", "/subjects/s/versions", nil, &versions)
	if fmt.Sprint(versions) != "[1 3 4 5]" {
		t.Fatalf("versions %v", versions)
	}
	h.ok("PUT", "/mode", map[string]string{"mode": "READWRITE"}, nil)
	h.ok("PUT", "/config", map[string]string{"compatibility": "NONE"}, nil)
	next := `{"type":"record","name":"Next","fields":[{"name":"x","type":"long"}]}`
	if id := h.register("t", schema(next)); id != 102 {
		t.Fatalf("id after import %d, want 102", id)
	}
	h.fail("PUT", "/mode", map[string]string{"mode": "IMPORT"}, 422, 42205)
	h.ok("PUT", "/mode?force=true", map[string]string{"mode": "IMPORT"}, nil)
	h.ok("PUT", "/mode", map[string]string{"mode": "READWRITE"}, nil)
	h.ok("PUT", "/mode/fresh", map[string]string{"mode": "IMPORT"}, nil)
	if id := h.register("fresh", map[string]any{"schema": userV1, "id": 100, "version": 1}); id != 100 {
		t.Fatalf("subject import reused id %d", id)
	}
	h.fail("PUT", "/mode/s", map[string]string{"mode": "IMPORT"}, 422, 42205)
	h.ok("PUT", "/mode/s?force=true", map[string]string{"mode": "IMPORT"}, nil)
	h.register("s", map[string]any{"schema": address, "id": 7, "version": 9})
	var counter int64
	resp, err := h.cli.Get(context.Background(), counterKey())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Sscan(string(resp.Kvs[0].Value), &counter)
	if counter != 102 {
		t.Fatalf("counter %d, want 102", counter)
	}
}

func TestConfigAndModePrecedence(t *testing.T) {
	h := newHarness(t, nil, Config{})
	var cfg map[string]string
	h.ok("GET", "/config", nil, &cfg)
	if cfg["compatibilityLevel"] != "BACKWARD" {
		t.Fatalf("default config %v", cfg)
	}
	h.ok("GET", "/config/s?defaultToGlobal=true", nil, &cfg)
	if cfg["compatibilityLevel"] != "BACKWARD" {
		t.Fatalf("subject default config %v", cfg)
	}
	h.ok("PUT", "/config", map[string]string{"compatibility": "NONE"}, &cfg)
	if cfg["compatibility"] != "NONE" {
		t.Fatalf("put config %v", cfg)
	}
	h.ok("GET", "/config/s?defaultToGlobal=true", nil, &cfg)
	if cfg["compatibilityLevel"] != "NONE" {
		t.Fatalf("subject inherits global %v", cfg)
	}
	h.ok("PUT", "/config/s", map[string]string{"compatibility": "BACKWARD"}, nil)
	h.ok("GET", "/config/s", nil, &cfg)
	if cfg["compatibilityLevel"] != "BACKWARD" {
		t.Fatalf("subject config %v", cfg)
	}
	h.register("s", schema(userV1))
	h.fail("POST", "/subjects/s/versions", schema(userV3), 409, 40901)
	h.register("other", schema(userV1))
	h.register("other", schema(userV3))
	h.ok("DELETE", "/config/s", nil, &cfg)
	if cfg["compatibilityLevel"] != "BACKWARD" {
		t.Fatalf("deleted config %v", cfg)
	}
	h.register("s", schema(userV3))
	var mode map[string]string
	h.ok("GET", "/mode", nil, &mode)
	if mode["mode"] != "READWRITE" {
		t.Fatalf("default mode %v", mode)
	}
	h.ok("PUT", "/mode", map[string]string{"mode": "READONLY"}, nil)
	h.ok("GET", "/mode/s?defaultToGlobal=true", nil, &mode)
	if mode["mode"] != "READONLY" {
		t.Fatalf("subject inherits global mode %v", mode)
	}
	h.fail("POST", "/subjects/s/versions", schema(userV2), 422, 42205)
	h.ok("PUT", "/mode/s", map[string]string{"mode": "READWRITE"}, nil)
	h.ok("GET", "/mode/s", nil, &mode)
	if mode["mode"] != "READWRITE" {
		t.Fatalf("subject mode %v", mode)
	}
	h.register("s", schema(userV2))
	h.fail("POST", "/subjects/other/versions", schema(userV2), 422, 42205)
	h.ok("DELETE", "/mode/s", nil, &mode)
	if mode["mode"] != "READWRITE" {
		t.Fatalf("deleted mode %v", mode)
	}
	h.fail("DELETE", "/subjects/s/versions/1", nil, 422, 42205)
}

func TestCompatibilityEndpoint(t *testing.T) {
	h := newHarness(t, nil, Config{})
	var missing map[string]bool
	h.ok("POST", "/compatibility/subjects/s/versions/latest", schema(userV2), &missing)
	if !missing["is_compatible"] {
		t.Fatalf("missing subject %v", missing)
	}
	h.fail("POST", "/compatibility/subjects/s/versions/2", schema(userV2), 404, 40402)
	h.register("s", schema(userV1))
	var out struct {
		IsCompatible bool     `json:"is_compatible"`
		Messages     []string `json:"messages"`
	}
	h.ok("POST", "/compatibility/subjects/s/versions/latest", schema(userV2), &out)
	if !out.IsCompatible || out.Messages != nil {
		t.Fatalf("compatible %+v", out)
	}
	h.ok("POST", "/compatibility/subjects/s/versions/1?verbose=true", schema(userV3), &out)
	if out.IsCompatible || len(out.Messages) != 4 {
		t.Fatalf("incompatible %+v", out)
	}
	h.ok("POST", "/compatibility/subjects/s/versions/1?verbose=true", schema(userV2), &out)
	if !out.IsCompatible || out.Messages == nil || len(out.Messages) != 0 {
		t.Fatalf("verbose compatible %+v", out)
	}
	h.fail("POST", "/compatibility/subjects/s/versions/2", schema(userV2), 404, 40402)
	h.fail("POST", "/compatibility/subjects/s/versions/1", schema(`{"type":"bad"}`), 422, 42201)
	h.ok("PUT", "/config/s", map[string]string{"compatibility": "NONE"}, nil)
	h.ok("POST", "/compatibility/subjects/s/versions/1", schema(userV3), &out)
	if !out.IsCompatible {
		t.Fatalf("NONE %+v", out)
	}
}

func TestLargeSchemaGoesToS3(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	s3 := storage.NewMemoryS3Client()
	h := newHarness(t, endpoints, Config{S3: s3, Namespace: "prod", InlineLimit: 64})
	// Model a prior attempt that wrote S3 text before publishing its etcd record.
	req := RegisterRequest{Schema: userV2}
	prepared, err := h.reg.prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	writer := &recordWriter{r: h.reg, req: req, p: prepared}
	if _, err := writer.record(context.Background()); err != nil {
		t.Fatal(err)
	}
	id := h.register("big", schema(userV2))
	resp, err := h.cli.Get(context.Background(), idKey(id))
	if err != nil {
		t.Fatal(err)
	}
	var rec schemaRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Schema != "" || !strings.HasPrefix(rec.S3Key, "prod/@schemas/sha256/") {
		t.Fatalf("record %+v", rec)
	}
	fresh := newHarness(t, endpoints, Config{S3: s3, Namespace: "prod", InlineLimit: 64})
	if _, data := fresh.do("GET", "/schemas/ids/1/schema", nil); string(data) != userV2 {
		t.Fatalf("text from S3 %s", data)
	}
	noS3 := newHarness(t, endpoints, Config{InlineLimit: 64})
	noS3.fail("GET", "/schemas/ids/1", nil, 500, 50001)
	noS3.fail("POST", "/subjects/other/versions", schema(userV3), 422, 42201)
	emptyS3 := newHarness(t, endpoints, Config{S3: storage.NewMemoryS3Client(), Namespace: "prod", InlineLimit: 64})
	emptyS3.fail("GET", "/schemas/ids/1", nil, 500, 50001)
}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error without client")
	}
	cli := &clientv3.Client{}
	if _, err := New(Config{Client: cli}); err == nil {
		t.Fatal("expected error without inline limit")
	}
	if _, err := New(Config{Client: cli, InlineLimit: 1, S3: storage.NewMemoryS3Client()}); err == nil {
		t.Fatal("expected error without namespace")
	}
}

func TestCorruptStateFailsClosed(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	ctx := context.Background()
	if _, err := h.cli.Put(ctx, versionKey("s", 2), "{"); err != nil {
		t.Fatal(err)
	}
	h.fail("GET", "/subjects/s/versions", nil, 500, 50001)
	h.fail("GET", "/subjects", nil, 500, 50001)
	h.fail("POST", "/subjects/s/versions", schema(userV2), 500, 50001)
	if _, err := h.cli.Put(ctx, versionKey("s", 2), `{"id":1}`); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cli.Put(ctx, versionsPrefix("s")+"x", `{"id":1}`); err != nil {
		t.Fatal(err)
	}
	h.fail("GET", "/subjects/s/versions", nil, 500, 50001)
	if _, err := h.cli.Delete(ctx, versionsPrefix("s")+"x"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cli.Put(ctx, idKey(9), "{"); err != nil {
		t.Fatal(err)
	}
	h.fail("GET", "/schemas/ids/9", nil, 500, 50001)
	if _, err := h.cli.Put(ctx, counterKey(), "x"); err != nil {
		t.Fatal(err)
	}
	h.fail("POST", "/subjects/n/versions", schema(address), 500, 50001)
	if _, err := h.cli.Put(ctx, versionKey("ghost", 1), `{"id":77}`); err != nil {
		t.Fatal(err)
	}
	h.fail("GET", "/subjects/ghost/versions/1", nil, 500, 50001)
	h.fail("POST", "/subjects/x/versions", map[string]any{"schema": person, "references": []Reference{{Name: "example.Address", Subject: "ghost", Version: 1}}}, 500, 50001)
}

func registerIDs(reg *Registry, ctx context.Context, subject string, req RegisterRequest) (int64, int, error) {
	v, err := reg.Register(ctx, subject, req)
	if err != nil {
		return 0, 0, err
	}
	return v.Schema.ID, v.Version, nil
}
