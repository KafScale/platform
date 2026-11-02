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
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/storage"
)

func TestNewEndpoints(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("addr", schema(address))
	h.register("person", map[string]any{"schema": person, "references": []Reference{{Name: "example.Address", Subject: "addr", Version: 1}}})
	h.register("other", map[string]any{"schema": person, "references": []Reference{{Name: "example.Address", Subject: "addr", Version: 1}}})
	var ids []int64
	h.ok("GET", "/subjects/addr/versions/1/referencedby", nil, &ids)
	if fmt.Sprint(ids) != "[2]" {
		t.Fatalf("referencedby %v", ids)
	}
	h.fail("GET", "/subjects/nope/versions/1/referencedby", nil, 404, 40401)
	h.fail("GET", "/subjects/addr/versions/9/referencedby", nil, 404, 40402)
	var subjects []string
	h.ok("GET", "/schemas/ids/2/subjects", nil, &subjects)
	if fmt.Sprint(subjects) != "[other person]" {
		t.Fatalf("id subjects %v", subjects)
	}
	h.fail("GET", "/schemas/ids/9/subjects", nil, 404, 40403)
	var contexts []string
	h.ok("GET", "/contexts", nil, &contexts)
	if fmt.Sprint(contexts) != "[.]" {
		t.Fatalf("contexts %v", contexts)
	}
	var all []map[string]any
	h.ok("GET", "/schemas", nil, &all)
	if len(all) != 3 || all[0]["subject"] != "addr" || all[2]["subject"] != "person" {
		t.Fatalf("schemas %v", all)
	}
	h.ok("GET", "/schemas?subjectPrefix=pe", nil, &all)
	if len(all) != 1 || all[0]["subject"] != "person" {
		t.Fatalf("prefix %v", all)
	}
	h.ok("GET", "/schemas?offset=1&limit=1", nil, &all)
	if len(all) != 1 || all[0]["subject"] != "other" {
		t.Fatalf("paged %v", all)
	}
	var cfg map[string]string
	h.ok("DELETE", "/config", nil, &cfg)
	if cfg["compatibilityLevel"] != CompatBackward {
		t.Fatalf("unset global delete %v", cfg)
	}
	h.ok("PUT", "/config", map[string]string{"compatibility": "NONE"}, nil)
	h.ok("DELETE", "/config", nil, &cfg)
	if cfg["compatibilityLevel"] != CompatNone {
		t.Fatalf("global delete %v", cfg)
	}
	h.ok("GET", "/config", nil, &cfg)
	if cfg["compatibilityLevel"] != CompatBackward {
		t.Fatalf("after delete %v", cfg)
	}
	var compat struct {
		IsCompatible bool `json:"is_compatible"`
	}
	h.ok("POST", "/compatibility/subjects/addr/versions", schema(address), &compat)
	if !compat.IsCompatible {
		t.Fatal("compatibility against latest")
	}
}

func TestResponseShapes(t *testing.T) {
	h := newHarness(t, nil, Config{})
	_, data := h.do("POST", "/subjects/s/versions", schema(userV1))
	if string(data) != `{"id":1,"version":1,"guid":"b4233e53-d42c-78fc-f52b-daf039258374","schemaType":"AVRO","schema":`+mustQuote(userV1)+`}` {
		t.Fatalf("register %s", data)
	}
	var reg map[string]any
	_ = json.Unmarshal(data, &reg)
	if _, ok := reg["subject"]; ok {
		t.Fatalf("register has subject %s", data)
	}
	var byID map[string]any
	h.ok("GET", "/schemas/ids/1", nil, &byID)
	if byID["subject"] != "s" || byID["version"] != float64(1) || byID["schemaType"] != "AVRO" {
		t.Fatalf("byID %v", byID)
	}
}

func mustQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestCanonicalDedupeAndNormalize(t *testing.T) {
	h := newHarness(t, nil, Config{})
	a := `{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"string","a":1,"b":2}}]}`
	b := `{"fields":[{"type":{"b":2,"type":"string","a":1},"name":"f"}],"name":"R","type":"record"}`
	spaced := "{ \"type\" : \"record\", \"name\":\"R\",\"fields\":[{\"name\":\"f\",\"type\":{\"type\":\"string\",\"a\":1,\"b\":2}}]}"
	id := h.register("s", schema(a))
	if got := h.register("s", schema(spaced)); got != id {
		t.Fatalf("whitespace gave id %d", got)
	}
	var vs []int
	got := h.register("s", schema(b))
	h.ok("GET", "/subjects/s/versions", nil, &vs)
	if got != id || fmt.Sprint(vs) != "[1]" {
		t.Fatalf("prop order within subject id %d versions %v", got, vs)
	}
	if other := h.register("t", schema(b)); other == id {
		t.Fatalf("different canonical text shared id %d", other)
	}
	var n, m struct{ ID int64 }
	h.ok("POST", "/subjects/u/versions?normalize=true", schema(b), &n)
	h.ok("POST", "/subjects/u/versions?normalize=true", schema(a), &m)
	if m.ID != n.ID {
		t.Fatalf("normalize ids %d %d", n, m)
	}
	var text string
	_, raw := h.do("GET", "/schemas/ids/1/schema", nil)
	text = string(raw)
	if text != `{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"string","a":1,"b":2}}]}` {
		t.Fatalf("stored text %s", text)
	}
}

func TestVersionNumbering(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.ok("PUT", "/config", map[string]string{"compatibility": "NONE"}, nil)
	h.register("s", schema(userV1))
	h.register("s", schema(userV2))
	h.ok("DELETE", "/subjects/s/versions/2", nil, nil)
	h.ok("DELETE", "/subjects/s/versions/2?permanent=true", nil, nil)
	var out struct{ Version int }
	h.ok("POST", "/subjects/s/versions", schema(userV3), &out)
	if out.Version != 2 {
		t.Fatalf("after hard delete of latest got version %d, want 2", out.Version)
	}
	h.ok("DELETE", "/subjects/s/versions/1", nil, nil)
	h.ok("POST", "/subjects/s/versions", schema(userV1), &out)
	var vs []int
	h.ok("GET", "/subjects/s/versions?deleted=true", nil, &vs)
	if out.Version != 3 || fmt.Sprint(vs) != "[2 3]" {
		t.Fatalf("re-register soft deleted version %d versions %v", out.Version, vs)
	}
}

func TestLookupReturnsHighestMatch(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.ok("PUT", "/config", map[string]string{"compatibility": "NONE"}, nil)
	h.ok("PUT", "/mode/s", map[string]string{"mode": "IMPORT"}, nil)
	h.register("s", map[string]any{"schema": userV1, "id": 5, "version": 1})
	h.register("s", map[string]any{"schema": userV2, "id": 6, "version": 2})
	h.register("s", map[string]any{"schema": userV1, "id": 7, "version": 3})
	var out map[string]any
	h.ok("POST", "/subjects/s", schema(userV1), &out)
	if out["version"] != float64(3) || out["id"] != float64(7) {
		t.Fatalf("lookup %v", out)
	}
	h.ok("DELETE", "/subjects/s/versions/3", nil, nil)
	h.ok("POST", "/subjects/s", schema(userV1), &out)
	if out["version"] != float64(1) {
		t.Fatalf("lookup after delete %v", out)
	}
	h.ok("POST", "/subjects/s?deleted=true", schema(userV1), &out)
	if out["version"] != float64(3) || out["id"] != float64(7) {
		t.Fatalf("lookup deleted %v", out)
	}
}

type countingS3 struct {
	*storage.MemoryS3Client
	uploads atomic.Int64
}

func (c *countingS3) UploadSegment(ctx context.Context, key string, body []byte) error {
	c.uploads.Add(1)
	return c.MemoryS3Client.UploadSegment(ctx, key, body)
}

func TestS3UploadOncePerRegister(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	s3 := &countingS3{MemoryS3Client: storage.NewMemoryS3Client()}
	h := newHarness(t, endpoints, Config{S3: s3, Namespace: "prod", InlineLimit: 64})
	bumps := 0
	h.reg.beforeCommit = func() {
		if bumps < 2 {
			bumps++
			if _, err := h.cli.Put(context.Background(), counterKey(), fmt.Sprint(bumps)); err != nil {
				t.Error(err)
			}
		}
	}
	h.register("big", schema(userV2))
	if bumps != 2 || s3.uploads.Load() != 1 {
		t.Fatalf("bumps %d uploads %d", bumps, s3.uploads.Load())
	}
}

func TestCacheResetsAfterEtcdRestore(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.ok("PUT", "/config", map[string]string{"compatibility": "NONE"}, nil)
	h.register("s", schema(userV1))
	h.register("s", schema(userV2))
	h.register("s", schema(userV3))
	h.reg.cli = newClient(t, testutil.StartEmbeddedEtcd(t))
	h.register("t", schema(address))
	_, data := h.do("GET", "/schemas/ids/1/schema", nil)
	if string(data) != address {
		t.Fatalf("stale cache %s", data)
	}
	if h.reg.CacheResets() == 0 {
		t.Fatal("no cache reset")
	}
}

func TestRegisterRaceWithImportBelowCounter(t *testing.T) {
	h := newHarness(t, nil, Config{})
	ctx := context.Background()
	h.ok("PUT", "/mode/seed", map[string]string{"mode": "IMPORT"}, nil)
	h.register("seed", map[string]any{"schema": userV2, "id": 100})
	h.ok("PUT", "/mode/imp", map[string]string{"mode": "IMPORT"}, nil)
	var once atomic.Bool
	peer := h.peer()
	h.reg.beforeCommit = func() {
		if once.CompareAndSwap(false, true) {
			if _, _, err := registerIDs(peer, ctx, "imp", RegisterRequest{Schema: userV1, ID: 50}); err != nil {
				t.Error(err)
			}
		}
	}
	id, _, err := registerIDs(h.reg, ctx, "s", RegisterRequest{Schema: userV1})
	if err != nil {
		t.Fatal(err)
	}
	if id != 50 {
		t.Fatalf("id %d, want imported id 50", id)
	}
}

func TestRegisterRaceSameSubject(t *testing.T) {
	h := newHarness(t, nil, Config{})
	ctx := context.Background()
	h.ok("PUT", "/config", map[string]string{"compatibility": "NONE"}, nil)
	h.register("s", schema(userV1))
	h.register("x", schema(userV2))
	var once atomic.Bool
	peer := h.peer()
	h.reg.beforeCommit = func() {
		if once.CompareAndSwap(false, true) {
			if _, _, err := registerIDs(peer, ctx, "s", RegisterRequest{Schema: userV3}); err != nil {
				t.Error(err)
			}
		}
	}
	if _, _, err := registerIDs(h.reg, ctx, "s", RegisterRequest{Schema: userV2}); err != nil {
		t.Fatal(err)
	}
	h.reg.beforeCommit = nil
	var vs []int
	h.ok("GET", "/subjects/s/versions", nil, &vs)
	seen := map[float64]bool{}
	for _, v := range vs {
		var out map[string]any
		h.ok("GET", fmt.Sprintf("/subjects/s/versions/%d", v), nil, &out)
		seen[out["id"].(float64)] = true
	}
	if fmt.Sprint(vs) != "[1 2 3]" || len(seen) != 3 {
		t.Fatalf("versions %v ids %v", vs, seen)
	}
}

func TestSoftDeletedVersionFlag(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	h.ok("DELETE", "/subjects/s/versions/1", nil, nil)
	var out map[string]any
	h.ok("GET", "/subjects/s/versions/1?deleted=true", nil, &out)
	if out["deleted"] != true {
		t.Fatalf("soft deleted version %v", out)
	}
}

func (h *harness) peer() *Registry {
	h.t.Helper()
	reg, err := New(Config{Client: h.cli, InlineLimit: DefaultInlineLimit})
	if err != nil {
		h.t.Fatal(err)
	}
	return reg
}
