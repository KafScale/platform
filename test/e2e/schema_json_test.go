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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/internal/testutil"
	"github.com/KafScale/platform/v2/pkg/schemaregistry"
	"github.com/KafScale/platform/v2/pkg/storage"
	"github.com/twmb/franz-go/pkg/sr"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Exercise the same concurrency assertions with storage that takes longer than
// the sr client's default five-second timeout, but less than the server budget.
func TestSchemaRegistryJSONConcurrentSlowStorageE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	endpoints := testutil.StartEmbeddedEtcd(t)
	release := make(chan struct{})
	store := &srSlowJSONStore{MemoryS3Client: storage.NewMemoryS3Client(), release: release}
	var urls []string
	for range 3 {
		cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cli.Close() })
		reg, err := schemaregistry.New(schemaregistry.Config{Client: cli, S3: store, Namespace: "json-slow", InlineLimit: 64})
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(schemaregistry.NewServer(reg, 10*time.Second))
		t.Cleanup(server.Close)
		urls = append(urls, server.URL)
	}
	timer := time.AfterFunc(6*time.Second, func() { close(release) })
	defer timer.Stop()
	h := &srHarness{t: t, ctx: ctx}
	h.jsonConcurrent(t, urls)
}

type srSlowJSONStore struct {
	*storage.MemoryS3Client
	release <-chan struct{}
}

func (s *srSlowJSONStore) UploadSegment(ctx context.Context, key string, body []byte) error {
	select {
	case <-s.release:
		return s.MemoryS3Client.UploadSegment(ctx, key, body)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Recheck the independently authored corpus against a real Confluent server,
// rather than relying only on its recorded responses in the unit suite.
func (h *srHarness) jsonParity(t *testing.T, cpURL, url string) {
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "pkg/schemaregistry/testdata/cp-json-compat.json"))
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Schemas       map[string]json.RawMessage
		Compatibility map[string]map[string]bool
	}
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(corpus.Schemas))
	for name := range corpus.Schemas {
		names = append(names, name)
	}
	slices.Sort(names)
	idMapping := map[int]int{}
	reverse := map[int]int{}
	subjects := map[string]string{}
	for i, name := range names {
		subject := fmt.Sprintf("json-parity-%d", i)
		subjects[name] = subject
		body := map[string]any{"schemaType": "JSON", "schema": string(corpus.Schemas[name])}
		var ids [2]int
		var canonical [2]string
		for j, base := range []string{cpURL, url} {
			srCall(t, http.MethodPut, base+"/config/"+subject, map[string]string{"compatibility": "BACKWARD"}, nil)
			var registered struct{ ID int }
			srCall(t, http.MethodPost, base+"/subjects/"+subject+"/versions", body, &registered)
			ids[j] = registered.ID
			var lookup struct {
				ID, Version        int
				SchemaType, Schema string
			}
			srCall(t, http.MethodPost, base+"/subjects/"+subject, body, &lookup)
			if lookup.ID != ids[j] || lookup.Version != 1 || lookup.SchemaType != "JSON" {
				t.Fatalf("lookup %s: %+v", name, lookup)
			}
			canonical[j] = lookup.Schema
		}
		if canonical[0] != canonical[1] {
			t.Fatalf("canonical %s: CP %s KafScale %s", name, canonical[0], canonical[1])
		}
		if prev, ok := idMapping[ids[0]]; ok && prev != ids[1] {
			t.Fatalf("dedupe mismatch %s: %v", name, ids)
		}
		if prev, ok := reverse[ids[1]]; ok && prev != ids[0] {
			t.Fatalf("over-deduplication %s: %v", name, ids)
		}
		idMapping[ids[0]] = ids[1]
		reverse[ids[1]] = ids[0]
	}
	count := 0
	for writer, readers := range corpus.Compatibility {
		for reader, want := range readers {
			body := map[string]any{"schemaType": "JSON", "schema": string(corpus.Schemas[reader])}
			for _, base := range []string{cpURL, url} {
				var result struct {
					IsCompatible bool `json:"is_compatible"`
				}
				srCall(t, http.MethodPost, base+"/compatibility/subjects/"+subjects[writer]+"/versions/latest", body, &result)
				if result.IsCompatible != want {
					t.Fatalf("%s -> %s at %s: got %v want %v", writer, reader, base, result.IsCompatible, want)
				}
			}
			count++
		}
	}
	h.jsonLevelParity(t, cpURL, url)
	t.Logf("JSON Schema live Confluent parity: %d compatibility cases, %d register/lookup/canonicalization/dedupe cases, zero mismatches", count, len(names))
}

func (h *srHarness) jsonConcurrent(t *testing.T, urls []string) {
	ctx, cancel := context.WithTimeout(h.ctx, 15*time.Second)
	defer cancel()
	clients := make([]*sr.Client, len(urls))
	for i, url := range urls {
		// The server allows ten seconds for a registration. Use the bounded
		// scenario context instead of the client's shorter five-second default.
		client, err := sr.NewClient(sr.URLs(url), sr.HTTPClient(&http.Client{}))
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = client
	}
	result := clients[0].SetCompatibility(ctx, sr.SetCompatibility{Level: sr.CompatNone}, "json-concurrent")
	if len(result) != 1 || result[0].Err != nil {
		t.Fatalf("set compatibility %+v", result)
	}
	var wg sync.WaitGroup
	type claim struct {
		schema      string
		id, version int
		err         error
		duration    time.Duration
	}
	claims := make(chan claim, 60)
	for i := range 60 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			text := fmt.Sprintf(`{"type":"object","title":"json-concurrent-%d","description":"schema text stored outside the process and shared across three independent registry processes","properties":{"name":{"type":"string"}}}`, i%20)
			start := time.Now()
			ss, err := clients[i%len(clients)].CreateSchema(ctx, "json-concurrent", sr.Schema{Type: sr.TypeJSON, Schema: text})
			if err != nil {
				err = fmt.Errorf("schema %d on %s: %w", i%20, urls[i%len(urls)], err)
			}
			claims <- claim{text, ss.ID, ss.Version, err, time.Since(start)}
		}()
	}
	wg.Wait()
	close(claims)
	ids := map[string]int{}
	owners := map[int]string{}
	versions := map[int]int{}
	var longest time.Duration
	for c := range claims {
		longest = max(longest, c.duration)
		if c.err != nil {
			t.Fatal(c.err)
		}
		if id, ok := ids[c.schema]; ok && id != c.id {
			t.Fatal("duplicate schema IDs")
		}
		if text, ok := owners[c.id]; ok && text != c.schema {
			t.Fatal("ID assigned to different schemas")
		}
		if id, ok := versions[c.version]; ok && id != c.id {
			t.Fatal("version assigned to different schemas")
		}
		ids[c.schema] = c.id
		owners[c.id] = c.schema
		versions[c.version] = c.id
	}
	if len(ids) != 20 || len(versions) != 20 {
		t.Fatalf("unique schemas=%d versions=%d", len(ids), len(versions))
	}
	t.Logf("60 concurrent registrations completed; longest request %s", longest)
	for _, client := range clients {
		all, err := client.Schemas(ctx, "json-concurrent")
		if err != nil || len(all) != 20 {
			t.Fatalf("read versions %d: %v", len(all), err)
		}
		for _, ss := range all {
			if ss.Type != sr.TypeJSON || ids[ss.Schema.Schema] != ss.ID {
				t.Fatalf("stored %+v", ss)
			}
		}
	}
}

func (h *srHarness) jsonLevelParity(t *testing.T, cpURL, url string) {
	for _, level := range []string{"NONE", "BACKWARD", "FORWARD", "FULL", "BACKWARD_TRANSITIVE", "FORWARD_TRANSITIVE", "FULL_TRANSITIVE"} {
		subject := "json-level-" + level
		first, second, candidate := `{"type":"string","minLength":0}`, `{"type":"string","minLength":2}`, `{"type":"string","minLength":1}`
		if strings.HasPrefix(level, "FORWARD") {
			first, second = second, first
		}
		if strings.HasPrefix(level, "FULL") {
			candidate = `{"type":"string","minLength":2,"title":"next"}`
		}
		if level == "NONE" {
			candidate = `{"type":"number"}`
		}
		for _, base := range []string{cpURL, url} {
			srCall(t, http.MethodPut, base+"/config/"+subject, map[string]string{"compatibility": "NONE"}, nil)
			for _, text := range []string{first, second} {
				var registered struct{ ID int }
				srCall(t, http.MethodPost, base+"/subjects/"+subject+"/versions", map[string]string{"schemaType": "JSON", "schema": text}, &registered)
			}
			srCall(t, http.MethodPut, base+"/config/"+subject, map[string]string{"compatibility": level}, nil)
		}
		body := map[string]string{"schemaType": "JSON", "schema": candidate}
		for _, suffix := range []string{"", "/latest", "/1"} {
			var cpResult, localResult struct {
				IsCompatible bool `json:"is_compatible"`
			}
			path := "/compatibility/subjects/" + subject + "/versions" + suffix
			srCall(t, http.MethodPost, cpURL+path, body, &cpResult)
			srCall(t, http.MethodPost, url+path, body, &localResult)
			if cpResult != localResult {
				t.Fatalf("%s %s: CP %+v KafScale %+v", level, suffix, cpResult, localResult)
			}
		}
		path := "/subjects/" + subject + "/versions"
		cpStatus := srCall(t, http.MethodPost, cpURL+path, body, nil)
		status := srCall(t, http.MethodPost, url+path, body, nil)
		if cpStatus != status {
			t.Fatalf("%s registration: CP %d KafScale %d", level, cpStatus, status)
		}
	}
	for _, base := range []string{cpURL, url} {
		var registered struct{ ID int }
		srCall(t, http.MethodPut, base+"/config/json-ref-leaf", map[string]string{"compatibility": "NONE"}, nil)
		for _, text := range []string{`{"type":"string"}`, `{"type":"number"}`} {
			srCall(t, http.MethodPost, base+"/subjects/json-ref-leaf/versions", map[string]string{"schemaType": "JSON", "schema": text}, &registered)
		}
		ref := []map[string]any{{"name": "leaf.json", "subject": "json-ref-leaf", "version": 1}}
		body := map[string]any{"schemaType": "JSON", "schema": `{"type":"object","properties":{"value":{"$ref":"leaf.json"}}}`, "references": ref}
		srCall(t, http.MethodPut, base+"/config/json-ref-root", map[string]string{"compatibility": "FULL_TRANSITIVE"}, nil)
		srCall(t, http.MethodPost, base+"/subjects/json-ref-root/versions", body, &registered)
		ref[0]["version"] = 2
		var result struct {
			IsCompatible bool `json:"is_compatible"`
		}
		srCall(t, http.MethodPost, base+"/compatibility/subjects/json-ref-root/versions", body, &result)
		if result.IsCompatible {
			t.Fatalf("%s: reference type change reported compatible", base)
		}
	}
	t.Log("JSON Schema live parity: all seven compatibility levels, version selection, registration rejection and referenced type changes")
}
