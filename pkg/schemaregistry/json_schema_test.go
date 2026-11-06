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
	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/storage"
	js "github.com/santhosh-tekuri/jsonschema/v6"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// The cases were authored for this implementation and their expected outcomes
// collected through the public API of a running Confluent 8.4.0 registry.
func TestJSONConfluentCompatibility(t *testing.T) {
	data, err := os.ReadFile("testdata/cp-json-compat.json")
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
	for writer, readers := range corpus.Compatibility {
		for reader, want := range readers {
			t.Run(writer+" -> "+reader, func(t *testing.T) {
				err := checkJSONReads(string(corpus.Schemas[reader]), nil, string(corpus.Schemas[writer]), nil)
				if (err == nil) != want {
					t.Errorf("compatible=%v, want %v: %v", err == nil, want, err)
				}
			})
		}
	}
}
func TestJSONConfluentCanonical(t *testing.T) {
	data, err := os.ReadFile("testdata/cp-json-canonical.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Text      string
		Normalize bool
		Stored    string
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		got, err := canonicalJSONSchema(tc.Text, tc.Normalize)
		if err != nil || got != tc.Stored {
			t.Errorf("%s normalize=%v: %s %v, want %s", tc.Text, tc.Normalize, got, err, tc.Stored)
		}
	}
}

func jsonBody(text string, refs ...Reference) map[string]any {
	body := map[string]any{"schemaType": "JSON", "schema": text}
	if len(refs) > 0 {
		body["references"] = refs
	}
	return body
}
func TestJSONRegistryEndpoints(t *testing.T) {
	h := newHarness(t, nil, Config{})
	text := `{"type":"object","properties":{"value":{"type":"string"}},"additionalProperties":false}`
	id := h.register("json", jsonBody(text))
	if again := h.register("json", jsonBody("  "+text+"  ")); again != id {
		t.Fatal("whitespace changed identity")
	}
	var got versionBody
	h.ok("POST", "/subjects/json", jsonBody(text), &got)
	if got.ID != id || got.Version != 1 || got.SchemaType != "JSON" || got.Schema != text {
		t.Fatalf("lookup %+v", got)
	}
	h.ok("GET", fmt.Sprintf("/schemas/ids/%d", id), nil, &got)
	if got.SchemaType != "JSON" || got.Schema != text {
		t.Fatalf("fetch %+v", got)
	}
	h.ok("GET", "/subjects/json/versions/1", nil, &got)
	if got.ID != id || got.SchemaType != "JSON" {
		t.Fatalf("version %+v", got)
	}
	h.fail("POST", "/subjects/json/versions", jsonBody(`{"type":"object","properties":{"value":{"type":"number"}},"additionalProperties":false}`), 409, 40901)
	h.fail("POST", "/subjects/json", jsonBody(`{"type":"object"}`), 404, 40403)
	for _, invalid := range []string{`[]`, `1`, `"x"`, `null`, `{`, `{"type":"bad"}`, `{"$ref":"file:///etc/passwd"}`, `{"$ref":"https://127.0.0.1:1/no"}`, `{"type":"string","pattern":"["}`, `{"minimum":1e10001}`} {
		h.fail("POST", "/subjects/bad/versions", jsonBody(invalid), 422, 42201)
	}
	// Reordered properties are distinct without normalize=true, including lookup.
	h.ok("PUT", "/config/json", map[string]string{"compatibility": "NONE"}, nil)
	reordered := `{"additionalProperties":false,"properties":{"value":{"type":"string"}},"type":"object"}`
	id2 := h.register("json", jsonBody(reordered))
	if id2 == id {
		t.Fatal("order ignored without normalization")
	}
	h.ok("POST", "/subjects/json?normalize=true", jsonBody(text), &got)
	if got.ID != id2 {
		t.Fatalf("normalized lookup %+v", got)
	}
	// Explicit null defaults are retained by the compiler.
	if err := checkJSONReads(`{"type":"object","properties":{"value":{"type":"string","default":null}},"required":["value"],"additionalProperties":false}`, nil, text, nil); err != nil {
		t.Fatal(err)
	}
}
func TestJSONReferences(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("leaf", jsonBody(`{"type":"string"}`))
	h.ok("PUT", "/config/leaf", map[string]string{"compatibility": "NONE"}, nil)
	h.register("leaf", jsonBody(`{"type":"number"}`))
	middle := `{"$ref":"leaf.json"}`
	h.register("middle", jsonBody(middle, Reference{Name: "leaf.json", Subject: "leaf", Version: 1}))
	root := `{"type":"object","properties":{"value":{"$ref":"middle.json"}}}`
	ref := Reference{Name: "middle.json", Subject: "middle", Version: 1}
	id := h.register("root", jsonBody(root, ref))
	var got versionBody
	h.ok("POST", "/subjects/root", jsonBody(root, ref), &got)
	if got.ID != id {
		t.Fatal("reference lookup differs")
	}
	h.fail("POST", "/subjects/missing/versions", jsonBody(root), 422, 42201)
	// Changing a referenced type participates in compatibility, even if root text is identical.
	h.ok("PUT", "/config/middle", map[string]string{"compatibility": "NONE"}, nil)
	h.register("middle", jsonBody(middle, Reference{Name: "leaf.json", Subject: "leaf", Version: 2}))
	var result struct {
		IsCompatible bool `json:"is_compatible"`
	}
	ref.Version = 2
	h.ok("POST", "/compatibility/subjects/root/versions/latest", jsonBody(root, ref), &result)
	if result.IsCompatible {
		t.Fatal("incompatible reference accepted")
	}
	h.register("avro", schema(userV1))
	h.fail("POST", "/subjects/wrongtype/versions", jsonBody(`{"$ref":"leaf.json"}`, Reference{Name: "leaf.json", Subject: "avro", Version: 1}), 422, 42201)
	h.fail("DELETE", "/subjects/leaf/versions/1", nil, 422, 42206)
	// Absolute IDs and relative resource names must resolve without network access.
	if _, err := parseJSONSchema(`{"$id":"https://example.invalid/root.json","$ref":"defs/leaf.json"}`, []resolvedReference{{Name: "defs/leaf.json", Text: `{"type":"string"}`, Type: "JSON"}}); err != nil {
		t.Fatal(err)
	}
}
func TestJSONGUIDCollision(t *testing.T) {
	for _, importMode := range []bool{false, true} {
		t.Run(fmt.Sprint(importMode), func(t *testing.T) {
			h := newHarness(t, nil, Config{})
			text := `{"type":"string","title":"cross-type"}`
			id := h.register("avro", schema(text))
			body := jsonBody(text)
			if importMode {
				h.ok("PUT", "/mode/json", map[string]string{"mode": "IMPORT"}, nil)
				body["id"] = id + 1
			}
			h.fail("POST", "/subjects/json/versions", body, 422, 42205)
			var got versionBody
			h.ok("GET", fmt.Sprintf("/schemas/ids/%d", id), nil, &got)
			if got.Schema != text || got.SchemaType != "AVRO" {
				t.Fatalf("Avro overwritten %+v", got)
			}
		})
	}
	// Independent instances racing on the same type-independent GUID must leave one winner.
	endpoints := testutil.StartEmbeddedEtcd(t)
	a := newHarness(t, endpoints, Config{})
	b := newHarness(t, endpoints, Config{})
	var wg sync.WaitGroup
	statuses := make(chan int, 2)
	for i, h := range []*harness{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := schema(`{"type":"string","title":"race"}`)
			if i == 1 {
				body["schemaType"] = "JSON"
			}
			status, _ := h.do("POST", fmt.Sprintf("/subjects/race%d/versions", i), body)
			statuses <- status
		}()
	}
	wg.Wait()
	close(statuses)
	wins, rejected := 0, 0
	for status := range statuses {
		if status == 200 {
			wins++
		} else if status == 422 {
			rejected++
		} else {
			t.Errorf("unexpected status %d", status)
		}
	}
	if wins != 1 || rejected != 1 {
		t.Fatalf("wins=%d rejected=%d", wins, rejected)
	}
}
func TestJSONThreeRegistryConcurrency(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	store := storage.NewMemoryS3Client()
	var pods []*harness
	for range 3 {
		pods = append(pods, newHarness(t, endpoints, Config{S3: store, Namespace: "json", InlineLimit: 64}))
	}
	pods[0].ok("PUT", "/config/concurrent", map[string]string{"compatibility": "NONE"}, nil)
	var wg sync.WaitGroup
	results := make(chan struct {
		Text string
		ID   int64
	}, 36)
	for i := range 36 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			text := fmt.Sprintf(`{"type":"object","description":"%s","properties":{"value":{"type":"string"}},"title":"v%d"}`, strings.Repeat("x", 80), i%12)
			id := pods[i%3].register("concurrent", jsonBody(text))
			results <- struct {
				Text string
				ID   int64
			}{text, id}
		}()
	}
	wg.Wait()
	close(results)
	ids := map[string]int64{}
	owners := map[int64]string{}
	for result := range results {
		if id, ok := ids[result.Text]; ok && id != result.ID {
			t.Fatal("duplicate IDs")
		}
		if owner, ok := owners[result.ID]; ok && owner != result.Text {
			t.Fatal("ID reused")
		}
		ids[result.Text] = result.ID
		owners[result.ID] = result.Text
	}
	var versions []int
	pods[0].ok("GET", "/subjects/concurrent/versions", nil, &versions)
	if len(versions) != 12 {
		t.Fatalf("versions %v", versions)
	}
	fresh := newHarness(t, endpoints, Config{S3: store, Namespace: "json", InlineLimit: 64})
	for text, id := range ids {
		var got versionBody
		fresh.ok("GET", fmt.Sprintf("/schemas/ids/%d", id), nil, &got)
		if got.Schema != text || got.SchemaType != "JSON" {
			t.Fatalf("cold read %+v", got)
		}
	}
}

func TestJSONCompatibilityLevels(t *testing.T) {
	h := newHarness(t, nil, Config{})
	for _, level := range []string{CompatNone, CompatBackward, CompatForward, CompatFull, CompatBackwardAll, CompatForwardAll, CompatFullAll} {
		subject := "level-" + level
		h.ok("PUT", "/config/"+subject, map[string]string{"compatibility": "NONE"}, nil)
		first, second, candidate := `{"type":"string","minLength":0}`, `{"type":"string","minLength":2}`, `{"type":"string","minLength":1}`
		if strings.HasPrefix(level, "FORWARD") {
			first, second = second, first
		}
		if strings.HasPrefix(level, "FULL") {
			candidate = `{"type":"string","minLength":2,"title":"new"}`
		}
		if level == CompatNone {
			candidate = `{"type":"number"}`
		}
		h.register(subject, jsonBody(first))
		h.register(subject, jsonBody(second))
		h.ok("PUT", "/config/"+subject, map[string]string{"compatibility": level}, nil)
		want := !strings.HasSuffix(level, "TRANSITIVE")
		var result struct {
			IsCompatible bool `json:"is_compatible"`
		}
		h.ok("POST", "/compatibility/subjects/"+subject+"/versions", jsonBody(candidate), &result)
		if result.IsCompatible != want {
			t.Fatalf("%s compatible=%v want %v", level, result.IsCompatible, want)
		}
		if want {
			h.register(subject, jsonBody(candidate))
		} else {
			h.fail("POST", "/subjects/"+subject+"/versions", jsonBody(candidate), 409, 40901)
		}
	}
}

func TestJSONValidationFailures(t *testing.T) {
	for _, tc := range []struct {
		text string
		refs []resolvedReference
	}{
		{`{`, nil},
		{`{"properties":[]}`, nil},
		{`{"$id":"%xx"}`, nil},
		{`{}`, []resolvedReference{{Name: "bad.json", Text: `{`}}},
		{`{}`, []resolvedReference{{Name: "%xx", Text: `{}`}}},
		{`{}`, []resolvedReference{{Name: "same.json", Text: `{}`}, {Name: "same.json", Text: `{}`}}},
		{`{"$id":"https://example.invalid/root.json","$ref":"same.json"}`, []resolvedReference{{Name: "same.json", Text: `{}`}, {Name: "https://kafscale.invalid/same.json", Text: `{}`}}},
		{`{}`, []resolvedReference{{Name: jsonSchemaURL, Text: `{}`}}},
		{`{"additionalItems":12}`, nil},
		{`{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalItems":12}`, nil},
		{`{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalItems":{"$ref":"missing"}}`, nil},
		{`{"$schema":"https://json-schema.org/draft/2020-12/schema","properties":{"a":{"additionalItems":{"$ref":"missing"}}}}`, nil},
		{`{"$schema":"https://json-schema.org/draft/2020-12/schema","properties":{"a":{"additionalItems":12}}}`, nil},
		{`{"$id":"https://example.invalid/root.json","$ref":"same.json"}`, []resolvedReference{{Name: "https://kafscale.invalid/same.json", Text: `{}`}, {Name: "same.json", Text: `{}`}}},
		{`{"properties":{"a":{"additionalItems":12}}}`, nil},
	} {
		if _, err := parseJSONSchema(tc.text, tc.refs); err == nil {
			t.Errorf("accepted %s %+v", tc.text, tc.refs)
		}
	}
	for _, pair := range [][2]string{{`{`, `{}`}, {`{}`, `{`}} {
		if err := checkJSONReads(pair[0], nil, pair[1], nil); err == nil {
			t.Fatal("invalid schema compatible")
		}
	}
}

func TestJSONCanonicalEdges(t *testing.T) {
	for raw, want := range map[string]string{"-0": "0", "17": "17", "-12.3400": "-12.3400", "1.23e12": "1.23E+12", "-1.234e-12": "-1.234E-12", "0e2": "0E+2"} {
		got, err := jsonSchemaNumber(raw)
		if err != nil || got != want {
			t.Errorf("%s => %s %v, want %s", raw, got, err, want)
		}
	}
	got, err := canonicalJSONSchema(`{"examples":[null,true,1,"x",[1,2]],"type":"string"}`, false)
	if err != nil || got != `{"examples":[null,true,1,"x",[1,2]],"type":"string"}` {
		t.Fatalf("canonical %s %v", got, err)
	}
	if _, err := canonicalJSONSchema(`{"examples":[1e10001]}`, false); err == nil {
		t.Fatal("unbounded exponent accepted")
	}
}

func TestJSONRecursiveAndPartialModels(t *testing.T) {
	for _, draft := range []string{"2019-09", "2020-12"} {
		anchor, ref := "$recursiveAnchor", "$recursiveRef"
		anchorValue := "true"
		refValue := "#"
		if draft == "2020-12" {
			anchor, ref = "$dynamicAnchor", "$dynamicRef"
			anchorValue = `"node"`
			refValue = "#node"
		}
		makeSchema := func(kind string) string {
			return fmt.Sprintf(`{"$schema":"https://json-schema.org/draft/%s/schema","%s":%s,"type":"object","properties":{"next":{"%s":"%s"},"value":{"type":"%s"}}}`, draft, anchor, anchorValue, ref, refValue, kind)
		}
		if err := checkJSONReads(makeSchema("string"), nil, makeSchema("string"), nil); err != nil {
			t.Fatal(err)
		}
		if err := checkJSONReads(makeSchema("number"), nil, makeSchema("string"), nil); err == nil {
			t.Fatal("recursive type change accepted")
		}
	}
	for _, text := range []string{`{"title":"annotation"}`, `{"minItems":1}`, `{"$id":"https://example.invalid/root.json","type":"object","properties":{"a/b~":{"type":"string"}}}`} {
		if err := checkJSONReads(text, nil, text, nil); err != nil {
			t.Fatalf("self comparison %s: %v", text, err)
		}
	}
	reader := `{"type":"object","patternProperties":{"^a":{"type":"string"},"a$":{"minLength":1}},"additionalProperties":false}`
	writer := `{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":false}`
	if err := checkJSONReads(reader, nil, writer, nil); err == nil {
		t.Fatal("multiple matching patterns ignored")
	}
	reader = `{"type":"array","items":[{"type":"string"}],"additionalItems":{"type":"number"}}`
	writer = `{"type":"array","items":[{"type":"string"},{"type":"string"}],"additionalItems":false}`
	if err := checkJSONReads(reader, nil, writer, nil); err == nil {
		t.Fatal("incompatible removed tuple item accepted")
	}
}

func TestJSONComparisonBudget(t *testing.T) {
	c := jsonComparison{active: map[jsonRulePair]bool{}, deadline: time.Now().Add(-time.Second)}
	if err := c.compare(jsonTrue, jsonTrue); err == nil {
		t.Fatal("expired comparison accepted")
	}
}

func TestJSONPatternTimeout(t *testing.T) {
	name := strings.Repeat("a", 100) + "!"
	writer := fmt.Sprintf(`{"type":"object","properties":{%q:false},"additionalProperties":false}`, name)
	reader := `{"type":"object","patternProperties":{"^(a+)+$":{}},"additionalProperties":false}`
	if err := checkJSONReads(reader, nil, writer, nil); err == nil || !strings.Contains(err.Error(), "pattern comparison") {
		t.Fatalf("timeout did not fail closed: %v", err)
	}
	// Test the writer's partial model too: a timeout must not make a newly added
	// property look as if it came from a closed writer model.
	if err := checkJSONReads(writer, nil, reader, nil); err == nil || !strings.Contains(err.Error(), "pattern comparison") {
		t.Fatalf("writer timeout did not fail closed: %v", err)
	}
}

func TestJSONCompilerPreservesInstanceValues(t *testing.T) {
	writer := `{"$schema":"https://json-schema.org/draft/2020-12/schema","enum":[{"prefixItems":[]}]}`
	reader := `{"$schema":"https://json-schema.org/draft/2020-12/schema","enum":[{}]}`
	if err := checkJSONReads(reader, nil, writer, nil); err == nil {
		t.Fatal("enum instance values changed during compilation")
	}
}

func TestJSONGUIDCorruptState(t *testing.T) {
	for _, name := range []string{"invalid-guid-id", "missing-schema", "corrupt-schema"} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, nil, Config{})
			text := `{"type":"string","title":"corrupt-guid"}`
			value := "1000"
			status, code := 422, 42205
			if name == "invalid-guid-id" {
				value = "invalid"
				status, code = 500, 50001
			}
			if _, err := h.cli.Put(context.Background(), guidKey(schemaGUID(text, nil)), value); err != nil {
				t.Fatal(err)
			}
			if name == "corrupt-schema" {
				if _, err := h.cli.Put(context.Background(), idKey(1000), "{"); err != nil {
					t.Fatal(err)
				}
				status, code = 500, 50001
			}
			h.fail("POST", "/subjects/corrupt/versions", jsonBody(text), status, code)
		})
	}
}

func TestJSONDraft7ReferenceIgnoresSiblings(t *testing.T) {
	referenced := `{"$defs":{"value":{"type":"string"}},"$ref":"#/$defs/value","additionalItems":false,"const":12,"not":{"type":"string"}}`
	if err := checkJSONReads(`{"type":"string"}`, nil, referenced, nil); err != nil {
		t.Fatal(err)
	}
	if err := checkJSONReads(referenced, nil, `{"type":"string"}`, nil); err != nil {
		t.Fatal(err)
	}
}

func TestJSONModernReferenceKeepsSiblings(t *testing.T) {
	referenced := `{"$schema":"https://json-schema.org/draft/2020-12/schema","$defs":{"value":{"type":"string"}},"$ref":"#/$defs/value","minLength":2}`
	if err := checkJSONReads(`{"type":"string"}`, nil, referenced, nil); err != nil {
		t.Fatal(err)
	}
	if err := checkJSONReads(referenced, nil, `{"type":"string"}`, nil); err == nil {
		t.Fatal("modern reference sibling constraint ignored")
	}
}

func TestJSONPatternWorkBudget(t *testing.T) {
	text := `{"type":"object","patternProperties":{"^(a+)+$(?:0)?":{},"^(a+)+$(?:1)?":{},"^(a+)+$(?:2)?":{},"^(a+)+$(?:3)?":{}}}`
	parsed, err := parseJSONSchema(text, nil)
	if err != nil {
		t.Fatal(err)
	}
	c := jsonComparison{builder: jsonRuleBuilder{cache: map[*js.Schema]*jsonRule{}, raw: parsed.raw}, deadline: time.Now().Add(-time.Second)}
	if _, err := c.propertyModel(parsed.root, "a"); err == nil {
		t.Fatal("pattern scan exceeded comparison deadline")
	}
	c.deadline = time.Now().Add(2 * time.Second)
	if _, err := c.propertyModel(parsed.root, strings.Repeat("a", 100)+"!"); err == nil {
		t.Fatal("pattern timeout accepted")
	}
	timedOut := 0
	for pattern := range parsed.root.PatternProperties {
		if pattern.(*jsonRegexp).err != nil {
			timedOut++
		}
	}
	if timedOut != 1 {
		t.Fatalf("continued scanning after %d pattern timeouts", timedOut)
	}
}
