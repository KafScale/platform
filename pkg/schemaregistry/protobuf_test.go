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
	"strings"
	"testing"
)

const protoHead = "syntax = \"proto3\";\npackage a;\n"

func protoSchemaBody(text string, refs ...Reference) map[string]any {
	body := map[string]any{"schema": text, "schemaType": "PROTOBUF"}
	if len(refs) > 0 {
		body["references"] = refs
	}
	return body
}

func TestProtoCompatibilityRules(t *testing.T) {
	cases := []struct {
		name, writer, reader, want string
	}{
		{"field added", "message M { string a = 1; }", "message M { string a = 1; int32 b = 2; }", ""},
		{"field removed", "message M { string a = 1; int32 b = 2; }", "message M { string a = 1; }", ""},
		{"field renamed", "message M { string a = 1; }", "message M { string b = 1; }", ""},
		{"scalar kind changed", "message M { string a = 1; }", "message M { int32 a = 1; }", "FIELD_SCALAR_KIND_CHANGED"},
		{"number group kept", "message M { int32 a = 1; }", "message M { int64 a = 1; }", ""},
		{"number group changed", "message M { int32 a = 1; }", "message M { sint32 a = 1; }", "FIELD_SCALAR_KIND_CHANGED"},
		{"string to bytes", "message M { string a = 1; }", "message M { bytes a = 1; }", ""},
		{"message removed", "message M { N n = 1; }\nmessage N { int32 x = 1; }", "message M { int32 n = 1; }", "FIELD_KIND_CHANGED"},
		{"type renamed", "message M { N n = 1; }\nmessage N { int32 x = 1; }", "message M { O n = 1; }\nmessage O { int32 x = 1; }", "FIELD_NAMED_TYPE_CHANGED"},
		{"oneof field moved in", "message M { int32 a = 1; int32 b = 2; }", "message M { oneof o { int32 a = 1; int32 b = 2; } }", "MULTIPLE_FIELDS_MOVED_TO_ONEOF"},
		{"enum value added", "enum E { A = 0; }\nmessage M { E e = 1; }", "enum E { A = 0; B = 1; }\nmessage M { E e = 1; }", ""},
		{"enum to int", "enum E { A = 0; }\nmessage M { E e = 1; }", "message M { int32 e = 1; }", ""},
	}
	for _, c := range cases {
		err := checkProtoReads(protoHead+c.reader, nil, protoHead+c.writer, nil)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: unexpected %v", c.name, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%s: got %v, want %s", c.name, err, c.want)
		}
	}
	if err := checkProtoReads("syntax = \"proto3\";\npackage b;\nmessage M {}", nil, protoHead+"message M {}", nil); err == nil || !strings.Contains(err.Error(), "PACKAGE_CHANGED") {
		t.Fatalf("package change %v", err)
	}
}

func TestProtoCanonicalForm(t *testing.T) {
	same := func(a, b string, normalize bool) bool {
		x, err := parseProto(a, nil)
		if err != nil {
			t.Fatal(err)
		}
		y, err := parseProto(b, nil)
		if err != nil {
			t.Fatal(err)
		}
		return x.canonical(normalize) == y.canonical(normalize)
	}
	m := "message N {}\n"
	pairs := []struct {
		a, b      string
		raw, norm bool
	}{
		{protoHead + "// c\nmessage M {  int32 a = 1 ; }", protoHead + "message M { int32 a = 1; }", true, true},
		{protoHead + m + "message M { .a.N n = 1; }", protoHead + m + "message M { . a . N n = 1; }", true, true},
		{protoHead + m + "message M { N n = 1; }", protoHead + m + "message M { .a.N n = 1; }", false, true},
		{protoHead + m + "message M { map<string, N> m = 1; }", protoHead + m + "message M { map < string , .a.N > m = 1; }", false, true},
		{protoHead + m + "message M { map<string, .a.N> m = 1; }", protoHead + m + "message M { map < string , .a.N > m = 1; }", true, true},
		{"syntax = \"proto2\";\npackage a;\nmessage M { optional int32 a = 1; }", "package a;\nmessage M { optional int32 a = 1; }", false, true},
		{protoHead + "option java_package = \"x\";\noption go_package = \"y\";\nmessage M {}", protoHead + "option go_package = \"y\";\noption java_package = \"x\";\nmessage M {}", false, true},
		{protoHead + "message M { int32 b = 2; int32 a = 1; }", protoHead + "message M { int32 a = 1; int32 b = 2; }", false, true},
		{protoHead + "message M { int32 a = 1; }", protoHead + "message M { int64 a = 1; }", false, false},
	}
	for i, p := range pairs {
		if got := same(p.a, p.b, false); got != p.raw {
			t.Errorf("pair %d raw equal %v, want %v", i, got, p.raw)
		}
		if got := same(p.a, p.b, true); got != p.norm {
			t.Errorf("pair %d normalized equal %v, want %v", i, got, p.norm)
		}
	}
}

func TestProtoRegisterLookupAndCompatibility(t *testing.T) {
	h := newHarness(t, nil, Config{})
	var types []string
	h.ok("GET", "/schemas/types", nil, &types)
	if strings.Join(types, ",") != "AVRO,PROTOBUF" {
		t.Fatalf("types %v", types)
	}
	v1 := protoHead + "message M { string a = 1; }"
	id := h.register("p", protoSchemaBody(v1))
	if again := h.register("p", protoSchemaBody(protoHead+"// note\nmessage M {\n  string a = 1;\n}")); again != id {
		t.Fatalf("formatting change got id %d, want %d", again, id)
	}
	var got versionBody
	h.ok("POST", "/subjects/p", protoSchemaBody(v1), &got)
	if got.ID != id || got.SchemaType != "PROTOBUF" {
		t.Fatalf("lookup %+v", got)
	}
	h.fail("POST", "/subjects/p", protoSchemaBody(protoHead+"message M { string a = 1; int32 b = 2; }"), 404, 40403)
	h.fail("POST", "/subjects/p/versions", protoSchemaBody(protoHead+"message M { int32 a = 1; }"), 409, 40901)
	h.fail("POST", "/subjects/p/versions", schema(userV1), 409, 40901)
	h.fail("POST", "/subjects/p/versions", protoSchemaBody("message M { int32 a = 1 }"), 422, 42201)
	var out struct {
		IsCompatible bool     `json:"is_compatible"`
		Messages     []string `json:"messages"`
	}
	h.ok("POST", "/compatibility/subjects/p/versions/latest?verbose=true", schema(userV1), &out)
	if out.IsCompatible || len(out.Messages) == 0 || !strings.Contains(out.Messages[0], "TYPE_MISMATCH") {
		t.Fatalf("type mismatch %+v", out)
	}

	h.register("common", protoSchemaBody("syntax = \"proto3\";\npackage c;\nmessage C { string x = 1; }"))
	h.ok("PUT", "/config/common", map[string]string{"compatibility": "NONE"}, nil)
	h.register("common", protoSchemaBody("syntax = \"proto3\";\npackage c;\nmessage C { int32 x = 1; }"))
	main := protoHead + "import \"c.proto\";\nimport \"google/protobuf/timestamp.proto\";\nmessage R { c.C c = 1; google.protobuf.Timestamp t = 2; }"
	ref := func(v int) Reference { return Reference{Name: "c.proto", Subject: "common", Version: v} }
	rid := h.register("r", protoSchemaBody(main, ref(1)))
	h.ok("POST", "/subjects/r", protoSchemaBody(main, ref(1)), &got)
	if got.ID != rid {
		t.Fatalf("lookup with references %+v", got)
	}
	h.ok("POST", "/compatibility/subjects/r/versions/latest", protoSchemaBody(main, ref(2)), &out)
	if out.IsCompatible {
		t.Fatalf("changed reference reported compatible")
	}
	h.fail("POST", "/subjects/r/versions", protoSchemaBody(main), 422, 42201)
}
