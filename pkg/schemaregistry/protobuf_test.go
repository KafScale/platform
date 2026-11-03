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
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
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
		{"field moved to existing oneof", "message M { oneof o { int32 a = 1; } int32 b = 2; }", "message M { oneof o { int32 a = 1; int32 b = 2; } }", "FIELD_MOVED_TO_EXISTING_ONEOF"},
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
	proto2 := "syntax = \"proto2\";\npackage a;\n"
	if err := checkProtoReads(proto2+"message M { repeated string a = 1; }", nil, proto2+"message M { optional string a = 1; }", nil); err != nil {
		t.Fatalf("string label change %v", err)
	}
	if err := checkProtoReads(proto2+"message M { repeated int32 a = 1; }", nil, proto2+"message M { optional int32 a = 1; }", nil); err == nil {
		t.Fatalf("number label change reported compatible")
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
	mapped, err := parseProto(protoHead+m+"message M { map<string, N> m = 1; }", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := mapped.canonical(true); !strings.Contains(got, "map<string, .a.N> m = 1") {
		t.Fatalf("normalized map %q", got)
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

func TestProtoDescriptorInput(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("common", protoSchemaBody("syntax = \"proto3\";\npackage c;\nmessage C { string x = 1; }"))
	text := protoHead + "import \"c.proto\";\nimport \"google/protobuf/timestamp.proto\";\nmessage R { c.C c = 1; google.protobuf.Timestamp t = 2; map<string, int32> m = 3; oneof o { string s = 4; int64 n = 5; } }"
	ref := Reference{Name: "c.proto", Subject: "common", Version: 1}
	id := h.register("r", protoSchemaBody(text, ref))
	var serialized versionBody
	h.ok("GET", "/schemas/ids/"+fmt.Sprint(id)+"?format=serialized", nil, &serialized)
	data, err := base64.StdEncoding.DecodeString(serialized.Schema)
	if err != nil {
		t.Fatal(err)
	}
	var fd descriptorpb.FileDescriptorProto
	if err := proto.Unmarshal(data, &fd); err != nil {
		t.Fatal(err)
	}
	if fd.GetName() != "default" {
		t.Fatalf("serialized file name %q", fd.GetName())
	}
	var got versionBody
	h.ok("POST", "/subjects/r", protoSchemaBody(serialized.Schema, ref), &got)
	if got.ID != id {
		t.Fatalf("descriptor lookup %+v", got)
	}
	again := h.register("d", protoSchemaBody(serialized.Schema, ref))
	if again != id {
		t.Fatalf("descriptor register id %d, want %d", again, id)
	}
	fd.MessageType[0].Field = append(fd.MessageType[0].Field, &descriptorpb.FieldDescriptorProto{Name: proto.String("extra"), Number: proto.Int32(9), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), JsonName: proto.String("extra")})
	data, err = proto.Marshal(&fd)
	if err != nil {
		t.Fatal(err)
	}
	h.register("d", protoSchemaBody(base64.StdEncoding.EncodeToString(data), ref))
	h.ok("GET", "/subjects/d/versions/2", nil, &got)
	if !strings.Contains(got.Schema, "string extra = 9;") || !strings.Contains(got.Schema, "import \"c.proto\";") {
		t.Fatalf("descriptor stored as %q", got.Schema)
	}
	if _, err := parseProto(got.Schema, []resolvedReference{{Name: "c.proto", Text: "syntax = \"proto3\";\npackage c;\nmessage C { string x = 1; }"}}); err != nil {
		t.Fatalf("stored text does not parse: %v", err)
	}
	if id2 := h.register("d", protoSchemaBody(base64.StdEncoding.EncodeToString(data), ref)); id2 != id+1 {
		t.Fatalf("descriptor register again %d", id2)
	}
	h.fail("POST", "/subjects/d/versions", protoSchemaBody("bm90IGEgZGVzY3JpcHRvcg==", ref), 422, 42201)
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
	var serialized versionBody
	h.ok("GET", "/schemas/ids/"+fmt.Sprint(rid)+"?format=serialized", nil, &serialized)
	data, err := base64.StdEncoding.DecodeString(serialized.Schema)
	if err != nil {
		t.Fatalf("serialized schema %q: %v", serialized.Schema, err)
	}
	var fd descriptorpb.FileDescriptorProto
	if err := proto.Unmarshal(data, &fd); err != nil {
		t.Fatal(err)
	}
	if fd.GetPackage() != "a" || fd.GetMessageType()[0].GetName() != "R" || strings.Join(fd.GetDependency(), ",") != "c.proto,google/protobuf/timestamp.proto" {
		t.Fatalf("descriptor %v", &fd)
	}
	h.ok("GET", "/subjects/common/versions/1?format=serialized", nil, &got)
	if strings.Contains(got.Schema, "syntax") {
		t.Fatalf("version not serialized %q", got.Schema)
	}
	h.ok("GET", "/subjects/common/versions/1", nil, &got)
	if !strings.Contains(got.Schema, "syntax") {
		t.Fatalf("default format %q", got.Schema)
	}
}

func protoDescriptorBase64(t *testing.T, text string, refs []resolvedReference, edit func(*descriptorpb.FileDescriptorProto)) string {
	t.Helper()
	s, err := parseProto(text, refs)
	if err != nil {
		t.Fatal(err)
	}
	fd := protodesc.ToFileDescriptorProto(s.file)
	fd.Name = proto.String("default")
	fd.SourceCodeInfo = nil
	if edit != nil {
		edit(fd)
	}
	data, err := proto.Marshal(fd)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(data)
}

func TestProtoRawTextAndDescriptorDedupe(t *testing.T) {
	common := "syntax = \"proto3\";\npackage mx.common;\nmessage Money { string currency = 1; int64 units = 2; }\n"
	text := "syntax = \"proto3\";\npackage mx;\nimport \"mx/common.proto\";\nmessage Ord { string id = 1; mx.common.Money total = 2; message L { string sku = 1; } repeated L lines = 3; oneof o { string a = 4; int32 b = 5; } int32 last = 6; }\n"
	ref := Reference{Name: "mx/common.proto", Subject: "mx-common", Version: 1}
	b64 := protoDescriptorBase64(t, text, []resolvedReference{{Name: ref.Name, Text: common}}, nil)
	h := newHarness(t, nil, Config{})
	h.register("mx-common", protoSchemaBody(common))
	for _, order := range [][2]string{{text, b64}, {b64, text}} {
		subject := fmt.Sprintf("ord-%d", len(order[0]))
		first := h.register(subject, protoSchemaBody(order[0], ref))
		second := h.register(subject, protoSchemaBody(order[1], ref))
		var versions []int
		h.ok("GET", "/subjects/"+subject+"/versions", nil, &versions)
		if first != second || len(versions) != 1 {
			t.Fatalf("subject %s ids %d %d versions %v", subject, first, second, versions)
		}
	}
	h.register("ord-other", protoSchemaBody(strings.Replace(text, "int32 last = 6;", "", 1), ref))
	var versions []int
	h.register("ord-other", protoSchemaBody(b64, ref))
	h.ok("GET", "/subjects/ord-other/versions", nil, &versions)
	if len(versions) != 2 {
		t.Fatalf("different schema deduped, versions %v", versions)
	}
}

func TestProtoDescriptorRelativeMapEntry(t *testing.T) {
	text := protoHead + "import \"google/protobuf/timestamp.proto\";\nmessage B { google.protobuf.Timestamp at = 1; map<string, int64> tags = 2; message In { map<string, string> user_labels = 1; } In in = 3; }\n"
	b64 := protoDescriptorBase64(t, text, nil, func(fd *descriptorpb.FileDescriptorProto) {
		b := fd.MessageType[0]
		for _, n := range b.NestedType {
			if n.GetName() == "TagsEntry" {
				n.Name = proto.String("tagsEntry")
			}
			if n.GetName() == "In" {
				n.NestedType[0].Name = proto.String("user_labelsEntry")
				n.Field[0].TypeName = proto.String("user_labelsEntry")
			}
		}
		b.Field[1].TypeName = proto.String("tagsEntry")
	})
	h := newHarness(t, nil, Config{})
	id := h.register("m", protoSchemaBody(b64))
	var got versionBody
	h.ok("GET", "/subjects/m/versions/1", nil, &got)
	if !strings.Contains(got.Schema, "map<string, int64> tags = 2;") || !strings.Contains(got.Schema, "map<string, string> user_labels = 1;") {
		t.Fatalf("stored %q", got.Schema)
	}
	if again := h.register("m", protoSchemaBody(text)); again != id {
		t.Fatalf("text after descriptor got id %d, want %d", again, id)
	}
}
