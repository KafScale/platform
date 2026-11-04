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
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestProtoDescriptorRejectsMapEntryInWrongScope(t *testing.T) {
	text := protoHead + "message M { map<string, int64> tags = 1; }"
	for _, name := range []string{".other.M.TagsEntry", ".a.Other.TagsEntry", "Other.TagsEntry"} {
		t.Run(name, func(t *testing.T) {
			encoded := protoDescriptorBase64(t, text, nil, func(fd *descriptorpb.FileDescriptorProto) {
				fd.MessageType[0].Field[0].TypeName = proto.String(name)
			})
			if out, from, err := protoFromDescriptor(encoded, nil); err == nil {
				t.Fatalf("accepted map entry from wrong scope: from=%v text=%q", from, out)
			}
		})
	}
}

func TestProtoDescriptorMapEntryScopes(t *testing.T) {
	for _, head := range []string{protoHead, "syntax = \"proto3\";\n"} {
		for _, name := range []string{"tagsEntry", "M.tagsEntry", "a.M.tagsEntry", ".a.M.tagsEntry"} {
			if !strings.Contains(head, "package") {
				name = strings.TrimPrefix(name, "a.")
				name = strings.ReplaceAll(name, ".a.", ".")
			}
			t.Run(head+name, func(t *testing.T) {
				encoded := protoDescriptorBase64(t, head+"message M { map<string, int64> tags = 1; }", nil, func(fd *descriptorpb.FileDescriptorProto) {
					fd.MessageType[0].NestedType[0].Name = proto.String("tagsEntry")
					fd.MessageType[0].Field[0].TypeName = proto.String(name)
					fd.MessageType[0].Field[0].Type = nil
				})
				out, from, err := protoFromDescriptor(encoded, nil)
				if err != nil || !from || !strings.Contains(out, "map<string, int64> tags = 1;") {
					t.Fatalf("map repair: from=%v err=%v text=%q", from, err, out)
				}
			})
		}
	}
}

// Exercise the descriptor printer through parsing and descriptor comparison, so
// options, defaults, nested declarations and message indexes survive conversion.
func TestProtoDescriptorRoundTrip(t *testing.T) {
	cases := []struct {
		name, text string
		refs       []resolvedReference
	}{
		{"proto2", `syntax = "proto2"; package a;
            option java_package = "example";
            message M {
                reserved 4, 8 to 10; reserved "gone";
                optional string user_id = 1 [default = "guest", json_name = "uid"];
                optional bytes data = 2 [default = "\001\377"];
                optional group Legacy = 3 { optional int32 x = 1 [default = -7]; }
                extensions 100 to 199;
                enum E { option allow_alias = true; ZERO = 0; ALSO_ZERO = 0; NEG = -1; }
                optional E e = 5 [default = NEG];
            }
            extend M { optional string extra = 100; }
            service API { rpc Call(stream M) returns (stream M) { option deprecated = true; } }`, nil},
		{"proto3", protoHead + `message M {
            optional string user_id = 1 [json_name = "uid"];
            map<string, M> children = 2;
            oneof choice { string s = 3; int64 n = 4; }
            message Nested { string x = 1; }
            repeated Nested nested = 5;
            enum E { ZERO = 0; ONE = 1; }
            E e = 6;
        }`, nil},
		{"transitive imports", protoHead + `import "middle.proto";
            message M { middle.Middle nested = 1; }`, []resolvedReference{
			{Name: "middle.proto", Text: `syntax = "proto3"; package middle;
                    import public "leaf.proto";
                    message Middle { leaf.Leaf value = 1; }`},
			{Name: "leaf.proto", Text: `syntax = "proto3"; package leaf; message Leaf { string s = 1; }`},
		}},
		{"custom options", `syntax = "proto2"; package a; import "options.proto";
            option (custom.note) = "hello";
            message M { optional string s = 1 [(custom.field_note) = "world"]; }`,
			[]resolvedReference{{Name: "options.proto", Text: `syntax = "proto2"; package custom;
                import "google/protobuf/descriptor.proto";
                extend google.protobuf.FileOptions { optional string note = 50001; }
                extend google.protobuf.FieldOptions { optional string field_note = 50002; }`}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := protoDescriptorBase64(t, tc.text, tc.refs, nil)
			out, from, err := protoFromDescriptor(encoded, tc.refs)
			if err != nil || !from {
				t.Fatalf("decode: from=%v err=%v", from, err)
			}
			original, err := parseProto(tc.text, tc.refs)
			if err != nil {
				t.Fatal(err)
			}
			printed, err := parseProto(out, tc.refs)
			if err != nil {
				t.Fatalf("printed schema: %v\n%s", err, out)
			}
			want, err := descriptorForm(original.file)
			if err != nil {
				t.Fatal(err)
			}
			got, err := descriptorForm(printed.file)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("descriptor changed after printing:\n%s", out)
			}
		})
	}
}

func TestProtoDescriptorCustomJSONNameDedupe(t *testing.T) {
	h := newHarness(t, nil, Config{})
	custom := protoHead + `message M { string user_id = 1 [json_name = "uid"]; }`
	plain := protoHead + `message M { string user_id = 1; }`
	for i, order := range [][2]string{{custom, protoDescriptorBase64(t, custom, nil, nil)}, {protoDescriptorBase64(t, custom, nil, nil), custom}} {
		subject := fmt.Sprintf("json-name-%d", i)
		first := h.register(subject, protoSchemaBody(order[0]))
		if second := h.register(subject, protoSchemaBody(order[1])); first != second {
			t.Fatalf("custom json_name failed dedupe: %d != %d", first, second)
		}
		var versions []int
		h.ok("GET", "/subjects/"+subject+"/versions", nil, &versions)
		if len(versions) != 1 {
			t.Fatalf("versions %v", versions)
		}
		if other := h.register(subject, protoSchemaBody(plain)); other == first {
			t.Fatal("custom and default json_name incorrectly deduped")
		}
	}
}

func TestProtoDescriptorInputErrors(t *testing.T) {
	for _, text := range []string{"", "message M {}", "!invalid-base64"} {
		out, from, err := protoFromDescriptor(text, nil)
		if out != text || from || err != nil {
			t.Fatalf("text fallback %q: %q %v %v", text, out, from, err)
		}
	}
	for _, tc := range []struct {
		name string
		edit func(*descriptorpb.FileDescriptorProto)
		want string
	}{
		{"missing import", func(fd *descriptorpb.FileDescriptorProto) { fd.Dependency = []string{"missing.proto"} }, "not a reference"},
		{"invalid descriptor", func(fd *descriptorpb.FileDescriptorProto) { fd.MessageType[0].Field[0].Number = proto.Int32(0) }, "invalid number"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded := protoDescriptorBase64(t, protoHead+"message M { string s = 1; }", nil, tc.edit)
			if _, from, err := protoFromDescriptor(encoded, nil); from || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("from=%v err=%v, want %s", from, err, tc.want)
			}
		})
	}
}
