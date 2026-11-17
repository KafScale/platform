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

package mcpserver

import (
	"slices"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

func TestToolInputSchemasRequiredFields(t *testing.T) {
	cases := []struct {
		name     string
		schema   *jsonschema.Schema
		required []string
	}{
		{"describe_topics", mustSchema[TopicNameInput](t), nil},
		{"describe_configs", mustSchema[TopicConfigInput](t), nil},
		{"fetch_offsets", mustSchema[FetchOffsetsInput](t), []string{"group_id"}},
		{"describe_group", mustSchema[GroupInput](t), []string{"group_id"}},
	}
	for _, tc := range cases {
		got := tc.schema.Required
		if got == nil {
			got = []string{}
		}
		want := tc.required
		if want == nil {
			want = []string{}
		}
		if !slices.Equal(got, want) {
			t.Errorf("%s required = %v, want %v", tc.name, got, want)
		}
	}
}

func mustSchema[T any](t *testing.T) *jsonschema.Schema {
	t.Helper()
	s, err := jsonschema.For[T](nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
