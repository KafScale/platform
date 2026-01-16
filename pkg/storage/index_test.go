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

package storage

import "testing"

func TestIndexBuilder(t *testing.T) {
	builder := NewIndexBuilder(2)
	builder.MaybeAdd(0, 32, 1)
	builder.MaybeAdd(5, 64, 1) // should not add due to interval
	builder.MaybeAdd(6, 96, 1) // should add

	entries := builder.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries got %d", len(entries))
	}
	if entries[1].Offset != 6 {
		t.Fatalf("unexpected offset %d", entries[1].Offset)
	}

	data, err := builder.BuildBytes()
	if err != nil {
		t.Fatalf("BuildBytes: %v", err)
	}
	parsed, err := ParseIndex(data)
	if err != nil {
		t.Fatalf("ParseIndex: %v", err)
	}
	if len(parsed) != 2 || parsed[0].Offset != 0 {
		t.Fatalf("parsed entries mismatch: %#v", parsed)
	}
}
