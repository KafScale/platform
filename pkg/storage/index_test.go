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

import (
	"strings"
	"testing"
)

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

func TestNewIndexBuilderZeroInterval(t *testing.T) {
	b := NewIndexBuilder(0)
	if b.interval != 1 {
		t.Fatalf("expected interval 1 for zero input, got %d", b.interval)
	}
	b2 := NewIndexBuilder(-5)
	if b2.interval != 1 {
		t.Fatalf("expected interval 1 for negative input, got %d", b2.interval)
	}
}

func TestIndexBuilderEntriesCopied(t *testing.T) {
	b := NewIndexBuilder(1)
	b.MaybeAdd(0, 32, 1)
	entries := b.Entries()
	entries[0] = nil // Modify returned slice
	origEntries := b.Entries()
	if origEntries[0] == nil {
		t.Fatal("Entries() should return a copy")
	}
}

func TestParseIndexTooSmall(t *testing.T) {
	_, err := ParseIndex([]byte("short"))
	if err == nil {
		t.Fatal("expected error for data < 16 bytes")
	}
}

func TestParseIndexInvalidMagic(t *testing.T) {
	data := make([]byte, 20)
	copy(data, "BAAD")
	_, err := ParseIndex(data)
	if err == nil || !strings.Contains(err.Error(), "magic") {
		t.Fatalf("expected invalid magic error, got: %v", err)
	}
}

func TestParseIndexBadVersion(t *testing.T) {
	// Build valid magic + version=99
	data := make([]byte, 20)
	copy(data, "IDX\x00")
	data[4] = 0  // version high byte
	data[5] = 99 // version low byte = 99
	_, err := ParseIndex(data)
	if err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("expected version error, got: %v", err)
	}
}

func TestIndexRoundTrip(t *testing.T) {
	b := NewIndexBuilder(1)
	for i := int64(0); i < 10; i++ {
		b.MaybeAdd(i*100, int32(i*64), 1)
	}
	data, err := b.BuildBytes()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := ParseIndex(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 10 {
		t.Fatalf("expected 10 entries, got %d", len(entries))
	}
	for i, e := range entries {
		if e.Offset != int64(i)*100 {
			t.Fatalf("entry %d: expected offset %d, got %d", i, i*100, e.Offset)
		}
	}
}
