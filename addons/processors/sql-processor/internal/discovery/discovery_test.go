// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package discovery

import "testing"

func TestParseSegmentKey(t *testing.T) {
	key, kind, ok := parseSegmentKey("prod/", "prod/orders/3/segment-00000000000000001000.kfs")
	if !ok || kind != "kfs" {
		t.Fatalf("expected kfs key")
	}
	if key.topic != "orders" || key.partition != 3 || key.baseOffset != 1000 {
		t.Fatalf("unexpected key: %+v", key)
	}

	key, kind, ok = parseSegmentKey("prod/", "prod/orders/3/segment-00000000000000001000.index")
	if !ok || kind != "index" {
		t.Fatalf("expected index key")
	}
}

func TestParseSegmentKeyInvalid(t *testing.T) {
	if _, _, ok := parseSegmentKey("prod/", "prod/orders/x/segment-000.kfs"); ok {
		t.Fatalf("expected invalid partition")
	}
	if _, _, ok := parseSegmentKey("prod/", "prod/orders/3/segment-abc.kfs"); ok {
		t.Fatalf("expected invalid base offset")
	}
	if _, _, ok := parseSegmentKey("prod/", "prod/orders/3/segment-00000000000000000001.tmp"); ok {
		t.Fatalf("expected unsupported extension")
	}
}

func TestNormalizePrefix(t *testing.T) {
	if normalizePrefix("") != "" {
		t.Fatalf("expected empty prefix")
	}
	if normalizePrefix("/prod/") != "prod/" {
		t.Fatalf("unexpected prefix")
	}
}
