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

package s3env

import "testing"

func TestTrimRemovesOnlyASCIIWhitespace(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{in: "", want: ""},
		{in: " \t\n\r\v\f", want: ""},
		{in: " \t\v\fkey\r\n", want: "key"},
		{in: "a b", want: "a b"},
		{in: "\u00a0key\u00a0", want: "\u00a0key\u00a0"},
		{in: "\u00a0", want: "\u00a0"},
		{in: "\u2003key\u3000", want: "\u2003key\u3000"},
		{in: "\u0085key", want: "\u0085key"},
		{in: " \u00a0key\u00a0\t", want: "\u00a0key\u00a0"},
	} {
		if got := Trim(tc.in); got != tc.want {
			t.Fatalf("Trim(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
