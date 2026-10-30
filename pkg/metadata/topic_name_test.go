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

package metadata

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateTopicName(t *testing.T) {
	for _, name := range []string{"orders", "a.b_c-D9", strings.Repeat("x", 249), "..a"} {
		if err := ValidateTopicName(name); err != nil {
			t.Fatalf("%q: %v", name, err)
		}
	}
	for _, name := range []string{"", ".", "..", "col@1", "a/b", "a b", "é", strings.Repeat("x", 250)} {
		if err := ValidateTopicName(name); !errors.Is(err, ErrInvalidTopic) {
			t.Fatalf("%q: got %v", name, err)
		}
	}
}
