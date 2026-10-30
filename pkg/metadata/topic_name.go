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

import "fmt"

const maxTopicNameLength = 249

func ValidateTopicName(name string) error {
	if name == "" || name == "." || name == ".." {
		return fmt.Errorf("%w: topic name %q is not allowed", ErrInvalidTopic, name)
	}
	if len(name) > maxTopicNameLength {
		return fmt.Errorf("%w: topic name is longer than %d characters", ErrInvalidTopic, maxTopicNameLength)
	}
	for _, c := range name {
		if !validTopicChar(c) {
			return fmt.Errorf("%w: topic name %q contains %q, only a-z, A-Z, 0-9, '.', '_' and '-' are allowed", ErrInvalidTopic, name, c)
		}
	}
	return nil
}

func validTopicChar(c rune) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-'
}
