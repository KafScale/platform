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

package proxy

import (
	"path"
	"strings"
)

type ACL struct {
	Allow []string
	Deny  []string
}

func (a ACL) Allows(topic string) bool {
	if matchPatterns(a.Deny, topic) {
		return false
	}
	if len(a.Allow) == 0 {
		return true
	}
	return matchPatterns(a.Allow, topic)
}

func (a ACL) AllowShowTopics() bool {
	if len(a.Deny) > 0 {
		return false
	}
	if len(a.Allow) == 0 {
		return true
	}
	return matchPatterns(a.Allow, "*")
}

func matchPatterns(patterns []string, topic string) bool {
	if len(patterns) == 0 {
		return false
	}
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if pattern == "*" {
			return true
		}
		if matched, err := path.Match(pattern, topic); err == nil && matched {
			return true
		}
		if pattern == topic {
			return true
		}
	}
	return false
}
