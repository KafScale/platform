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

package main

import "testing"

func TestLeaderElectionIDDefault(t *testing.T) {
	t.Setenv("KAFSCALE_OPERATOR_LEADER_KEY", "")
	if got := leaderElectionID(); got != "kafscale-operator" {
		t.Fatalf("expected default leader election id, got %q", got)
	}
}

func TestLeaderElectionIDOverride(t *testing.T) {
	t.Setenv("KAFSCALE_OPERATOR_LEADER_KEY", "kafscale-operator-custom")
	if got := leaderElectionID(); got != "kafscale-operator-custom" {
		t.Fatalf("expected override leader election id, got %q", got)
	}
}
