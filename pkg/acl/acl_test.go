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

package acl

import "testing"

func TestAuthorizerDefaultAllow(t *testing.T) {
	auth := NewAuthorizer(Config{
		Enabled:       true,
		DefaultPolicy: "allow",
	})
	if !auth.Allows("unknown", ActionFetch, ResourceTopic, "orders") {
		t.Fatalf("expected default allow")
	}
}

func TestAuthorizerDefaultDeny(t *testing.T) {
	auth := NewAuthorizer(Config{
		Enabled:       true,
		DefaultPolicy: "deny",
	})
	if auth.Allows("unknown", ActionFetch, ResourceTopic, "orders") {
		t.Fatalf("expected default deny")
	}
}

func TestAuthorizerAllowOverridesDefaultDeny(t *testing.T) {
	auth := NewAuthorizer(Config{
		Enabled:       true,
		DefaultPolicy: "deny",
		Principals: []PrincipalRules{
			{
				Name:  "client-a",
				Allow: []Rule{{Action: ActionFetch, Resource: ResourceTopic, Name: "orders"}},
			},
		},
	})
	if !auth.Allows("client-a", ActionFetch, ResourceTopic, "orders") {
		t.Fatalf("expected allow rule to permit access")
	}
	if auth.Allows("client-a", ActionFetch, ResourceTopic, "payments") {
		t.Fatalf("expected deny for unmatched topic")
	}
}

func TestAuthorizerDenyOverridesAllow(t *testing.T) {
	auth := NewAuthorizer(Config{
		Enabled:       true,
		DefaultPolicy: "allow",
		Principals: []PrincipalRules{
			{
				Name:  "client-a",
				Allow: []Rule{{Action: ActionFetch, Resource: ResourceTopic, Name: "orders"}},
				Deny:  []Rule{{Action: ActionFetch, Resource: ResourceTopic, Name: "orders"}},
			},
		},
	})
	if auth.Allows("client-a", ActionFetch, ResourceTopic, "orders") {
		t.Fatalf("expected deny to override allow")
	}
}

func TestAuthorizerWildcardName(t *testing.T) {
	auth := NewAuthorizer(Config{
		Enabled:       true,
		DefaultPolicy: "deny",
		Principals: []PrincipalRules{
			{
				Name:  "client-a",
				Allow: []Rule{{Action: ActionProduce, Resource: ResourceTopic, Name: "orders-*"}},
			},
		},
	})
	if !auth.Allows("client-a", ActionProduce, ResourceTopic, "orders-2025") {
		t.Fatalf("expected prefix wildcard match")
	}
}
