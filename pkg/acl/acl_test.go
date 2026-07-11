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

func TestAuthorizerEnabled(t *testing.T) {
	auth := NewAuthorizer(Config{Enabled: true})
	if !auth.Enabled() {
		t.Fatal("expected Enabled() = true")
	}
	auth2 := NewAuthorizer(Config{Enabled: false})
	if auth2.Enabled() {
		t.Fatal("expected Enabled() = false")
	}
}

func TestAuthorizerNilReceiver(t *testing.T) {
	var auth *Authorizer
	if auth.Enabled() {
		t.Fatal("nil Authorizer should not be enabled")
	}
	if !auth.Allows("user", ActionFetch, ResourceTopic, "orders") {
		t.Fatal("nil Authorizer should allow all")
	}
}

func TestAuthorizerDisabledAllows(t *testing.T) {
	auth := NewAuthorizer(Config{
		Enabled:       false,
		DefaultPolicy: "deny",
	})
	if !auth.Allows("any", ActionAdmin, ResourceCluster, "test") {
		t.Fatal("disabled authorizer should allow all")
	}
}

func TestAuthorizerAnonymousPrincipal(t *testing.T) {
	auth := NewAuthorizer(Config{
		Enabled:       true,
		DefaultPolicy: "deny",
		Principals: []PrincipalRules{
			{
				Name:  "anonymous",
				Allow: []Rule{{Action: ActionFetch, Resource: ResourceTopic, Name: "public"}},
			},
		},
	})
	// Empty principal maps to "anonymous"
	if !auth.Allows("", ActionFetch, ResourceTopic, "public") {
		t.Fatal("empty principal should map to anonymous")
	}
	if !auth.Allows("  ", ActionFetch, ResourceTopic, "public") {
		t.Fatal("whitespace principal should map to anonymous")
	}
}

func TestAuthorizerEmptyPrincipalInConfig(t *testing.T) {
	auth := NewAuthorizer(Config{
		Enabled:       true,
		DefaultPolicy: "deny",
		Principals: []PrincipalRules{
			{Name: "", Allow: []Rule{{Action: ActionAny, Resource: ResourceAny}}},
		},
	})
	// Empty name principal should be skipped
	if auth.Allows("unknown", ActionFetch, ResourceTopic, "orders") {
		t.Fatal("empty principal name should be skipped in config")
	}
}

func TestAuthorizerWildcardActionAndResource(t *testing.T) {
	auth := NewAuthorizer(Config{
		Enabled:       true,
		DefaultPolicy: "deny",
		Principals: []PrincipalRules{
			{
				Name:  "superuser",
				Allow: []Rule{{Action: ActionAny, Resource: ResourceAny, Name: "*"}},
			},
		},
	})
	if !auth.Allows("superuser", ActionProduce, ResourceTopic, "anything") {
		t.Fatal("wildcard action+resource+name should allow all")
	}
	if !auth.Allows("superuser", ActionAdmin, ResourceCluster, "cluster-1") {
		t.Fatal("wildcard should allow admin on cluster")
	}
}

func TestAuthorizerCaseInsensitivePolicy(t *testing.T) {
	auth := NewAuthorizer(Config{
		Enabled:       true,
		DefaultPolicy: "ALLOW",
	})
	if !auth.Allows("unknown", ActionFetch, ResourceTopic, "orders") {
		t.Fatal("ALLOW (uppercase) should work")
	}
}

func TestNameMatchesExact(t *testing.T) {
	if !nameMatches("orders", "orders") {
		t.Fatal("exact match should succeed")
	}
	if nameMatches("orders", "other") {
		t.Fatal("different name should not match")
	}
}

func TestNameMatchesEmptyAndWildcard(t *testing.T) {
	if !nameMatches("", "anything") {
		t.Fatal("empty ruleName should match anything")
	}
	if !nameMatches("*", "anything") {
		t.Fatal("* ruleName should match anything")
	}
	if !nameMatches("  ", "anything") {
		t.Fatal("whitespace ruleName should match anything")
	}
}

func TestActionMatches(t *testing.T) {
	if !actionMatches("", ActionFetch) {
		t.Fatal("empty rule action should match any action")
	}
	if !actionMatches(ActionAny, ActionFetch) {
		t.Fatal("* action should match any action")
	}
	if !actionMatches(ActionFetch, ActionFetch) {
		t.Fatal("same action should match")
	}
	if actionMatches(ActionFetch, ActionProduce) {
		t.Fatal("different actions should not match")
	}
}

func TestResourceMatches(t *testing.T) {
	if !resourceMatches("", ResourceTopic) {
		t.Fatal("empty rule resource should match any resource")
	}
	if !resourceMatches(ResourceAny, ResourceTopic) {
		t.Fatal("* resource should match any resource")
	}
	if !resourceMatches(ResourceTopic, ResourceTopic) {
		t.Fatal("same resource should match")
	}
	if resourceMatches(ResourceTopic, ResourceGroup) {
		t.Fatal("different resources should not match")
	}
}

func TestAuthorizerFallbackToDefault(t *testing.T) {
	auth := NewAuthorizer(Config{
		Enabled:       true,
		DefaultPolicy: "allow",
		Principals: []PrincipalRules{
			{
				Name:  "client-a",
				Allow: []Rule{{Action: ActionFetch, Resource: ResourceTopic, Name: "orders"}},
			},
		},
	})
	// Action that doesn't match any rule falls back to default
	if !auth.Allows("client-a", ActionAdmin, ResourceCluster, "cluster") {
		t.Fatal("unmatched action should fall back to default (allow)")
	}
}
