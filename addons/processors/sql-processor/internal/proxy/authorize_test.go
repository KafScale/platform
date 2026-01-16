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

import "testing"

func TestAuthorizeQuery(t *testing.T) {
	acl := ACL{Allow: []string{"orders", "payments"}}
	allowed, _, _, _ := authorizeQuery(acl, "SELECT * FROM orders LAST 1h;")
	if !allowed {
		t.Fatalf("expected orders query allowed")
	}
	allowed, _, _, _ = authorizeQuery(acl, "SELECT * FROM shipments LAST 1h;")
	if allowed {
		t.Fatalf("expected shipments denied")
	}
	allowed, _, _, _ = authorizeQuery(acl, "SELECT * FROM orders o JOIN payments p ON o._key = p._key WITHIN 10m LAST 1h;")
	if !allowed {
		t.Fatalf("expected join allowed")
	}
}

func TestAuthorizeShowTopics(t *testing.T) {
	acl := ACL{Allow: []string{"orders"}}
	allowed, _, _, _ := authorizeQuery(acl, "SHOW TOPICS;")
	if allowed {
		t.Fatalf("expected show topics denied when scoped")
	}
	allowAll := ACL{}
	allowed, _, _, _ = authorizeQuery(allowAll, "SHOW TOPICS;")
	if !allowed {
		t.Fatalf("expected show topics allowed by default")
	}
}

func TestAuthorizeSpecialCases(t *testing.T) {
	acl := ACL{Allow: []string{"orders"}}
	allowed, _, _, _ := authorizeQuery(acl, "SET client_encoding = 'UTF8';")
	if !allowed {
		t.Fatalf("expected set allowed")
	}
	allowed, _, _, _ = authorizeQuery(acl, "RESET ALL;")
	if !allowed {
		t.Fatalf("expected reset allowed")
	}
	allowed, reason, _, _ := authorizeQuery(acl, "INSERT INTO orders VALUES (1);")
	if allowed || reason == "" {
		t.Fatalf("expected unsupported statement to be denied")
	}
}
