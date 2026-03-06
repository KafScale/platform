// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
)

func TestGroupLeaseKeyToGroupID(t *testing.T) {
	tests := []struct {
		etcdKey string
		want    string
		wantOK  bool
	}{
		{groupLeasePrefix + "/my-group", "my-group", true},
		{groupLeasePrefix + "/consumer-group-1", "consumer-group-1", true},
		{groupLeasePrefix + "/group.with.dots", "group.with.dots", true},

		// Invalid cases.
		{"/wrong/prefix/my-group", "", false},
		{groupLeasePrefix + "/", "", false},
		{"", "", false},
		{groupLeasePrefix, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.etcdKey, func(t *testing.T) {
			got, ok := groupLeaseKeyToGroupID(tc.etcdKey)
			if ok != tc.wantOK {
				t.Fatalf("groupLeaseKeyToGroupID(%q): ok=%v, want %v", tc.etcdKey, ok, tc.wantOK)
			}
			if got != tc.want {
				t.Fatalf("groupLeaseKeyToGroupID(%q) = %q, want %q", tc.etcdKey, got, tc.want)
			}
		})
	}
}


// Router reflects group lease acquisition.
func TestGroupRouterReflectsAcquisition(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()

	routerCli := newEtcdClientForTest(t, endpoints)
	router, err := NewGroupRouter(ctx, routerCli, slog.Default())
	if err != nil {
		t.Fatalf("create router: %v", err)
	}
	t.Cleanup(router.Stop)

	brokerA := newGroupLeaseManager(t, endpoints, "broker-a", 30)
	if err := brokerA.Acquire(ctx, "my-group"); err != nil {
		t.Fatalf("broker-a acquire: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if owner := router.LookupOwner("my-group"); owner == "broker-a" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("router did not reflect broker-a ownership of my-group (got %q)", router.LookupOwner("my-group"))
}

// Router reflects group lease release.
func TestGroupRouterReflectsRelease(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()

	routerCli := newEtcdClientForTest(t, endpoints)
	router, err := NewGroupRouter(ctx, routerCli, slog.Default())
	if err != nil {
		t.Fatalf("create router: %v", err)
	}
	t.Cleanup(router.Stop)

	brokerA := newGroupLeaseManager(t, endpoints, "broker-a", 30)
	if err := brokerA.Acquire(ctx, "my-group"); err != nil {
		t.Fatalf("broker-a acquire: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if router.LookupOwner("my-group") == "broker-a" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if router.LookupOwner("my-group") != "broker-a" {
		t.Fatalf("router did not reflect initial acquisition")
	}

	brokerA.Release("my-group")

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if owner := router.LookupOwner("my-group"); owner == "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("router did not reflect release of my-group (still shows %q)", router.LookupOwner("my-group"))
}

// Invalidate clears the router cache and reloads.
func TestGroupRouterInvalidate(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()

	routerCli := newEtcdClientForTest(t, endpoints)
	router, err := NewGroupRouter(ctx, routerCli, slog.Default())
	if err != nil {
		t.Fatalf("create router: %v", err)
	}
	t.Cleanup(router.Stop)

	brokerA := newGroupLeaseManager(t, endpoints, "broker-a", 30)
	if err := brokerA.Acquire(ctx, "inv-group"); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if router.LookupOwner("inv-group") == "broker-a" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Invalidate removes the cached route
	router.Invalidate("inv-group")

	// Immediately after invalidate, the route should be empty
	if owner := router.LookupOwner("inv-group"); owner != "" {
		t.Fatalf("expected empty owner after invalidate, got %q", owner)
	}
}

// Multiple groups route to different brokers.
func TestGroupRouterMultipleBrokers(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()

	routerCli := newEtcdClientForTest(t, endpoints)
	router, err := NewGroupRouter(ctx, routerCli, slog.Default())
	if err != nil {
		t.Fatalf("create router: %v", err)
	}
	t.Cleanup(router.Stop)

	brokerA := newGroupLeaseManager(t, endpoints, "broker-a", 30)
	brokerB := newGroupLeaseManager(t, endpoints, "broker-b", 30)

	if err := brokerA.Acquire(ctx, "group-1"); err != nil {
		t.Fatalf("broker-a acquire group-1: %v", err)
	}
	if err := brokerB.Acquire(ctx, "group-2"); err != nil {
		t.Fatalf("broker-b acquire group-2: %v", err)
	}

	expected := map[string]string{
		"group-1": "broker-a",
		"group-2": "broker-b",
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allMatch := true
		for groupID, wantBroker := range expected {
			if router.LookupOwner(groupID) != wantBroker {
				allMatch = false
				break
			}
		}
		if allMatch {
			routes := router.AllRoutes()
			if len(routes) != len(expected) {
				t.Fatalf("expected %d routes, got %d", len(expected), len(routes))
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	for groupID, wantBroker := range expected {
		got := router.LookupOwner(groupID)
		if got != wantBroker {
			t.Errorf("LookupOwner(%s) = %q, want %q", groupID, got, wantBroker)
		}
	}
	t.Fatalf("router did not converge to expected state")
}
