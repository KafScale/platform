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
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
)

func TestLeaseKeyToRouteKey(t *testing.T) {
	tests := []struct {
		etcdKey string
		want    string
		wantOK  bool
	}{
		{partitionLeasePrefix + "/orders/0", "orders:0", true},
		{partitionLeasePrefix + "/orders/123", "orders:123", true},
		{partitionLeasePrefix + "/my.namespace.topic/7", "my.namespace.topic:7", true},
		{partitionLeasePrefix + "/topic-with-dashes/99", "topic-with-dashes:99", true},
		// Topic name containing slashes (e.g. "ns/topic").
		{partitionLeasePrefix + "/ns/topic/3", "ns/topic:3", true},

		// Invalid cases.
		{"/wrong/prefix/orders/0", "", false},
		{partitionLeasePrefix + "/", "", false},
		{partitionLeasePrefix + "/orders/", "", false},
		{partitionLeasePrefix + "/orders/notanumber", "", false},
		{"", "", false},
		{partitionLeasePrefix, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.etcdKey, func(t *testing.T) {
			got, ok := leaseKeyToRouteKey(tc.etcdKey)
			if ok != tc.wantOK {
				t.Fatalf("leaseKeyToRouteKey(%q): ok=%v, want %v", tc.etcdKey, ok, tc.wantOK)
			}
			if got != tc.want {
				t.Fatalf("leaseKeyToRouteKey(%q) = %q, want %q", tc.etcdKey, got, tc.want)
			}
		})
	}
}

func TestParsePartitionKey(t *testing.T) {
	tests := []struct {
		key           string
		wantTopic     string
		wantPartition int32
		wantOK        bool
	}{
		{"orders:0", "orders", 0, true},
		{"orders:123", "orders", 123, true},
		{"my.namespace.topic:7", "my.namespace.topic", 7, true},
		{"ns/topic:3", "ns/topic", 3, true},
		// Colons in topic name: last colon wins.
		{"topic:with:colons:5", "topic:with:colons", 5, true},

		// Invalid cases.
		{"", "", 0, false},
		{"nocolon", "", 0, false},
		{":0", "", 0, false},
		{"orders:", "", 0, false},
		{"orders:abc", "", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			topic, partition, ok := parsePartitionKey(tc.key)
			if ok != tc.wantOK {
				t.Fatalf("parsePartitionKey(%q): ok=%v, want %v", tc.key, ok, tc.wantOK)
			}
			if topic != tc.wantTopic || partition != tc.wantPartition {
				t.Fatalf("parsePartitionKey(%q) = (%q, %d), want (%q, %d)",
					tc.key, topic, partition, tc.wantTopic, tc.wantPartition)
			}
		})
	}
}

func TestLeaseKeyRoundTrip(t *testing.T) {
	// partitionKey -> partitionLeaseKey -> leaseKeyToRouteKey should produce
	// the same string as partitionKey.
	cases := []struct {
		topic     string
		partition int32
	}{
		{"orders", 0},
		{"events", 42},
		{"my.ns.topic", 7},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%d", tc.topic, tc.partition), func(t *testing.T) {
			pk := partitionKey(tc.topic, tc.partition)
			leaseKey := partitionLeaseKey(tc.topic, tc.partition)
			routeKey, ok := leaseKeyToRouteKey(leaseKey)
			if !ok {
				t.Fatalf("leaseKeyToRouteKey(%q) failed", leaseKey)
			}
			if routeKey != pk {
				t.Fatalf("round-trip mismatch: partitionKey=%q, leaseKeyToRouteKey=%q", pk, routeKey)
			}
		})
	}
}

func TestPartitionLeaseKeyFormat(t *testing.T) {
	key := partitionLeaseKey("orders", 3)
	want := partitionLeasePrefix + "/orders/3"
	if key != want {
		t.Fatalf("partitionLeaseKey = %q, want %q", key, want)
	}
}

// Scenario 5: Router reflects lease acquisition.
// Broker A acquires a partition. The router watching etcd should eventually
// return broker A's ID for LookupOwner.
func TestRouterReflectsAcquisition(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()

	routerCli := newEtcdClientForTest(t, endpoints)
	router, err := NewPartitionRouter(ctx, routerCli, slog.Default())
	if err != nil {
		t.Fatalf("create router: %v", err)
	}
	t.Cleanup(router.Stop)

	brokerA := newLeaseManager(t, endpoints, "broker-a", 30)
	if err := brokerA.Acquire(ctx, "orders", 0); err != nil {
		t.Fatalf("broker-a acquire: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if owner := router.LookupOwner("orders", 0); owner == "broker-a" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("router did not reflect broker-a ownership of orders/0 (got %q)", router.LookupOwner("orders", 0))
}

// Scenario 6: Router reflects lease release/expiry.
// Broker A acquires, then releases. Router should eventually return "" for that partition.
func TestRouterReflectsRelease(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()

	routerCli := newEtcdClientForTest(t, endpoints)
	router, err := NewPartitionRouter(ctx, routerCli, slog.Default())
	if err != nil {
		t.Fatalf("create router: %v", err)
	}
	t.Cleanup(router.Stop)

	brokerA := newLeaseManager(t, endpoints, "broker-a", 30)
	if err := brokerA.Acquire(ctx, "orders", 0); err != nil {
		t.Fatalf("broker-a acquire: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if router.LookupOwner("orders", 0) == "broker-a" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if router.LookupOwner("orders", 0) != "broker-a" {
		t.Fatalf("router did not reflect initial acquisition")
	}

	brokerA.Release("orders", 0)

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if owner := router.LookupOwner("orders", 0); owner == "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("router did not reflect release of orders/0 (still shows %q)", router.LookupOwner("orders", 0))
}

// Scenario 7: Multiple partitions route to different brokers.
func TestRouterMultipleBrokersMultiplePartitions(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()

	routerCli := newEtcdClientForTest(t, endpoints)
	router, err := NewPartitionRouter(ctx, routerCli, slog.Default())
	if err != nil {
		t.Fatalf("create router: %v", err)
	}
	t.Cleanup(router.Stop)

	brokerA := newLeaseManager(t, endpoints, "broker-a", 30)
	brokerB := newLeaseManager(t, endpoints, "broker-b", 30)

	if err := brokerA.Acquire(ctx, "orders", 0); err != nil {
		t.Fatalf("broker-a acquire orders/0: %v", err)
	}
	if err := brokerA.Acquire(ctx, "orders", 1); err != nil {
		t.Fatalf("broker-a acquire orders/1: %v", err)
	}
	if err := brokerB.Acquire(ctx, "events", 0); err != nil {
		t.Fatalf("broker-b acquire events/0: %v", err)
	}
	if err := brokerB.Acquire(ctx, "events", 1); err != nil {
		t.Fatalf("broker-b acquire events/1: %v", err)
	}

	expected := map[string]string{
		"orders:0": "broker-a",
		"orders:1": "broker-a",
		"events:0": "broker-b",
		"events:1": "broker-b",
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allMatch := true
		for key, wantBroker := range expected {
			topic, partition, ok := parsePartitionKey(key)
			if !ok {
				t.Fatalf("bad test key: %s", key)
			}
			if router.LookupOwner(topic, partition) != wantBroker {
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

	for key, wantBroker := range expected {
		topic, partition, _ := parsePartitionKey(key)
		got := router.LookupOwner(topic, partition)
		if got != wantBroker {
			t.Errorf("LookupOwner(%s, %d) = %q, want %q", topic, partition, got, wantBroker)
		}
	}
	t.Fatalf("router did not converge to expected state")
}
