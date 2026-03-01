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
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/KafScale/platform/internal/testutil"
)

func newEtcdClientForTest(t *testing.T, endpoints []string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("create etcd client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

func newLeaseManager(t *testing.T, endpoints []string, brokerID string, ttlSeconds int) *PartitionLeaseManager {
	t.Helper()
	cli := newEtcdClientForTest(t, endpoints)
	return NewPartitionLeaseManager(cli, PartitionLeaseConfig{
		BrokerID:        brokerID,
		LeaseTTLSeconds: ttlSeconds,
		Logger:          slog.Default(),
	})
}

// Scenario 1: Two brokers can't write to the same partition simultaneously.
// Broker A acquires orders/0, broker B must get ErrNotOwner.
func TestLeaseExclusivity(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)

	brokerA := newLeaseManager(t, endpoints, "broker-a", 10)
	brokerB := newLeaseManager(t, endpoints, "broker-b", 10)

	ctx := context.Background()

	if err := brokerA.Acquire(ctx, "orders", 0); err != nil {
		t.Fatalf("broker-a acquire: %v", err)
	}

	err := brokerB.Acquire(ctx, "orders", 0)
	if err == nil {
		t.Fatalf("broker-b should not be able to acquire partition owned by broker-a")
	}
	if err != ErrNotOwner {
		t.Fatalf("expected ErrNotOwner, got: %v", err)
	}

	if !brokerA.Owns("orders", 0) {
		t.Fatalf("broker-a should own orders/0")
	}
	if brokerB.Owns("orders", 0) {
		t.Fatalf("broker-b should not own orders/0")
	}
}

// Scenario 2: Lease expiry enables failover.
// Broker A acquires, its session closes, after TTL broker B can acquire.
func TestLeaseExpiryFailover(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)

	// Use a short TTL so the test doesn't take too long.
	ttl := 2

	cliA := newEtcdClientForTest(t, endpoints)
	brokerA := NewPartitionLeaseManager(cliA, PartitionLeaseConfig{
		BrokerID:        "broker-a",
		LeaseTTLSeconds: ttl,
		Logger:          slog.Default(),
	})
	brokerB := newLeaseManager(t, endpoints, "broker-b", ttl)

	ctx := context.Background()

	if err := brokerA.Acquire(ctx, "orders", 0); err != nil {
		t.Fatalf("broker-a acquire: %v", err)
	}

	// Simulate broker A crashing by closing its etcd client.
	// This terminates the session keepalive, so the lease expires after TTL.
	cliA.Close()

	if err := brokerB.Acquire(ctx, "orders", 0); err == nil {
		t.Fatalf("broker-b should not acquire before lease expires")
	}

	// Wait for TTL + buffer for etcd to expire the lease.
	time.Sleep(time.Duration(ttl+1) * time.Second)

	if err := brokerB.Acquire(ctx, "orders", 0); err != nil {
		t.Fatalf("broker-b should acquire after lease expiry: %v", err)
	}
	if !brokerB.Owns("orders", 0) {
		t.Fatalf("broker-b should own orders/0 after failover")
	}
}

// Scenario 3: Graceful shutdown releases immediately.
// Broker A acquires, calls ReleaseAll, broker B can acquire without waiting.
func TestGracefulReleaseImmediate(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)

	brokerA := newLeaseManager(t, endpoints, "broker-a", 30)
	brokerB := newLeaseManager(t, endpoints, "broker-b", 30)

	ctx := context.Background()

	if err := brokerA.Acquire(ctx, "orders", 0); err != nil {
		t.Fatalf("broker-a acquire: %v", err)
	}
	if err := brokerA.Acquire(ctx, "orders", 1); err != nil {
		t.Fatalf("broker-a acquire partition 1: %v", err)
	}

	brokerA.ReleaseAll()

	if brokerA.Owns("orders", 0) || brokerA.Owns("orders", 1) {
		t.Fatalf("broker-a should not own any partitions after ReleaseAll")
	}

	// Broker B should acquire immediately, no TTL wait.
	if err := brokerB.Acquire(ctx, "orders", 0); err != nil {
		t.Fatalf("broker-b acquire after release: %v", err)
	}
	if err := brokerB.Acquire(ctx, "orders", 1); err != nil {
		t.Fatalf("broker-b acquire partition 1 after release: %v", err)
	}
}

// Scenario 4: Reacquire after restart.
// Broker A acquires, its session dies but the old lease key still has broker A's ID.
// Broker A reconnects and acquires again — should succeed via reacquire path.
func TestReacquireAfterRestart(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)

	ttl := 5

	cliA1 := newEtcdClientForTest(t, endpoints)
	brokerA1 := NewPartitionLeaseManager(cliA1, PartitionLeaseConfig{
		BrokerID:        "broker-a",
		LeaseTTLSeconds: ttl,
		Logger:          slog.Default(),
	})

	ctx := context.Background()

	if err := brokerA1.Acquire(ctx, "orders", 0); err != nil {
		t.Fatalf("broker-a (session 1) acquire: %v", err)
	}

	// Simulate restart: close the old client but don't wait for expiry.
	// The lease key still exists in etcd with value "broker-a".
	cliA1.Close()

	brokerA2 := newLeaseManager(t, endpoints, "broker-a", ttl)

	if err := brokerA2.Acquire(ctx, "orders", 0); err != nil {
		t.Fatalf("broker-a (session 2) reacquire should succeed: %v", err)
	}
	if !brokerA2.Owns("orders", 0) {
		t.Fatalf("broker-a (session 2) should own orders/0 after reacquire")
	}

	brokerB := newLeaseManager(t, endpoints, "broker-b", ttl)
	if err := brokerB.Acquire(ctx, "orders", 0); err != ErrNotOwner {
		t.Fatalf("broker-b should get ErrNotOwner after broker-a reacquire, got: %v", err)
	}
}

// Scenario 9: Concurrent acquire race.
// Two brokers race to acquire the same unowned partition. Exactly one must win.
func TestConcurrentAcquireRace(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)

	const brokerCount = 5
	managers := make([]*PartitionLeaseManager, brokerCount)
	for i := range managers {
		managers[i] = newLeaseManager(t, endpoints, fmt.Sprintf("broker-%d", i), 30)
	}

	ctx := context.Background()
	results := make([]error, brokerCount)
	var wg sync.WaitGroup

	for i := range managers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = managers[idx].Acquire(ctx, "contested", 0)
		}(i)
	}
	wg.Wait()

	winners := 0
	losers := 0
	for i, err := range results {
		switch err {
		case nil:
			winners++
			if !managers[i].Owns("contested", 0) {
				t.Errorf("broker-%d won but doesn't report ownership", i)
			}
		case ErrNotOwner:
			losers++
		default:
			t.Errorf("broker-%d got unexpected error: %v", i, err)
		}
	}

	if winners != 1 {
		t.Fatalf("expected exactly 1 winner, got %d (losers=%d)", winners, losers)
	}
	if losers != brokerCount-1 {
		t.Fatalf("expected %d losers, got %d", brokerCount-1, losers)
	}
}
