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

	"github.com/KafScale/platform/internal/testutil"
)

func newGroupLeaseManager(t *testing.T, endpoints []string, brokerID string, ttlSeconds int) *GroupLeaseManager {
	t.Helper()
	cli := newEtcdClientForTest(t, endpoints)
	return NewGroupLeaseManager(cli, GroupLeaseConfig{
		BrokerID:        brokerID,
		LeaseTTLSeconds: ttlSeconds,
		Logger:          slog.Default(),
	})
}

// Two brokers can't coordinate the same group simultaneously.
func TestGroupLeaseExclusivity(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)

	brokerA := newGroupLeaseManager(t, endpoints, "broker-a", 10)
	brokerB := newGroupLeaseManager(t, endpoints, "broker-b", 10)

	ctx := context.Background()

	if err := brokerA.Acquire(ctx, "my-group"); err != nil {
		t.Fatalf("broker-a acquire: %v", err)
	}

	err := brokerB.Acquire(ctx, "my-group")
	if err == nil {
		t.Fatalf("broker-b should not be able to acquire group owned by broker-a")
	}
	if err != ErrNotOwner {
		t.Fatalf("expected ErrNotOwner, got: %v", err)
	}

	if !brokerA.Owns("my-group") {
		t.Fatalf("broker-a should own my-group")
	}
	if brokerB.Owns("my-group") {
		t.Fatalf("broker-b should not own my-group")
	}
}

// Lease expiry enables failover.
func TestGroupLeaseExpiryFailover(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)

	ttl := 2

	cliA := newEtcdClientForTest(t, endpoints)
	brokerA := NewGroupLeaseManager(cliA, GroupLeaseConfig{
		BrokerID:        "broker-a",
		LeaseTTLSeconds: ttl,
		Logger:          slog.Default(),
	})
	brokerB := newGroupLeaseManager(t, endpoints, "broker-b", ttl)

	ctx := context.Background()

	if err := brokerA.Acquire(ctx, "my-group"); err != nil {
		t.Fatalf("broker-a acquire: %v", err)
	}

	cliA.Close()

	if err := brokerB.Acquire(ctx, "my-group"); err == nil {
		t.Fatalf("broker-b should not acquire before lease expires")
	}

	time.Sleep(time.Duration(ttl+1) * time.Second)

	if err := brokerB.Acquire(ctx, "my-group"); err != nil {
		t.Fatalf("broker-b should acquire after lease expiry: %v", err)
	}
	if !brokerB.Owns("my-group") {
		t.Fatalf("broker-b should own my-group after failover")
	}
}

// Graceful shutdown releases immediately.
func TestGroupGracefulReleaseImmediate(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)

	brokerA := newGroupLeaseManager(t, endpoints, "broker-a", 30)
	brokerB := newGroupLeaseManager(t, endpoints, "broker-b", 30)

	ctx := context.Background()

	if err := brokerA.Acquire(ctx, "group-1"); err != nil {
		t.Fatalf("broker-a acquire group-1: %v", err)
	}
	if err := brokerA.Acquire(ctx, "group-2"); err != nil {
		t.Fatalf("broker-a acquire group-2: %v", err)
	}

	brokerA.ReleaseAll()

	if brokerA.Owns("group-1") || brokerA.Owns("group-2") {
		t.Fatalf("broker-a should not own any groups after ReleaseAll")
	}

	if err := brokerB.Acquire(ctx, "group-1"); err != nil {
		t.Fatalf("broker-b acquire group-1 after release: %v", err)
	}
	if err := brokerB.Acquire(ctx, "group-2"); err != nil {
		t.Fatalf("broker-b acquire group-2 after release: %v", err)
	}
}

// Reacquire after restart.
func TestGroupReacquireAfterRestart(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)

	ttl := 5

	cliA1 := newEtcdClientForTest(t, endpoints)
	brokerA1 := NewGroupLeaseManager(cliA1, GroupLeaseConfig{
		BrokerID:        "broker-a",
		LeaseTTLSeconds: ttl,
		Logger:          slog.Default(),
	})

	ctx := context.Background()

	if err := brokerA1.Acquire(ctx, "my-group"); err != nil {
		t.Fatalf("broker-a (session 1) acquire: %v", err)
	}

	cliA1.Close()

	brokerA2 := newGroupLeaseManager(t, endpoints, "broker-a", ttl)

	if err := brokerA2.Acquire(ctx, "my-group"); err != nil {
		t.Fatalf("broker-a (session 2) reacquire should succeed: %v", err)
	}
	if !brokerA2.Owns("my-group") {
		t.Fatalf("broker-a (session 2) should own my-group after reacquire")
	}

	brokerB := newGroupLeaseManager(t, endpoints, "broker-b", ttl)
	if err := brokerB.Acquire(ctx, "my-group"); err != ErrNotOwner {
		t.Fatalf("broker-b should get ErrNotOwner after broker-a reacquire, got: %v", err)
	}
}

// Concurrent acquire race: exactly one must win.
func TestGroupConcurrentAcquireRace(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)

	const brokerCount = 5
	managers := make([]*GroupLeaseManager, brokerCount)
	for i := range managers {
		managers[i] = newGroupLeaseManager(t, endpoints, fmt.Sprintf("broker-%d", i), 30)
	}

	ctx := context.Background()
	results := make([]error, brokerCount)
	var wg sync.WaitGroup

	for i := range managers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = managers[idx].Acquire(ctx, "contested-group")
		}(i)
	}
	wg.Wait()

	winners := 0
	losers := 0
	for i, err := range results {
		switch err {
		case nil:
			winners++
			if !managers[i].Owns("contested-group") {
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

// Different groups can be owned by different brokers.
func TestGroupLeaseMultipleGroups(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)

	brokerA := newGroupLeaseManager(t, endpoints, "broker-a", 30)
	brokerB := newGroupLeaseManager(t, endpoints, "broker-b", 30)

	ctx := context.Background()

	if err := brokerA.Acquire(ctx, "group-1"); err != nil {
		t.Fatalf("broker-a acquire group-1: %v", err)
	}
	if err := brokerB.Acquire(ctx, "group-2"); err != nil {
		t.Fatalf("broker-b acquire group-2: %v", err)
	}

	if !brokerA.Owns("group-1") {
		t.Fatalf("broker-a should own group-1")
	}
	if brokerA.Owns("group-2") {
		t.Fatalf("broker-a should not own group-2")
	}
	if !brokerB.Owns("group-2") {
		t.Fatalf("broker-b should own group-2")
	}
	if brokerB.Owns("group-1") {
		t.Fatalf("broker-b should not own group-1")
	}
}
