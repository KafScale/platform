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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"golang.org/x/sync/singleflight"
)

const (
	// partitionLeasePrefix is the etcd key prefix for partition ownership leases.
	partitionLeasePrefix = "/kafscale/partition-leases"

	// defaultLeaseTTLSeconds is the TTL for partition leases. When a broker dies,
	// its leases expire after this many seconds, allowing another broker to take over.
	defaultLeaseTTLSeconds = 10
)

var (
	// ErrNotOwner is returned when a broker tries to write to a partition it does not own.
	ErrNotOwner = errors.New("broker does not own this partition")

	// ErrShuttingDown is returned when a lease acquisition is attempted after
	// ReleaseAll has been called. Callers should treat this the same as
	// ErrNotOwner — the broker is draining and should not accept new writes.
	ErrShuttingDown = errors.New("lease manager is shut down")
)

// PartitionLeaseConfig configures the lease manager.
type PartitionLeaseConfig struct {
	// BrokerID identifies this broker in lease keys.
	BrokerID string
	// LeaseTTLSeconds controls how long a lease persists after the broker stops refreshing.
	LeaseTTLSeconds int
	// Logger for operational messages.
	Logger *slog.Logger
}

// PartitionLeaseManager uses etcd leases to ensure exclusive partition ownership.
//
// All partition lease keys are attached to a single shared etcd session/lease,
// so the keepalive cost is O(1) regardless of partition count. When the session
// dies (broker crash, network partition), etcd expires all keys after the TTL
// and the manager bulk-clears its local ownership map.
//
// Concurrent Acquire calls for the same partition are deduplicated via
// singleflight to avoid redundant etcd round-trips and session leaks.
type PartitionLeaseManager struct {
	client   *clientv3.Client
	brokerID string
	ttl      int
	logger   *slog.Logger
	closed   atomic.Bool

	mu         sync.RWMutex
	partitions map[string]struct{} // key: "topic:partition"
	session    *concurrency.Session

	acquireFlight singleflight.Group
}

// NewPartitionLeaseManager creates a lease manager backed by the given etcd client.
func NewPartitionLeaseManager(client *clientv3.Client, cfg PartitionLeaseConfig) *PartitionLeaseManager {
	ttl := cfg.LeaseTTLSeconds
	if ttl <= 0 {
		ttl = defaultLeaseTTLSeconds
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &PartitionLeaseManager{
		client:     client,
		brokerID:   cfg.BrokerID,
		ttl:        ttl,
		logger:     logger,
		partitions: make(map[string]struct{}),
	}
}

// partitionLeaseKey returns the etcd key for a partition ownership lease.
func partitionLeaseKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/%s/%d", partitionLeasePrefix, topic, partition)
}

// PartitionLeasePrefix returns the etcd prefix for watching partition leases.
func PartitionLeasePrefix() string {
	return partitionLeasePrefix
}

// Acquire tries to grab the partition lease. If this broker already owns it,
// it returns nil immediately. If another broker owns it, it returns ErrNotOwner.
//
// The lease is kept alive automatically via the shared etcd session. When the
// broker crashes the session expires and all partition keys are removed by etcd.
func (m *PartitionLeaseManager) Acquire(ctx context.Context, topic string, partition int32) error {
	if m.closed.Load() {
		return ErrShuttingDown
	}

	key := partitionKey(topic, partition)

	m.mu.RLock()
	if _, ok := m.partitions[key]; ok {
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	// Deduplicate concurrent Acquire calls for the same partition. Two produce
	// requests arriving simultaneously for an unowned partition would otherwise
	// both create sessions and race on the CAS, leaking the loser's session.
	_, err, _ := m.acquireFlight.Do(key, func() (interface{}, error) {
		return nil, m.doAcquire(ctx, topic, partition)
	})
	return err
}

func (m *PartitionLeaseManager) doAcquire(ctx context.Context, topic string, partition int32) error {
	key := partitionKey(topic, partition)

	// Re-check under read lock: another goroutine sharing this singleflight
	// call may have already stored the result from a previous batch.
	m.mu.RLock()
	if _, ok := m.partitions[key]; ok {
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	session, err := m.getOrCreateSession(ctx)
	if err != nil {
		return fmt.Errorf("get session: %w", err)
	}

	leaseKey := partitionLeaseKey(topic, partition)

	// Try to create the key only if it does not exist (CreateRevision == 0).
	// This is the atomic compare-and-swap that prevents two brokers from
	// both claiming the same partition.
	txnCtx, txnCancel := context.WithTimeout(ctx, 5*time.Second)
	defer txnCancel()

	txnResp, err := m.client.Txn(txnCtx).
		If(clientv3.Compare(clientv3.CreateRevision(leaseKey), "=", 0)).
		Then(clientv3.OpPut(leaseKey, m.brokerID, clientv3.WithLease(session.Lease()))).
		Else(clientv3.OpGet(leaseKey)).
		Commit()

	if err != nil {
		return fmt.Errorf("partition lease txn: %w", err)
	}

	if !txnResp.Succeeded {
		// Someone else owns the lease. Check if it's us (e.g. after a restart
		// where the old session hasn't expired yet).
		if len(txnResp.Responses) > 0 {
			rangeResp := txnResp.Responses[0].GetResponseRange()
			if rangeResp != nil && len(rangeResp.Kvs) > 0 {
				owner := string(rangeResp.Kvs[0].Value)
				if owner == m.brokerID {
					return m.reacquire(ctx, topic, partition, leaseKey, session)
				}
			}
		}
		return ErrNotOwner
	}

	// Store ownership under the lock. Verify the session is still the active
	// one — if it died between getOrCreateSession and now, our etcd key will
	// expire and we must not claim local ownership.
	m.mu.Lock()
	if m.session != session {
		m.mu.Unlock()
		return fmt.Errorf("session changed during acquire")
	}
	m.partitions[key] = struct{}{}
	m.mu.Unlock()

	m.logger.Info("acquired partition lease",
		"topic", topic, "partition", partition, "broker", m.brokerID)
	return nil
}

// reacquire overwrites the lease key with our current session when we discover
// that we still own the key from a previous (possibly expiring) session.
func (m *PartitionLeaseManager) reacquire(ctx context.Context, topic string, partition int32, leaseKey string, session *concurrency.Session) error {
	key := partitionKey(topic, partition)

	txnCtx, txnCancel := context.WithTimeout(ctx, 5*time.Second)
	defer txnCancel()

	// Conditional Put: only overwrite if we still own the key. This prevents a race
	// where our old session expires and another broker acquires between the read in
	// Acquire and the write here.
	txnResp, err := m.client.Txn(txnCtx).
		If(clientv3.Compare(clientv3.Value(leaseKey), "=", m.brokerID)).
		Then(clientv3.OpPut(leaseKey, m.brokerID, clientv3.WithLease(session.Lease()))).
		Commit()
	if err != nil {
		return fmt.Errorf("reacquire partition lease: %w", err)
	}
	if !txnResp.Succeeded {
		return ErrNotOwner
	}

	m.mu.Lock()
	if m.session != session {
		m.mu.Unlock()
		return fmt.Errorf("session changed during reacquire")
	}
	m.partitions[key] = struct{}{}
	m.mu.Unlock()

	m.logger.Info("reacquired partition lease",
		"topic", topic, "partition", partition, "broker", m.brokerID)
	return nil
}

// getOrCreateSession returns the shared session, creating one if necessary.
// A single monitoring goroutine watches for session death and bulk-clears
// all owned partitions when it happens.
func (m *PartitionLeaseManager) getOrCreateSession(ctx context.Context) (*concurrency.Session, error) {
	m.mu.Lock()
	if m.session != nil {
		select {
		case <-m.session.Done():
			// Session died — clear stale state.
			m.session = nil
			m.partitions = make(map[string]struct{})
		default:
			s := m.session
			m.mu.Unlock()
			return s, nil
		}
	}
	m.mu.Unlock()

	// Create a new session outside the lock (network round-trip).
	session, err := concurrency.NewSession(m.client, concurrency.WithTTL(m.ttl))
	if err != nil {
		return nil, fmt.Errorf("create etcd session: %w", err)
	}

	m.mu.Lock()
	// ReleaseAll may have fired while we were creating the session. If so,
	// close the session we just created and reject the acquisition.
	if m.closed.Load() {
		m.mu.Unlock()
		session.Close()
		return nil, ErrShuttingDown
	}
	// Double-check: another goroutine may have created a session while we
	// were blocked on the network call.
	if m.session != nil {
		select {
		case <-m.session.Done():
			// Still dead, use ours.
		default:
			// Someone else won the race — close ours and use theirs.
			s := m.session
			m.mu.Unlock()
			session.Close()
			return s, nil
		}
	}
	m.session = session
	go m.monitorSession(session)
	m.mu.Unlock()
	return session, nil
}

// monitorSession watches the session lifetime. When it dies (etcd connectivity
// loss, lease expiry), all partition ownership is invalidated. Next Acquire
// call will create a fresh session.
func (m *PartitionLeaseManager) monitorSession(session *concurrency.Session) {
	<-session.Done()

	m.mu.Lock()
	// Only clear if this is still the active session (ReleaseAll may have
	// already replaced it with nil and started a new one).
	if m.session == session {
		m.session = nil
		count := len(m.partitions)
		m.partitions = make(map[string]struct{})
		m.mu.Unlock()
		m.logger.Warn("partition lease session expired, cleared all ownership",
			"broker", m.brokerID, "count", count)
	} else {
		m.mu.Unlock()
	}
}

// PartitionID identifies a topic-partition pair.
type PartitionID struct {
	Topic     string
	Partition int32
}

// AcquireResult holds the outcome of a single partition lease acquisition.
type AcquireResult struct {
	Partition PartitionID
	Err       error
}

// AcquireAll attempts to acquire leases for all given partitions concurrently.
// Partitions already owned by this broker are skipped (no etcd round-trip).
// Returns a result per partition; callers should check each Err.
func (m *PartitionLeaseManager) AcquireAll(ctx context.Context, partitions []PartitionID) []AcquireResult {
	results := make([]AcquireResult, len(partitions))

	// Fast path: filter out already-owned partitions.
	var needAcquire []int
	m.mu.RLock()
	for i, p := range partitions {
		results[i].Partition = p
		key := partitionKey(p.Topic, p.Partition)
		if _, ok := m.partitions[key]; !ok {
			needAcquire = append(needAcquire, i)
		}
	}
	m.mu.RUnlock()

	if len(needAcquire) == 0 {
		return results
	}

	// Acquire remaining partitions concurrently.
	var wg sync.WaitGroup
	for _, idx := range needAcquire {
		idx := idx
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[idx].Err = m.Acquire(ctx, partitions[idx].Topic, partitions[idx].Partition)
		}()
	}
	wg.Wait()
	return results
}

// Owns returns true if this broker currently holds the lease for the partition.
func (m *PartitionLeaseManager) Owns(topic string, partition int32) bool {
	key := partitionKey(topic, partition)
	m.mu.RLock()
	_, ok := m.partitions[key]
	m.mu.RUnlock()
	return ok
}

// Release explicitly gives up ownership of a single partition.
func (m *PartitionLeaseManager) Release(topic string, partition int32) {
	key := partitionKey(topic, partition)
	m.mu.Lock()
	_, ok := m.partitions[key]
	if ok {
		delete(m.partitions, key)
	}
	m.mu.Unlock()

	if ok {
		// Delete the etcd key so another broker can acquire immediately.
		leaseKey := partitionLeaseKey(topic, partition)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := m.client.Delete(ctx, leaseKey); err != nil {
			m.logger.Warn("failed to delete partition lease key",
				"key", leaseKey, "error", err)
		}
		m.logger.Info("released partition lease",
			"topic", topic, "partition", partition, "broker", m.brokerID)
	}
}

// ReleaseAll releases all partition leases. Called during graceful shutdown.
// Closing the shared session revokes the underlying etcd lease, which removes
// all attached keys atomically.
func (m *PartitionLeaseManager) ReleaseAll() {
	m.closed.Store(true)
	m.mu.Lock()
	count := len(m.partitions)
	m.partitions = make(map[string]struct{})
	session := m.session
	m.session = nil
	m.mu.Unlock()

	if session != nil {
		session.Close()
	}
	m.logger.Info("released all partition leases", "broker", m.brokerID, "count", count)
}

// CurrentOwner queries etcd to find the current owner of a partition.
// Returns the broker ID of the owner, or empty string if unowned.
func (m *PartitionLeaseManager) CurrentOwner(ctx context.Context, topic string, partition int32) (string, error) {
	leaseKey := partitionLeaseKey(topic, partition)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := m.client.Get(ctx, leaseKey)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}
