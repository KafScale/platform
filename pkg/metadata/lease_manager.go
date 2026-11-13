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
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"golang.org/x/sync/singleflight"
)

const defaultLeaseManagerTTLSeconds = 10

// LeaseManagerConfig configures a generic lease manager.
type LeaseManagerConfig struct {
	BrokerID        string
	Prefix          string // etcd key prefix, e.g. "/kafscale/partition-leases"
	LeaseTTLSeconds int
	Logger          *slog.Logger
	ResourceKind    string // used in log messages, e.g. "partition" or "group"
}

// LeaseManagerOptions adds ownership callbacks without changing the published config.
type LeaseManagerOptions struct {
	LeaseManagerConfig
	// OnLost runs under the ownership lock; it must not call back into this manager.
	OnLost func(resourceID string, token uint64)
}

// LeaseManager uses etcd leases to ensure exclusive ownership of named resources.
//
// All lease keys are attached to a single shared etcd session/lease, so the
// keepalive cost is O(1) regardless of resource count. When the session dies
// (broker crash, network partition), etcd expires all keys after the TTL and
// the manager bulk-clears its local ownership map.
//
// Concurrent Acquire calls for the same resource are deduplicated via
// singleflight to avoid redundant etcd round-trips and session leaks.
type LeaseManager struct {
	client       *clientv3.Client
	brokerID     string
	prefix       string
	ttl          int
	logger       *slog.Logger
	resourceKind string
	closed       atomic.Bool

	mu      sync.RWMutex
	owned   map[string]uint64 // key: resource identifier
	session *concurrency.Session
	onLost  func(string, uint64)

	acquireFlight singleflight.Group
}

// NewLeaseManager creates a lease manager backed by the given etcd client.
func NewLeaseManager(client *clientv3.Client, cfg LeaseManagerConfig) *LeaseManager {
	return NewLeaseManagerWithOptions(client, LeaseManagerOptions{LeaseManagerConfig: cfg})
}

// NewLeaseManagerWithOptions constructs a manager with ownership callbacks.
func NewLeaseManagerWithOptions(client *clientv3.Client, cfg LeaseManagerOptions) *LeaseManager {
	ttl := cfg.LeaseTTLSeconds
	if ttl <= 0 {
		ttl = defaultLeaseManagerTTLSeconds
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	kind := cfg.ResourceKind
	if kind == "" {
		kind = "resource"
	}
	return &LeaseManager{
		client:       client,
		brokerID:     cfg.BrokerID,
		prefix:       cfg.Prefix,
		ttl:          ttl,
		logger:       logger,
		resourceKind: kind,
		owned:        make(map[string]uint64),
		onLost:       cfg.OnLost,
	}
}

// leaseKey returns the etcd key for a resource.
func (m *LeaseManager) leaseKey(resourceID string) string {
	return fmt.Sprintf("%s/%s", m.prefix, resourceID)
}

// Prefix returns the etcd prefix for watching leases.
func (m *LeaseManager) Prefix() string {
	return m.prefix
}

// Acquire tries to grab the lease for the named resource. If this broker
// already owns it, it returns nil immediately. If another broker owns it,
// it returns ErrNotOwner.
func (m *LeaseManager) Acquire(ctx context.Context, resourceID string) error {
	if m.closed.Load() {
		return ErrShuttingDown
	}

	if m.Owns(resourceID) {
		return nil
	}

	_, err, _ := m.acquireFlight.Do(resourceID, func() (interface{}, error) {
		return nil, m.doAcquire(ctx, resourceID)
	})
	return err
}

func (m *LeaseManager) doAcquire(ctx context.Context, resourceID string) error {
	// Re-check under read lock.
	if m.Owns(resourceID) {
		return nil
	}

	session, err := m.getOrCreateSession(ctx)
	if err != nil {
		return fmt.Errorf("get session: %w", err)
	}

	leaseKey := m.leaseKey(resourceID)

	txnCtx, txnCancel := context.WithTimeout(ctx, 5*time.Second)
	defer txnCancel()

	txnResp, err := m.client.Txn(txnCtx).
		If(clientv3.Compare(clientv3.CreateRevision(leaseKey), "=", 0)).
		Then(clientv3.OpPut(leaseKey, m.brokerID, clientv3.WithLease(session.Lease()))).
		Else(clientv3.OpGet(leaseKey)).
		Commit()

	if err != nil {
		return fmt.Errorf("%s lease txn: %w", m.resourceKind, err)
	}

	if !txnResp.Succeeded {
		if len(txnResp.Responses) > 0 {
			rangeResp := txnResp.Responses[0].GetResponseRange()
			if rangeResp != nil && len(rangeResp.Kvs) > 0 {
				owner := string(rangeResp.Kvs[0].Value)
				if owner == m.brokerID {
					return m.reacquire(ctx, resourceID, leaseKey, session)
				}
			}
		}
		return ErrNotOwner
	}

	m.mu.Lock()
	if m.session != session {
		m.mu.Unlock()
		return fmt.Errorf("session changed during acquire")
	}
	select {
	case <-session.Done():
		m.mu.Unlock()
		return ErrNotOwner
	default:
	}
	m.owned[resourceID] = uint64(txnResp.Header.Revision)
	m.mu.Unlock()

	m.logger.Info(fmt.Sprintf("acquired %s lease", m.resourceKind),
		m.resourceKind, resourceID, "broker", m.brokerID)
	return nil
}

func (m *LeaseManager) reacquire(ctx context.Context, resourceID, leaseKey string, session *concurrency.Session) error {
	txnCtx, txnCancel := context.WithTimeout(ctx, 5*time.Second)
	defer txnCancel()

	txnResp, err := m.client.Txn(txnCtx).
		If(clientv3.Compare(clientv3.Value(leaseKey), "=", m.brokerID)).
		Then(clientv3.OpPut(leaseKey, m.brokerID, clientv3.WithLease(session.Lease()))).
		Commit()
	if err != nil {
		return fmt.Errorf("reacquire %s lease: %w", m.resourceKind, err)
	}
	if !txnResp.Succeeded {
		return ErrNotOwner
	}

	m.mu.Lock()
	if m.session != session {
		m.mu.Unlock()
		return fmt.Errorf("session changed during reacquire")
	}
	select {
	case <-session.Done():
		m.mu.Unlock()
		return ErrNotOwner
	default:
	}
	m.owned[resourceID] = uint64(txnResp.Header.Revision)
	m.mu.Unlock()

	m.logger.Info(fmt.Sprintf("reacquired %s lease", m.resourceKind),
		m.resourceKind, resourceID, "broker", m.brokerID)
	return nil
}

func (m *LeaseManager) getOrCreateSession(ctx context.Context) (*concurrency.Session, error) {
	m.mu.Lock()
	if m.session != nil {
		select {
		case <-m.session.Done():
			m.session = nil
			m.clearOwnershipLocked()
		default:
			s := m.session
			m.mu.Unlock()
			return s, nil
		}
	}
	m.mu.Unlock()

	session, err := concurrency.NewSession(m.client, concurrency.WithTTL(m.ttl))
	if err != nil {
		return nil, fmt.Errorf("create etcd session: %w", err)
	}

	m.mu.Lock()
	if m.closed.Load() {
		m.mu.Unlock()
		_ = session.Close()
		return nil, ErrShuttingDown
	}
	if m.session != nil {
		select {
		case <-m.session.Done():
			m.clearOwnershipLocked()
		default:
			s := m.session
			m.mu.Unlock()
			_ = session.Close()
			return s, nil
		}
	}
	m.session = session
	go m.monitorSession(session)
	m.mu.Unlock()
	return session, nil
}

func (m *LeaseManager) monitorSession(session *concurrency.Session) {
	<-session.Done()

	m.mu.Lock()
	if m.session == session {
		m.session = nil
		count := len(m.owned)
		m.clearOwnershipLocked()
		m.mu.Unlock()
		m.logger.Warn(fmt.Sprintf("%s lease session expired, cleared all ownership", m.resourceKind),
			"broker", m.brokerID, "count", count)
	} else {
		m.mu.Unlock()
	}
}

// clearOwnershipLocked invalidates dependent state before ownership can be reacquired.
func (m *LeaseManager) clearOwnershipLocked() {
	for id, token := range m.owned {
		if m.onLost != nil {
			m.onLost(id, token)
		}
	}
	m.owned = make(map[string]uint64)
}

// OwnershipToken identifies one uninterrupted acquisition, or zero if not owned.
// Check session.Done directly so callers never depend on monitor scheduling.
func (m *LeaseManager) OwnershipToken(resourceID string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.session == nil {
		return 0
	}
	select {
	case <-m.session.Done():
		return 0
	default:
		return m.owned[resourceID]
	}
}

// Owns returns true if this broker currently holds a live lease.
func (m *LeaseManager) Owns(resourceID string) bool {
	return m.OwnershipToken(resourceID) != 0
}

// Release explicitly gives up ownership of a single resource.
func (m *LeaseManager) Release(resourceID string) {
	m.mu.Lock()
	_, ok := m.owned[resourceID]
	session := m.session
	if ok {
		if m.onLost != nil {
			m.onLost(resourceID, m.owned[resourceID])
		}
		delete(m.owned, resourceID)
	}
	m.mu.Unlock()

	if ok {
		leaseKey := m.leaseKey(resourceID)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		checks := []clientv3.Cmp{clientv3.Compare(clientv3.Value(leaseKey), "=", m.brokerID)}
		if session != nil {
			checks = append(checks, clientv3.Compare(clientv3.LeaseValue(leaseKey), "=", int64(session.Lease())))
		}
		if _, err := m.client.Txn(ctx).If(checks...).Then(clientv3.OpDelete(leaseKey)).Commit(); err != nil {
			m.logger.Warn(fmt.Sprintf("failed to delete %s lease key", m.resourceKind),
				"key", leaseKey, "error", err)
		}
		m.logger.Info(fmt.Sprintf("released %s lease", m.resourceKind),
			m.resourceKind, resourceID, "broker", m.brokerID)
	}
}

// ReleaseAll releases all leases. Called during graceful shutdown.
func (m *LeaseManager) ReleaseAll() {
	m.closed.Store(true)
	m.mu.Lock()
	count := len(m.owned)
	m.clearOwnershipLocked()
	session := m.session
	m.session = nil
	m.mu.Unlock()

	if session != nil {
		_ = session.Close()
	}
	m.logger.Info(fmt.Sprintf("released all %s leases", m.resourceKind),
		"broker", m.brokerID, "count", count)
}

// CurrentOwner queries etcd to find the current owner of a resource.
// Returns the broker ID of the owner, or empty string if unowned.
func (m *LeaseManager) CurrentOwner(ctx context.Context, resourceID string) (string, error) {
	leaseKey := m.leaseKey(resourceID)
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

// EtcdClient returns the underlying etcd client. Used by routers that need
// to watch the same prefix.
func (m *LeaseManager) EtcdClient() *clientv3.Client {
	return m.client
}
