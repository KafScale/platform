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
	"strconv"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// partitionLeasePrefix is the etcd key prefix for partition ownership leases.
	partitionLeasePrefix = "/kafscale/partition-leases"
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

// PartitionLeaseOptions adds callbacks without changing the published config.
type PartitionLeaseOptions struct {
	PartitionLeaseConfig
	// OnLost invalidates state for an expired or released acquisition. It must not call this manager.
	OnLost func(topic string, partition int32, token uint64)
	// OnHandover runs after new acquisitions are blocked. It must finish active
	// writes and invoke release before returning. It runs without ownership locks.
	OnHandover func(topic string, partition int32, release func())
}

// PartitionLeaseManager uses etcd leases to ensure exclusive partition ownership.
// It delegates to the generic LeaseManager, translating topic+partition pairs
// into string resource IDs.
type PartitionLeaseManager struct {
	lm         *LeaseManager
	holds      partitionHolds
	onHandover func(string, int32, func())
}

type partitionHolds struct {
	mu    sync.Mutex
	until map[string]time.Time
}

// NewPartitionLeaseManager creates a lease manager backed by the given etcd client.
func NewPartitionLeaseManager(client *clientv3.Client, cfg PartitionLeaseConfig) *PartitionLeaseManager {
	return NewPartitionLeaseManagerWithOptions(client, PartitionLeaseOptions{PartitionLeaseConfig: cfg})
}

// NewPartitionLeaseManagerWithOptions constructs a manager with ownership callbacks.
func NewPartitionLeaseManagerWithOptions(client *clientv3.Client, cfg PartitionLeaseOptions) *PartitionLeaseManager {
	return &PartitionLeaseManager{
		holds:      partitionHolds{until: make(map[string]time.Time)},
		onHandover: cfg.OnHandover,
		lm: NewLeaseManagerWithOptions(client, LeaseManagerOptions{
			LeaseManagerConfig: LeaseManagerConfig{
				BrokerID:        cfg.BrokerID,
				Prefix:          partitionLeasePrefix,
				LeaseTTLSeconds: cfg.LeaseTTLSeconds,
				Logger:          cfg.Logger,
				ResourceKind:    "partition",
			},
			OnLost: func(id string, token uint64) {
				if cfg.OnLost == nil {
					return
				}
				i := strings.LastIndexByte(id, '/')
				if i < 0 {
					return
				}
				p, err := strconv.ParseInt(id[i+1:], 10, 32)
				if err == nil {
					cfg.OnLost(id[:i], int32(p), token)
				}
			},
		}),
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

// partitionResourceID returns the resource ID used internally by LeaseManager.
func partitionResourceID(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}

// Acquire tries to grab the partition lease. If this broker already owns it,
// it returns nil immediately. If another broker owns it, it returns ErrNotOwner.
func (m *PartitionLeaseManager) Acquire(ctx context.Context, topic string, partition int32) error {
	id := partitionResourceID(topic, partition)
	if m.held(id) {
		return ErrNotOwner
	}
	if err := m.lm.Acquire(ctx, id); err != nil {
		return err
	}
	// Handover may install the hold while the etcd transaction is in flight.
	// Release that late acquisition before exposing ownership to a caller.
	if m.held(id) {
		m.lm.Release(id)
		return ErrNotOwner
	}
	return nil
}

// Handover releases a partition and prevents reacquisition for the hold duration.
func (m *PartitionLeaseManager) Handover(topic string, partition int32, hold time.Duration) {
	id := partitionResourceID(topic, partition)
	m.holds.mu.Lock()
	// A zero deadline blocks acquisitions throughout draining and release,
	// even when an append takes longer than the configured hold duration.
	m.holds.until[id] = time.Time{}
	m.holds.mu.Unlock()
	release := func() { m.lm.Release(id) }
	if m.onHandover != nil {
		m.onHandover(topic, partition, release)
	} else {
		release()
	}
	m.holds.mu.Lock()
	m.holds.until[id] = time.Now().Add(hold)
	m.holds.mu.Unlock()
}

func (m *PartitionLeaseManager) held(id string) bool {
	m.holds.mu.Lock()
	defer m.holds.mu.Unlock()
	until, ok := m.holds.until[id]
	if !ok {
		return false
	}
	if until.IsZero() || time.Now().Before(until) {
		return true
	}
	delete(m.holds.until, id)
	return false
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

	var needAcquire []int
	for i, p := range partitions {
		results[i].Partition = p
		if !m.lm.Owns(partitionResourceID(p.Topic, p.Partition)) {
			needAcquire = append(needAcquire, i)
		}
	}

	if len(needAcquire) == 0 {
		return results
	}

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
	return m.lm.Owns(partitionResourceID(topic, partition))
}

// Release explicitly gives up ownership of a single partition.
func (m *PartitionLeaseManager) Release(topic string, partition int32) {
	m.lm.Release(partitionResourceID(topic, partition))
}

// ReleaseAll releases all partition leases. Called during graceful shutdown.
func (m *PartitionLeaseManager) ReleaseAll() {
	m.lm.ReleaseAll()
}

// CurrentOwner queries etcd to find the current owner of a partition.
// Returns the broker ID of the owner, or empty string if unowned.
func (m *PartitionLeaseManager) CurrentOwner(ctx context.Context, topic string, partition int32) (string, error) {
	return m.lm.CurrentOwner(ctx, partitionResourceID(topic, partition))
}

// EtcdClient returns the underlying etcd client.
func (m *PartitionLeaseManager) EtcdClient() *clientv3.Client {
	return m.lm.EtcdClient()
}

// OwnershipToken identifies the current uninterrupted partition acquisition.
func (m *PartitionLeaseManager) OwnershipToken(topic string, partition int32) uint64 {
	return m.lm.OwnershipToken(partitionResourceID(topic, partition))
}

// CurrentOwners reads one authoritative snapshot for a broker Metadata response.
func (m *PartitionLeaseManager) CurrentOwners(ctx context.Context) (map[PartitionID]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	prefix := PartitionLeasePrefix() + "/"
	resp, err := m.EtcdClient().Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	owners := make(map[PartitionID]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		id := strings.TrimPrefix(string(kv.Key), prefix)
		i := strings.LastIndexByte(id, '/')
		if i < 0 {
			continue
		}
		partition, err := strconv.ParseInt(id[i+1:], 10, 32)
		if err != nil {
			continue
		}
		owners[PartitionID{Topic: id[:i], Partition: int32(partition)}] = string(kv.Value)
	}
	return owners, nil
}
