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

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// groupLeasePrefix is the etcd key prefix for group coordination leases.
	groupLeasePrefix = "/kafscale/group-leases"
)

// GroupLeaseConfig configures the group lease manager.
type GroupLeaseConfig struct {
	BrokerID        string
	LeaseTTLSeconds int
	Logger          *slog.Logger
}

// GroupLeaseManager uses etcd leases to ensure exclusive group coordination
// ownership. It delegates to the generic LeaseManager.
type GroupLeaseManager struct {
	lm *LeaseManager
}

// NewGroupLeaseManager creates a group lease manager backed by the given etcd client.
func NewGroupLeaseManager(client *clientv3.Client, cfg GroupLeaseConfig) *GroupLeaseManager {
	return &GroupLeaseManager{
		lm: NewLeaseManager(client, LeaseManagerConfig{
			BrokerID:        cfg.BrokerID,
			Prefix:          groupLeasePrefix,
			LeaseTTLSeconds: cfg.LeaseTTLSeconds,
			Logger:          cfg.Logger,
			ResourceKind:    "group",
		}),
	}
}

// GroupLeasePrefix returns the etcd prefix for watching group leases.
func GroupLeasePrefix() string {
	return groupLeasePrefix
}

// Acquire tries to grab the group coordination lease. If this broker already
// owns it, it returns nil immediately. If another broker owns it, it returns
// ErrNotOwner.
func (m *GroupLeaseManager) Acquire(ctx context.Context, groupID string) error {
	return m.lm.Acquire(ctx, groupID)
}

// Owns returns true if this broker currently holds the lease for the group.
func (m *GroupLeaseManager) Owns(groupID string) bool {
	return m.lm.Owns(groupID)
}

// Release explicitly gives up ownership of a single group.
func (m *GroupLeaseManager) Release(groupID string) {
	m.lm.Release(groupID)
}

// ReleaseAll releases all group leases. Called during graceful shutdown.
func (m *GroupLeaseManager) ReleaseAll() {
	m.lm.ReleaseAll()
}

// CurrentOwner queries etcd to find the current owner of a group.
// Returns the broker ID of the owner, or empty string if unowned.
func (m *GroupLeaseManager) CurrentOwner(ctx context.Context, groupID string) (string, error) {
	return m.lm.CurrentOwner(ctx, groupID)
}

// EtcdClient returns the underlying etcd client.
func (m *GroupLeaseManager) EtcdClient() *clientv3.Client {
	return m.lm.EtcdClient()
}
