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

package checkpoint

import "context"

// Lease ties a worker to a partition with TTL-based ownership.
type Lease struct {
	Topic     string
	Partition int32
	OwnerID   string
	ExpiresAt int64
}

// OffsetState tracks the last committed offset for a partition.
type OffsetState struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp int64
}

// Store persists leases and offsets.
type Store interface {
	ClaimLease(ctx context.Context, topic string, partition int32, ownerID string) (Lease, error)
	RenewLease(ctx context.Context, lease Lease) error
	ReleaseLease(ctx context.Context, lease Lease) error
	LoadOffset(ctx context.Context, topic string, partition int32) (OffsetState, error)
	CommitOffset(ctx context.Context, state OffsetState) error
}

// New returns a placeholder store.
func New() Store {
	return &noopStore{}
}

type noopStore struct{}

func (n *noopStore) ClaimLease(ctx context.Context, topic string, partition int32, ownerID string) (Lease, error) {
	return Lease{Topic: topic, Partition: partition, OwnerID: ownerID}, nil
}

func (n *noopStore) RenewLease(ctx context.Context, lease Lease) error {
	return nil
}

func (n *noopStore) ReleaseLease(ctx context.Context, lease Lease) error {
	return nil
}

func (n *noopStore) LoadOffset(ctx context.Context, topic string, partition int32) (OffsetState, error) {
	return OffsetState{Topic: topic, Partition: partition, Offset: 0}, nil
}

func (n *noopStore) CommitOffset(ctx context.Context, state OffsetState) error {
	return nil
}
