package checkpoint

import (
	"context"

	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/config"
)

// Lease ties a worker to a partition with TTL-based ownership.
type Lease struct {
	Topic     string
	Partition int32
	OwnerID   string
	ExpiresAt int64
	LeaseID   int64
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

// New returns an offset store based on configuration.
func New(cfg config.Config) (Store, error) {
	if cfg.Offsets.Backend == "etcd" {
		return NewEtcdStore(cfg)
	}
	return &noopStore{}, nil
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
