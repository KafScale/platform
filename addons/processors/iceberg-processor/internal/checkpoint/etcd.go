package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/config"
)

const defaultLeaseTTLSeconds = 30
const defaultKeyPrefix = "processors"

type etcdStore struct {
	client          *clientv3.Client
	prefix          string
	leaseTTLSeconds int
}

func NewEtcdStore(cfg config.Config) (Store, error) {
	if len(cfg.Etcd.Endpoints) == 0 {
		return nil, fmt.Errorf("etcd.endpoints is required for etcd offsets")
	}

	ttlSeconds := cfg.Offsets.LeaseTTLSeconds
	if ttlSeconds <= 0 {
		ttlSeconds = defaultLeaseTTLSeconds
	}
	keyPrefix := cfg.Offsets.KeyPrefix
	if keyPrefix == "" {
		keyPrefix = defaultKeyPrefix
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Etcd.Endpoints,
		Username:    cfg.Etcd.Username,
		Password:    cfg.Etcd.Password,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &etcdStore{client: client, prefix: keyPrefix, leaseTTLSeconds: ttlSeconds}, nil
}

func (s *etcdStore) ClaimLease(ctx context.Context, topic string, partition int32, ownerID string) (Lease, error) {
	lease, err := s.client.Grant(ctx, int64(s.leaseTTLSeconds))
	if err != nil {
		return Lease{}, err
	}

	now := time.Now()
	payload := leaseState{
		OwnerID:        ownerID,
		LeaseExpiresAt: now.Add(time.Duration(s.leaseTTLSeconds) * time.Second).UnixMilli(),
		Generation:     1,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return Lease{}, err
	}

	key := s.leaseKey(topic, partition)
	txn := s.client.Txn(ctx)
	resp, err := txn.If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, string(data), clientv3.WithLease(lease.ID))).
		Commit()
	if err != nil {
		_, _ = s.client.Revoke(ctx, lease.ID)
		return Lease{}, err
	}
	if !resp.Succeeded {
		_, _ = s.client.Revoke(ctx, lease.ID)
		return Lease{}, errors.New("lease already held")
	}

	return Lease{
		Topic:     topic,
		Partition: partition,
		OwnerID:   ownerID,
		ExpiresAt: payload.LeaseExpiresAt,
		LeaseID:   int64(lease.ID),
	}, nil
}

func (s *etcdStore) RenewLease(ctx context.Context, lease Lease) error {
	if lease.LeaseID == 0 {
		return nil
	}

	_, err := s.client.KeepAliveOnce(ctx, clientv3.LeaseID(lease.LeaseID))
	if err != nil {
		return err
	}

	payload := leaseState{
		OwnerID:        lease.OwnerID,
		LeaseExpiresAt: time.Now().Add(time.Duration(s.leaseTTLSeconds) * time.Second).UnixMilli(),
		Generation:     1,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	key := s.leaseKey(lease.Topic, lease.Partition)
	_, err = s.client.Put(ctx, key, string(data), clientv3.WithLease(clientv3.LeaseID(lease.LeaseID)))
	return err
}

func (s *etcdStore) ReleaseLease(ctx context.Context, lease Lease) error {
	if lease.LeaseID != 0 {
		_, _ = s.client.Revoke(ctx, clientv3.LeaseID(lease.LeaseID))
	}
	_, err := s.client.Delete(ctx, s.leaseKey(lease.Topic, lease.Partition))
	return err
}

func (s *etcdStore) LoadOffset(ctx context.Context, topic string, partition int32) (OffsetState, error) {
	resp, err := s.client.Get(ctx, s.offsetKey(topic, partition))
	if err != nil {
		return OffsetState{}, err
	}
	if len(resp.Kvs) == 0 {
		return OffsetState{Topic: topic, Partition: partition, Offset: -1}, nil
	}

	var state offsetState
	if err := json.Unmarshal(resp.Kvs[0].Value, &state); err != nil {
		return OffsetState{}, err
	}

	return OffsetState{
		Topic:     topic,
		Partition: partition,
		Offset:    state.Offset,
		Timestamp: state.LastTimestampMs,
	}, nil
}

func (s *etcdStore) CommitOffset(ctx context.Context, state OffsetState) error {
	now := time.Now().UnixMilli()
	payload := offsetState{
		Offset:          state.Offset,
		LastTimestampMs: state.Timestamp,
		UpdatedAt:       now,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	if _, err := s.client.Put(ctx, s.offsetKey(state.Topic, state.Partition), string(data)); err != nil {
		return err
	}
	if _, err := s.client.Put(ctx, s.partitionWatermarkKey(state.Topic, state.Partition), string(data)); err != nil {
		return err
	}

	return s.updateTopicWatermark(ctx, state.Topic)
}

func (s *etcdStore) updateTopicWatermark(ctx context.Context, topic string) error {
	resp, err := s.client.Get(ctx, s.partitionWatermarkPrefix(topic), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return nil
	}

	min, err := minWatermark(resp.Kvs)
	if err != nil {
		return err
	}

	data, err := json.Marshal(min)
	if err != nil {
		return err
	}
	_, err = s.client.Put(ctx, s.topicWatermarkKey(topic), string(data))
	return err
}

func (s *etcdStore) leaseKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/leases/%s/%d", s.prefix, topic, partition)
}

func (s *etcdStore) offsetKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/offsets/%s/%d", s.prefix, topic, partition)
}

func (s *etcdStore) partitionWatermarkKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/watermarks/%s/%d", s.prefix, topic, partition)
}

func (s *etcdStore) topicWatermarkKey(topic string) string {
	return fmt.Sprintf("%s/watermarks/%s", s.prefix, topic)
}

func (s *etcdStore) partitionWatermarkPrefix(topic string) string {
	return fmt.Sprintf("%s/watermarks/%s/", s.prefix, topic)
}

type leaseState struct {
	OwnerID        string `json:"owner_id"`
	LeaseExpiresAt int64  `json:"lease_expires_at"`
	Generation     int64  `json:"generation"`
}

type offsetState struct {
	Offset          int64 `json:"offset"`
	LastTimestampMs int64 `json:"last_timestamp_ms"`
	UpdatedAt       int64 `json:"updated_at"`
}

func minWatermark(kvs []*mvccpb.KeyValue) (offsetState, error) {
	var min offsetState
	minSet := false

	for _, kv := range kvs {
		var state offsetState
		if err := json.Unmarshal(kv.Value, &state); err != nil {
			return offsetState{}, err
		}
		if !minSet || state.Offset < min.Offset {
			min = state
			minSet = true
		}
	}
	if !minSet {
		return offsetState{}, fmt.Errorf("no watermark entries")
	}

	return min, nil
}
