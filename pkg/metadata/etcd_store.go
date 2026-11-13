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

package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

// EtcdStoreConfig defines how we connect to etcd for metadata/offsets.
type EtcdStoreConfig struct {
	Endpoints        []string
	Username         string
	Password         string
	DialTimeout      time.Duration
	StorageNamespace string
}

// etcdError wraps an error for storage in an atomic.Value, which requires a
// consistent concrete type across all Store calls.
type etcdError struct{ err error }

// EtcdStore uses etcd for offset persistence while delegating metadata to an in-memory snapshot.
type EtcdStore struct {
	client       *clientv3.Client
	metadata     *InMemoryStore
	cancel       context.CancelFunc
	available    int32
	lastError    atomic.Value // stores etcdError
	persistMu    sync.Mutex   // serializes local cache installs and snapshot mutations
	groupWriteMu sync.Mutex
	groupWrites  map[string]*consumerGroupWrite

	storageNamespace string
}

func (s *EtcdStore) EtcdClient() *clientv3.Client {
	return s.client
}

type consumerOffsetRecord struct {
	Offset      int64  `json:"offset"`
	Metadata    string `json:"metadata"`
	CommittedAt string `json:"committed_at"`
}

// NewEtcdStore initializes a store backed by etcd.
func NewEtcdStore(ctx context.Context, snapshot ClusterMetadata, cfg EtcdStoreConfig) (*EtcdStore, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("etcd endpoints required")
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		Username:    cfg.Username,
		Password:    cfg.Password,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("connect etcd: %w", err)
	}
	store := &EtcdStore{
		client:    cli,
		metadata:  NewInMemoryStore(snapshot),
		available: 1,

		storageNamespace: cfg.StorageNamespace,
	}
	if store.storageNamespace == "" {
		store.storageNamespace = "default"
	}
	_ = store.refreshSnapshot(ctx) // best-effort; snapshot may not exist yet
	store.startWatchers()
	return store, nil
}

// Metadata delegates to the snapshot captured at startup (operator keeps it fresh).
func (s *EtcdStore) Metadata(ctx context.Context, topics []string) (*ClusterMetadata, error) {
	return s.metadata.Metadata(ctx, topics)
}

// RefreshSnapshot reloads cluster metadata from etcd. Callers use this to recover
// when the initial snapshot read failed or the watch stream was interrupted.
func (s *EtcdStore) RefreshSnapshot(ctx context.Context) error {
	return s.refreshSnapshot(ctx)
}

// Close stops snapshot watchers and closes the etcd client.
func (s *EtcdStore) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// Available reports whether the most recent etcd operation succeeded.
func (s *EtcdStore) Available() bool {
	return atomic.LoadInt32(&s.available) == 1
}

func (s *EtcdStore) recordEtcdResult(err error) {
	if err == nil {
		atomic.StoreInt32(&s.available, 1)
		return
	}
	atomic.StoreInt32(&s.available, 0)
	s.lastError.Store(etcdError{err})
}

// NextOffset reads the last committed offset from etcd and returns the next offset to assign.
func (s *EtcdStore) NextOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	exists, err := s.partitionExists(ctx, topic, partition)
	if err != nil {
		return 0, err
	}
	if !exists {
		if ValidateTopicName(topic) != nil {
			return 0, ErrInvalidTopic
		}
		return 0, ErrUnknownTopic
	}
	key := offsetKey(topic, partition)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		s.recordEtcdResult(err)
		return 0, err
	}
	s.recordEtcdResult(nil)
	if len(resp.Kvs) == 0 {
		return 0, nil
	}
	val := strings.TrimSpace(string(resp.Kvs[0].Value))
	if val == "" {
		return 0, nil
	}
	offset, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse offset for %s: %w", key, err)
	}
	return offset, nil
}

// UpdateOffsets stores the next offset (last + 1) so future producers pick up from there.
func (s *EtcdStore) UpdateOffsets(ctx context.Context, topic string, partition int32, lastOffset int64) error {
	return s.updateOffsets(ctx, topic, partition, lastOffset, "")
}

// UpdateOffsetsForStorage fences callbacks from logs belonging to a retired topic.
func (s *EtcdStore) UpdateOffsetsForStorage(ctx context.Context, topic string, partition int32, lastOffset int64, storageTopic string) error {
	return s.updateOffsets(ctx, topic, partition, lastOffset, storageTopic)
}

// UpdateOffsetsForLease rejects callbacks from an earlier partition acquisition.
func (s *EtcdStore) UpdateOffsetsForLease(ctx context.Context, topic string, partition int32, lastOffset int64, storageTopic string, revision uint64) error {
	if revision == 0 {
		return ErrNotOwner
	}
	return s.updateOffsets(ctx, topic, partition, lastOffset, storageTopic,
		clientv3.Compare(clientv3.ModRevision(partitionLeaseKey(topic, partition)), "=", int64(revision)))
}

func (s *EtcdStore) updateOffsets(ctx context.Context, topic string, partition int32, lastOffset int64, storageTopic string, leaseGuards ...clientv3.Cmp) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	read, err := s.client.Txn(ctx).Then(clientv3.OpGet(snapshotKey()), clientv3.OpGet(TopicGenerationKey(topic))).Commit()
	s.recordEtcdResult(err)
	if err != nil {
		return err
	}
	s.metadata.mu.RLock()
	state := cloneMetadata(s.metadata.state)
	s.metadata.mu.RUnlock()
	snapshot := read.Responses[0].GetResponseRange().Kvs
	if len(snapshot) > 0 {
		if err := json.Unmarshal(snapshot[0].Value, &state); err != nil {
			return err
		}
	}
	if !topicHasPartition(state.Topics, topic, partition) {
		return ErrUnknownTopic
	}
	generations := read.Responses[1].GetResponseRange().Kvs
	var generation int64
	if len(generations) > 0 {
		generation, err = strconv.ParseInt(string(generations[0].Value), 10, 64)
		if err != nil {
			return err
		}
	}
	if storageTopic != "" && TopicStorageName(topic, generation) != storageTopic {
		return ErrTopicGenerationChanged
	}
	ops := []clientv3.Op{clientv3.OpPut(offsetKey(topic, partition), strconv.FormatInt(lastOffset+1, 10))}
	guard := generationIs(TopicGenerationKey(topic), generation)
	if len(leaseGuards) == 0 {
		return s.commitGuarded(ctx, guard, ops)
	}
	resp, err := s.client.Txn(ctx).If(append([]clientv3.Cmp{guard}, leaseGuards...)...).Then(ops...).Commit()
	s.recordEtcdResult(err)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return ErrNotOwner
	}
	return nil
}

func offsetKey(topic string, partition int32) string {
	return fmt.Sprintf("/kafscale/topics/%s/partitions/%d/next_offset", topic, partition)
}

func consumerOffsetKey(group, topic string, partition int32) string {
	return fmt.Sprintf("/kafscale/consumers/%s/offsets/%s/%d", group, topic, partition)
}

// CommitConsumerOffset implements Store.CommitConsumerOffset.
func (s *EtcdStore) CommitConsumerOffset(ctx context.Context, group, topic string, partition int32, offset int64, metadata string) error {
	return s.commitConsumerOffset(ctx, group, 0, "", 0, topic, partition, offset, metadata, false, false)
}

// FetchConsumerOffset implements Store.FetchConsumerOffset.
func (s *EtcdStore) FetchConsumerOffset(ctx context.Context, group, topic string, partition int32) (int64, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := s.client.Get(ctx, consumerOffsetKey(group, topic, partition))
	if err != nil {
		s.recordEtcdResult(err)
		return 0, "", err
	}
	s.recordEtcdResult(nil)
	if len(resp.Kvs) == 0 {
		return 0, "", nil
	}
	var rec consumerOffsetRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		return 0, "", err
	}
	return rec.Offset, rec.Metadata, nil
}

// ListConsumerOffsets returns all committed offsets stored in etcd.
func (s *EtcdStore) ListConsumerOffsets(ctx context.Context) ([]ConsumerOffset, error) {
	return s.listConsumerOffsets(ctx, ConsumerGroupPrefix(), "", false)
}

// ListGroupConsumerOffsets reads only the requested group's offset prefix.
func (s *EtcdStore) ListGroupConsumerOffsets(ctx context.Context, group string) ([]ConsumerOffset, error) {
	return s.listConsumerOffsets(ctx, fmt.Sprintf("%s/%s/offsets/", ConsumerGroupPrefix(), group), group, true)
}

func (s *EtcdStore) listConsumerOffsets(ctx context.Context, prefix, groupID string, filterGroup bool) ([]ConsumerOffset, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		s.recordEtcdResult(err)
		return nil, err
	}
	s.recordEtcdResult(nil)
	offsets := make([]ConsumerOffset, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		group, topic, partition, ok := ParseConsumerOffsetKey(string(kv.Key))
		if !ok || (filterGroup && group != groupID) {
			continue
		}
		var rec consumerOffsetRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			return nil, err
		}
		offsets = append(offsets, ConsumerOffset{
			Group:     group,
			Topic:     topic,
			Partition: partition,
			Offset:    rec.Offset,
			Metadata:  rec.Metadata,
		})
	}
	return offsets, nil
}

// PutConsumerGroup persists consumer group metadata in etcd.
func (s *EtcdStore) PutConsumerGroup(ctx context.Context, group *metadatapb.ConsumerGroup) error {
	_, err := s.putConsumerGroup(ctx, group)
	return err
}

func (s *EtcdStore) PutConsumerGroupRevision(ctx context.Context, group *metadatapb.ConsumerGroup) (int64, error) {
	return s.putConsumerGroup(ctx, group)
}

// FetchConsumerGroup loads consumer group metadata from etcd.
func (s *EtcdStore) FetchConsumerGroup(ctx context.Context, groupID string) (*metadatapb.ConsumerGroup, error) {
	group, _, err := s.FetchConsumerGroupRevision(ctx, groupID)
	return group, err
}

func (s *EtcdStore) FetchConsumerGroupRevision(ctx context.Context, groupID string) (*metadatapb.ConsumerGroup, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := s.client.Get(ctx, ConsumerGroupKey(groupID))
	if err != nil {
		s.recordEtcdResult(err)
		return nil, 0, err
	}
	s.recordEtcdResult(nil)
	if len(resp.Kvs) == 0 {
		return nil, 0, nil
	}
	group, err := DecodeConsumerGroup(resp.Kvs[0].Value)
	return group, resp.Kvs[0].CreateRevision, err
}

// ListConsumerGroups discovers groups from keys without reading every offset value.
func (s *EtcdStore) ListConsumerGroups(ctx context.Context) ([]*metadatapb.ConsumerGroup, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ids, _, rev, err := s.scanConsumerGroupIDs(ctx)
	if err != nil {
		return nil, err
	}
	return s.loadConsumerGroups(ctx, ids, rev)
}

// DeleteConsumerGroup removes only keys owned by this group, including offsets.
// Batches are retryable if a store error interrupts deletion of a large group.
func (s *EtcdStore) DeleteConsumerGroup(ctx context.Context, groupID string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := s.client.Get(ctx, fmt.Sprintf("%s/%s/offsets/", ConsumerGroupPrefix(), groupID), clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		s.recordEtcdResult(err)
		return err
	}
	ops := make([]clientv3.Op, 0, len(resp.Kvs)+1)
	for _, kv := range resp.Kvs {
		if id, _, _, ok := ParseConsumerOffsetKey(string(kv.Key)); ok && id == groupID {
			ops = append(ops, clientv3.OpDelete(string(kv.Key)))
		}
	}
	// Remove metadata last so an interrupted deletion remains discoverable for retry.
	ops = append(ops, clientv3.OpDelete(ConsumerGroupKey(groupID)), clientv3.OpDelete(ConsumerGroupIndexKey(groupID)))
	for start := 0; start < len(ops); start += 100 {
		_, err = s.client.Txn(ctx).Then(ops[start:min(start+100, len(ops))]...).Commit()
		if err != nil {
			s.recordEtcdResult(err)
			return err
		}
	}
	s.recordEtcdResult(nil)
	return nil
}

// FetchTopicConfig loads topic configuration from etcd or falls back to defaults.
func (s *EtcdStore) FetchTopicConfig(ctx context.Context, topic string) (*metadatapb.TopicConfig, error) {
	if topic == "" {
		return nil, ErrInvalidTopic
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := s.client.Get(ctx, TopicConfigKey(topic))
	if err != nil {
		s.recordEtcdResult(err)
		return nil, err
	}
	s.recordEtcdResult(nil)
	if len(resp.Kvs) > 0 {
		return DecodeTopicConfig(resp.Kvs[0].Value)
	}
	meta, err := s.metadata.Metadata(ctx, []string{topic})
	if err != nil {
		return nil, err
	}
	if len(meta.Topics) == 0 || meta.Topics[0].ErrorCode != 0 {
		return nil, ErrUnknownTopic
	}
	return defaultTopicConfigFromTopic(&meta.Topics[0], int16(len(meta.Topics[0].Partitions))), nil
}

// UpdateTopicConfig persists topic configuration into etcd.
func (s *EtcdStore) UpdateTopicConfig(ctx context.Context, cfg *metadatapb.TopicConfig) error {
	if cfg == nil || cfg.Name == "" {
		return ErrInvalidTopic
	}
	meta, err := s.metadata.Metadata(ctx, []string{cfg.Name})
	if err != nil {
		return err
	}
	if len(meta.Topics) == 0 || meta.Topics[0].ErrorCode != 0 {
		return ErrUnknownTopic
	}
	if cfg.Partitions == 0 {
		cfg.Partitions = int32(len(meta.Topics[0].Partitions))
	}
	if err := s.metadata.UpdateTopicConfig(ctx, cfg); err != nil {
		return err
	}
	payload, err := EncodeTopicConfig(cfg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err = s.client.Put(ctx, TopicConfigKey(cfg.Name), string(payload))
	s.recordEtcdResult(err)
	return err
}

// CreatePartitions expands a topic using the authoritative snapshot. Each batch
// publishes its partition states atomically with the matching snapshot revision.
func (s *EtcdStore) CreatePartitions(ctx context.Context, topic string, partitionCount int32) error {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var observedGeneration *int64
	for {
		complete := false
		_, err := s.mutateSnapshot(ctx, func(candidate *InMemoryStore, revision int64) ([]clientv3.Cmp, []clientv3.Op, error) {
			meta, err := candidate.Metadata(ctx, []string{topic})
			if err != nil {
				return nil, nil, err
			}
			if len(meta.Topics) == 0 || meta.Topics[0].ErrorCode != 0 {
				return nil, nil, ErrUnknownTopic
			}
			generation, err := s.topicGenerationAt(ctx, topic, revision)
			if err != nil {
				return nil, nil, err
			}
			if observedGeneration != nil && *observedGeneration != generation {
				return nil, nil, ErrTopicGenerationChanged
			}
			observedGeneration = &generation
			current := int32(len(meta.Topics[0].Partitions))
			// Each conditional initialization uses a nested txn plus its put.
			next := min(partitionCount, current+(maxTxnOps-1)/2)
			if err := candidate.CreatePartitions(ctx, topic, next); err != nil {
				return nil, nil, err
			}
			updated, err := candidate.Metadata(ctx, []string{topic})
			if err != nil {
				return nil, nil, err
			}
			ops := make([]clientv3.Op, 0, next-current)
			for _, part := range updated.Topics[0].Partitions[current:next] {
				payload, err := EncodePartitionState(&metadatapb.PartitionState{
					Topic: topic, Partition: part.Partition,
					LeaderBroker: fmt.Sprintf("%d", part.Leader), LeaderEpoch: part.LeaderEpoch,
				})
				if err != nil {
					return nil, nil, err
				}
				key := PartitionStateKey(topic, part.Partition)
				ops = append(ops, clientv3.OpTxn(
					[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(key), "=", 0)},
					[]clientv3.Op{clientv3.OpPut(key, string(payload))}, nil,
				))
			}
			complete = next == partitionCount
			return nil, ops, nil
		})
		if err != nil || complete {
			return err
		}
	}
}

// CreateTopic applies only the requested creation to the latest etcd snapshot.
func (s *EtcdStore) CreateTopic(ctx context.Context, spec TopicSpec) (*protocol.MetadataTopic, error) {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var topic *protocol.MetadataTopic
	_, err := s.mutateSnapshot(ctx, func(candidate *InMemoryStore, _ int64) ([]clientv3.Cmp, []clientv3.Op, error) {
		var err error
		topic, err = candidate.CreateTopic(ctx, spec)
		return nil, nil, err
	})
	if err != nil {
		return nil, err
	}
	return topic, nil
}

func (s *EtcdStore) partitionExists(ctx context.Context, topic string, partition int32) (bool, error) {
	meta, err := s.metadata.Metadata(ctx, []string{topic})
	if err != nil {
		return false, err
	}
	if len(meta.Topics) == 0 {
		return false, nil
	}
	t := meta.Topics[0]
	if t.ErrorCode != 0 {
		return false, nil
	}
	for _, part := range t.Partitions {
		if part.Partition == partition {
			return true, nil
		}
	}
	return false, nil
}

// DeleteTopic updates the local snapshot so admin APIs behave consistently.
func (s *EtcdStore) DeleteTopic(ctx context.Context, name string) error {
	return s.RetireTopic(ctx, name, s.storageNamespace)
}

func (s *EtcdStore) startWatchers() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	go s.watchSnapshot(ctx)
}

func (s *EtcdStore) watchSnapshot(ctx context.Context) {
	for ctx.Err() == nil {
		// Watch from the full read's revision so updates between Get and Watch
		// are replayed. A failed initial read is retried even with no new events.
		revision, err := s.refreshSnapshotRevision(ctx)
		if err == nil {
			watchCtx, cancel := context.WithCancel(ctx)
			watchChan := s.client.Watch(watchCtx, snapshotKey(), clientv3.WithRev(revision+1))
			for resp := range watchChan {
				if resp.Err() != nil {
					break
				}
				if err := s.refreshSnapshot(ctx); err != nil {
					break
				}
			}
			cancel()
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *EtcdStore) refreshSnapshot(ctx context.Context) error {
	_, err := s.refreshSnapshotRevision(ctx)
	return err
}

func (s *EtcdStore) refreshSnapshotRevision(ctx context.Context) (int64, error) {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := s.client.Get(ctx, snapshotKey())
	if err != nil {
		s.recordEtcdResult(err)
		return 0, err
	}
	s.recordEtcdResult(nil)
	if len(resp.Kvs) == 0 {
		return resp.Header.Revision, nil
	}
	var snapshot ClusterMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &snapshot); err != nil {
		return 0, err
	}
	s.metadata.Update(snapshot)
	return resp.Header.Revision, nil
}

func snapshotKey() string {
	return "/kafscale/metadata/snapshot"
}
