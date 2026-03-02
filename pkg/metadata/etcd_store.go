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
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

// EtcdStoreConfig defines how we connect to etcd for metadata/offsets.
type EtcdStoreConfig struct {
	Endpoints   []string
	Username    string
	Password    string
	DialTimeout time.Duration
}

// EtcdStore uses etcd for offset persistence while delegating metadata to an in-memory snapshot.
type EtcdStore struct {
	client    *clientv3.Client
	metadata  *InMemoryStore
	cancel    context.CancelFunc
	available int32
	lastError atomic.Value
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
	}
	if err := store.refreshSnapshot(ctx); err != nil {
		// ignore if snapshot missing; operator will populate later
	}
	store.startWatchers()
	return store, nil
}

// Metadata delegates to the snapshot captured at startup (operator keeps it fresh).
func (s *EtcdStore) Metadata(ctx context.Context, topics []string) (*ClusterMetadata, error) {
	return s.metadata.Metadata(ctx, topics)
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
	s.lastError.Store(err)
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
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	next := lastOffset + 1
	_, err := s.client.Put(ctx, offsetKey(topic, partition), strconv.FormatInt(next, 10))
	s.recordEtcdResult(err)
	return err
}

func offsetKey(topic string, partition int32) string {
	return fmt.Sprintf("/kafscale/topics/%s/partitions/%d/next_offset", topic, partition)
}

func consumerOffsetKey(group, topic string, partition int32) string {
	return fmt.Sprintf("/kafscale/consumers/%s/offsets/%s/%d", group, topic, partition)
}

// CommitConsumerOffset implements Store.CommitConsumerOffset.
func (s *EtcdStore) CommitConsumerOffset(ctx context.Context, group, topic string, partition int32, offset int64, metadata string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	rec := consumerOffsetRecord{
		Offset:      offset,
		Metadata:    metadata,
		CommittedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	bytes, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = s.client.Put(ctx, consumerOffsetKey(group, topic, partition), string(bytes))
	s.recordEtcdResult(err)
	return err
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
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := s.client.Get(ctx, ConsumerGroupPrefix(), clientv3.WithPrefix())
	if err != nil {
		s.recordEtcdResult(err)
		return nil, err
	}
	s.recordEtcdResult(nil)
	offsets := make([]ConsumerOffset, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		group, topic, partition, ok := ParseConsumerOffsetKey(string(kv.Key))
		if !ok {
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
		})
	}
	return offsets, nil
}

// PutConsumerGroup persists consumer group metadata in etcd.
func (s *EtcdStore) PutConsumerGroup(ctx context.Context, group *metadatapb.ConsumerGroup) error {
	if group == nil || group.GroupId == "" {
		return errors.New("consumer group id required")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	payload, err := EncodeConsumerGroup(group)
	if err != nil {
		return err
	}
	_, err = s.client.Put(ctx, ConsumerGroupKey(group.GroupId), string(payload))
	s.recordEtcdResult(err)
	return err
}

// FetchConsumerGroup loads consumer group metadata from etcd.
func (s *EtcdStore) FetchConsumerGroup(ctx context.Context, groupID string) (*metadatapb.ConsumerGroup, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := s.client.Get(ctx, ConsumerGroupKey(groupID))
	if err != nil {
		s.recordEtcdResult(err)
		return nil, err
	}
	s.recordEtcdResult(nil)
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return DecodeConsumerGroup(resp.Kvs[0].Value)
}

// ListConsumerGroups returns all persisted consumer groups.
func (s *EtcdStore) ListConsumerGroups(ctx context.Context) ([]*metadatapb.ConsumerGroup, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := s.client.Get(ctx, ConsumerGroupPrefix(), clientv3.WithPrefix())
	if err != nil {
		s.recordEtcdResult(err)
		return nil, err
	}
	s.recordEtcdResult(nil)
	groups := make([]*metadatapb.ConsumerGroup, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		if _, ok := ParseConsumerGroupID(string(kv.Key)); !ok {
			continue
		}
		group, err := DecodeConsumerGroup(kv.Value)
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	return groups, nil
}

// DeleteConsumerGroup removes persisted consumer group metadata.
func (s *EtcdStore) DeleteConsumerGroup(ctx context.Context, groupID string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err := s.client.Delete(ctx, ConsumerGroupKey(groupID))
	s.recordEtcdResult(err)
	return err
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

// CreatePartitions expands a topic and writes new partition state entries.
func (s *EtcdStore) CreatePartitions(ctx context.Context, topic string, partitionCount int32) error {
	meta, err := s.metadata.Metadata(ctx, []string{topic})
	if err != nil {
		return err
	}
	if len(meta.Topics) == 0 || meta.Topics[0].ErrorCode != 0 {
		return ErrUnknownTopic
	}
	current := int32(len(meta.Topics[0].Partitions))
	if partitionCount <= current {
		return ErrInvalidTopic
	}
	if err := s.metadata.CreatePartitions(ctx, topic, partitionCount); err != nil {
		return err
	}
	if err := s.persistSnapshot(ctx); err != nil {
		return err
	}
	updated, err := s.metadata.Metadata(ctx, []string{topic})
	if err != nil {
		return err
	}
	if len(updated.Topics) == 0 || updated.Topics[0].ErrorCode != 0 {
		return ErrUnknownTopic
	}
	for i := current; i < partitionCount; i++ {
		part := updated.Topics[0].Partitions[i]
		state := &metadatapb.PartitionState{
			Topic:          topic,
			Partition:      part.PartitionIndex,
			LeaderBroker:   fmt.Sprintf("%d", part.LeaderID),
			LeaderEpoch:    part.LeaderEpoch,
			LogStartOffset: 0,
			LogEndOffset:   0,
			HighWatermark:  0,
			ActiveSegment:  "",
		}
		payload, err := EncodePartitionState(state)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err = s.client.Put(ctx, PartitionStateKey(topic, part.PartitionIndex), string(payload))
		cancel()
		if err != nil {
			s.recordEtcdResult(err)
			return err
		}
		s.recordEtcdResult(nil)
	}
	return nil
}

// CreateTopic currently updates only the in-memory snapshot; the operator is still responsible
// for reconciling durable topic configuration into etcd/S3.
func (s *EtcdStore) CreateTopic(ctx context.Context, spec TopicSpec) (*protocol.MetadataTopic, error) {
	topic, err := s.metadata.CreateTopic(ctx, spec)
	if err != nil {
		return nil, err
	}
	if err := s.persistSnapshot(ctx); err != nil {
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
		if part.PartitionIndex == partition {
			return true, nil
		}
	}
	return false, nil
}

// DeleteTopic updates the local snapshot so admin APIs behave consistently.
func (s *EtcdStore) DeleteTopic(ctx context.Context, name string) error {
	metaCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	state, err := s.metadata.Metadata(metaCtx, []string{name})
	if err != nil {
		return err
	}
	var found bool
	for _, topic := range state.Topics {
		if topic.Name == name {
			found = true
			break
		}
	}
	if !found {
		return ErrUnknownTopic
	}
	if err := s.metadata.DeleteTopic(ctx, name); err != nil {
		return err
	}
	if err := s.deleteTopicOffsets(ctx, name); err != nil {
		return err
	}
	if err := s.deleteConsumerOffsets(ctx, name); err != nil {
		return err
	}
	if err := s.persistSnapshot(ctx); err != nil {
		return err
	}
	return nil
}

func (s *EtcdStore) startWatchers() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	go s.watchSnapshot(ctx)
}

func (s *EtcdStore) watchSnapshot(ctx context.Context) {
	watchChan := s.client.Watch(ctx, snapshotKey(), clientv3.WithPrefix())
	for resp := range watchChan {
		if resp.Err() != nil {
			continue
		}
		if err := s.refreshSnapshot(ctx); err != nil {
			continue
		}
	}
}

func (s *EtcdStore) refreshSnapshot(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := s.client.Get(ctx, snapshotKey())
	if err != nil {
		s.recordEtcdResult(err)
		return err
	}
	s.recordEtcdResult(nil)
	if len(resp.Kvs) == 0 {
		return nil
	}
	var snapshot ClusterMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &snapshot); err != nil {
		return err
	}
	s.metadata.Update(snapshot)
	return nil
}

func snapshotKey() string {
	return "/kafscale/metadata/snapshot"
}

func (s *EtcdStore) persistSnapshot(ctx context.Context) error {
	state, err := s.metadata.Metadata(context.Background(), nil)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	putCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = s.client.Put(putCtx, snapshotKey(), string(payload))
	s.recordEtcdResult(err)
	return err
}

func (s *EtcdStore) deleteTopicOffsets(ctx context.Context, topic string) error {
	delCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/kafscale/topics/%s/", topic)
	_, err := s.client.Delete(delCtx, prefix, clientv3.WithPrefix())
	s.recordEtcdResult(err)
	return err
}

func (s *EtcdStore) deleteConsumerOffsets(ctx context.Context, topic string) error {
	getCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := s.client.Get(getCtx, "/kafscale/consumers/", clientv3.WithPrefix())
	if err != nil {
		s.recordEtcdResult(err)
		return err
	}
	s.recordEtcdResult(nil)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if strings.Contains(key, fmt.Sprintf("/offsets/%s/", topic)) {
			delCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			_, delErr := s.client.Delete(delCtx, key)
			cancel()
			if delErr != nil {
				s.recordEtcdResult(delErr)
				return delErr
			}
			s.recordEtcdResult(nil)
		}
	}
	return nil
}
