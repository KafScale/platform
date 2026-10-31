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
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ConsumerGroupDeleter is an optional capability for atomic group deletion.
// Store implementations predating this capability remain source compatible.
type ConsumerGroupDeleter interface {
	DeleteConsumerGroupAndOffsets(context.Context, string) error
}

const maxGroupDeleteOps = 120

var ErrConsumerGroupChanged = errors.New("consumer group changed during deletion; retry")

var nonAtomicDeleteWarning sync.Once

func DeleteConsumerGroupAndOffsets(ctx context.Context, store Store, groupID string) error {
	if deleter, ok := store.(ConsumerGroupDeleter); ok {
		return deleter.DeleteConsumerGroupAndOffsets(ctx, groupID)
	}
	nonAtomicDeleteWarning.Do(func() {
		slog.Default().Warn("consumer group delete is not atomic for this metadata store; implement metadata.ConsumerGroupDeleter to make it atomic", "store", fmt.Sprintf("%T", store), "group", groupID)
	})
	return store.DeleteConsumerGroup(ctx, groupID)
}

// A group's own offset keys have exactly topic/partition after its literal
// prefix. Nested group records and offsets have extra components or a nonnumeric
// suffix. Do not normalize paths: slashes are valid in existing group IDs.
func ownedConsumerOffsetKey(key, prefix string) bool {
	suffix, ok := strings.CutPrefix(key, prefix)
	if !ok {
		return false
	}
	parts := strings.Split(suffix, "/")
	if len(parts) != 2 || parts[0] == "" {
		return false
	}
	_, err := strconv.ParseInt(parts[1], 10, 32)
	return err == nil
}

// DeleteConsumerGroupAndOffsets atomically removes only this group's keys.
func (s *EtcdStore) DeleteConsumerGroupAndOffsets(ctx context.Context, groupID string) error {
	return s.deleteConsumerGroupAndOffsets(ctx, groupID, 0, 0)
}

// Fixed revisions fence retention against a join or commit after its eligibility
// check. Explicit DeleteGroups keeps its existing retryable snapshot semantics.
func (s *EtcdStore) deleteConsumerGroupAndOffsets(ctx context.Context, groupID string, groupRevision, offsetRevision int64) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("%s/%s/offsets/", ConsumerGroupPrefix(), groupID)
	end := clientv3.GetPrefixRangeEnd(prefix)
	for attempt := 0; attempt < 3; attempt++ {
		snapshot, err := s.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if err != nil {
			s.recordEtcdResult(err)
			return err
		}
		var ranges [][2]string
		// Compact consecutive owned keys into ranges while leaving every existing
		// foreign key outside the deletion ranges. This supports large groups without
		// one transaction operation per partition.
		start := ""
		for _, kv := range snapshot.Kvs {
			key := string(kv.Key)
			if ownedConsumerOffsetKey(key, prefix) {
				if start == "" {
					start = key
				}
			} else if start != "" {
				ranges = append(ranges, [2]string{start, key})
				start = ""
			}
		}
		if start != "" {
			ranges = append(ranges, [2]string{start, end})
		}
		revision := snapshot.Header.Revision + 1
		if offsetRevision > 0 {
			revision = offsetRevision + 1
		}
		for first := 0; ; first += maxGroupDeleteOps {
			last := min(first+maxGroupDeleteOps, len(ranges))
			chunk := ranges[first:last]
			ops := make([]clientv3.Op, 0, len(chunk)+1)
			cmps := []clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(ConsumerGroupKey(groupID)), "<", revision)}
			if groupRevision > 0 {
				cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(ConsumerGroupKey(groupID)), "=", groupRevision))
			}
			for _, r := range chunk {
				ops = append(ops, clientv3.OpDelete(r[0], clientv3.WithRange(r[1])))
			}
			if len(chunk) > 0 {
				cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(chunk[0][0]), "<", revision).WithRange(chunk[len(chunk)-1][1]))
			}
			if last == len(ranges) {
				ops = append(ops, clientv3.OpDelete(ConsumerGroupKey(groupID)))
				cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(prefix), "<", revision).WithRange(end))
			}
			result, err := s.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
			if err != nil {
				s.recordEtcdResult(err)
				return err
			}
			s.recordEtcdResult(nil)
			if !result.Succeeded {
				break
			}
			if last == len(ranges) {
				return nil
			}
		}
	}
	return ErrConsumerGroupChanged
}

// DeleteConsumerGroupAndOffsets is used only for an explicit group deletion.
func (s *InMemoryStore) DeleteConsumerGroupAndOffsets(ctx context.Context, groupID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.consumerGroups, groupID)
	for key := range s.consumerOffsets {
		group, _, _, ok := parseConsumerKey(key)
		if ok && group == groupID {
			delete(s.consumerOffsets, key)
			delete(s.consumerMeta, key)
		}
	}
	return nil
}

// ConsumerGroupOffsetCommitter is an optional capability that refuses offset
// commits for a group whose metadata was deleted.
type ConsumerGroupOffsetCommitter interface {
	CommitConsumerOffsetIfGroupExists(ctx context.Context, group string, createRevision int64, memberID string, generation int32, topic string, partition int32, offset int64, metadata string) error
}

var ErrConsumerGroupDeleted = errors.New("consumer group deleted")

var ErrConsumerGroupNotEmpty = errors.New("consumer group has active members")

var ErrConsumerGroupMemberGone = errors.New("consumer group member not found")

var ErrConsumerGroupGenerationChanged = errors.New("consumer group generation changed")

type ConsumerGroupSimpleOffsetCommitter interface {
	CommitSimpleConsumerOffset(ctx context.Context, group, topic string, partition int32, offset int64, metadata string) error
}

func CommitSimpleConsumerOffset(ctx context.Context, store Store, group, topic string, partition int32, offset int64, metadata string) error {
	if committer, ok := store.(ConsumerGroupSimpleOffsetCommitter); ok {
		return committer.CommitSimpleConsumerOffset(ctx, group, topic, partition, offset, metadata)
	}
	return store.CommitConsumerOffset(ctx, group, topic, partition, offset, metadata)
}

func (s *EtcdStore) CommitSimpleConsumerOffset(ctx context.Context, group, topic string, partition int32, offset int64, metadata string) error {
	return s.commitConsumerOffset(ctx, group, 0, "", 0, topic, partition, offset, metadata, false, true)
}

func (s *InMemoryStore) CommitSimpleConsumerOffset(ctx context.Context, group, topic string, partition int32, offset int64, metadata string) error {
	return s.commitConsumerOffset(ctx, group, "", 0, topic, partition, offset, metadata, false, true)
}

type ConsumerGroupRevisioner interface {
	FetchConsumerGroupRevision(context.Context, string) (*metadatapb.ConsumerGroup, int64, error)
	PutConsumerGroupRevision(context.Context, *metadatapb.ConsumerGroup) (int64, error)
}

func FetchConsumerGroupRevision(ctx context.Context, store Store, groupID string) (*metadatapb.ConsumerGroup, int64, error) {
	if revisioner, ok := store.(ConsumerGroupRevisioner); ok {
		return revisioner.FetchConsumerGroupRevision(ctx, groupID)
	}
	group, err := store.FetchConsumerGroup(ctx, groupID)
	return group, 0, err
}

func PutConsumerGroupRevision(ctx context.Context, store Store, group *metadatapb.ConsumerGroup) (int64, error) {
	if revisioner, ok := store.(ConsumerGroupRevisioner); ok {
		return revisioner.PutConsumerGroupRevision(ctx, group)
	}
	return 0, store.PutConsumerGroup(ctx, group)
}

func CommitConsumerOffsetIfGroupExists(ctx context.Context, store Store, group string, createRevision int64, memberID string, generation int32, topic string, partition int32, offset int64, metadata string) error {
	if committer, ok := store.(ConsumerGroupOffsetCommitter); ok {
		return committer.CommitConsumerOffsetIfGroupExists(ctx, group, createRevision, memberID, generation, topic, partition, offset, metadata)
	}
	return store.CommitConsumerOffset(ctx, group, topic, partition, offset, metadata)
}

func (s *EtcdStore) CommitConsumerOffsetIfGroupExists(ctx context.Context, group string, createRevision int64, memberID string, generation int32, topic string, partition int32, offset int64, metadata string) error {
	return s.commitConsumerOffset(ctx, group, createRevision, memberID, generation, topic, partition, offset, metadata, true, false)
}

func (s *InMemoryStore) CommitConsumerOffsetIfGroupExists(ctx context.Context, group string, _ int64, memberID string, generation int32, topic string, partition int32, offset int64, metadata string) error {
	return s.commitConsumerOffset(ctx, group, memberID, generation, topic, partition, offset, metadata, true, false)
}

func (s *EtcdStore) commitConsumerOffset(ctx context.Context, group string, createRevision int64, memberID string, generation int32, topic string, partition int32, offset int64, meta string, requireGroup, simple bool) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for range 3 {
		current, revision, _, err := s.groupSnapshot(ctx, group)
		if err != nil {
			return err
		}
		if current == nil && requireGroup {
			return ErrConsumerGroupDeleted
		}
		if current != nil && simple && len(current.Members) > 0 {
			return ErrConsumerGroupNotEmpty
		}
		if err := checkCommitMember(current, memberID, generation); err != nil {
			return err
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		payload, _ := json.Marshal(consumerOffsetRecord{Offset: offset, Metadata: meta, CommittedAt: now})
		ops := []clientv3.Op{clientv3.OpPut(consumerOffsetKey(group, topic, partition), string(payload))}
		if current != nil && len(current.Members) == 0 {
			current.EmptySince = now
			// The snapshot was decoded successfully; changing only its ASCII
			// timestamp preserves protobuf encodability.
			encoded, _ := EncodeConsumerGroup(current)
			ops = append(ops, clientv3.OpPut(ConsumerGroupKey(group), string(encoded)))
		}
		var cmps []clientv3.Cmp
		if createRevision > 0 {
			cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(ConsumerGroupKey(group)), "=", createRevision))
		}
		if createRevision == 0 || len(ops) > 1 {
			cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(ConsumerGroupKey(group)), "=", revision))
		}
		result, err := s.client.Txn(ctx).If(cmps...).Then(ops...).Else(clientv3.OpGet(ConsumerGroupKey(group))).Commit()
		s.recordEtcdResult(err)
		if err != nil {
			return err
		}
		if result.Succeeded {
			return nil
		}
		if kvs := result.Responses[0].GetResponseRange().Kvs; createRevision > 0 && (len(kvs) == 0 || kvs[0].CreateRevision != createRevision) {
			return ErrConsumerGroupDeleted
		}
	}
	return ErrConsumerGroupChanged
}

func (s *InMemoryStore) commitConsumerOffset(ctx context.Context, group, memberID string, generation int32, topic string, partition int32, offset int64, meta string, requireGroup, simple bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.consumerGroups[group]
	if current == nil && requireGroup {
		return ErrConsumerGroupDeleted
	}
	if current != nil && simple && len(current.Members) > 0 {
		return ErrConsumerGroupNotEmpty
	}
	if err := checkCommitMember(current, memberID, generation); err != nil {
		return err
	}
	if current != nil && len(current.Members) == 0 {
		current.EmptySince = time.Now().UTC().Format(time.RFC3339Nano)
	}
	key := consumerKey(group, topic, partition)
	s.consumerOffsets[key], s.consumerMeta[key] = offset, meta
	return nil
}

func checkCommitMember(current *metadatapb.ConsumerGroup, memberID string, generation int32) error {
	if current == nil || memberID == "" {
		return nil
	}
	if current.Members[memberID] == nil {
		return ErrConsumerGroupMemberGone
	}
	if current.GenerationId != generation {
		return ErrConsumerGroupGenerationChanged
	}
	return nil
}
