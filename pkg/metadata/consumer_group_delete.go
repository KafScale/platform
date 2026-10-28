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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// ConsumerGroupDeleter is an optional capability for atomic group deletion.
// Store implementations predating this capability remain source compatible.
type ConsumerGroupDeleter interface {
	DeleteConsumerGroupAndOffsets(context.Context, string) error
}

var (
	ErrAtomicGroupDeletionUnsupported = errors.New("store cannot atomically delete committed group offsets")
	ErrConsumerGroupChanged           = errors.New("consumer group changed during deletion; retry")
)

// DeleteConsumerGroupAndOffsets preserves legacy deletion for stores without
// committed offsets. It fails without mutation when atomic deletion is needed
// but the store does not implement the optional capability.
func DeleteConsumerGroupAndOffsets(ctx context.Context, store Store, groupID string) error {
	if deleter, ok := store.(ConsumerGroupDeleter); ok {
		return deleter.DeleteConsumerGroupAndOffsets(ctx, groupID)
	}
	offsets, err := store.ListGroupConsumerOffsets(ctx, groupID)
	if err != nil {
		return err
	}
	if len(offsets) > 0 {
		return ErrAtomicGroupDeletionUnsupported
	}
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
		ops := []clientv3.Op{clientv3.OpDelete(ConsumerGroupKey(groupID))}
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
				ops = append(ops, clientv3.OpDelete(start, clientv3.WithRange(key)))
				start = ""
			}
		}
		if start != "" {
			ops = append(ops, clientv3.OpDelete(start, clientv3.WithRange(end)))
		}
		// A new foreign key could otherwise appear in a planned range after the
		// read. Fence all offset-prefix changes and concurrent group metadata writes.
		revision := snapshot.Header.Revision + 1
		result, err := s.client.Txn(ctx).If(
			clientv3.Compare(clientv3.ModRevision(prefix), "<", revision).WithRange(end),
			clientv3.Compare(clientv3.ModRevision(ConsumerGroupKey(groupID)), "<", revision),
		).Then(ops...).Commit()
		if err != nil {
			s.recordEtcdResult(err)
			return err
		}
		s.recordEtcdResult(nil)
		if result.Succeeded {
			return nil
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
