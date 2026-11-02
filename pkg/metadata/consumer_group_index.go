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
	"sort"
	"strings"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const consumerGroupIndexPrefix = "/kafscale/consumer-groups/"

var consumerGroupPageSize int64 = 1000

type ConsumerGroupRetentionLister interface {
	ListConsumerGroupsForRetention(ctx context.Context, full bool) ([]*metadatapb.ConsumerGroup, error)
}

func ConsumerGroupIndexKey(groupID string) string {
	return consumerGroupIndexPrefix + groupID
}

func (s *EtcdStore) rangeKeys(ctx context.Context, prefix string, rev int64, visit func(string)) (int64, error) {
	end := clientv3.GetPrefixRangeEnd(prefix)
	from := prefix
	for {
		opts := []clientv3.OpOption{clientv3.WithRange(end), clientv3.WithKeysOnly(), clientv3.WithLimit(consumerGroupPageSize)}
		if rev > 0 {
			opts = append(opts, clientv3.WithRev(rev))
		}
		resp, err := s.client.Get(ctx, from, opts...)
		s.recordEtcdResult(err)
		if err != nil {
			return 0, err
		}
		if rev == 0 {
			rev = resp.Header.Revision
		}
		for _, kv := range resp.Kvs {
			visit(string(kv.Key))
		}
		if !resp.More || len(resp.Kvs) == 0 {
			return rev, nil
		}
		from = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
}

func (s *EtcdStore) scanConsumerGroupIDs(ctx context.Context) ([]string, map[string]bool, int64, error) {
	ids := make(map[string]bool)
	rev, err := s.rangeKeys(ctx, ConsumerGroupPrefix()+"/", 0, func(key string) {
		if id, ok := ParseConsumerGroupID(key); ok {
			ids[id] = true
		} else if id, _, _, ok := ParseConsumerOffsetKey(key); ok {
			if _, seen := ids[id]; !seen {
				ids[id] = false
			}
		}
	})
	if err != nil {
		return nil, nil, 0, err
	}
	return sortedIDs(ids), ids, rev, nil
}

func (s *EtcdStore) indexedConsumerGroupIDs(ctx context.Context, rev int64) ([]string, int64, error) {
	ids := make(map[string]bool)
	rev, err := s.rangeKeys(ctx, consumerGroupIndexPrefix, rev, func(key string) {
		if id := strings.TrimPrefix(key, consumerGroupIndexPrefix); id != "" {
			ids[id] = true
		}
	})
	if err != nil {
		return nil, 0, err
	}
	return sortedIDs(ids), rev, nil
}

func sortedIDs(ids map[string]bool) []string {
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (s *EtcdStore) ListConsumerGroupsForRetention(ctx context.Context, full bool) ([]*metadatapb.ConsumerGroup, error) {
	if !full {
		ids, rev, err := s.indexedConsumerGroupIDs(ctx, 0)
		if err != nil {
			return nil, err
		}
		return s.loadConsumerGroups(ctx, ids, rev)
	}
	ids, hasMetadata, rev, err := s.scanConsumerGroupIDs(ctx)
	if err != nil {
		return nil, err
	}
	indexed, _, err := s.indexedConsumerGroupIDs(ctx, rev)
	if err != nil {
		return nil, err
	}
	inIndex := make(map[string]bool, len(indexed))
	for _, id := range indexed {
		inIndex[id] = true
	}
	for _, id := range ids {
		if hasMetadata[id] && !inIndex[id] {
			if err := s.backfillConsumerGroupIndex(ctx, id); err != nil {
				return nil, err
			}
		}
	}
	for _, id := range indexed {
		if _, ok := hasMetadata[id]; !ok {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return s.loadConsumerGroups(ctx, ids, rev)
}

func (s *EtcdStore) backfillConsumerGroupIndex(ctx context.Context, id string) error {
	_, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(ConsumerGroupKey(id)), "!=", 0)).
		Then(clientv3.OpPut(ConsumerGroupIndexKey(id), "")).
		Commit()
	s.recordEtcdResult(err)
	return err
}

func (s *EtcdStore) loadConsumerGroups(ctx context.Context, ids []string, rev int64) ([]*metadatapb.ConsumerGroup, error) {
	groups := make([]*metadatapb.ConsumerGroup, 0, len(ids))
	for start := 0; start < len(ids); start += 100 {
		end := min(start+100, len(ids))
		ops := make([]clientv3.Op, 0, end-start)
		for _, id := range ids[start:end] {
			ops = append(ops, clientv3.OpGet(ConsumerGroupKey(id), clientv3.WithRev(rev)))
		}
		batch, err := s.client.Txn(ctx).Then(ops...).Commit()
		s.recordEtcdResult(err)
		if err != nil {
			return nil, err
		}
		for i, result := range batch.Responses {
			kvs := result.GetResponseRange().Kvs
			group := &metadatapb.ConsumerGroup{GroupId: ids[start+i], State: "empty", ProtocolType: "consumer"}
			if len(kvs) > 0 {
				group, err = DecodeConsumerGroup(kvs[0].Value)
				if err != nil {
					return nil, err
				}
			}
			groups = append(groups, group)
		}
	}
	return groups, nil
}
