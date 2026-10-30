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
	"time"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ConsumerGroupRetainer is optional: automatic expiry requires a store that can
// fence deletion against concurrent membership and offset changes.
type ConsumerGroupRetainer interface {
	ExpireConsumerGroup(context.Context, string, time.Time, time.Time) (bool, error)
}

func emptySince(group *metadatapb.ConsumerGroup) time.Time {
	if group == nil {
		return time.Time{}
	}
	stamp, _ := time.Parse(time.RFC3339Nano, group.EmptySince)
	return stamp
}

// Keep the original Empty timestamp across metadata writes. A new active
// membership clears it; the next transition to Empty starts a fresh clock.
func retainedGroup(group, previous *metadatapb.ConsumerGroup, now time.Time) *metadatapb.ConsumerGroup {
	next := cloneConsumerGroup(group)
	if len(next.Members) > 0 {
		next.EmptySince = ""
		return next
	}
	since := emptySince(next)
	if previous != nil {
		since = emptySince(previous)
		if len(previous.Members) > 0 {
			since = time.Time{}
		}
	}
	if since.IsZero() {
		since = now
	}
	next.EmptySince = since.UTC().Format(time.RFC3339Nano)
	return next
}

func (s *EtcdStore) groupSnapshot(ctx context.Context, id string) (*metadatapb.ConsumerGroup, int64, int64, error) {
	response, err := s.client.Get(ctx, ConsumerGroupKey(id))
	s.recordEtcdResult(err)
	if err != nil {
		return nil, 0, 0, err
	}
	if len(response.Kvs) == 0 {
		return nil, 0, response.Header.Revision, nil
	}
	group, err := DecodeConsumerGroup(response.Kvs[0].Value)
	return group, response.Kvs[0].ModRevision, response.Header.Revision, err
}

func (s *EtcdStore) putConsumerGroup(ctx context.Context, group *metadatapb.ConsumerGroup) (int64, error) {
	if group == nil || group.GroupId == "" {
		return 0, errors.New("consumer group id required")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	// Active membership clears retention without adding a read to heartbeats.
	if len(group.Members) > 0 {
		payload, err := EncodeConsumerGroup(retainedGroup(group, nil, time.Now()))
		if err != nil {
			return 0, err
		}
		resp, err := s.client.Put(ctx, ConsumerGroupKey(group.GroupId), string(payload), clientv3.WithPrevKV())
		s.recordEtcdResult(err)
		if err != nil {
			return 0, err
		}
		if resp.PrevKv != nil {
			return resp.PrevKv.CreateRevision, nil
		}
		return resp.Header.Revision, nil
	}
	for range 3 {
		previous, revision, _, err := s.groupSnapshot(ctx, group.GroupId)
		if err != nil {
			return 0, err
		}
		payload, err := EncodeConsumerGroup(retainedGroup(group, previous, time.Now()))
		if err != nil {
			return 0, err
		}
		result, err := s.client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(ConsumerGroupKey(group.GroupId)), "=", revision)).Then(clientv3.OpPut(ConsumerGroupKey(group.GroupId), string(payload)), clientv3.OpGet(ConsumerGroupKey(group.GroupId))).Commit()
		s.recordEtcdResult(err)
		if err != nil {
			return 0, err
		}
		if result.Succeeded {
			return result.Responses[1].GetResponseRange().Kvs[0].CreateRevision, nil
		}
	}
	return 0, ErrConsumerGroupChanged
}

func (s *InMemoryStore) ExpireConsumerGroup(ctx context.Context, id string, cutoff, now time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	group := s.consumerGroups[id]
	if group != nil && len(group.Members) > 0 {
		return false, nil
	}
	if group == nil {
		found := false
		for key := range s.consumerOffsets {
			if owner, _, _, ok := parseConsumerKey(key); ok && owner == id {
				found = true
				break
			}
		}
		if !found {
			return false, nil
		}
		group = &metadatapb.ConsumerGroup{GroupId: id, State: "empty", ProtocolType: "consumer"}
	}
	since := emptySince(group)
	if since.IsZero() {
		group.EmptySince = now.UTC().Format(time.RFC3339Nano)
		s.consumerGroups[id] = group
		return false, nil
	}
	if since.After(cutoff) {
		return false, nil
	}
	delete(s.consumerGroups, id)
	for key := range s.consumerOffsets {
		if owner, _, _, ok := parseConsumerKey(key); ok && owner == id {
			delete(s.consumerOffsets, key)
			delete(s.consumerMeta, key)
		}
	}
	return true, nil
}

func (s *EtcdStore) ExpireConsumerGroup(ctx context.Context, id string, cutoff, now time.Time) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	group, revision, snapshotRevision, err := s.groupSnapshot(ctx, id)
	if err != nil {
		return false, err
	}
	if group != nil && len(group.Members) > 0 {
		return false, nil
	}
	since := emptySince(group)
	if !since.IsZero() && since.After(cutoff) {
		return false, nil
	}
	prefix := fmt.Sprintf("%s/%s/offsets/", ConsumerGroupPrefix(), id)
	// Read values only for legacy initialization or an expiry candidate, not for
	// every retained group on every sweep. This also handles old-broker commits.
	offsets, err := s.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(snapshotRevision))
	s.recordEtcdResult(err)
	if err != nil {
		return false, err
	}
	found := false
	latest := since
	for _, kv := range offsets.Kvs {
		if !ownedConsumerOffsetKey(string(kv.Key), prefix) {
			continue
		}
		found = true
		var record consumerOffsetRecord
		if err := json.Unmarshal(kv.Value, &record); err != nil {
			return false, err
		}
		committed, _ := time.Parse(time.RFC3339Nano, record.CommittedAt)
		if committed.After(latest) {
			latest = committed
		}
	}
	if group == nil {
		if !found {
			return false, nil
		}
		group = &metadatapb.ConsumerGroup{GroupId: id, State: "empty", ProtocolType: "consumer"}
	}
	if since.IsZero() || (latest.After(since) && latest.After(cutoff)) {
		if since.IsZero() {
			latest = now
		}
		group.EmptySince = latest.UTC().Format(time.RFC3339Nano)
		encoded, err := EncodeConsumerGroup(group)
		if err != nil {
			return false, err
		}
		result, err := s.client.Txn(ctx).If(
			clientv3.Compare(clientv3.ModRevision(ConsumerGroupKey(id)), "=", revision),
			clientv3.Compare(clientv3.ModRevision(prefix), "<", snapshotRevision+1).WithRange(clientv3.GetPrefixRangeEnd(prefix)),
		).Then(clientv3.OpPut(ConsumerGroupKey(id), string(encoded))).Commit()
		s.recordEtcdResult(err)
		if err != nil {
			return false, err
		}
		if !result.Succeeded {
			return false, ErrConsumerGroupChanged
		}
		return false, nil
	}
	if err := s.deleteConsumerGroupAndOffsets(ctx, id, revision, snapshotRevision); err != nil {
		return false, err
	}
	return true, nil
}
