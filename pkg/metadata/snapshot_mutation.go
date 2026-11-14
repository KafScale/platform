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
	"encoding/json"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// mutateSnapshot is called with persistMu held and a bounded context. Retry only
// compare failures: an RPC error can represent a successfully committed write.
// The callback must rebuild its changes on each authoritative read.
func (s *EtcdStore) mutateSnapshot(ctx context.Context, change func(*InMemoryStore, int64) ([]clientv3.Cmp, []clientv3.Op, error)) (int64, error) {
	for {
		resp, err := s.client.Get(ctx, snapshotKey())
		s.recordEtcdResult(err)
		if err != nil {
			return 0, err
		}
		var state ClusterMetadata
		var revision int64
		if len(resp.Kvs) > 0 {
			if err := json.Unmarshal(resp.Kvs[0].Value, &state); err != nil {
				return 0, err
			}
			revision = resp.Kvs[0].ModRevision
		} else {
			// Standalone clusters may bootstrap from configured broker addresses,
			// but startup/demo topics are never authoritative topic records.
			seed, err := s.metadata.Metadata(ctx, nil)
			if err != nil {
				return 0, err
			}
			state = *seed
			state.Topics = nil
		}
		// A validation error (for example, topic already exists) must still
		// replace a stale startup view with the authoritative state just read.
		s.metadata.Update(state)
		candidate := NewInMemoryStore(state)
		guards, ops, err := change(candidate, resp.Header.Revision)
		if err != nil {
			return 0, err
		}
		next, err := candidate.Metadata(ctx, nil)
		if err != nil {
			return 0, err
		}
		payload, err := json.Marshal(next)
		if err != nil {
			return 0, err
		}
		guards = append(guards, clientv3.Compare(clientv3.ModRevision(snapshotKey()), "=", revision))
		ops = append([]clientv3.Op{clientv3.OpPut(snapshotKey(), string(payload))}, ops...)
		txn, err := s.client.Txn(ctx).If(guards...).Then(ops...).Commit()
		s.recordEtcdResult(err)
		if err != nil {
			return 0, err
		}
		if !txn.Succeeded {
			continue
		}
		// Never expose speculative metadata when the transaction fails.
		s.metadata.Update(*next)
		return txn.Header.Revision, nil
	}
}
