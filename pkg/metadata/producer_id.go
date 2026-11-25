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
	"errors"
	"fmt"
	"math"
	"strconv"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// ProducerIDAllocator is optional so existing Store implementations remain valid.
// Every nontransactional InitProducerId allocates a fresh ID with epoch zero.
type ProducerIDAllocator interface {
	AllocateProducerID(context.Context) (int64, error)
}

const producerIDKey = "/kafscale/producer-id"

func (s *InMemoryStore) AllocateProducerID(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.producerID == math.MaxInt64 {
		return -1, errors.New("producer IDs exhausted")
	}
	s.producerID++
	return s.producerID, nil
}

// AllocateProducerID uses a linearizable CAS. An ambiguous commit can waste an
// ID, but no response may expose an ID unless its allocation was confirmed.
func (s *EtcdStore) AllocateProducerID(ctx context.Context) (int64, error) {
	for {
		resp, err := s.client.Get(ctx, producerIDKey)
		s.recordEtcdResult(err)
		if err != nil {
			return -1, err
		}
		var last, revision int64
		if len(resp.Kvs) != 0 {
			last, err = strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
			if err != nil || last < 0 {
				return -1, fmt.Errorf("invalid producer ID counter")
			}
			revision = resp.Kvs[0].ModRevision
		}
		if last == math.MaxInt64 {
			return -1, errors.New("producer IDs exhausted")
		}
		id := last + 1
		txn, err := s.client.Txn(ctx).If(
			clientv3.Compare(clientv3.ModRevision(producerIDKey), "=", revision),
		).Then(clientv3.OpPut(producerIDKey, strconv.FormatInt(id, 10))).Commit()
		s.recordEtcdResult(err)
		if err != nil {
			return -1, err
		}
		if txn.Succeeded {
			return id, nil
		}
	}
}
