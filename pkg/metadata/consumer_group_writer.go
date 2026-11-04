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

import "context"

type consumerGroupWrite struct {
	ready chan struct{}
	users int
}

// Queue local group writes so heartbeats cannot repeatedly invalidate an offset
// commit's snapshot. Etcd comparisons still fence writers on other brokers.
// Entries count holders and waiters and are removed when neither remains.
func (s *EtcdStore) lockConsumerGroupWrite(ctx context.Context, id string) (func(), error) {
	s.groupWriteMu.Lock()
	if s.groupWrites == nil {
		s.groupWrites = make(map[string]*consumerGroupWrite)
	}
	entry := s.groupWrites[id]
	if entry == nil {
		entry = &consumerGroupWrite{ready: make(chan struct{}, 1)}
		s.groupWrites[id] = entry
	}
	entry.users++
	s.groupWriteMu.Unlock()
	unref := func() {
		s.groupWriteMu.Lock()
		entry.users--
		if entry.users == 0 {
			delete(s.groupWrites, id)
		}
		s.groupWriteMu.Unlock()
	}
	select {
	case entry.ready <- struct{}{}:
		return func() { <-entry.ready; unref() }, nil
	case <-ctx.Done():
		unref()
		return nil, ctx.Err()
	}
}
