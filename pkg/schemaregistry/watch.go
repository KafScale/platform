// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package schemaregistry

import (
	"context"
	"errors"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const maxWatchBackoff = 10 * time.Second

var errWatchClosed = errors.New("watch closed")

func (r *Registry) Watch(ctx context.Context) {
	var from int64
	failures := 0
	for ctx.Err() == nil {
		next, err := r.watchOnce(ctx, from)
		from = next
		if err == nil {
			failures = 0
			continue
		}
		if ctx.Err() != nil {
			return
		}
		failures++
		log.Printf("schema registry etcd watch: %v", err)
		wait := min(100*time.Millisecond<<min(failures, 7), maxWatchBackoff)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (r *Registry) watchOnce(ctx context.Context, from int64) (int64, error) {
	if from == 0 {
		resp, err := r.cli.Get(ctx, counterKey(), clientv3.WithCountOnly())
		if err != nil {
			return 0, err
		}
		from = resp.Header.Revision
	}
	wctx, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
	defer cancel()
	ch := r.cli.Watch(wctx, keyPrefix, clientv3.WithPrefix(), clientv3.WithRev(from+1), clientv3.WithProgressNotify())
	for resp := range ch {
		if resp.CompactRevision > 0 || (resp.Header.Revision > 0 && resp.Header.Revision < from) {
			r.restored(ctx)
			return 0, nil
		}
		if err := resp.Err(); err != nil {
			return from, err
		}
		from = max(from, resp.Header.Revision)
	}
	if ctx.Err() != nil {
		return from, ctx.Err()
	}
	return from, errWatchClosed
}

func (r *Registry) restored(ctx context.Context) {
	r.mu.Lock()
	r.resetLocked()
	r.counter, r.revision, r.raftTerm = 0, 0, 0
	r.mu.Unlock()
	if err := r.syncCounter(ctx); err != nil && ctx.Err() == nil {
		log.Printf("read schema id counter after etcd restore: %v", err)
	}
}
