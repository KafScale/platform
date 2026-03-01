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
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// GroupRoute maps a group ID to the broker that currently coordinates it.
type GroupRoute struct {
	GroupID  string
	BrokerID string
}

// GroupRouter watches etcd group lease keys and maintains an in-memory
// routing table. The proxy uses this to send group coordination requests
// to the correct broker without round-tripping to etcd on every request.
type GroupRouter struct {
	client *clientv3.Client
	logger *slog.Logger
	cancel context.CancelFunc

	mu     sync.RWMutex
	routes map[string]string // groupID -> brokerID
}

// NewGroupRouter creates a router and starts watching etcd for group lease changes.
func NewGroupRouter(ctx context.Context, client *clientv3.Client, logger *slog.Logger) (*GroupRouter, error) {
	if logger == nil {
		logger = slog.Default()
	}
	watchCtx, cancel := context.WithCancel(ctx)
	r := &GroupRouter{
		client: client,
		logger: logger,
		cancel: cancel,
		routes: make(map[string]string),
	}

	if err := r.loadAll(ctx); err != nil {
		cancel()
		return nil, fmt.Errorf("load initial group routes: %w", err)
	}

	go r.watch(watchCtx)
	return r, nil
}

// LookupOwner returns the broker ID that coordinates a group, or "" if unknown/unowned.
func (r *GroupRouter) LookupOwner(groupID string) string {
	r.mu.RLock()
	brokerID := r.routes[groupID]
	r.mu.RUnlock()
	return brokerID
}

// AllRoutes returns a snapshot of the current routing table.
func (r *GroupRouter) AllRoutes() []GroupRoute {
	r.mu.RLock()
	defer r.mu.RUnlock()
	routes := make([]GroupRoute, 0, len(r.routes))
	for groupID, brokerID := range r.routes {
		routes = append(routes, GroupRoute{
			GroupID:  groupID,
			BrokerID: brokerID,
		})
	}
	return routes
}

// Invalidate removes a group from the routing table. Called when the proxy
// discovers (via NOT_COORDINATOR) that the cached route is stale. The
// next watch event will repopulate it with the correct owner.
func (r *GroupRouter) Invalidate(groupID string) {
	r.mu.Lock()
	delete(r.routes, groupID)
	r.mu.Unlock()
}

// Stop terminates the background watcher.
func (r *GroupRouter) Stop() {
	r.cancel()
}

func (r *GroupRouter) loadAll(ctx context.Context) error {
	resp, err := r.client.Get(ctx, groupLeasePrefix+"/", clientv3.WithPrefix())
	if err != nil {
		return err
	}
	fresh := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		groupID, ok := groupLeaseKeyToGroupID(string(kv.Key))
		if !ok {
			continue
		}
		fresh[groupID] = string(kv.Value)
	}
	r.mu.Lock()
	r.routes = fresh
	r.mu.Unlock()
	r.logger.Info("loaded group routes from etcd", "count", len(fresh))
	return nil
}

func (r *GroupRouter) watch(ctx context.Context) {
	for {
		watchChan := r.client.Watch(ctx, groupLeasePrefix+"/", clientv3.WithPrefix(), clientv3.WithPrevKV())
		for resp := range watchChan {
			if resp.Err() != nil {
				r.logger.Warn("group lease watch error", "error", resp.Err())
				continue
			}
			r.mu.Lock()
			for _, ev := range resp.Events {
				etcdKey := string(ev.Kv.Key)
				groupID, ok := groupLeaseKeyToGroupID(etcdKey)
				if !ok {
					continue
				}
				switch ev.Type {
				case clientv3.EventTypePut:
					r.routes[groupID] = string(ev.Kv.Value)
					r.logger.Debug("group route updated",
						"group", groupID, "broker", string(ev.Kv.Value))
				case clientv3.EventTypeDelete:
					delete(r.routes, groupID)
					r.logger.Debug("group route removed", "group", groupID)
				}
			}
			r.mu.Unlock()
		}

		if ctx.Err() != nil {
			return
		}

		r.logger.Warn("group lease watch stream closed, reconnecting")
		time.Sleep(time.Second)
		if err := r.loadAll(ctx); err != nil {
			r.logger.Warn("group lease watch reconnect: reload failed", "error", err)
		}
	}
}

// groupLeaseKeyToGroupID converts "/kafscale/group-leases/mygroup" -> "mygroup"
func groupLeaseKeyToGroupID(etcdKey string) (string, bool) {
	prefix := groupLeasePrefix + "/"
	if !strings.HasPrefix(etcdKey, prefix) {
		return "", false
	}
	groupID := strings.TrimPrefix(etcdKey, prefix)
	if groupID == "" {
		return "", false
	}
	return groupID, true
}
