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
	"strconv"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// PartitionRoute maps a topic/partition to the broker that currently owns it.
type PartitionRoute struct {
	Topic     string
	Partition int32
	BrokerID  string
}

// PartitionRouter watches etcd partition lease keys and maintains an in-memory
// routing table. The proxy uses this to send produce requests to the correct broker
// without round-tripping to etcd on every request.
//
// The routing table is best-effort: it can be briefly stale after a lease change.
// The broker's own ownership check is the authoritative guard.
type PartitionRouter struct {
	client *clientv3.Client
	logger *slog.Logger
	cancel context.CancelFunc

	mu     sync.RWMutex
	routes map[string]string // "topic/partition" -> brokerID
}

// NewPartitionRouter creates a router and starts watching etcd for lease changes.
func NewPartitionRouter(ctx context.Context, client *clientv3.Client, logger *slog.Logger) (*PartitionRouter, error) {
	if logger == nil {
		logger = slog.Default()
	}
	watchCtx, cancel := context.WithCancel(ctx)
	r := &PartitionRouter{
		client: client,
		logger: logger,
		cancel: cancel,
		routes: make(map[string]string),
	}

	if err := r.loadAll(ctx); err != nil {
		cancel()
		return nil, fmt.Errorf("load initial partition routes: %w", err)
	}

	go r.watch(watchCtx)
	return r, nil
}

// LookupOwner returns the broker ID that owns a partition, or "" if unknown/unowned.
func (r *PartitionRouter) LookupOwner(topic string, partition int32) string {
	key := partitionKey(topic, partition)
	r.mu.RLock()
	brokerID := r.routes[key]
	r.mu.RUnlock()
	return brokerID
}

// AllRoutes returns a snapshot of the current routing table.
func (r *PartitionRouter) AllRoutes() []PartitionRoute {
	r.mu.RLock()
	defer r.mu.RUnlock()
	routes := make([]PartitionRoute, 0, len(r.routes))
	for key, brokerID := range r.routes {
		topic, partition, ok := parsePartitionKey(key)
		if !ok {
			continue
		}
		routes = append(routes, PartitionRoute{
			Topic:     topic,
			Partition: partition,
			BrokerID:  brokerID,
		})
	}
	return routes
}

// Invalidate removes a partition from the routing table. Called when the proxy
// discovers (via NOT_LEADER_OR_FOLLOWER) that the cached route is stale. The
// next watch event will repopulate it with the correct owner.
func (r *PartitionRouter) Invalidate(topic string, partition int32) {
	key := partitionKey(topic, partition)
	r.mu.Lock()
	delete(r.routes, key)
	r.mu.Unlock()
}

// Stop terminates the background watcher.
func (r *PartitionRouter) Stop() {
	r.cancel()
}

func (r *PartitionRouter) loadAll(ctx context.Context) error {
	resp, err := r.client.Get(ctx, partitionLeasePrefix+"/", clientv3.WithPrefix())
	if err != nil {
		return err
	}
	fresh := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		etcdKey := string(kv.Key)
		routeKey, ok := leaseKeyToRouteKey(etcdKey)
		if !ok {
			continue
		}
		fresh[routeKey] = string(kv.Value)
	}
	r.mu.Lock()
	r.routes = fresh
	r.mu.Unlock()
	r.logger.Info("loaded partition routes from etcd", "count", len(fresh))
	return nil
}

func (r *PartitionRouter) watch(ctx context.Context) {
	for {
		watchChan := r.client.Watch(ctx, partitionLeasePrefix+"/", clientv3.WithPrefix(), clientv3.WithPrevKV())
		for resp := range watchChan {
			if resp.Err() != nil {
				r.logger.Warn("partition lease watch error", "error", resp.Err())
				continue
			}
			r.mu.Lock()
			for _, ev := range resp.Events {
				etcdKey := string(ev.Kv.Key)
				routeKey, ok := leaseKeyToRouteKey(etcdKey)
				if !ok {
					continue
				}
				switch ev.Type {
				case clientv3.EventTypePut:
					r.routes[routeKey] = string(ev.Kv.Value)
					r.logger.Debug("partition route updated",
						"key", routeKey, "broker", string(ev.Kv.Value))
				case clientv3.EventTypeDelete:
					delete(r.routes, routeKey)
					r.logger.Debug("partition route removed", "key", routeKey)
				}
			}
			r.mu.Unlock()
		}

		// Watch channel closed. If the context is done, exit for good.
		if ctx.Err() != nil {
			return
		}

		// Transient failure (etcd leader election, compaction, network blip).
		// Reseed the routing table from a full read and re-establish the watch.
		r.logger.Warn("partition lease watch stream closed, reconnecting")
		time.Sleep(time.Second)
		if err := r.loadAll(ctx); err != nil {
			r.logger.Warn("partition lease watch reconnect: reload failed", "error", err)
		}
	}
}

// leaseKeyToRouteKey converts "/kafscale/partition-leases/topic/partition" -> "topic:partition"
func leaseKeyToRouteKey(etcdKey string) (string, bool) {
	prefix := partitionLeasePrefix + "/"
	if !strings.HasPrefix(etcdKey, prefix) {
		return "", false
	}
	remainder := strings.TrimPrefix(etcdKey, prefix)
	// remainder is "topic/partition"
	lastSlash := strings.LastIndex(remainder, "/")
	if lastSlash <= 0 || lastSlash == len(remainder)-1 {
		return "", false
	}
	topic := remainder[:lastSlash]
	partStr := remainder[lastSlash+1:]
	partition, err := strconv.ParseInt(partStr, 10, 32)
	if err != nil {
		return "", false
	}
	return fmt.Sprintf("%s:%d", topic, int32(partition)), true
}

// parsePartitionKey is the inverse of partitionKey ("topic:partition").
func parsePartitionKey(key string) (string, int32, bool) {
	lastColon := strings.LastIndex(key, ":")
	if lastColon <= 0 || lastColon == len(key)-1 {
		return "", 0, false
	}
	topic := key[:lastColon]
	partition, err := strconv.ParseInt(key[lastColon+1:], 10, 32)
	if err != nil {
		return "", 0, false
	}
	return topic, int32(partition), true
}
