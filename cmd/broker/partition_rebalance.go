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

package main

import (
	"context"
	"net"
	"strconv"
	"time"
)

const defaultPartitionRebalanceInterval = 10 * time.Second

func (h *handler) startPartitionRebalance(ctx context.Context) {
	interval := time.Duration(parseEnvInt("KAFSCALE_PARTITION_REBALANCE_INTERVAL_MS", int(defaultPartitionRebalanceInterval/time.Millisecond))) * time.Millisecond
	if h.leaseManager == nil || !h.flushOnAck || interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.rebalancePartitions(ctx, brokerListening, 3*interval)
			}
		}
	}()
}

func brokerListening(ctx context.Context, addr string) bool {
	dialer := net.Dialer{Timeout: time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func (h *handler) rebalancePartitions(ctx context.Context, listening func(context.Context, string) bool, hold time.Duration) int {
	if h.leaseManager == nil || !h.flushOnAck {
		return 0
	}
	meta, err := h.store.Metadata(ctx, nil)
	if err != nil || meta == nil {
		return 0
	}
	addrs := make(map[int32]string, len(meta.Brokers))
	for _, b := range meta.Brokers {
		addrs[b.NodeID] = net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port)))
	}
	up := make(map[int32]bool)
	released := 0
	for _, topic := range meta.Topics {
		if topic.Topic == nil || topic.ErrorCode != 0 {
			continue
		}
		for _, part := range topic.Partitions {
			if part.ErrorCode != 0 || part.Leader == h.brokerInfo.NodeID || !h.leaseManager.Owns(*topic.Topic, part.Partition) {
				continue
			}
			addr, ok := addrs[part.Leader]
			if !ok {
				continue
			}
			reachable, checked := up[part.Leader]
			if !checked {
				reachable = listening(ctx, addr)
				up[part.Leader] = reachable
			}
			if !reachable {
				continue
			}
			h.leaseManager.Handover(*topic.Topic, part.Partition, hold)
			released++
			h.logger.Info("handed partition to preferred broker", "topic", *topic.Topic, "partition", part.Partition, "broker", part.Leader)
		}
	}
	return released
}
