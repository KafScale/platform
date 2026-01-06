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

package main

import (
	"context"
	"fmt"
	"time"
)

func (h *handler) startConsumerLagSampler(parent context.Context) {
	if h == nil || h.store == nil || h.consumerLag == nil {
		return
	}
	interval := time.Duration(parseEnvInt("KAFSCALE_LAG_SAMPLE_INTERVAL_SEC", 5)) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
	}
	h.refreshConsumerLag(parent)
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-parent.Done():
				return
			case <-ticker.C:
				h.refreshConsumerLag(parent)
			}
		}
	}()
}

func (h *handler) refreshConsumerLag(parent context.Context) {
	if h == nil || h.store == nil || h.consumerLag == nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	offsets, err := h.store.ListConsumerOffsets(ctx)
	if err != nil {
		h.logger.Warn("consumer lag fetch failed", "error", err)
		return
	}
	if len(offsets) == 0 {
		h.consumerLag.Update(nil)
		return
	}
	nextOffsets := make(map[string]int64, len(offsets))
	lags := make([]int64, 0, len(offsets))
	for _, offset := range offsets {
		key := fmt.Sprintf("%s:%d", offset.Topic, offset.Partition)
		next, ok := nextOffsets[key]
		if !ok {
			next, err = h.store.NextOffset(ctx, offset.Topic, offset.Partition)
			if err != nil {
				continue
			}
			nextOffsets[key] = next
		}
		lag := next - offset.Offset
		if lag < 0 {
			lag = 0
		}
		lags = append(lags, lag)
	}
	h.consumerLag.Update(lags)
}
