// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"time"

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Probe first so a backlog can drain over pooled sockets without waiting for
// caught-up brokers. A successful round below MinBytes may begin a long poll.
func (p *proxy) fanOutFetch(ctx context.Context, header *protocol.RequestHeader, groups map[string]*kmsg.FetchRequest, originalPayload []byte, triedBackends map[string]bool, pool *connPool) []fetchFanOutResult {
	coalesceFetchFallback(groups)
	if len(groups) <= 1 {
		return p.fanOutFetchOnce(ctx, header, groups, originalPayload, triedBackends, pool)
	}
	var waitMillis, minimumBytes int32
	for _, request := range groups {
		if request.MaxWaitMillis <= 0 || request.MinBytes <= 0 {
			return p.fanOutFetchOnce(ctx, header, groups, originalPayload, triedBackends, pool)
		}
		if request.MinBytes > minimumBytes {
			minimumBytes = request.MinBytes
		}
		if waitMillis == 0 || request.MaxWaitMillis < waitMillis {
			waitMillis = request.MaxWaitMillis
		}
	}
	deadline := time.Now().Add(time.Duration(waitMillis) * time.Millisecond)
	results := p.fanOutFetchOnce(ctx, header, fetchGroupsWithWait(groups, 0), nil, triedBackends, pool)
	remaining := int32(time.Until(deadline) / time.Millisecond)
	if !fetchResultsCanWait(results, minimumBytes) || ctx.Err() != nil || remaining <= 0 {
		return results
	}
	for _, result := range results {
		if result.conn != nil {
			pool.Return(result.target, result.conn)
		}
	}
	return p.fanOutFetchOnce(ctx, header, fetchGroupsWithWait(groups, remaining), nil, triedBackends, pool)
}

func fetchGroupsWithWait(groups map[string]*kmsg.FetchRequest, millis int32) map[string]*kmsg.FetchRequest {
	copies := make(map[string]*kmsg.FetchRequest, len(groups))
	for addr, request := range groups {
		copy := *request
		copy.MaxWaitMillis = millis
		copies[addr] = &copy
	}
	return copies
}

func fetchResponseHasRecords(response *kmsg.FetchResponse) bool {
	if response == nil || response.ErrorCode != 0 {
		return false
	}
	for _, topic := range response.Topics {
		for _, part := range topic.Partitions {
			if part.ErrorCode == 0 && len(part.RecordBatches) > 0 {
				return true
			}
		}
	}
	return false
}

func fetchResultsCanWait(results []fetchFanOutResult, minimumBytes int32) bool {
	bytes := 0
	for _, result := range results {
		if result.err != nil || result.subResp == nil || result.subResp.ErrorCode != 0 {
			return false
		}
		for _, topic := range result.subResp.Topics {
			for _, part := range topic.Partitions {
				if part.ErrorCode != 0 {
					return false
				}
				bytes += len(part.RecordBatches)
			}
		}
	}
	return bytes < int(minimumBytes)
}
