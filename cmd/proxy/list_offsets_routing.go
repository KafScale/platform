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
	"fmt"
	"net"
	"sync"

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type listOffsetsResult struct {
	subReq  *kmsg.ListOffsetsRequest
	subResp *kmsg.ListOffsetsResponse
	conn    net.Conn
	target  string
	err     error
}

func (p *proxy) routeListOffsets(ctx context.Context, payload []byte) (*kmsg.ListOffsetsRequest, map[string]*kmsg.ListOffsetsRequest) {
	_, parsed, err := protocol.ParseRequest(payload)
	if err != nil {
		return nil, nil
	}
	req, ok := parsed.(*kmsg.ListOffsetsRequest)
	if !ok {
		return nil, nil
	}
	groups := p.groupListOffsetsByBroker(ctx, req, nil)
	if _, unknown := groups[""]; len(groups) == 0 || (len(groups) == 1 && unknown) {
		return nil, nil
	}
	return req, groups
}

func (p *proxy) groupListOffsetsByBroker(ctx context.Context, req *kmsg.ListOffsetsRequest, include map[string]map[int32]bool) map[string]*kmsg.ListOffsetsRequest {
	groups := make(map[string]*kmsg.ListOffsetsRequest)
	for _, topic := range req.Topics {
		for _, part := range topic.Partitions {
			if include != nil && !include[topic.Topic][part.Partition] {
				continue
			}
			addr := p.partitionOwnerAddr(ctx, topic.Topic, part.Partition)
			sub := groups[addr]
			if sub == nil {
				copied := *req
				copied.Topics = nil
				sub = &copied
				groups[addr] = sub
			}
			if n := len(sub.Topics); n == 0 || sub.Topics[n-1].Topic != topic.Topic {
				sub.Topics = append(sub.Topics, kmsg.ListOffsetsRequestTopic{Topic: topic.Topic})
			}
			last := &sub.Topics[len(sub.Topics)-1]
			last.Partitions = append(last.Partitions, part)
		}
	}
	return groups
}

func (p *proxy) forwardListOffsets(ctx context.Context, header *protocol.RequestHeader, req *kmsg.ListOffsetsRequest, groups map[string]*kmsg.ListOffsetsRequest, pool *connPool) ([]byte, error) {
	ctx, stopWaiting := protocol.ContextUntilClientGone(ctx)
	defer stopWaiting()

	const maxRetries = 3
	answered := make(map[string]map[int32]kmsg.ListOffsetsResponseTopicPartition)
	var throttle int32
	for attempt := 0; attempt < maxRetries && len(groups) > 0; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		lastAttempt := attempt == maxRetries-1
		failed := make(map[string]map[int32]bool)
		markFailed := func(topic string, partition int32) {
			if failed[topic] == nil {
				failed[topic] = make(map[int32]bool)
			}
			failed[topic][partition] = true
		}
		for _, r := range p.fanOutListOffsets(ctx, header, groups, pool) {
			if r.err != nil {
				p.logger.Warn("list offsets forward failed", "target", r.target, "error", r.err)
				if !lastAttempt {
					for _, topic := range r.subReq.Topics {
						for _, part := range topic.Partitions {
							markFailed(topic.Topic, part.Partition)
						}
					}
				}
				continue
			}
			pool.Return(r.target, r.conn)
			throttle = max(throttle, r.subResp.ThrottleMillis)
			for _, topic := range r.subResp.Topics {
				for _, part := range topic.Partitions {
					if part.ErrorCode == protocol.NOT_LEADER_OR_FOLLOWER && !lastAttempt {
						p.forgetPartitionRoute(topic.Topic, part.Partition)
						markFailed(topic.Topic, part.Partition)
						continue
					}
					if answered[topic.Topic] == nil {
						answered[topic.Topic] = make(map[int32]kmsg.ListOffsetsResponseTopicPartition)
					}
					answered[topic.Topic][part.Partition] = part
				}
			}
		}
		if len(failed) == 0 {
			break
		}
		p.logger.Debug("retrying list offsets partitions", "attempt", attempt+1, "topics", len(failed))
		groups = p.groupListOffsetsByBroker(ctx, req, failed)
	}

	merged := kmsg.NewPtrListOffsetsResponse()
	merged.ThrottleMillis = throttle
	for _, topic := range req.Topics {
		rt := kmsg.NewListOffsetsResponseTopic()
		rt.Topic = topic.Topic
		for _, part := range topic.Partitions {
			rp, ok := answered[topic.Topic][part.Partition]
			if !ok {
				rp = kmsg.NewListOffsetsResponseTopicPartition()
				rp.Partition = part.Partition
				rp.ErrorCode = protocol.REQUEST_TIMED_OUT
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		merged.Topics = append(merged.Topics, rt)
	}
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, merged), nil
}

func (p *proxy) fanOutListOffsets(ctx context.Context, header *protocol.RequestHeader, groups map[string]*kmsg.ListOffsetsRequest, pool *connPool) []listOffsetsResult {
	results := make([]listOffsetsResult, 0, len(groups))
	for addr, subReq := range groups {
		conn, target, err := p.connectForAddr(ctx, addr, nil, pool)
		if err != nil {
			target = addr
		}
		results = append(results, listOffsetsResult{subReq: subReq, conn: conn, target: target, err: err})
	}
	var wg sync.WaitGroup
	for i := range results {
		r := &results[i]
		if r.err != nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.subResp, r.err = p.sendListOffsets(ctx, header, r.conn, r.target, r.subReq)
			if r.err != nil {
				r.conn.Close()
			}
		}()
	}
	wg.Wait()
	return results
}

func (p *proxy) sendListOffsets(ctx context.Context, header *protocol.RequestHeader, conn net.Conn, addr string, req *kmsg.ListOffsetsRequest) (*kmsg.ListOffsetsResponse, error) {
	formatter := kmsg.NewRequestFormatter(kmsg.FormatterClientID(clientIDStr(header.ClientID)))
	payload := formatter.AppendRequest(nil, req, header.CorrelationID)[4:]
	data, err := p.forwardToBackend(ctx, conn, addr, payload)
	if err != nil {
		return nil, err
	}
	body, ok := protocol.SkipResponseHeader(protocol.APIKeyListOffsets, header.APIVersion, data)
	if !ok {
		return nil, fmt.Errorf("list offsets response too short or malformed header")
	}
	resp := kmsg.NewPtrListOffsetsResponse()
	resp.SetVersion(header.APIVersion)
	if err := resp.ReadFrom(body); err != nil {
		return nil, fmt.Errorf("decode list offsets response v%d: %w", header.APIVersion, err)
	}
	return resp, nil
}
