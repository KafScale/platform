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
	"time"

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type topicPartition struct {
	topic     string
	partition int32
}

type partitionReply struct {
	errorCode int16
	body      any
}

type ownerResult struct {
	include map[topicPartition]bool
	resp    kmsg.Response
	conn    net.Conn
	target  string
	err     error
}

func (p *proxy) routeByPartitionOwner(ctx context.Context, payload []byte) (kmsg.Request, map[string]map[topicPartition]bool) {
	_, req, err := protocol.ParseRequest(payload)
	if err != nil {
		return nil, nil
	}
	groups := p.groupByPartitionOwner(ctx, requestPartitions(req))
	if _, unknown := groups[""]; len(groups) == 0 || (len(groups) == 1 && unknown) {
		return nil, nil
	}
	return req, groups
}

func requestPartitions(req kmsg.Request) []topicPartition {
	var parts []topicPartition
	switch req := req.(type) {
	case *kmsg.ListOffsetsRequest:
		for _, topic := range req.Topics {
			for _, part := range topic.Partitions {
				parts = append(parts, topicPartition{topic.Topic, part.Partition})
			}
		}
	case *kmsg.OffsetForLeaderEpochRequest:
		for _, topic := range req.Topics {
			for _, part := range topic.Partitions {
				parts = append(parts, topicPartition{topic.Topic, part.Partition})
			}
		}
	}
	return parts
}

func (p *proxy) groupByPartitionOwner(ctx context.Context, parts []topicPartition) map[string]map[topicPartition]bool {
	groups := make(map[string]map[topicPartition]bool)
	leaders := make(map[string]map[int32]int32)
	for _, tp := range parts {
		addr := p.partitionOwnerAddr(ctx, tp.topic, tp.partition)
		if addr == "" {
			topicLeaders, ok := leaders[tp.topic]
			if !ok {
				topicLeaders = p.preferredLeaders(ctx, tp.topic)
				leaders[tp.topic] = topicLeaders
			}
			addr = p.preferredLeaderAddr(ctx, topicLeaders, tp.partition)
		}
		if groups[addr] == nil {
			groups[addr] = make(map[topicPartition]bool)
		}
		groups[addr][tp] = true
	}
	return groups
}

func (p *proxy) refreshPartitionRoute(ctx context.Context, tp topicPartition, refusedBy string) {
	if p.router == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := p.router.Refresh(ctx, tp.topic, tp.partition); err != nil {
		p.logger.Warn("partition route refresh failed", "topic", tp.topic, "partition", tp.partition, "error", err)
		p.forgetPartitionRoute(tp.topic, tp.partition)
		return
	}
	if p.partitionOwnerAddr(ctx, tp.topic, tp.partition) == refusedBy {
		p.forgetPartitionRoute(tp.topic, tp.partition)
	}
}

func (p *proxy) forwardByPartitionOwner(ctx context.Context, header *protocol.RequestHeader, req kmsg.Request, groups map[string]map[topicPartition]bool, pool *connPool) ([]byte, error) {
	ctx, stopWaiting := protocol.ContextUntilClientGone(ctx)
	defer stopWaiting()

	const maxRetries = 3
	answered := make(map[topicPartition]partitionReply)
	var throttle int32
	for attempt := 0; attempt < maxRetries && len(groups) > 0; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		lastAttempt := attempt == maxRetries-1
		var failed []topicPartition
		for _, r := range p.fanOutByPartitionOwner(ctx, header, req, groups, pool) {
			if r.err != nil {
				p.logger.Warn("partition owner forward failed", "target", r.target, "error", r.err)
				for tp := range r.include {
					if lastAttempt {
						answered[tp] = partitionReply{errorCode: protocol.REQUEST_TIMED_OUT}
						continue
					}
					failed = append(failed, tp)
				}
				continue
			}
			pool.Return(r.target, r.conn)
			replyThrottle, replies := responseReplies(r.resp)
			throttle = max(throttle, replyThrottle)
			for tp, reply := range replies {
				if reply.errorCode == protocol.NOT_LEADER_OR_FOLLOWER && !lastAttempt {
					p.refreshPartitionRoute(ctx, tp, r.target)
					failed = append(failed, tp)
					continue
				}
				answered[tp] = reply
			}
		}
		if len(failed) == 0 {
			break
		}
		p.logger.Debug("retrying partitions on their owner", "attempt", attempt+1, "partitions", len(failed))
		groups = p.groupByPartitionOwner(ctx, failed)
	}
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, mergeReplies(req, answered, throttle)), nil
}

func (p *proxy) fanOutByPartitionOwner(ctx context.Context, header *protocol.RequestHeader, req kmsg.Request, groups map[string]map[topicPartition]bool, pool *connPool) []ownerResult {
	results := make([]ownerResult, 0, len(groups))
	for addr, include := range groups {
		conn, target, err := p.connectForAddr(ctx, addr, nil, pool)
		if err != nil {
			target = addr
		}
		results = append(results, ownerResult{include: include, conn: conn, target: target, err: err})
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
			r.resp, r.err = p.sendToOwner(ctx, header, r.conn, r.target, subRequest(req, r.include))
			if r.err != nil {
				r.conn.Close()
			}
		}()
	}
	wg.Wait()
	return results
}

func (p *proxy) sendToOwner(ctx context.Context, header *protocol.RequestHeader, conn net.Conn, addr string, req kmsg.Request) (kmsg.Response, error) {
	formatter := kmsg.NewRequestFormatter(kmsg.FormatterClientID(clientIDStr(header.ClientID)))
	payload := formatter.AppendRequest(nil, req, header.CorrelationID)[4:]
	data, err := p.forwardToBackend(ctx, conn, addr, payload)
	if err != nil {
		return nil, err
	}
	body, ok := protocol.SkipResponseHeader(req.Key(), header.APIVersion, data)
	if !ok {
		return nil, fmt.Errorf("api key %d response too short or malformed header", req.Key())
	}
	resp := req.ResponseKind()
	resp.SetVersion(header.APIVersion)
	if err := resp.ReadFrom(body); err != nil {
		return nil, fmt.Errorf("decode api key %d response v%d: %w", req.Key(), header.APIVersion, err)
	}
	return resp, nil
}

func subRequest(req kmsg.Request, include map[topicPartition]bool) kmsg.Request {
	switch req := req.(type) {
	case *kmsg.ListOffsetsRequest:
		sub := *req
		sub.Topics = nil
		for _, topic := range req.Topics {
			st := topic
			st.Partitions = nil
			for _, part := range topic.Partitions {
				if include[topicPartition{topic.Topic, part.Partition}] {
					st.Partitions = append(st.Partitions, part)
				}
			}
			if len(st.Partitions) > 0 {
				sub.Topics = append(sub.Topics, st)
			}
		}
		return &sub
	case *kmsg.OffsetForLeaderEpochRequest:
		sub := *req
		sub.Topics = nil
		for _, topic := range req.Topics {
			st := topic
			st.Partitions = nil
			for _, part := range topic.Partitions {
				if include[topicPartition{topic.Topic, part.Partition}] {
					st.Partitions = append(st.Partitions, part)
				}
			}
			if len(st.Partitions) > 0 {
				sub.Topics = append(sub.Topics, st)
			}
		}
		return &sub
	}
	return req
}

func responseReplies(resp kmsg.Response) (int32, map[topicPartition]partitionReply) {
	replies := make(map[topicPartition]partitionReply)
	switch resp := resp.(type) {
	case *kmsg.ListOffsetsResponse:
		for _, topic := range resp.Topics {
			for _, part := range topic.Partitions {
				replies[topicPartition{topic.Topic, part.Partition}] = partitionReply{errorCode: part.ErrorCode, body: part}
			}
		}
		return resp.ThrottleMillis, replies
	case *kmsg.OffsetForLeaderEpochResponse:
		for _, topic := range resp.Topics {
			for _, part := range topic.Partitions {
				replies[topicPartition{topic.Topic, part.Partition}] = partitionReply{errorCode: part.ErrorCode, body: part}
			}
		}
		return resp.ThrottleMillis, replies
	}
	return 0, replies
}

func mergeReplies(req kmsg.Request, answered map[topicPartition]partitionReply, throttle int32) kmsg.Response {
	switch req := req.(type) {
	case *kmsg.ListOffsetsRequest:
		merged := kmsg.NewPtrListOffsetsResponse()
		merged.ThrottleMillis = throttle
		for _, topic := range req.Topics {
			rt := kmsg.NewListOffsetsResponseTopic()
			rt.Topic = topic.Topic
			for _, part := range topic.Partitions {
				reply, ok := answered[topicPartition{topic.Topic, part.Partition}]
				if !ok {
					continue
				}
				rp, ok := reply.body.(kmsg.ListOffsetsResponseTopicPartition)
				if !ok {
					rp = kmsg.NewListOffsetsResponseTopicPartition()
					rp.Partition = part.Partition
					rp.ErrorCode = reply.errorCode
				}
				rt.Partitions = append(rt.Partitions, rp)
			}
			if len(rt.Partitions) > 0 {
				merged.Topics = append(merged.Topics, rt)
			}
		}
		return merged
	case *kmsg.OffsetForLeaderEpochRequest:
		merged := kmsg.NewPtrOffsetForLeaderEpochResponse()
		merged.ThrottleMillis = throttle
		for _, topic := range req.Topics {
			rt := kmsg.NewOffsetForLeaderEpochResponseTopic()
			rt.Topic = topic.Topic
			for _, part := range topic.Partitions {
				reply, ok := answered[topicPartition{topic.Topic, part.Partition}]
				if !ok {
					continue
				}
				rp, ok := reply.body.(kmsg.OffsetForLeaderEpochResponseTopicPartition)
				if !ok {
					rp = kmsg.NewOffsetForLeaderEpochResponseTopicPartition()
					rp.Partition = part.Partition
					rp.ErrorCode = reply.errorCode
				}
				rt.Partitions = append(rt.Partitions, rp)
			}
			if len(rt.Partitions) > 0 {
				merged.Topics = append(merged.Topics, rt)
			}
		}
		return merged
	}
	return nil
}
