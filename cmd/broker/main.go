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

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/novatechflow/kafscale/pkg/broker"
	"github.com/novatechflow/kafscale/pkg/cache"
	controlpb "github.com/novatechflow/kafscale/pkg/gen/control"
	metadatapb "github.com/novatechflow/kafscale/pkg/gen/metadata"
	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/protocol"
	"github.com/novatechflow/kafscale/pkg/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	defaultKafkaAddr      = ":19092"
	defaultKafkaPort      = 19092
	defaultMetricsAddr    = ":19093"
	defaultControlAddr    = ":19094"
	defaultMinioBucket    = "kafscale"
	defaultMinioRegion    = "us-east-1"
	defaultMinioEndpoint  = "http://127.0.0.1:9000"
	defaultMinioAccessKey = "minioadmin"
	defaultMinioSecretKey = "minioadmin"
	brokerVersion         = "dev"
)

type handler struct {
	apiVersions          []protocol.ApiVersion
	store                metadata.Store
	s3                   storage.S3Client
	cache                *cache.SegmentCache
	logs                 map[string]map[int32]*storage.PartitionLog
	logMu                sync.Mutex
	logConfig            storage.PartitionLogConfig
	coordinator          *broker.GroupCoordinator
	s3Health             *broker.S3HealthMonitor
	s3Namespace          string
	brokerInfo           protocol.MetadataBroker
	logger               *slog.Logger
	autoCreateTopics     bool
	autoCreatePartitions int32
	traceKafka           bool
	produceRate          *throughputTracker
	fetchRate            *throughputTracker
	cacheSize            int
	readAhead            int
	segmentBytes         int
	flushInterval        time.Duration
	adminMetrics         *adminMetrics
}

func (h *handler) Handle(ctx context.Context, header *protocol.RequestHeader, req protocol.Request) ([]byte, error) {
	if h.traceKafka {
		h.logger.Debug("received request", "api_key", header.APIKey, "api_version", header.APIVersion, "correlation", header.CorrelationID, "client_id", header.ClientID)
	}
	switch req.(type) {
	case *protocol.ApiVersionsRequest:
		errorCode := protocol.NONE
		if header.APIVersion != 0 {
			errorCode = protocol.UNSUPPORTED_VERSION
		}
		resp := &protocol.ApiVersionsResponse{
			CorrelationID: header.CorrelationID,
			ErrorCode:     errorCode,
			Versions:      h.apiVersions,
		}
		if h.traceKafka {
			h.logger.Debug("api versions response", "versions", resp.Versions)
		}
		return protocol.EncodeApiVersionsResponse(resp)
	case *protocol.MetadataRequest:
		metaReq := req.(*protocol.MetadataRequest)
		if h.traceKafka {
			h.logger.Debug("metadata request", "topics", metaReq.Topics, "topic_ids", len(metaReq.TopicIDs))
		}
		if h.autoCreateTopics && len(metaReq.Topics) > 0 {
			for _, name := range metaReq.Topics {
				if strings.TrimSpace(name) == "" {
					continue
				}
				if err := h.ensureTopic(ctx, name, 0); err != nil {
					return nil, fmt.Errorf("auto-create topic %s: %w", name, err)
				}
			}
		}
		meta, err := func() (*metadata.ClusterMetadata, error) {
			zeroID := [16]byte{}
			useIDs := false
			for _, id := range metaReq.TopicIDs {
				if id != zeroID {
					useIDs = true
					break
				}
			}
			if !useIDs {
				return h.store.Metadata(ctx, metaReq.Topics)
			}
			all, err := h.store.Metadata(ctx, nil)
			if err != nil {
				return nil, err
			}
			index := make(map[[16]byte]protocol.MetadataTopic, len(all.Topics))
			for _, topic := range all.Topics {
				index[topic.TopicID] = topic
			}
			filtered := make([]protocol.MetadataTopic, 0, len(metaReq.TopicIDs))
			for _, id := range metaReq.TopicIDs {
				if id == zeroID {
					continue
				}
				if topic, ok := index[id]; ok {
					filtered = append(filtered, topic)
				} else {
					filtered = append(filtered, protocol.MetadataTopic{
						ErrorCode: protocol.UNKNOWN_TOPIC_ID,
						TopicID:   id,
					})
				}
			}
			return &metadata.ClusterMetadata{
				Brokers:      all.Brokers,
				ClusterID:    all.ClusterID,
				ControllerID: all.ControllerID,
				Topics:       filtered,
			}, nil
		}()
		if err != nil {
			return nil, fmt.Errorf("load metadata: %w", err)
		}
		resp := &protocol.MetadataResponse{
			CorrelationID: header.CorrelationID,
			ThrottleMs:    0,
			Brokers:       meta.Brokers,
			ClusterID:     meta.ClusterID,
			ControllerID:  meta.ControllerID,
			Topics:        meta.Topics,
		}
		if h.traceKafka {
			topicSummaries := make([]string, 0, len(meta.Topics))
			for _, topic := range meta.Topics {
				topicSummaries = append(topicSummaries, fmt.Sprintf("%s(error=%d partitions=%d)", topic.Name, topic.ErrorCode, len(topic.Partitions)))
			}
			brokerAddrs := make([]string, 0, len(meta.Brokers))
			for _, broker := range meta.Brokers {
				brokerAddrs = append(brokerAddrs, fmt.Sprintf("%s:%d", broker.Host, broker.Port))
			}
			h.logger.Debug("metadata response", "topics", topicSummaries, "brokers", brokerAddrs)
		}
		return protocol.EncodeMetadataResponse(resp, header.APIVersion)
	case *protocol.ProduceRequest:
		return h.handleProduce(ctx, header, req.(*protocol.ProduceRequest))
	case *protocol.FetchRequest:
		return h.handleFetch(ctx, header, req.(*protocol.FetchRequest))
	case *protocol.FindCoordinatorRequest:
		resp := h.coordinator.FindCoordinatorResponse(header.CorrelationID, protocol.NONE)
		return protocol.EncodeFindCoordinatorResponse(resp, header.APIVersion)
	case *protocol.JoinGroupRequest:
		resp, err := h.coordinator.JoinGroup(ctx, req.(*protocol.JoinGroupRequest), header.CorrelationID)
		if err != nil {
			return nil, err
		}
		return protocol.EncodeJoinGroupResponse(resp, header.APIVersion)
	case *protocol.SyncGroupRequest:
		resp, err := h.coordinator.SyncGroup(ctx, req.(*protocol.SyncGroupRequest), header.CorrelationID)
		if err != nil {
			return nil, err
		}
		return protocol.EncodeSyncGroupResponse(resp, header.APIVersion)
	case *protocol.DescribeGroupsRequest:
		return h.withAdminMetrics(header.APIKey, func() ([]byte, error) {
			resp, err := h.coordinator.DescribeGroups(ctx, req.(*protocol.DescribeGroupsRequest), header.CorrelationID)
			if err != nil {
				return nil, err
			}
			return protocol.EncodeDescribeGroupsResponse(resp, header.APIVersion)
		})
	case *protocol.ListGroupsRequest:
		return h.withAdminMetrics(header.APIKey, func() ([]byte, error) {
			resp, err := h.coordinator.ListGroups(ctx, req.(*protocol.ListGroupsRequest), header.CorrelationID)
			if err != nil {
				return nil, err
			}
			return protocol.EncodeListGroupsResponse(resp, header.APIVersion)
		})
	case *protocol.HeartbeatRequest:
		resp := h.coordinator.Heartbeat(ctx, req.(*protocol.HeartbeatRequest), header.CorrelationID)
		return protocol.EncodeHeartbeatResponse(resp, header.APIVersion)
	case *protocol.LeaveGroupRequest:
		resp := h.coordinator.LeaveGroup(ctx, req.(*protocol.LeaveGroupRequest), header.CorrelationID)
		return protocol.EncodeLeaveGroupResponse(resp)
	case *protocol.OffsetCommitRequest:
		resp, err := h.coordinator.OffsetCommit(ctx, req.(*protocol.OffsetCommitRequest), header.CorrelationID)
		if err != nil {
			return nil, err
		}
		return protocol.EncodeOffsetCommitResponse(resp)
	case *protocol.OffsetFetchRequest:
		resp, err := h.coordinator.OffsetFetch(ctx, req.(*protocol.OffsetFetchRequest), header.CorrelationID)
		if err != nil {
			return nil, err
		}
		return protocol.EncodeOffsetFetchResponse(resp, header.APIVersion)
	case *protocol.OffsetForLeaderEpochRequest:
		return h.withAdminMetrics(header.APIKey, func() ([]byte, error) {
			return h.handleOffsetForLeaderEpoch(ctx, header, req.(*protocol.OffsetForLeaderEpochRequest))
		})
	case *protocol.DescribeConfigsRequest:
		return h.withAdminMetrics(header.APIKey, func() ([]byte, error) {
			return h.handleDescribeConfigs(ctx, header, req.(*protocol.DescribeConfigsRequest))
		})
	case *protocol.AlterConfigsRequest:
		return h.withAdminMetrics(header.APIKey, func() ([]byte, error) {
			return h.handleAlterConfigs(ctx, header, req.(*protocol.AlterConfigsRequest))
		})
	case *protocol.CreatePartitionsRequest:
		return h.withAdminMetrics(header.APIKey, func() ([]byte, error) {
			return h.handleCreatePartitions(ctx, header, req.(*protocol.CreatePartitionsRequest))
		})
	case *protocol.DeleteGroupsRequest:
		return h.withAdminMetrics(header.APIKey, func() ([]byte, error) {
			resp, err := h.coordinator.DeleteGroups(ctx, req.(*protocol.DeleteGroupsRequest), header.CorrelationID)
			if err != nil {
				return nil, err
			}
			return protocol.EncodeDeleteGroupsResponse(resp, header.APIVersion)
		})
	case *protocol.CreateTopicsRequest:
		return h.handleCreateTopics(ctx, header, req.(*protocol.CreateTopicsRequest))
	case *protocol.DeleteTopicsRequest:
		return h.handleDeleteTopics(ctx, header, req.(*protocol.DeleteTopicsRequest))
	case *protocol.ListOffsetsRequest:
		return h.handleListOffsets(ctx, header, req.(*protocol.ListOffsetsRequest))
	default:
		return nil, ErrUnsupportedAPI
	}
}

var ErrUnsupportedAPI = fmt.Errorf("unsupported api")

func (h *handler) backpressureErrorCode() int16 {
	switch h.s3Health.State() {
	case broker.S3StateDegraded:
		return protocol.REQUEST_TIMED_OUT
	case broker.S3StateUnavailable:
		return protocol.UNKNOWN_SERVER_ERROR
	default:
		return protocol.UNKNOWN_SERVER_ERROR
	}
}

func (h *handler) metricsHandler(w http.ResponseWriter, r *http.Request) {
	snap := h.s3Health.Snapshot()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintln(w, "# HELP kafscale_s3_health_state Current broker view of S3 health.")
	fmt.Fprintln(w, "# TYPE kafscale_s3_health_state gauge")
	for _, state := range []broker.S3HealthState{broker.S3StateHealthy, broker.S3StateDegraded, broker.S3StateUnavailable} {
		value := 0
		if snap.State == state {
			value = 1
		}
		fmt.Fprintf(w, "kafscale_s3_health_state{state=%q} %d\n", state, value)
	}
	fmt.Fprintln(w, "# HELP kafscale_s3_latency_ms_avg Average S3 latency in the sliding window.")
	fmt.Fprintln(w, "# TYPE kafscale_s3_latency_ms_avg gauge")
	fmt.Fprintf(w, "kafscale_s3_latency_ms_avg %f\n", float64(snap.AvgLatency)/float64(time.Millisecond))
	fmt.Fprintln(w, "# HELP kafscale_s3_error_rate Fraction of S3 operations that failed in the sliding window.")
	fmt.Fprintln(w, "# TYPE kafscale_s3_error_rate gauge")
	fmt.Fprintf(w, "kafscale_s3_error_rate %f\n", snap.ErrorRate)
	if !snap.Since.IsZero() {
		fmt.Fprintln(w, "# HELP kafscale_s3_state_duration_seconds Seconds spent in the current S3 state.")
		fmt.Fprintln(w, "# TYPE kafscale_s3_state_duration_seconds gauge")
		fmt.Fprintf(w, "kafscale_s3_state_duration_seconds %f\n", time.Since(snap.Since).Seconds())
	}
	fmt.Fprintln(w, "# HELP kafscale_produce_rps Broker ingest throughput measured over the sliding window.")
	fmt.Fprintln(w, "# TYPE kafscale_produce_rps gauge")
	fmt.Fprintf(w, "kafscale_produce_rps %f\n", h.produceRate.rate())
	fmt.Fprintln(w, "# HELP kafscale_fetch_rps Broker fetch throughput measured over the sliding window.")
	fmt.Fprintln(w, "# TYPE kafscale_fetch_rps gauge")
	fmt.Fprintf(w, "kafscale_fetch_rps %f\n", h.fetchRate.rate())
	h.adminMetrics.writePrometheus(w)
}

func (h *handler) recordS3Op(op string, latency time.Duration, err error) {
	if h.s3Health == nil {
		return
	}
	h.s3Health.RecordOperation(op, latency, err)
}

func (h *handler) withAdminMetrics(apiKey int16, fn func() ([]byte, error)) ([]byte, error) {
	start := time.Now()
	payload, err := fn()
	h.adminMetrics.Record(apiKey, time.Since(start), err)
	return payload, err
}

func (h *handler) handleProduce(ctx context.Context, header *protocol.RequestHeader, req *protocol.ProduceRequest) ([]byte, error) {
	topicResponses := make([]protocol.ProduceTopicResponse, 0, len(req.Topics))
	now := time.Now().UnixMilli()
	var producedMessages int64

	for _, topic := range req.Topics {
		if h.traceKafka {
			h.logger.Debug("produce request received", "topic", topic.Name, "partitions", len(topic.Partitions), "acks", req.Acks, "timeout_ms", req.TimeoutMs)
		}
		partitionResponses := make([]protocol.ProducePartitionResponse, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			if h.s3Health.State() != broker.S3StateHealthy {
				partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
					Partition: part.Partition,
					ErrorCode: h.backpressureErrorCode(),
				})
				if h.traceKafka {
					h.logger.Debug("produce rejected due to S3 health", "topic", topic.Name, "partition", part.Partition, "s3_state", h.s3Health.State())
				}
				continue
			}
			plog, err := h.getPartitionLog(ctx, topic.Name, part.Partition)
			if err != nil {
				h.logger.Error("partition log init failed", "error", err, "topic", topic.Name, "partition", part.Partition)
				partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
					Partition: part.Partition,
					ErrorCode: protocol.UNKNOWN_SERVER_ERROR,
				})
				continue
			}
			batch, err := storage.NewRecordBatchFromBytes(part.Records)
			if err != nil {
				partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
					Partition: part.Partition,
					ErrorCode: protocol.UNKNOWN_SERVER_ERROR,
				})
				if h.traceKafka {
					h.logger.Debug("produce record batch decode failed", "topic", topic.Name, "partition", part.Partition, "error", err)
				}
				continue
			}
			result, err := plog.AppendBatch(ctx, batch)
			if err != nil {
				partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
					Partition: part.Partition,
					ErrorCode: h.backpressureErrorCode(),
				})
				if h.traceKafka {
					h.logger.Debug("produce append failed", "topic", topic.Name, "partition", part.Partition, "error", err)
				}
				continue
			}
			if req.Acks != 0 {
				if err := plog.Flush(ctx); err != nil {
					h.logger.Error("flush failed", "error", err, "topic", topic.Name, "partition", part.Partition)
					partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
						Partition: part.Partition,
						ErrorCode: h.backpressureErrorCode(),
					})
					continue
				}
			}
			partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
				Partition:       part.Partition,
				ErrorCode:       0,
				BaseOffset:      result.BaseOffset,
				LogAppendTimeMs: now,
				LogStartOffset:  0,
			})
			producedMessages += int64(batch.MessageCount)
			if h.traceKafka {
				h.logger.Debug("produce append success", "topic", topic.Name, "partition", part.Partition, "base_offset", result.BaseOffset, "last_offset", result.LastOffset)
			}
		}
		topicResponses = append(topicResponses, protocol.ProduceTopicResponse{
			Name:       topic.Name,
			Partitions: partitionResponses,
		})
	}

	if producedMessages > 0 {
		h.produceRate.add(producedMessages)
	}

	if req.Acks == 0 {
		return nil, nil
	}

	return protocol.EncodeProduceResponse(&protocol.ProduceResponse{
		CorrelationID: header.CorrelationID,
		Topics:        topicResponses,
		ThrottleMs:    0,
	}, header.APIVersion)
}

func (h *handler) handleCreateTopics(ctx context.Context, header *protocol.RequestHeader, req *protocol.CreateTopicsRequest) ([]byte, error) {
	results := make([]protocol.CreateTopicResult, 0, len(req.Topics))
	for _, topic := range req.Topics {
		_, err := h.store.CreateTopic(ctx, metadata.TopicSpec{
			Name:              topic.Name,
			NumPartitions:     topic.NumPartitions,
			ReplicationFactor: topic.ReplicationFactor,
		})
		result := protocol.CreateTopicResult{Name: topic.Name}
		if err != nil {
			switch {
			case errors.Is(err, metadata.ErrTopicExists):
				result.ErrorCode = protocol.TOPIC_ALREADY_EXISTS
			case errors.Is(err, metadata.ErrInvalidTopic):
				result.ErrorCode = protocol.INVALID_TOPIC_EXCEPTION
			default:
				result.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			}
			result.ErrorMessage = err.Error()
		}
		results = append(results, result)
	}
	return protocol.EncodeCreateTopicsResponse(&protocol.CreateTopicsResponse{
		CorrelationID: header.CorrelationID,
		Topics:        results,
	})
}

func (h *handler) handleDeleteTopics(ctx context.Context, header *protocol.RequestHeader, req *protocol.DeleteTopicsRequest) ([]byte, error) {
	results := make([]protocol.DeleteTopicResult, 0, len(req.TopicNames))
	for _, name := range req.TopicNames {
		result := protocol.DeleteTopicResult{Name: name}
		if err := h.store.DeleteTopic(ctx, name); err != nil {
			switch {
			case errors.Is(err, metadata.ErrUnknownTopic):
				result.ErrorCode = protocol.UNKNOWN_TOPIC_OR_PARTITION
			default:
				result.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			}
			result.ErrorMessage = err.Error()
		}
		results = append(results, result)
	}
	return protocol.EncodeDeleteTopicsResponse(&protocol.DeleteTopicsResponse{
		CorrelationID: header.CorrelationID,
		Topics:        results,
	})
}

const (
	configRetentionMs    = "retention.ms"
	configRetentionBytes = "retention.bytes"
	configSegmentBytes   = "segment.bytes"
	configBrokerID       = "broker.id"
	configAdvertised     = "advertised.listeners"
	configS3Bucket       = "kafscale.s3.bucket"
	configS3Region       = "kafscale.s3.region"
	configS3Endpoint     = "kafscale.s3.endpoint"
	configCacheBytes     = "kafscale.cache.bytes"
	configReadAhead      = "kafscale.readahead.segments"
	configSegmentBytesB  = "kafscale.segment.bytes"
	configFlushInterval  = "kafscale.flush.interval.ms"
)

func (h *handler) handleOffsetForLeaderEpoch(ctx context.Context, header *protocol.RequestHeader, req *protocol.OffsetForLeaderEpochRequest) ([]byte, error) {
	topics := make([]string, 0, len(req.Topics))
	for _, topic := range req.Topics {
		topics = append(topics, topic.Name)
	}
	meta, err := h.store.Metadata(ctx, topics)
	if err != nil {
		return nil, err
	}
	metaIndex := make(map[string]protocol.MetadataTopic, len(meta.Topics))
	for _, topic := range meta.Topics {
		metaIndex[topic.Name] = topic
	}
	respTopics := make([]protocol.OffsetForLeaderEpochTopicResponse, 0, len(req.Topics))
	for _, topic := range req.Topics {
		metaTopic, ok := metaIndex[topic.Name]
		partIndex := make(map[int32]protocol.MetadataPartition, len(metaTopic.Partitions))
		if ok {
			for _, part := range metaTopic.Partitions {
				partIndex[part.PartitionIndex] = part
			}
		}
		partitions := make([]protocol.OffsetForLeaderEpochPartitionResponse, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			if !ok {
				partitions = append(partitions, protocol.OffsetForLeaderEpochPartitionResponse{
					Partition:   part.Partition,
					ErrorCode:   protocol.UNKNOWN_TOPIC_OR_PARTITION,
					LeaderEpoch: -1,
					EndOffset:   -1,
				})
				continue
			}
			metaPart, exists := partIndex[part.Partition]
			if !exists {
				partitions = append(partitions, protocol.OffsetForLeaderEpochPartitionResponse{
					Partition:   part.Partition,
					ErrorCode:   protocol.UNKNOWN_TOPIC_OR_PARTITION,
					LeaderEpoch: -1,
					EndOffset:   -1,
				})
				continue
			}
			nextOffset, err := h.store.NextOffset(ctx, topic.Name, part.Partition)
			if err != nil {
				partitions = append(partitions, protocol.OffsetForLeaderEpochPartitionResponse{
					Partition:   part.Partition,
					ErrorCode:   protocol.UNKNOWN_SERVER_ERROR,
					LeaderEpoch: -1,
					EndOffset:   -1,
				})
				continue
			}
			partitions = append(partitions, protocol.OffsetForLeaderEpochPartitionResponse{
				Partition:   part.Partition,
				ErrorCode:   protocol.NONE,
				LeaderEpoch: metaPart.LeaderEpoch,
				EndOffset:   nextOffset,
			})
		}
		respTopics = append(respTopics, protocol.OffsetForLeaderEpochTopicResponse{
			Name:       topic.Name,
			Partitions: partitions,
		})
	}
	return protocol.EncodeOffsetForLeaderEpochResponse(&protocol.OffsetForLeaderEpochResponse{
		CorrelationID: header.CorrelationID,
		ThrottleMs:    0,
		Topics:        respTopics,
	}, header.APIVersion)
}

func (h *handler) handleDescribeConfigs(ctx context.Context, header *protocol.RequestHeader, req *protocol.DescribeConfigsRequest) ([]byte, error) {
	resources := make([]protocol.DescribeConfigsResponseResource, 0, len(req.Resources))
	for _, resource := range req.Resources {
		switch resource.ResourceType {
		case protocol.ConfigResourceTopic:
			cfg, err := h.store.FetchTopicConfig(ctx, resource.ResourceName)
			if err != nil {
				resources = append(resources, protocol.DescribeConfigsResponseResource{
					ErrorCode:    protocol.UNKNOWN_TOPIC_OR_PARTITION,
					ResourceType: resource.ResourceType,
					ResourceName: resource.ResourceName,
				})
				continue
			}
			configs := h.topicConfigEntries(cfg, resource.ConfigNames)
			resources = append(resources, protocol.DescribeConfigsResponseResource{
				ErrorCode:    protocol.NONE,
				ResourceType: resource.ResourceType,
				ResourceName: resource.ResourceName,
				Configs:      configs,
			})
		case protocol.ConfigResourceBroker:
			configs := h.brokerConfigEntries(resource.ConfigNames)
			resources = append(resources, protocol.DescribeConfigsResponseResource{
				ErrorCode:    protocol.NONE,
				ResourceType: resource.ResourceType,
				ResourceName: resource.ResourceName,
				Configs:      configs,
			})
		default:
			resources = append(resources, protocol.DescribeConfigsResponseResource{
				ErrorCode:    protocol.INVALID_REQUEST,
				ResourceType: resource.ResourceType,
				ResourceName: resource.ResourceName,
			})
		}
	}
	return protocol.EncodeDescribeConfigsResponse(&protocol.DescribeConfigsResponse{
		CorrelationID: header.CorrelationID,
		ThrottleMs:    0,
		Resources:     resources,
	}, header.APIVersion)
}

func (h *handler) handleAlterConfigs(ctx context.Context, header *protocol.RequestHeader, req *protocol.AlterConfigsRequest) ([]byte, error) {
	resources := make([]protocol.AlterConfigsResponseResource, 0, len(req.Resources))
	for _, resource := range req.Resources {
		if resource.ResourceType != protocol.ConfigResourceTopic || resource.ResourceName == "" {
			resources = append(resources, protocol.AlterConfigsResponseResource{
				ErrorCode:    protocol.INVALID_REQUEST,
				ResourceType: resource.ResourceType,
				ResourceName: resource.ResourceName,
			})
			continue
		}
		cfg, err := h.store.FetchTopicConfig(ctx, resource.ResourceName)
		if err != nil {
			resources = append(resources, protocol.AlterConfigsResponseResource{
				ErrorCode:    protocol.UNKNOWN_TOPIC_OR_PARTITION,
				ResourceType: resource.ResourceType,
				ResourceName: resource.ResourceName,
			})
			continue
		}
		updated := proto.Clone(cfg).(*metadatapb.TopicConfig)
		if updated.Config == nil {
			updated.Config = make(map[string]string)
		}
		errorCode := protocol.NONE
		for _, entry := range resource.Configs {
			if entry.Value == nil {
				errorCode = protocol.INVALID_CONFIG
				break
			}
			switch entry.Name {
			case configRetentionMs:
				value, err := parseConfigInt64(*entry.Value)
				if err != nil || (value < 0 && value != -1) {
					errorCode = protocol.INVALID_CONFIG
					break
				}
				updated.RetentionMs = value
			case configRetentionBytes:
				value, err := parseConfigInt64(*entry.Value)
				if err != nil || (value < 0 && value != -1) {
					errorCode = protocol.INVALID_CONFIG
					break
				}
				updated.RetentionBytes = value
			case configSegmentBytes:
				value, err := parseConfigInt64(*entry.Value)
				if err != nil || value <= 0 {
					errorCode = protocol.INVALID_CONFIG
					break
				}
				updated.SegmentBytes = value
			default:
				errorCode = protocol.INVALID_CONFIG
			}
			if errorCode != protocol.NONE {
				break
			}
		}
		if errorCode == protocol.NONE && !req.ValidateOnly {
			if err := h.store.UpdateTopicConfig(ctx, updated); err != nil {
				errorCode = protocol.UNKNOWN_SERVER_ERROR
			}
		}
		resources = append(resources, protocol.AlterConfigsResponseResource{
			ErrorCode:    errorCode,
			ResourceType: resource.ResourceType,
			ResourceName: resource.ResourceName,
		})
	}
	return protocol.EncodeAlterConfigsResponse(&protocol.AlterConfigsResponse{
		CorrelationID: header.CorrelationID,
		ThrottleMs:    0,
		Resources:     resources,
	}, header.APIVersion)
}

func (h *handler) handleCreatePartitions(ctx context.Context, header *protocol.RequestHeader, req *protocol.CreatePartitionsRequest) ([]byte, error) {
	results := make([]protocol.CreatePartitionsResponseTopic, 0, len(req.Topics))
	seen := make(map[string]struct{}, len(req.Topics))
	for _, topic := range req.Topics {
		result := protocol.CreatePartitionsResponseTopic{Name: topic.Name}
		if strings.TrimSpace(topic.Name) == "" {
			result.ErrorCode = protocol.INVALID_TOPIC_EXCEPTION
			msg := "invalid topic name"
			result.ErrorMessage = &msg
			results = append(results, result)
			continue
		}
		if _, ok := seen[topic.Name]; ok {
			result.ErrorCode = protocol.INVALID_REQUEST
			msg := "duplicate topic in request"
			result.ErrorMessage = &msg
			results = append(results, result)
			continue
		}
		seen[topic.Name] = struct{}{}
		if topic.Count <= 0 {
			result.ErrorCode = protocol.INVALID_PARTITIONS
			msg := "invalid partition count"
			result.ErrorMessage = &msg
			results = append(results, result)
			continue
		}
		if len(topic.Assignments) > 0 {
			result.ErrorCode = protocol.INVALID_REQUEST
			msg := "replica assignment not supported"
			result.ErrorMessage = &msg
			results = append(results, result)
			continue
		}
		var err error
		if req.ValidateOnly {
			err = h.validateCreatePartitions(ctx, topic.Name, topic.Count)
		} else {
			err = h.store.CreatePartitions(ctx, topic.Name, topic.Count)
		}
		if err != nil {
			switch {
			case errors.Is(err, metadata.ErrUnknownTopic):
				result.ErrorCode = protocol.UNKNOWN_TOPIC_OR_PARTITION
			case errors.Is(err, metadata.ErrInvalidTopic):
				result.ErrorCode = protocol.INVALID_PARTITIONS
			default:
				result.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			}
			msg := err.Error()
			result.ErrorMessage = &msg
		}
		results = append(results, result)
	}
	return protocol.EncodeCreatePartitionsResponse(&protocol.CreatePartitionsResponse{
		CorrelationID: header.CorrelationID,
		ThrottleMs:    0,
		Topics:        results,
	}, header.APIVersion)
}

func (h *handler) validateCreatePartitions(ctx context.Context, topic string, count int32) error {
	meta, err := h.store.Metadata(ctx, []string{topic})
	if err != nil {
		return err
	}
	if len(meta.Topics) == 0 || meta.Topics[0].ErrorCode != 0 {
		return metadata.ErrUnknownTopic
	}
	current := int32(len(meta.Topics[0].Partitions))
	if count <= current {
		return metadata.ErrInvalidTopic
	}
	return nil
}

func (h *handler) topicConfigEntries(cfg *metadatapb.TopicConfig, requested []string) []protocol.DescribeConfigsResponseConfig {
	allow := configNameSet(requested)
	entries := make([]protocol.DescribeConfigsResponseConfig, 0, 3)
	retentionMs, retentionMsDefault := normalizeRetention(cfg.RetentionMs)
	retentionBytes, retentionBytesDefault := normalizeRetention(cfg.RetentionBytes)
	segmentBytes, segmentDefault := normalizeSegmentBytes(cfg.SegmentBytes, int64(h.segmentBytes))

	entries = appendConfigEntry(entries, allow, configRetentionMs, retentionMs, retentionMsDefault, protocol.ConfigTypeLong, false)
	entries = appendConfigEntry(entries, allow, configRetentionBytes, retentionBytes, retentionBytesDefault, protocol.ConfigTypeLong, false)
	entries = appendConfigEntry(entries, allow, configSegmentBytes, segmentBytes, segmentDefault, protocol.ConfigTypeInt, false)
	return entries
}

func (h *handler) brokerConfigEntries(requested []string) []protocol.DescribeConfigsResponseConfig {
	allow := configNameSet(requested)
	entries := make([]protocol.DescribeConfigsResponseConfig, 0, 8)
	entries = appendConfigEntry(entries, allow, configBrokerID, fmt.Sprintf("%d", h.brokerInfo.NodeID), true, protocol.ConfigTypeInt, true)
	entries = appendConfigEntry(entries, allow, configAdvertised, fmt.Sprintf("%s:%d", h.brokerInfo.Host, h.brokerInfo.Port), true, protocol.ConfigTypeString, true)
	entries = appendConfigEntry(entries, allow, configS3Bucket, os.Getenv("KAFSCALE_S3_BUCKET"), true, protocol.ConfigTypeString, true)
	entries = appendConfigEntry(entries, allow, configS3Region, os.Getenv("KAFSCALE_S3_REGION"), true, protocol.ConfigTypeString, true)
	entries = appendConfigEntry(entries, allow, configS3Endpoint, os.Getenv("KAFSCALE_S3_ENDPOINT"), true, protocol.ConfigTypeString, true)
	entries = appendConfigEntry(entries, allow, configCacheBytes, fmt.Sprintf("%d", h.cacheSize), true, protocol.ConfigTypeLong, true)
	entries = appendConfigEntry(entries, allow, configReadAhead, fmt.Sprintf("%d", h.readAhead), true, protocol.ConfigTypeInt, true)
	entries = appendConfigEntry(entries, allow, configSegmentBytesB, fmt.Sprintf("%d", h.segmentBytes), true, protocol.ConfigTypeInt, true)
	entries = appendConfigEntry(entries, allow, configFlushInterval, fmt.Sprintf("%d", int64(h.flushInterval/time.Millisecond)), true, protocol.ConfigTypeLong, true)
	return entries
}

func configNameSet(names []string) map[string]struct{} {
	if names == nil {
		return nil
	}
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	return set
}

func appendConfigEntry(entries []protocol.DescribeConfigsResponseConfig, allow map[string]struct{}, name string, value string, isDefault bool, configType int8, readOnly bool) []protocol.DescribeConfigsResponseConfig {
	if allow != nil {
		if _, ok := allow[name]; !ok {
			return entries
		}
	}
	val := value
	entries = append(entries, protocol.DescribeConfigsResponseConfig{
		Name:        name,
		Value:       &val,
		ReadOnly:    readOnly,
		IsDefault:   isDefault,
		Source:      chooseConfigSource(isDefault, readOnly),
		IsSensitive: false,
		Synonyms:    nil,
		ConfigType:  configType,
	})
	return entries
}

func chooseConfigSource(isDefault bool, readOnly bool) int8 {
	if isDefault {
		return protocol.ConfigSourceDefaultConfig
	}
	if readOnly {
		return protocol.ConfigSourceStaticBroker
	}
	return protocol.ConfigSourceDynamicTopic
}

func normalizeRetention(value int64) (string, bool) {
	if value == 0 {
		value = -1
	}
	return fmt.Sprintf("%d", value), value == -1
}

func normalizeSegmentBytes(value int64, fallback int64) (string, bool) {
	if value <= 0 {
		return fmt.Sprintf("%d", fallback), true
	}
	return fmt.Sprintf("%d", value), false
}

func parseConfigInt64(value string) (int64, error) {
	trimmed := strings.TrimSpace(value)
	return strconv.ParseInt(trimmed, 10, 64)
}

func (h *handler) handleListOffsets(ctx context.Context, header *protocol.RequestHeader, req *protocol.ListOffsetsRequest) ([]byte, error) {
	topicResponses := make([]protocol.ListOffsetsTopicResponse, 0, len(req.Topics))
	for _, topic := range req.Topics {
		partitions := make([]protocol.ListOffsetsPartitionResponse, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			resp := protocol.ListOffsetsPartitionResponse{
				Partition:   part.Partition,
				LeaderEpoch: -1,
			}
			offset, err := func() (int64, error) {
				switch part.Timestamp {
				case -2:
					plog, err := h.getPartitionLog(ctx, topic.Name, part.Partition)
					if err != nil {
						return 0, err
					}
					return plog.EarliestOffset(), nil
				default:
					return h.store.NextOffset(ctx, topic.Name, part.Partition)
				}
			}()
			if err != nil {
				resp.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			} else {
				resp.Timestamp = part.Timestamp
				resp.Offset = offset
				if header.APIVersion == 0 {
					max := part.MaxNumOffsets
					if max <= 0 {
						max = 1
					}
					resp.OldStyleOffsets = make([]int64, 0, max)
					resp.OldStyleOffsets = append(resp.OldStyleOffsets, offset)
				}
			}
			partitions = append(partitions, resp)
		}
		topicResponses = append(topicResponses, protocol.ListOffsetsTopicResponse{
			Name:       topic.Name,
			Partitions: partitions,
		})
	}
	return protocol.EncodeListOffsetsResponse(header.APIVersion, &protocol.ListOffsetsResponse{
		CorrelationID: header.CorrelationID,
		Topics:        topicResponses,
	})
}

func (h *handler) handleFetch(ctx context.Context, header *protocol.RequestHeader, req *protocol.FetchRequest) ([]byte, error) {
	if header.APIVersion < 11 || header.APIVersion > 13 {
		return nil, fmt.Errorf("fetch version %d not supported", header.APIVersion)
	}
	topicResponses := make([]protocol.FetchTopicResponse, 0, len(req.Topics))
	maxWait := time.Duration(req.MaxWaitMs) * time.Millisecond
	if maxWait < 0 {
		maxWait = 0
	}
	var fetchedMessages int64
	zeroID := [16]byte{}
	idToName := map[[16]byte]string{}
	for _, topic := range req.Topics {
		if topic.TopicID != zeroID {
			meta, err := h.store.Metadata(ctx, nil)
			if err != nil {
				return nil, fmt.Errorf("load metadata: %w", err)
			}
			for _, t := range meta.Topics {
				idToName[t.TopicID] = t.Name
			}
			break
		}
	}

	for _, topic := range req.Topics {
		topicName := topic.Name
		if topicName == "" && topic.TopicID != zeroID {
			if resolved, ok := idToName[topic.TopicID]; ok {
				topicName = resolved
			} else {
				partitionResponses := make([]protocol.FetchPartitionResponse, 0, len(topic.Partitions))
				for _, part := range topic.Partitions {
					partitionResponses = append(partitionResponses, protocol.FetchPartitionResponse{
						Partition: part.Partition,
						ErrorCode: protocol.UNKNOWN_TOPIC_ID,
					})
				}
				topicResponses = append(topicResponses, protocol.FetchTopicResponse{
					Name:       topicName,
					TopicID:    topic.TopicID,
					Partitions: partitionResponses,
				})
				continue
			}
		}
		partitionResponses := make([]protocol.FetchPartitionResponse, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			if h.traceKafka {
				h.logger.Debug("fetch partition request", "topic", topicName, "partition", part.Partition, "fetch_offset", part.FetchOffset, "max_bytes", part.MaxBytes)
			}
			switch h.s3Health.State() {
			case broker.S3StateDegraded, broker.S3StateUnavailable:
				partitionResponses = append(partitionResponses, protocol.FetchPartitionResponse{
					Partition: part.Partition,
					ErrorCode: h.backpressureErrorCode(),
				})
				continue
			}
			plog, err := h.getPartitionLog(ctx, topicName, part.Partition)
			if err != nil {
				partitionResponses = append(partitionResponses, protocol.FetchPartitionResponse{
					Partition: part.Partition,
					ErrorCode: protocol.UNKNOWN_SERVER_ERROR,
				})
				continue
			}
			nextOffset, offsetErr := h.waitForFetchData(ctx, topicName, part.Partition, part.FetchOffset, maxWait)
			if offsetErr != nil {
				if errors.Is(offsetErr, context.Canceled) || errors.Is(offsetErr, context.DeadlineExceeded) {
					partitionResponses = append(partitionResponses, protocol.FetchPartitionResponse{
						Partition: part.Partition,
						ErrorCode: protocol.UNKNOWN_SERVER_ERROR,
					})
					continue
				}
				nextOffset = 0
			}
			errorCode := int16(0)
			var recordSet []byte
			switch {
			case part.FetchOffset > nextOffset:
				errorCode = protocol.OFFSET_OUT_OF_RANGE
			case part.FetchOffset == nextOffset:
				// At the high watermark; Kafka returns an empty set rather than an error.
				recordSet = nil
			default:
				recordSet, err = plog.Read(ctx, part.FetchOffset, part.MaxBytes)
				if err != nil {
					if errors.Is(err, storage.ErrOffsetOutOfRange) {
						errorCode = protocol.OFFSET_OUT_OF_RANGE
					} else {
						errorCode = h.backpressureErrorCode()
						if h.traceKafka {
							h.logger.Debug("fetch read error", "topic", topicName, "partition", part.Partition, "error", err)
						}
					}
				}
			}

			highWatermark := nextOffset
			if errorCode == 0 {
				if h.traceKafka {
					h.logger.Debug("fetch partition response", "topic", topicName, "partition", part.Partition, "records_bytes", len(recordSet), "high_watermark", highWatermark)
				}
				if len(recordSet) > 0 {
					fetchedMessages += int64(storage.CountRecordBatchMessages(recordSet))
				}
			} else if h.traceKafka {
				h.logger.Debug("fetch partition error", "topic", topicName, "partition", part.Partition, "error_code", errorCode)
			}
			partitionResponses = append(partitionResponses, protocol.FetchPartitionResponse{
				Partition:            part.Partition,
				ErrorCode:            errorCode,
				HighWatermark:        highWatermark,
				LastStableOffset:     highWatermark,
				LogStartOffset:       0,
				PreferredReadReplica: -1,
				RecordSet:            recordSet,
			})
		}
		topicResponses = append(topicResponses, protocol.FetchTopicResponse{
			Name:       topicName,
			TopicID:    topic.TopicID,
			Partitions: partitionResponses,
		})
	}

	if fetchedMessages > 0 {
		h.fetchRate.add(fetchedMessages)
	}

	return protocol.EncodeFetchResponse(&protocol.FetchResponse{
		CorrelationID: header.CorrelationID,
		Topics:        topicResponses,
		ThrottleMs:    0,
		ErrorCode:     0,
		SessionID:     0,
	}, header.APIVersion)
}

const fetchPollInterval = 10 * time.Millisecond

func (h *handler) waitForFetchData(ctx context.Context, topic string, partition int32, fetchOffset int64, maxWait time.Duration) (int64, error) {
	nextOffset, err := h.store.NextOffset(ctx, topic, partition)
	if err != nil || maxWait == 0 || fetchOffset < nextOffset {
		return nextOffset, err
	}

	deadline := time.Now().Add(maxWait)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nextOffset, err
		}
		sleep := fetchPollInterval
		if remaining < sleep {
			sleep = remaining
		}
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nextOffset, ctx.Err()
		case <-timer.C:
		}

		nextOffset, err = h.store.NextOffset(ctx, topic, partition)
		if err != nil || fetchOffset < nextOffset {
			return nextOffset, err
		}
	}
}

func (h *handler) ensureTopic(ctx context.Context, topic string, partition int32) error {
	desired := int32(h.autoCreatePartitions)
	if desired < partition+1 {
		desired = partition + 1
	}
	spec := metadata.TopicSpec{
		Name:              topic,
		NumPartitions:     desired,
		ReplicationFactor: 1,
	}
	_, err := h.store.CreateTopic(ctx, spec)
	if err != nil {
		if errors.Is(err, metadata.ErrTopicExists) {
			return nil
		}
		return err
	}
	h.logger.Info("auto-created topic", "topic", topic, "partitions", desired)
	return nil
}

func (h *handler) getPartitionLog(ctx context.Context, topic string, partition int32) (*storage.PartitionLog, error) {
	for {
		h.logMu.Lock()
		partitions := h.logs[topic]
		if partitions == nil {
			partitions = make(map[int32]*storage.PartitionLog)
			h.logs[topic] = partitions
		}
		if log, ok := partitions[partition]; ok {
			h.logMu.Unlock()
			return log, nil
		}
		nextOffset, err := h.store.NextOffset(ctx, topic, partition)
		if err != nil {
			h.logMu.Unlock()
			if errors.Is(err, metadata.ErrUnknownTopic) && h.autoCreateTopics {
				if err := h.ensureTopic(ctx, topic, partition); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		plog := storage.NewPartitionLog(h.s3Namespace, topic, partition, nextOffset, h.s3, h.cache, h.logConfig, func(cbCtx context.Context, artifact *storage.SegmentArtifact) {
			if err := h.store.UpdateOffsets(cbCtx, topic, partition, artifact.LastOffset); err != nil {
				h.logger.Error("update offsets failed", "error", err, "topic", topic, "partition", partition)
			}
		}, h.recordS3Op)
		lastOffset, err := plog.RestoreFromS3(ctx)
		if err != nil {
			h.logMu.Unlock()
			return nil, err
		}
		if lastOffset >= nextOffset {
			if err := h.store.UpdateOffsets(ctx, topic, partition, lastOffset); err != nil {
				h.logger.Error("sync offsets from S3 failed", "error", err, "topic", topic, "partition", partition)
			}
		}
		partitions[partition] = plog
		h.logMu.Unlock()
		return plog, nil
	}
}

func newHandler(store metadata.Store, s3Client storage.S3Client, brokerInfo protocol.MetadataBroker, logger *slog.Logger) *handler {
	readAhead := parseEnvInt("KAFSCALE_READAHEAD_SEGMENTS", 2)
	cacheSize := parseEnvInt("KAFSCALE_CACHE_BYTES", 0)
	if cacheSize <= 0 {
		cacheSize = parseEnvInt("KAFSCALE_CACHE_SIZE", 0)
	}
	if cacheSize <= 0 {
		cacheSize = 32 << 20
	}
	autoCreate := parseEnvBool("KAFSCALE_AUTO_CREATE_TOPICS", true)
	autoPartitions := parseEnvInt32("KAFSCALE_AUTO_CREATE_PARTITIONS", 1)
	traceKafka := parseEnvBool("KAFSCALE_TRACE_KAFKA", false)
	throughputWindow := time.Duration(parseEnvInt("KAFSCALE_THROUGHPUT_WINDOW_SEC", 60)) * time.Second
	s3Namespace := envOrDefault("KAFSCALE_S3_NAMESPACE", "default")
	segmentBytes := parseEnvInt("KAFSCALE_SEGMENT_BYTES", 4<<20)
	flushInterval := time.Duration(parseEnvInt("KAFSCALE_FLUSH_INTERVAL_MS", 500)) * time.Millisecond
	if autoPartitions < 1 {
		autoPartitions = 1
	}
	health := broker.NewS3HealthMonitor(broker.S3HealthConfig{
		Window:      time.Duration(parseEnvInt("KAFSCALE_S3_HEALTH_WINDOW_SEC", 60)) * time.Second,
		LatencyWarn: time.Duration(parseEnvInt("KAFSCALE_S3_LATENCY_WARN_MS", 500)) * time.Millisecond,
		LatencyCrit: time.Duration(parseEnvInt("KAFSCALE_S3_LATENCY_CRIT_MS", 3000)) * time.Millisecond,
		ErrorWarn:   parseEnvFloat("KAFSCALE_S3_ERROR_RATE_WARN", 0.2),
		ErrorCrit:   parseEnvFloat("KAFSCALE_S3_ERROR_RATE_CRIT", 0.6),
	})
	return &handler{
		apiVersions: generateApiVersions(),
		store:       store,
		s3:          s3Client,
		cache:       cache.NewSegmentCache(cacheSize),
		logs:        make(map[string]map[int32]*storage.PartitionLog),
		logConfig: storage.PartitionLogConfig{
			Buffer: storage.WriteBufferConfig{
				MaxBytes:      segmentBytes,
				FlushInterval: flushInterval,
			},
			Segment: storage.SegmentWriterConfig{
				IndexIntervalMessages: 100,
			},
			ReadAheadSegments: readAhead,
			CacheEnabled:      true,
		},
		coordinator:          broker.NewGroupCoordinator(store, brokerInfo, nil),
		s3Health:             health,
		s3Namespace:          s3Namespace,
		brokerInfo:           brokerInfo,
		logger:               logger.With("component", "handler"),
		autoCreateTopics:     autoCreate,
		autoCreatePartitions: autoPartitions,
		traceKafka:           traceKafka,
		produceRate:          newThroughputTracker(throughputWindow),
		fetchRate:            newThroughputTracker(throughputWindow),
		cacheSize:            cacheSize,
		readAhead:            readAhead,
		segmentBytes:         segmentBytes,
		flushInterval:        flushInterval,
		adminMetrics:         newAdminMetrics(),
	}
}

func (h *handler) runStartupChecks(parent context.Context) error {
	timeout := time.Duration(parseEnvInt("KAFSCALE_STARTUP_TIMEOUT_SEC", 30)) * time.Second
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	h.logger.Info("running startup checks", "timeout", timeout)
	if err := h.verifyMetadata(ctx); err != nil {
		return err
	}
	if err := h.verifyS3(ctx); err != nil {
		return err
	}
	h.logger.Info("startup checks passed")
	return nil
}

func (h *handler) verifyMetadata(ctx context.Context) error {
	if _, err := h.store.Metadata(ctx, nil); err != nil {
		return fmt.Errorf("metadata readiness check failed: %w", err)
	}
	return nil
}

func (h *handler) verifyS3(ctx context.Context) error {
	payload := []byte("kafscale-startup-probe")
	probeKey := fmt.Sprintf("__health/startup_probe_%d", time.Now().UnixNano())
	backoff := 500 * time.Millisecond

	for {
		start := time.Now()
		err := h.s3.UploadSegment(ctx, probeKey, payload)
		h.recordS3Op("startup_probe", time.Since(start), err)
		if err == nil {
			return nil
		}
		h.logger.Warn("startup S3 probe failed, retrying", "error", err, "key", probeKey)
		select {
		case <-ctx.Done():
			return fmt.Errorf("s3 readiness check failed: %w", err)
		case <-time.After(backoff):
		}
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := newLogger()
	brokerInfo := buildBrokerInfo()
	store := buildStore(ctx, brokerInfo, logger)
	s3Client := buildS3Client(ctx, logger)
	handler := newHandler(store, s3Client, brokerInfo, logger)
	if err := handler.runStartupChecks(ctx); err != nil {
		logger.Error("startup checks failed", "error", err)
		os.Exit(1)
	}
	metricsAddr := envOrDefault("KAFSCALE_METRICS_ADDR", defaultMetricsAddr)
	controlAddr := envOrDefault("KAFSCALE_CONTROL_ADDR", defaultControlAddr)
	startMetricsServer(ctx, metricsAddr, handler, logger)
	startControlServer(ctx, controlAddr, handler, logger)
	kafkaAddr := envOrDefault("KAFSCALE_BROKER_ADDR", defaultKafkaAddr)
	srv := &broker.Server{
		Addr:    kafkaAddr,
		Handler: handler,
	}
	if err := srv.ListenAndServe(ctx); err != nil {
		logger.Error("broker server error", "error", err)
		os.Exit(1)
	}
	srv.Wait()
}

func buildS3Client(ctx context.Context, logger *slog.Logger) storage.S3Client {
	if parseEnvBool("KAFSCALE_USE_MEMORY_S3", false) {
		logger.Info("using in-memory S3 client", "env", "KAFSCALE_USE_MEMORY_S3=1")
		return storage.NewMemoryS3Client()
	}

	bucket := envOrDefault("KAFSCALE_S3_BUCKET", defaultMinioBucket)
	region := envOrDefault("KAFSCALE_S3_REGION", defaultMinioRegion)
	endpoint := envOrDefault("KAFSCALE_S3_ENDPOINT", defaultMinioEndpoint)
	forcePathStyle := parseEnvBool("KAFSCALE_S3_PATH_STYLE", true)
	kmsARN := os.Getenv("KAFSCALE_S3_KMS_ARN")
	usingDefaultMinio := bucket == defaultMinioBucket && region == defaultMinioRegion && endpoint == defaultMinioEndpoint
	accessKey := os.Getenv("KAFSCALE_S3_ACCESS_KEY")
	secretKey := os.Getenv("KAFSCALE_S3_SECRET_KEY")
	sessionToken := os.Getenv("KAFSCALE_S3_SESSION_TOKEN")
	if accessKey == "" && secretKey == "" && usingDefaultMinio {
		accessKey = defaultMinioAccessKey
		secretKey = defaultMinioSecretKey
	}
	credsProvided := accessKey != "" && secretKey != ""

	client, err := storage.NewS3Client(ctx, storage.S3Config{
		Bucket:          bucket,
		Region:          region,
		Endpoint:        endpoint,
		ForcePathStyle:  forcePathStyle,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
		KMSKeyARN:       kmsARN,
	})
	if err != nil {
		logger.Error("failed to create S3 client; using in-memory", "error", err, "bucket", bucket, "region", region, "endpoint", endpoint)
		return storage.NewMemoryS3Client()
	}

	if err := client.EnsureBucket(ctx); err != nil {
		logger.Error("failed to ensure S3 bucket", "bucket", bucket, "error", err)
		os.Exit(1)
	}

	logger.Info("using AWS-compatible S3 client", "bucket", bucket, "region", region, "endpoint", endpoint, "force_path_style", forcePathStyle, "kms_configured", kmsARN != "", "default_minio", usingDefaultMinio, "credentials_provided", credsProvided)

	readBucket := os.Getenv("KAFSCALE_S3_READ_BUCKET")
	readRegion := os.Getenv("KAFSCALE_S3_READ_REGION")
	readEndpoint := os.Getenv("KAFSCALE_S3_READ_ENDPOINT")
	if readBucket != "" || readRegion != "" || readEndpoint != "" {
		if readBucket == "" {
			readBucket = bucket
		}
		if readRegion == "" {
			readRegion = region
		}
		if readEndpoint == "" {
			readEndpoint = endpoint
		}
		readClient, err := storage.NewS3Client(ctx, storage.S3Config{
			Bucket:          readBucket,
			Region:          readRegion,
			Endpoint:        readEndpoint,
			ForcePathStyle:  forcePathStyle,
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
			SessionToken:    sessionToken,
			KMSKeyARN:       kmsARN,
		})
		if err != nil {
			logger.Error("failed to create read S3 client; using write client", "error", err, "bucket", readBucket, "region", readRegion, "endpoint", readEndpoint)
			return client
		}
		logger.Info("using S3 read replica", "bucket", readBucket, "region", readRegion, "endpoint", readEndpoint)
		return newDualS3Client(client, readClient)
	}

	return client
}

func metadataForBroker(broker protocol.MetadataBroker) metadata.ClusterMetadata {
	clusterID := "kafscale-cluster"
	return metadata.ClusterMetadata{
		ControllerID: broker.NodeID,
		ClusterID:    &clusterID,
		Brokers: []protocol.MetadataBroker{
			broker,
		},
		Topics: []protocol.MetadataTopic{
			{
				ErrorCode:  0,
				Name:       "orders",
				TopicID:    metadata.TopicIDForName("orders"),
				IsInternal: false,
				Partitions: []protocol.MetadataPartition{
					{
						ErrorCode:      0,
						PartitionIndex: 0,
						LeaderID:       broker.NodeID,
						ReplicaNodes:   []int32{broker.NodeID},
						ISRNodes:       []int32{broker.NodeID},
					},
				},
			},
		},
	}
}

func defaultMetadata() metadata.ClusterMetadata {
	return metadataForBroker(buildBrokerInfo())
}

func buildStore(ctx context.Context, brokerInfo protocol.MetadataBroker, logger *slog.Logger) metadata.Store {
	meta := metadataForBroker(brokerInfo)
	endpoints := strings.TrimSpace(os.Getenv("KAFSCALE_ETCD_ENDPOINTS"))
	if endpoints == "" {
		return metadata.NewInMemoryStore(meta)
	}
	cfg := metadata.EtcdStoreConfig{
		Endpoints: strings.Split(endpoints, ","),
		Username:  os.Getenv("KAFSCALE_ETCD_USERNAME"),
		Password:  os.Getenv("KAFSCALE_ETCD_PASSWORD"),
	}
	store, err := metadata.NewEtcdStore(ctx, meta, cfg)
	if err != nil {
		logger.Error("failed to initialize etcd store; using in-memory", "error", err)
		return metadata.NewInMemoryStore(meta)
	}
	logger.Info("using etcd-backed metadata store", "endpoints", cfg.Endpoints)
	return store
}

func startMetricsServer(ctx context.Context, addr string, h *handler, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", h.metricsHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "ok state=%s\n", h.s3Health.State())
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if ready, state := h.readiness(); !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "not ready state=%s\n", state)
		} else {
			fmt.Fprintf(w, "ready state=%s\n", state)
		}
	})
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server error", "error", err)
		}
	}()
}

func startControlServer(ctx context.Context, addr string, h *handler, logger *slog.Logger) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("control server listen error", "error", err)
		return
	}
	server := grpc.NewServer()
	controlpb.RegisterBrokerControlServer(server, &controlServer{handler: h})
	go func() {
		<-ctx.Done()
		done := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			server.Stop()
		}
	}()
	go func() {
		if err := server.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("control server error", "error", err)
		}
	}()
}

type controlServer struct {
	controlpb.UnimplementedBrokerControlServer
	handler *handler
}

func (s *controlServer) GetStatus(ctx context.Context, req *controlpb.BrokerStatusRequest) (*controlpb.BrokerStatusResponse, error) {
	snap := s.handler.s3Health.Snapshot()
	state := string(snap.State)
	status := &controlpb.PartitionStatus{
		Topic:          "__s3_health",
		Partition:      0,
		Leader:         true,
		State:          state,
		LogStartOffset: int64(snap.AvgLatency / time.Millisecond),
		LogEndOffset:   int64(snap.AvgLatency / time.Millisecond),
		HighWatermark:  int64(snap.ErrorRate * 1000),
	}
	ready := snap.State == broker.S3StateHealthy
	return &controlpb.BrokerStatusResponse{
		BrokerId: fmt.Sprintf("%d", s.handler.brokerInfo.NodeID),
		Version:  brokerVersion,
		Ready:    ready,
		Partitions: []*controlpb.PartitionStatus{
			status,
		},
	}, nil
}

func (s *controlServer) DrainPartitions(ctx context.Context, req *controlpb.DrainPartitionsRequest) (*controlpb.DrainPartitionsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "DrainPartitions not implemented")
}

func (s *controlServer) TriggerFlush(ctx context.Context, req *controlpb.TriggerFlushRequest) (*controlpb.TriggerFlushResponse, error) {
	return nil, status.Error(codes.Unimplemented, "TriggerFlush not implemented")
}

func (s *controlServer) StreamMetrics(stream grpc.ClientStreamingServer[controlpb.MetricsSample, controlpb.Ack]) error {
	return status.Error(codes.Unimplemented, "StreamMetrics not implemented")
}

func (h *handler) readiness() (bool, string) {
	snap := h.s3Health.Snapshot()
	return snap.State != broker.S3StateUnavailable, string(snap.State)
}

func newLogger() *slog.Logger {
	level := slog.LevelWarn
	switch strings.ToLower(os.Getenv("KAFSCALE_LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	})
	return slog.New(handler).With("component", "broker")
}
func parseEnvInt(name string, fallback int) int {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			return parsed
		}
	}
	return fallback
}

func parseEnvInt32(name string, fallback int32) int32 {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		if parsed, err := strconv.ParseInt(val, 10, 32); err == nil {
			return int32(parsed)
		}
	}
	return fallback
}

func intToInt32(value int, fallback int32) int32 {
	const minInt32 = -1 << 31
	const maxInt32 = 1<<31 - 1
	if value < minInt32 || value > maxInt32 {
		return fallback
	}
	return int32(value)
}

func envOrDefault(name, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		return val
	}
	return fallback
}

func parseEnvFloat(name string, fallback float64) float64 {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func parseEnvBool(name string, fallback bool) bool {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		switch strings.ToLower(val) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return fallback
}

type apiVersionSupport struct {
	key                    int16
	minVersion, maxVersion int16
}

func generateApiVersions() []protocol.ApiVersion {
	supported := []apiVersionSupport{
		{key: protocol.APIKeyApiVersion, minVersion: 0, maxVersion: 0},
		{key: protocol.APIKeyMetadata, minVersion: 0, maxVersion: 12},
		{key: protocol.APIKeyProduce, minVersion: 0, maxVersion: 9},
		{key: protocol.APIKeyFetch, minVersion: 11, maxVersion: 13},
		{key: protocol.APIKeyFindCoordinator, minVersion: 3, maxVersion: 3},
		{key: protocol.APIKeyListOffsets, minVersion: 0, maxVersion: 0},
		{key: protocol.APIKeyJoinGroup, minVersion: 4, maxVersion: 4},
		{key: protocol.APIKeySyncGroup, minVersion: 4, maxVersion: 4},
		{key: protocol.APIKeyHeartbeat, minVersion: 4, maxVersion: 4},
		{key: protocol.APIKeyLeaveGroup, minVersion: 4, maxVersion: 4},
		{key: protocol.APIKeyOffsetCommit, minVersion: 3, maxVersion: 3},
		{key: protocol.APIKeyOffsetFetch, minVersion: 5, maxVersion: 5},
		{key: protocol.APIKeyDescribeGroups, minVersion: 5, maxVersion: 5},
		{key: protocol.APIKeyListGroups, minVersion: 5, maxVersion: 5},
		{key: protocol.APIKeyOffsetForLeaderEpoch, minVersion: 3, maxVersion: 3},
		{key: protocol.APIKeyDescribeConfigs, minVersion: 4, maxVersion: 4},
		{key: protocol.APIKeyAlterConfigs, minVersion: 1, maxVersion: 1},
		{key: protocol.APIKeyCreatePartitions, minVersion: 0, maxVersion: 3},
		{key: protocol.APIKeyCreateTopics, minVersion: 0, maxVersion: 0},
		{key: protocol.APIKeyDeleteTopics, minVersion: 0, maxVersion: 0},
		{key: protocol.APIKeyDeleteGroups, minVersion: 0, maxVersion: 2},
	}
	unsupported := []int16{
		4, 5, 6, 7,
		21, 22,
		24, 25, 26,
	}

	entries := make([]protocol.ApiVersion, 0, len(supported)+len(unsupported))
	for _, entry := range supported {
		entries = append(entries, protocol.ApiVersion{
			APIKey:     entry.key,
			MinVersion: entry.minVersion,
			MaxVersion: entry.maxVersion,
		})
	}
	for _, key := range unsupported {
		entries = append(entries, protocol.ApiVersion{
			APIKey:     key,
			MinVersion: -1,
			MaxVersion: -1,
		})
	}
	return entries
}

func buildBrokerInfo() protocol.MetadataBroker {
	id := parseEnvInt32("KAFSCALE_BROKER_ID", 1)
	host := os.Getenv("KAFSCALE_BROKER_HOST")
	port := parseEnvInt("KAFSCALE_BROKER_PORT", defaultKafkaPort)
	if addr := strings.TrimSpace(os.Getenv("KAFSCALE_BROKER_ADDR")); addr != "" {
		parsedHost, parsedPort := parseBrokerAddr(addr)
		if parsedHost != "" {
			host = parsedHost
		}
		port = parsedPort
	}
	if host == "" {
		host = "localhost"
	}
	return protocol.MetadataBroker{
		NodeID: id,
		Host:   host,
		Port:   intToInt32(port, int32(defaultKafkaPort)),
	}
}

func parseBrokerAddr(addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return strings.TrimSpace(addr), defaultKafkaPort
	}
	port := defaultKafkaPort
	if parsedPort, err := strconv.Atoi(portStr); err == nil {
		port = parsedPort
	}
	return host, port
}
