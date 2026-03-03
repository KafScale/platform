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
	"encoding/json"
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

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/KafScale/platform/pkg/acl"
	"github.com/KafScale/platform/pkg/broker"
	"github.com/KafScale/platform/pkg/cache"
	controlpb "github.com/KafScale/platform/pkg/gen/control"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	defaultKafkaAddr     = ":19092"
	defaultKafkaPort     = 19092
	defaultMetricsAddr   = ":19093"
	defaultControlAddr   = ":19094"
	defaultS3Concurrency = 64
	brokerVersion        = "dev"
)

type handler struct {
	apiVersions          []kmsg.ApiVersionsResponseApiKey
	store                metadata.Store
	s3                   storage.S3Client
	cache                *cache.SegmentCache
	logs                 map[string]map[int32]*storage.PartitionLog
	logMu                sync.RWMutex
	logInit              singleflight.Group
	logConfig            storage.PartitionLogConfig
	coordinator          *broker.GroupCoordinator
	leaseManager         *metadata.PartitionLeaseManager
	groupLeaseManager    *metadata.GroupLeaseManager
	s3Health             *broker.S3HealthMonitor
	s3Namespace          string
	brokerInfo           protocol.MetadataBroker
	logger               *slog.Logger
	autoCreateTopics     bool
	autoCreatePartitions int32
	allowAdminAPIs       bool
	traceKafka           bool
	produceRate          *throughputTracker
	fetchRate            *throughputTracker
	produceLatency       *histogram
	consumerLag          *lagMetrics
	startTime            time.Time
	cpuTracker           *cpuTracker
	cacheSize            int
	readAhead            int
	segmentBytes         int
	flushInterval        time.Duration
	flushOnAck           bool
	adminMetrics         *adminMetrics
	authorizer           *acl.Authorizer
	authMetrics          *authMetrics
	authLogMu            sync.Mutex
	authLogLast          map[string]time.Time
	s3sem                *semaphore.Weighted
}

type etcdAvailability interface {
	Available() bool
}

func (h *handler) Handle(ctx context.Context, header *protocol.RequestHeader, req kmsg.Request) ([]byte, error) {
	if h.traceKafka {
		h.logger.Debug("received request", "api_key", header.APIKey, "api_version", header.APIVersion, "correlation", header.CorrelationID, "client_id", header.ClientID)
	}
	principal := principalFromContext(ctx, header)
	switch req.(type) {
	case *kmsg.ApiVersionsRequest:
		errorCode := int16(protocol.NONE)
		responseVersion := header.APIVersion
		if responseVersion > 4 {
			errorCode = int16(protocol.UNSUPPORTED_VERSION)
			responseVersion = 0
		}
		resp := kmsg.NewPtrApiVersionsResponse()
		resp.ErrorCode = errorCode
		resp.ApiKeys = h.apiVersions
		if h.traceKafka {
			h.logger.Debug("api versions response", "versions", resp.ApiKeys)
		}
		return protocol.EncodeResponse(header.CorrelationID, responseVersion, resp), nil
	case *kmsg.MetadataRequest:
		metaReq := req.(*kmsg.MetadataRequest)
		if h.traceKafka {
			h.logger.Debug("metadata request", "topics", len(metaReq.Topics))
		}
		// Extract topic names from request for auto-create and store lookup.
		var topicNames []string
		for _, t := range metaReq.Topics {
			if t.Topic != nil {
				topicNames = append(topicNames, *t.Topic)
			}
		}
		if h.autoCreateTopics && len(topicNames) > 0 {
			for _, name := range topicNames {
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
			for _, t := range metaReq.Topics {
				if t.TopicID != zeroID {
					useIDs = true
					break
				}
			}
			if !useIDs {
				return h.store.Metadata(ctx, topicNames)
			}
			all, err := h.store.Metadata(ctx, nil)
			if err != nil {
				return nil, err
			}
			index := make(map[[16]byte]protocol.MetadataTopic, len(all.Topics))
			for _, topic := range all.Topics {
				index[topic.TopicID] = topic
			}
			filtered := make([]protocol.MetadataTopic, 0, len(metaReq.Topics))
			for _, t := range metaReq.Topics {
				if t.TopicID == zeroID {
					continue
				}
				if topic, ok := index[t.TopicID]; ok {
					filtered = append(filtered, topic)
				} else {
					filtered = append(filtered, protocol.MetadataTopic{
						ErrorCode: protocol.UNKNOWN_TOPIC_ID,
						TopicID:   t.TopicID,
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
		resp := kmsg.NewPtrMetadataResponse()
		resp.Brokers = meta.Brokers
		resp.ClusterID = meta.ClusterID
		resp.ControllerID = meta.ControllerID
		resp.Topics = meta.Topics
		if h.traceKafka {
			topicSummaries := make([]string, 0, len(meta.Topics))
			for _, topic := range meta.Topics {
				topicSummaries = append(topicSummaries, fmt.Sprintf("%s(error=%d partitions=%d)", *topic.Topic, topic.ErrorCode, len(topic.Partitions)))
			}
			brokerAddrs := make([]string, 0, len(meta.Brokers))
			for _, b := range meta.Brokers {
				brokerAddrs = append(brokerAddrs, fmt.Sprintf("%s:%d", b.Host, b.Port))
			}
			h.logger.Debug("metadata response", "topics", topicSummaries, "brokers", brokerAddrs)
		}
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	case *kmsg.ProduceRequest:
		return h.handleProduce(ctx, header, req.(*kmsg.ProduceRequest))
	case *kmsg.FetchRequest:
		return h.handleFetch(ctx, header, req.(*kmsg.FetchRequest))
	case *kmsg.FindCoordinatorRequest:
		coord := h.coordinatorBroker(ctx)
		resp := kmsg.NewPtrFindCoordinatorResponse()
		resp.ErrorCode = protocol.NONE
		resp.NodeID = coord.NodeID
		resp.Host = coord.Host
		resp.Port = coord.Port
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	case *kmsg.JoinGroupRequest:
		req := req.(*kmsg.JoinGroupRequest)
		if !h.allowGroup(principal, req.Group, acl.ActionGroupWrite) {
			h.recordAuthzDeniedWithPrincipal(principal, acl.ActionGroupWrite, acl.ResourceGroup, req.Group)
			resp := kmsg.NewPtrJoinGroupResponse()
			resp.ErrorCode = protocol.GROUP_AUTHORIZATION_FAILED
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		if errCode := h.acquireGroupLease(ctx, req.Group); errCode != 0 {
			resp := kmsg.NewPtrJoinGroupResponse()
			resp.ErrorCode = errCode
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		if !h.etcdAvailable() {
			resp := kmsg.NewPtrJoinGroupResponse()
			resp.ErrorCode = protocol.REQUEST_TIMED_OUT
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		resp, err := h.coordinator.JoinGroup(ctx, req)
		if err != nil {
			return nil, err
		}
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	case *kmsg.SyncGroupRequest:
		req := req.(*kmsg.SyncGroupRequest)
		if !h.allowGroup(principal, req.Group, acl.ActionGroupWrite) {
			h.recordAuthzDeniedWithPrincipal(principal, acl.ActionGroupWrite, acl.ResourceGroup, req.Group)
			resp := kmsg.NewPtrSyncGroupResponse()
			resp.ErrorCode = protocol.GROUP_AUTHORIZATION_FAILED
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		if errCode := h.acquireGroupLease(ctx, req.Group); errCode != 0 {
			resp := kmsg.NewPtrSyncGroupResponse()
			resp.ErrorCode = errCode
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		if !h.etcdAvailable() {
			resp := kmsg.NewPtrSyncGroupResponse()
			resp.ErrorCode = protocol.REQUEST_TIMED_OUT
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		resp, err := h.coordinator.SyncGroup(ctx, req)
		if err != nil {
			return nil, err
		}
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	case *kmsg.DescribeGroupsRequest:
		req := req.(*kmsg.DescribeGroupsRequest)
		return h.withAdminMetrics(header.APIKey, func() ([]byte, error) {
			allowed := make([]string, 0, len(req.Groups))
			denied := make(map[string]struct{})
			leaseErrors := make(map[string]int16)
			for _, groupID := range req.Groups {
				if !h.allowGroup(principal, groupID, acl.ActionGroupRead) {
					denied[groupID] = struct{}{}
					h.recordAuthzDeniedWithPrincipal(principal, acl.ActionGroupRead, acl.ResourceGroup, groupID)
					continue
				}
				if errCode := h.acquireGroupLease(ctx, groupID); errCode != 0 {
					leaseErrors[groupID] = errCode
					continue
				}
				allowed = append(allowed, groupID)
			}

			responseByGroup := make(map[string]kmsg.DescribeGroupsResponseGroup, len(req.Groups))
			if len(allowed) > 0 {
				if !h.etcdAvailable() {
					for _, groupID := range allowed {
						g := kmsg.NewDescribeGroupsResponseGroup()
						g.ErrorCode = protocol.REQUEST_TIMED_OUT
						g.Group = groupID
						responseByGroup[groupID] = g
					}
				} else {
					allowedReq := *req
					allowedReq.Groups = allowed
					resp, err := h.coordinator.DescribeGroups(ctx, &allowedReq)
					if err != nil {
						return nil, err
					}
					for _, group := range resp.Groups {
						responseByGroup[group.Group] = group
					}
				}
			}

			results := make([]kmsg.DescribeGroupsResponseGroup, 0, len(req.Groups))
			for _, groupID := range req.Groups {
				if _, ok := denied[groupID]; ok {
					g := kmsg.NewDescribeGroupsResponseGroup()
					g.ErrorCode = protocol.GROUP_AUTHORIZATION_FAILED
					g.Group = groupID
					results = append(results, g)
					continue
				}
				if errCode, ok := leaseErrors[groupID]; ok {
					g := kmsg.NewDescribeGroupsResponseGroup()
					g.ErrorCode = errCode
					g.Group = groupID
					results = append(results, g)
					continue
				}
				if group, ok := responseByGroup[groupID]; ok {
					results = append(results, group)
				} else {
					g := kmsg.NewDescribeGroupsResponseGroup()
					g.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
					g.Group = groupID
					results = append(results, g)
				}
			}

			resp := kmsg.NewPtrDescribeGroupsResponse()
			resp.Groups = results
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		})
	case *kmsg.ListGroupsRequest:
		if !h.allowGroup(principal, "*", acl.ActionGroupRead) {
			h.recordAuthzDeniedWithPrincipal(principal, acl.ActionGroupRead, acl.ResourceGroup, "*")
			resp := kmsg.NewPtrListGroupsResponse()
			resp.ErrorCode = protocol.GROUP_AUTHORIZATION_FAILED
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		return h.withAdminMetrics(header.APIKey, func() ([]byte, error) {
			if !h.etcdAvailable() {
				resp := kmsg.NewPtrListGroupsResponse()
				resp.ErrorCode = protocol.REQUEST_TIMED_OUT
				return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
			}
			resp, err := h.coordinator.ListGroups(ctx, req.(*kmsg.ListGroupsRequest))
			if err != nil {
				return nil, err
			}
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		})
	case *kmsg.HeartbeatRequest:
		req := req.(*kmsg.HeartbeatRequest)
		if !h.allowGroup(principal, req.Group, acl.ActionGroupWrite) {
			h.recordAuthzDeniedWithPrincipal(principal, acl.ActionGroupWrite, acl.ResourceGroup, req.Group)
			resp := kmsg.NewPtrHeartbeatResponse()
			resp.ErrorCode = protocol.GROUP_AUTHORIZATION_FAILED
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		if errCode := h.acquireGroupLease(ctx, req.Group); errCode != 0 {
			resp := kmsg.NewPtrHeartbeatResponse()
			resp.ErrorCode = errCode
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		if !h.etcdAvailable() {
			resp := kmsg.NewPtrHeartbeatResponse()
			resp.ErrorCode = protocol.REQUEST_TIMED_OUT
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		resp := h.coordinator.Heartbeat(ctx, req)
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	case *kmsg.LeaveGroupRequest:
		req := req.(*kmsg.LeaveGroupRequest)
		if !h.allowGroup(principal, req.Group, acl.ActionGroupWrite) {
			h.recordAuthzDeniedWithPrincipal(principal, acl.ActionGroupWrite, acl.ResourceGroup, req.Group)
			resp := kmsg.NewPtrLeaveGroupResponse()
			resp.ErrorCode = protocol.GROUP_AUTHORIZATION_FAILED
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		if errCode := h.acquireGroupLease(ctx, req.Group); errCode != 0 {
			resp := kmsg.NewPtrLeaveGroupResponse()
			resp.ErrorCode = errCode
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		if !h.etcdAvailable() {
			resp := kmsg.NewPtrLeaveGroupResponse()
			resp.ErrorCode = protocol.REQUEST_TIMED_OUT
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		resp := h.coordinator.LeaveGroup(ctx, req)
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	case *kmsg.OffsetCommitRequest:
		req := req.(*kmsg.OffsetCommitRequest)
		if !h.allowGroup(principal, req.Group, acl.ActionGroupWrite) {
			h.recordAuthzDeniedWithPrincipal(principal, acl.ActionGroupWrite, acl.ResourceGroup, req.Group)
			resp := kmsg.NewPtrOffsetCommitResponse()
			resp.Topics = offsetCommitErrorTopics(req, protocol.GROUP_AUTHORIZATION_FAILED)
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		if errCode := h.acquireGroupLease(ctx, req.Group); errCode != 0 {
			resp := kmsg.NewPtrOffsetCommitResponse()
			resp.Topics = offsetCommitErrorTopics(req, errCode)
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		if !h.etcdAvailable() {
			resp := kmsg.NewPtrOffsetCommitResponse()
			resp.Topics = offsetCommitErrorTopics(req, protocol.REQUEST_TIMED_OUT)
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		resp, err := h.coordinator.OffsetCommit(ctx, req)
		if err != nil {
			return nil, err
		}
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	case *kmsg.OffsetFetchRequest:
		req := req.(*kmsg.OffsetFetchRequest)
		if !h.allowGroup(principal, req.Group, acl.ActionGroupRead) {
			h.recordAuthzDeniedWithPrincipal(principal, acl.ActionGroupRead, acl.ResourceGroup, req.Group)
			resp := kmsg.NewPtrOffsetFetchResponse()
			resp.Topics = offsetFetchErrorTopics(req, protocol.GROUP_AUTHORIZATION_FAILED)
			resp.ErrorCode = protocol.GROUP_AUTHORIZATION_FAILED
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		if errCode := h.acquireGroupLease(ctx, req.Group); errCode != 0 {
			resp := kmsg.NewPtrOffsetFetchResponse()
			resp.Topics = offsetFetchErrorTopics(req, errCode)
			resp.ErrorCode = errCode
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		if !h.etcdAvailable() {
			resp := kmsg.NewPtrOffsetFetchResponse()
			resp.Topics = offsetFetchErrorTopics(req, protocol.REQUEST_TIMED_OUT)
			resp.ErrorCode = protocol.REQUEST_TIMED_OUT
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		}
		resp, err := h.coordinator.OffsetFetch(ctx, req)
		if err != nil {
			return nil, err
		}
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	case *kmsg.OffsetForLeaderEpochRequest:
		return h.withAdminMetrics(header.APIKey, func() ([]byte, error) {
			offsetReq := req.(*kmsg.OffsetForLeaderEpochRequest)
			if !h.allowTopics(principal, topicsFromOffsetForLeaderEpoch(offsetReq), acl.ActionFetch) {
				return h.unauthorizedOffsetForLeaderEpoch(principal, header, offsetReq)
			}
			return h.handleOffsetForLeaderEpoch(ctx, header, offsetReq)
		})
	case *kmsg.DescribeConfigsRequest:
		return h.withAdminMetrics(header.APIKey, func() ([]byte, error) {
			return h.handleDescribeConfigs(ctx, header, req.(*kmsg.DescribeConfigsRequest))
		})
	case *kmsg.AlterConfigsRequest:
		return h.withAdminMetrics(header.APIKey, func() ([]byte, error) {
			alterReq := req.(*kmsg.AlterConfigsRequest)
			if !h.allowAdmin(principal) {
				return h.unauthorizedAlterConfigs(principal, header, alterReq)
			}
			return h.handleAlterConfigs(ctx, header, alterReq)
		})
	case *kmsg.CreatePartitionsRequest:
		return h.withAdminMetrics(header.APIKey, func() ([]byte, error) {
			createReq := req.(*kmsg.CreatePartitionsRequest)
			if !h.allowAdmin(principal) {
				return h.unauthorizedCreatePartitions(principal, header, createReq)
			}
			return h.handleCreatePartitions(ctx, header, createReq)
		})
	case *kmsg.DeleteGroupsRequest:
		return h.withAdminMetrics(header.APIKey, func() ([]byte, error) {
			deleteReq := req.(*kmsg.DeleteGroupsRequest)
			allowed := make([]string, 0, len(deleteReq.Groups))
			denied := make(map[string]struct{})
			for _, groupID := range deleteReq.Groups {
				if h.allowGroup(principal, groupID, acl.ActionGroupAdmin) {
					allowed = append(allowed, groupID)
				} else {
					denied[groupID] = struct{}{}
					h.recordAuthzDeniedWithPrincipal(principal, acl.ActionGroupAdmin, acl.ResourceGroup, groupID)
				}
			}

			responseByGroup := make(map[string]kmsg.DeleteGroupsResponseGroup, len(deleteReq.Groups))
			if len(allowed) > 0 {
				if !h.etcdAvailable() {
					for _, groupID := range allowed {
						g := kmsg.NewDeleteGroupsResponseGroup()
						g.Group = groupID
						g.ErrorCode = protocol.REQUEST_TIMED_OUT
						responseByGroup[groupID] = g
					}
				} else {
					allowedReq := *deleteReq
					allowedReq.Groups = allowed
					resp, err := h.coordinator.DeleteGroups(ctx, &allowedReq)
					if err != nil {
						return nil, err
					}
					for _, group := range resp.Groups {
						responseByGroup[group.Group] = group
					}
				}
			}

			results := make([]kmsg.DeleteGroupsResponseGroup, 0, len(deleteReq.Groups))
			for _, groupID := range deleteReq.Groups {
				if _, denied := denied[groupID]; denied {
					g := kmsg.NewDeleteGroupsResponseGroup()
					g.Group = groupID
					g.ErrorCode = protocol.GROUP_AUTHORIZATION_FAILED
					results = append(results, g)
					continue
				}
				if group, ok := responseByGroup[groupID]; ok {
					results = append(results, group)
				} else {
					g := kmsg.NewDeleteGroupsResponseGroup()
					g.Group = groupID
					g.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
					results = append(results, g)
				}
			}

			resp := kmsg.NewPtrDeleteGroupsResponse()
			resp.Groups = results
			return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
		})
	case *kmsg.CreateTopicsRequest:
		createReq := req.(*kmsg.CreateTopicsRequest)
		if !h.allowAdmin(principal) {
			return h.unauthorizedCreateTopics(principal, header, createReq)
		}
		return h.handleCreateTopics(ctx, header, createReq)
	case *kmsg.DeleteTopicsRequest:
		deleteReq := req.(*kmsg.DeleteTopicsRequest)
		if !h.allowAdmin(principal) {
			return h.unauthorizedDeleteTopics(principal, header, deleteReq)
		}
		return h.handleDeleteTopics(ctx, header, deleteReq)
	case *kmsg.ListOffsetsRequest:
		listReq := req.(*kmsg.ListOffsetsRequest)
		if !h.allowTopics(principal, topicsFromListOffsets(listReq), acl.ActionFetch) {
			return h.unauthorizedListOffsets(principal, header, listReq)
		}
		return h.handleListOffsets(ctx, header, listReq)
	default:
		return nil, ErrUnsupportedAPI
	}
}

var ErrUnsupportedAPI = fmt.Errorf("unsupported api")

func (h *handler) coordinatorBroker(ctx context.Context) protocol.MetadataBroker {
	meta, err := h.store.Metadata(ctx, nil)
	if err == nil {
		if broker, ok := brokerByID(meta.Brokers, h.brokerInfo.NodeID); ok {
			return broker
		}
		if len(meta.Brokers) > 0 {
			return meta.Brokers[0]
		}
	}
	return h.brokerInfo
}

func brokerByID(brokers []protocol.MetadataBroker, id int32) (protocol.MetadataBroker, bool) {
	for _, broker := range brokers {
		if broker.NodeID == id {
			return broker, true
		}
	}
	return protocol.MetadataBroker{}, false
}

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
	if h.produceLatency != nil {
		h.produceLatency.WritePrometheus(w, "kafscale_produce_latency_ms", "Produce request latency in milliseconds.")
	}
	if h.consumerLag != nil {
		h.consumerLag.WritePrometheus(w)
	}
	h.writeRuntimeMetrics(w)
	h.adminMetrics.writePrometheus(w)
	h.authMetrics.writePrometheus(w)
}

func (h *handler) recordS3Op(op string, latency time.Duration, err error) {
	if h.s3Health == nil {
		return
	}
	h.s3Health.RecordOperation(op, latency, err)
}

func (h *handler) recordProduceLatency(latency time.Duration) {
	if h.produceLatency == nil {
		return
	}
	h.produceLatency.Observe(float64(latency.Milliseconds()))
}

func (h *handler) recordAuthzDenied(action acl.Action, resource acl.Resource) {
	if h.authMetrics == nil {
		return
	}
	h.authMetrics.RecordDenied(action, resource)
}

func (h *handler) recordAuthzDeniedWithPrincipal(principal string, action acl.Action, resource acl.Resource, name string) {
	h.recordAuthzDenied(action, resource)
	h.logAuthzDenied(principal, action, resource, name)
}

func (h *handler) logAuthzDenied(principal string, action acl.Action, resource acl.Resource, name string) {
	if h == nil || h.logger == nil {
		return
	}
	key := fmt.Sprintf("%s|%s|%s|%s", action, resource, principal, name)
	now := time.Now()
	h.authLogMu.Lock()
	if len(h.authLogLast) > 10000 {
		h.authLogLast = make(map[string]time.Time)
	}
	last := h.authLogLast[key]
	if now.Sub(last) < time.Minute {
		h.authLogMu.Unlock()
		return
	}
	h.authLogLast[key] = now
	h.authLogMu.Unlock()
	h.logger.Warn("authorization denied", "principal", principal, "action", action, "resource", resource, "name", name)
}

func (h *handler) withAdminMetrics(apiKey int16, fn func() ([]byte, error)) ([]byte, error) {
	start := time.Now()
	payload, err := fn()
	h.adminMetrics.Record(apiKey, time.Since(start), err)
	return payload, err
}

func principalFromContext(ctx context.Context, header *protocol.RequestHeader) string {
	if info := broker.ConnInfoFromContext(ctx); info != nil {
		if strings.TrimSpace(info.Principal) != "" {
			return strings.TrimSpace(info.Principal)
		}
	}
	if header == nil || header.ClientID == nil {
		return "anonymous"
	}
	if strings.TrimSpace(*header.ClientID) == "" {
		return "anonymous"
	}
	return *header.ClientID
}

func (h *handler) allowTopic(principal string, topic string, action acl.Action) bool {
	if h.authorizer == nil || !h.authorizer.Enabled() {
		return true
	}
	return h.authorizer.Allows(principal, action, acl.ResourceTopic, topic)
}

func (h *handler) allowTopics(principal string, topics []string, action acl.Action) bool {
	for _, topic := range topics {
		if !h.allowTopic(principal, topic, action) {
			return false
		}
	}
	return true
}

func (h *handler) allowGroup(principal string, group string, action acl.Action) bool {
	if h.authorizer == nil || !h.authorizer.Enabled() {
		return true
	}
	return h.authorizer.Allows(principal, action, acl.ResourceGroup, group)
}

func (h *handler) allowGroups(principal string, groups []string, action acl.Action) bool {
	for _, group := range groups {
		if !h.allowGroup(principal, group, action) {
			return false
		}
	}
	return true
}

func (h *handler) allowCluster(principal string, action acl.Action) bool {
	if h.authorizer == nil || !h.authorizer.Enabled() {
		return true
	}
	return h.authorizer.Allows(principal, action, acl.ResourceCluster, "cluster")
}

func (h *handler) allowAdmin(principal string) bool {
	return h.allowCluster(principal, acl.ActionAdmin)
}

func topicsFromListOffsets(req *kmsg.ListOffsetsRequest) []string {
	topics := make([]string, 0, len(req.Topics))
	for _, topic := range req.Topics {
		topics = append(topics, topic.Topic)
	}
	return topics
}

func topicsFromOffsetForLeaderEpoch(req *kmsg.OffsetForLeaderEpochRequest) []string {
	topics := make([]string, 0, len(req.Topics))
	for _, topic := range req.Topics {
		topics = append(topics, topic.Topic)
	}
	return topics
}

func topicsFromCreateTopics(req *kmsg.CreateTopicsRequest) []string {
	topics := make([]string, 0, len(req.Topics))
	for _, topic := range req.Topics {
		topics = append(topics, topic.Topic)
	}
	return topics
}

func topicsFromCreatePartitions(req *kmsg.CreatePartitionsRequest) []string {
	topics := make([]string, 0, len(req.Topics))
	for _, topic := range req.Topics {
		topics = append(topics, topic.Topic)
	}
	return topics
}

func (h *handler) unauthorizedOffsetForLeaderEpoch(principal string, header *protocol.RequestHeader, req *kmsg.OffsetForLeaderEpochRequest) ([]byte, error) {
	h.recordAuthzDeniedWithPrincipal(principal, acl.ActionFetch, acl.ResourceTopic, strings.Join(topicsFromOffsetForLeaderEpoch(req), ","))
	respTopics := make([]kmsg.OffsetForLeaderEpochResponseTopic, 0, len(req.Topics))
	for _, topic := range req.Topics {
		partitions := make([]kmsg.OffsetForLeaderEpochResponseTopicPartition, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			p := kmsg.NewOffsetForLeaderEpochResponseTopicPartition()
			p.Partition = part.Partition
			p.ErrorCode = protocol.TOPIC_AUTHORIZATION_FAILED
			partitions = append(partitions, p)
		}
		t := kmsg.NewOffsetForLeaderEpochResponseTopic()
		t.Topic = topic.Topic
		t.Partitions = partitions
		respTopics = append(respTopics, t)
	}
	resp := kmsg.NewPtrOffsetForLeaderEpochResponse()
	resp.Topics = respTopics
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (h *handler) unauthorizedAlterConfigs(principal string, header *protocol.RequestHeader, req *kmsg.AlterConfigsRequest) ([]byte, error) {
	h.recordAuthzDeniedWithPrincipal(principal, acl.ActionAdmin, acl.ResourceCluster, "cluster")
	resources := make([]kmsg.AlterConfigsResponseResource, 0, len(req.Resources))
	for _, resource := range req.Resources {
		errorCode := int16(protocol.CLUSTER_AUTHORIZATION_FAILED)
		if resource.ResourceType == kmsg.ConfigResourceTypeTopic {
			errorCode = protocol.TOPIC_AUTHORIZATION_FAILED
		}
		r := kmsg.NewAlterConfigsResponseResource()
		r.ErrorCode = errorCode
		r.ResourceType = resource.ResourceType
		r.ResourceName = resource.ResourceName
		resources = append(resources, r)
	}
	resp := kmsg.NewPtrAlterConfigsResponse()
	resp.Resources = resources
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (h *handler) unauthorizedCreatePartitions(principal string, header *protocol.RequestHeader, req *kmsg.CreatePartitionsRequest) ([]byte, error) {
	h.recordAuthzDeniedWithPrincipal(principal, acl.ActionAdmin, acl.ResourceTopic, strings.Join(topicsFromCreatePartitions(req), ","))
	results := make([]kmsg.CreatePartitionsResponseTopic, 0, len(req.Topics))
	for _, topic := range req.Topics {
		t := kmsg.NewCreatePartitionsResponseTopic()
		t.Topic = topic.Topic
		t.ErrorCode = protocol.TOPIC_AUTHORIZATION_FAILED
		results = append(results, t)
	}
	resp := kmsg.NewPtrCreatePartitionsResponse()
	resp.Topics = results
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (h *handler) unauthorizedDeleteGroups(principal string, header *protocol.RequestHeader, req *kmsg.DeleteGroupsRequest) ([]byte, error) {
	h.recordAuthzDeniedWithPrincipal(principal, acl.ActionGroupAdmin, acl.ResourceGroup, strings.Join(req.Groups, ","))
	results := make([]kmsg.DeleteGroupsResponseGroup, 0, len(req.Groups))
	for _, groupID := range req.Groups {
		g := kmsg.NewDeleteGroupsResponseGroup()
		g.Group = groupID
		g.ErrorCode = protocol.GROUP_AUTHORIZATION_FAILED
		results = append(results, g)
	}
	resp := kmsg.NewPtrDeleteGroupsResponse()
	resp.Groups = results
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (h *handler) unauthorizedCreateTopics(principal string, header *protocol.RequestHeader, req *kmsg.CreateTopicsRequest) ([]byte, error) {
	h.recordAuthzDeniedWithPrincipal(principal, acl.ActionAdmin, acl.ResourceTopic, strings.Join(topicsFromCreateTopics(req), ","))
	results := make([]kmsg.CreateTopicsResponseTopic, 0, len(req.Topics))
	for _, topic := range req.Topics {
		t := kmsg.NewCreateTopicsResponseTopic()
		t.Topic = topic.Topic
		t.ErrorCode = protocol.TOPIC_AUTHORIZATION_FAILED
		t.ErrorMessage = kmsg.StringPtr("unauthorized")
		results = append(results, t)
	}
	resp := kmsg.NewPtrCreateTopicsResponse()
	resp.Topics = results
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (h *handler) unauthorizedDeleteTopics(principal string, header *protocol.RequestHeader, req *kmsg.DeleteTopicsRequest) ([]byte, error) {
	h.recordAuthzDeniedWithPrincipal(principal, acl.ActionAdmin, acl.ResourceTopic, strings.Join(req.TopicNames, ","))
	results := make([]kmsg.DeleteTopicsResponseTopic, 0, len(req.TopicNames))
	for _, name := range req.TopicNames {
		t := kmsg.NewDeleteTopicsResponseTopic()
		t.Topic = kmsg.StringPtr(name)
		t.ErrorCode = protocol.TOPIC_AUTHORIZATION_FAILED
		t.ErrorMessage = kmsg.StringPtr("unauthorized")
		results = append(results, t)
	}
	resp := kmsg.NewPtrDeleteTopicsResponse()
	resp.Topics = results
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (h *handler) unauthorizedListOffsets(principal string, header *protocol.RequestHeader, req *kmsg.ListOffsetsRequest) ([]byte, error) {
	h.recordAuthzDeniedWithPrincipal(principal, acl.ActionFetch, acl.ResourceTopic, strings.Join(topicsFromListOffsets(req), ","))
	topicResponses := make([]kmsg.ListOffsetsResponseTopic, 0, len(req.Topics))
	for _, topic := range req.Topics {
		partitions := make([]kmsg.ListOffsetsResponseTopicPartition, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			p := kmsg.NewListOffsetsResponseTopicPartition()
			p.Partition = part.Partition
			p.ErrorCode = protocol.TOPIC_AUTHORIZATION_FAILED
			partitions = append(partitions, p)
		}
		t := kmsg.NewListOffsetsResponseTopic()
		t.Topic = topic.Topic
		t.Partitions = partitions
		topicResponses = append(topicResponses, t)
	}
	resp := kmsg.NewPtrListOffsetsResponse()
	resp.Topics = topicResponses
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

// acquireGroupLease attempts to acquire the coordination lease for the given
// group. Returns 0 if this broker is (or becomes) the coordinator. Returns a
// non-zero Kafka error code if the request should be rejected:
//   - NOT_COORDINATOR when another broker owns the group or this broker is
//     shutting down (so the proxy can redirect to the correct broker).
//   - REQUEST_TIMED_OUT for transient errors (etcd timeout, session failure).
func (h *handler) acquireGroupLease(ctx context.Context, groupID string) int16 {
	if h.groupLeaseManager == nil {
		return 0
	}
	err := h.groupLeaseManager.Acquire(ctx, groupID)
	if err == nil {
		return 0
	}
	if errors.Is(err, metadata.ErrNotOwner) || errors.Is(err, metadata.ErrShuttingDown) {
		return protocol.NOT_COORDINATOR
	}
	h.logger.Warn("group lease acquire failed", "group", groupID, "error", err)
	return protocol.REQUEST_TIMED_OUT
}

// acquirePartitionLeases acquires leases for all partitions in the request
// concurrently. Returns a map of partition -> error for partitions that failed.
// Partitions already owned by this broker complete instantly (map lookup).
func (h *handler) acquirePartitionLeases(ctx context.Context, req *kmsg.ProduceRequest) map[metadata.PartitionID]error {
	if h.leaseManager == nil {
		return nil
	}
	var partitions []metadata.PartitionID
	for _, topic := range req.Topics {
		for _, part := range topic.Partitions {
			partitions = append(partitions, metadata.PartitionID{
				Topic:     topic.Topic,
				Partition: part.Partition,
			})
		}
	}
	if len(partitions) == 0 {
		return nil
	}
	results := h.leaseManager.AcquireAll(ctx, partitions)
	var errs map[metadata.PartitionID]error
	for _, r := range results {
		if r.Err != nil {
			if errs == nil {
				errs = make(map[metadata.PartitionID]error)
			}
			errs[r.Partition] = r.Err
		}
	}
	return errs
}

func offsetCommitErrorTopics(req *kmsg.OffsetCommitRequest, errorCode int16) []kmsg.OffsetCommitResponseTopic {
	topics := make([]kmsg.OffsetCommitResponseTopic, 0, len(req.Topics))
	for _, topic := range req.Topics {
		partitions := make([]kmsg.OffsetCommitResponseTopicPartition, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			p := kmsg.NewOffsetCommitResponseTopicPartition()
			p.Partition = part.Partition
			p.ErrorCode = errorCode
			partitions = append(partitions, p)
		}
		t := kmsg.NewOffsetCommitResponseTopic()
		t.Topic = topic.Topic
		t.Partitions = partitions
		topics = append(topics, t)
	}
	return topics
}

func offsetFetchErrorTopics(req *kmsg.OffsetFetchRequest, errorCode int16) []kmsg.OffsetFetchResponseTopic {
	topics := make([]kmsg.OffsetFetchResponseTopic, 0, len(req.Topics))
	for _, topic := range req.Topics {
		partitions := make([]kmsg.OffsetFetchResponseTopicPartition, 0, len(topic.Partitions))
		for _, partID := range topic.Partitions {
			p := kmsg.NewOffsetFetchResponseTopicPartition()
			p.Partition = partID
			p.Offset = -1
			p.LeaderEpoch = -1
			p.ErrorCode = errorCode
			partitions = append(partitions, p)
		}
		t := kmsg.NewOffsetFetchResponseTopic()
		t.Topic = topic.Topic
		t.Partitions = partitions
		topics = append(topics, t)
	}
	return topics
}

func (h *handler) handleProduce(ctx context.Context, header *protocol.RequestHeader, req *kmsg.ProduceRequest) ([]byte, error) {
	start := time.Now()
	defer func() {
		h.recordProduceLatency(time.Since(start))
	}()
	topicResponses := make([]kmsg.ProduceResponseTopic, 0, len(req.Topics))
	now := time.Now().UnixMilli()
	var producedMessages int64
	principal := principalFromContext(ctx, header)

	leaseErrors := h.acquirePartitionLeases(ctx, req)

	for _, topic := range req.Topics {
		if h.traceKafka {
			h.logger.Debug("produce request received", "topic", topic.Topic, "partitions", len(topic.Partitions), "acks", req.Acks, "timeout_ms", req.TimeoutMillis)
		}
		partitionResponses := make([]kmsg.ProduceResponseTopicPartition, 0, len(topic.Partitions))
		if !h.allowTopic(principal, topic.Topic, acl.ActionProduce) {
			h.recordAuthzDeniedWithPrincipal(principal, acl.ActionProduce, acl.ResourceTopic, topic.Topic)
			for _, part := range topic.Partitions {
				p := kmsg.NewProduceResponseTopicPartition()
				p.Partition = part.Partition
				p.ErrorCode = protocol.TOPIC_AUTHORIZATION_FAILED
				partitionResponses = append(partitionResponses, p)
			}
			t := kmsg.NewProduceResponseTopic()
			t.Topic = topic.Topic
			t.Partitions = partitionResponses
			topicResponses = append(topicResponses, t)
			continue
		}
		for _, part := range topic.Partitions {
			if !h.etcdAvailable() {
				p := kmsg.NewProduceResponseTopicPartition()
				p.Partition = part.Partition
				p.ErrorCode = protocol.REQUEST_TIMED_OUT
				partitionResponses = append(partitionResponses, p)
				if h.traceKafka {
					h.logger.Debug("produce rejected due to etcd availability", "topic", topic.Topic, "partition", part.Partition)
				}
				continue
			}
			if leaseErr, hasErr := leaseErrors[metadata.PartitionID{Topic: topic.Topic, Partition: part.Partition}]; hasErr {
				if errors.Is(leaseErr, metadata.ErrNotOwner) || errors.Is(leaseErr, metadata.ErrShuttingDown) {
					p := kmsg.NewProduceResponseTopicPartition()
					p.Partition = part.Partition
					p.ErrorCode = protocol.NOT_LEADER_OR_FOLLOWER
					partitionResponses = append(partitionResponses, p)
					if h.traceKafka {
						h.logger.Debug("produce rejected: not partition owner", "topic", topic.Topic, "partition", part.Partition)
					}
					continue
				}
				p := kmsg.NewProduceResponseTopicPartition()
				p.Partition = part.Partition
				p.ErrorCode = protocol.REQUEST_TIMED_OUT
				partitionResponses = append(partitionResponses, p)
				h.logger.Warn("partition lease acquire failed", "topic", topic.Topic, "partition", part.Partition, "error", leaseErr)
				continue
			}
			if h.s3Health.State() != broker.S3StateHealthy {
				p := kmsg.NewProduceResponseTopicPartition()
				p.Partition = part.Partition
				p.ErrorCode = h.backpressureErrorCode()
				partitionResponses = append(partitionResponses, p)
				if h.traceKafka {
					h.logger.Debug("produce rejected due to S3 health", "topic", topic.Topic, "partition", part.Partition, "s3_state", h.s3Health.State())
				}
				continue
			}
			plog, err := h.getPartitionLog(ctx, topic.Topic, part.Partition)
			if err != nil {
				h.logger.Error("partition log init failed", "error", err, "topic", topic.Topic, "partition", part.Partition)
				p := kmsg.NewProduceResponseTopicPartition()
				p.Partition = part.Partition
				p.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
				partitionResponses = append(partitionResponses, p)
				continue
			}
			batch, err := storage.NewRecordBatchFromBytes(part.Records)
			if err != nil {
				p := kmsg.NewProduceResponseTopicPartition()
				p.Partition = part.Partition
				p.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
				partitionResponses = append(partitionResponses, p)
				if h.traceKafka {
					h.logger.Debug("produce record batch decode failed", "topic", topic.Topic, "partition", part.Partition, "error", err)
				}
				continue
			}
			result, err := plog.AppendBatch(ctx, batch)
			if err != nil {
				p := kmsg.NewProduceResponseTopicPartition()
				p.Partition = part.Partition
				p.ErrorCode = h.backpressureErrorCode()
				partitionResponses = append(partitionResponses, p)
				if h.traceKafka {
					h.logger.Debug("produce append failed", "topic", topic.Topic, "partition", part.Partition, "error", err)
				}
				continue
			}
			if req.Acks != 0 && h.flushOnAck {
				if err := plog.Flush(ctx); err != nil {
					h.logger.Error("flush failed", "error", err, "topic", topic.Topic, "partition", part.Partition)
					p := kmsg.NewProduceResponseTopicPartition()
					p.Partition = part.Partition
					p.ErrorCode = h.backpressureErrorCode()
					partitionResponses = append(partitionResponses, p)
					continue
				}
			}
			p := kmsg.NewProduceResponseTopicPartition()
			p.Partition = part.Partition
			p.ErrorCode = 0
			p.BaseOffset = result.BaseOffset
			p.LogAppendTime = now
			p.LogStartOffset = 0
			partitionResponses = append(partitionResponses, p)
			producedMessages += int64(batch.MessageCount)
			if h.traceKafka {
				h.logger.Debug("produce append success", "topic", topic.Topic, "partition", part.Partition, "base_offset", result.BaseOffset, "last_offset", result.LastOffset)
			}
		}
		t := kmsg.NewProduceResponseTopic()
		t.Topic = topic.Topic
		t.Partitions = partitionResponses
		topicResponses = append(topicResponses, t)
	}

	if producedMessages > 0 {
		h.produceRate.add(producedMessages)
	}

	if req.Acks == 0 {
		return nil, nil
	}

	resp := kmsg.NewPtrProduceResponse()
	resp.Topics = topicResponses
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (h *handler) handleCreateTopics(ctx context.Context, header *protocol.RequestHeader, req *kmsg.CreateTopicsRequest) ([]byte, error) {
	if header.APIVersion < 0 || header.APIVersion > 2 {
		return nil, fmt.Errorf("create topics version %d not supported", header.APIVersion)
	}
	results := make([]kmsg.CreateTopicsResponseTopic, 0, len(req.Topics))
	if !h.allowAdminAPIs {
		for _, topic := range req.Topics {
			t := kmsg.NewCreateTopicsResponseTopic()
			t.Topic = topic.Topic
			t.ErrorCode = protocol.TOPIC_AUTHORIZATION_FAILED
			t.ErrorMessage = kmsg.StringPtr("admin APIs disabled")
			results = append(results, t)
		}
		resp := kmsg.NewPtrCreateTopicsResponse()
		resp.Topics = results
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	}
	if !h.etcdAvailable() {
		for _, topic := range req.Topics {
			t := kmsg.NewCreateTopicsResponseTopic()
			t.Topic = topic.Topic
			t.ErrorCode = protocol.REQUEST_TIMED_OUT
			t.ErrorMessage = kmsg.StringPtr("etcd unavailable")
			results = append(results, t)
		}
		resp := kmsg.NewPtrCreateTopicsResponse()
		resp.Topics = results
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	}
	for _, topic := range req.Topics {
		if req.ValidateOnly {
			err := h.validateCreateTopic(ctx, topic)
			t := kmsg.NewCreateTopicsResponseTopic()
			t.Topic = topic.Topic
			if err != nil {
				switch {
				case errors.Is(err, metadata.ErrTopicExists):
					t.ErrorCode = protocol.TOPIC_ALREADY_EXISTS
				case errors.Is(err, metadata.ErrInvalidTopic):
					t.ErrorCode = protocol.INVALID_TOPIC_EXCEPTION
				default:
					t.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
				}
				t.ErrorMessage = kmsg.StringPtr(err.Error())
			}
			results = append(results, t)
			continue
		}
		_, err := h.store.CreateTopic(ctx, metadata.TopicSpec{
			Name:              topic.Topic,
			NumPartitions:     topic.NumPartitions,
			ReplicationFactor: topic.ReplicationFactor,
		})
		t := kmsg.NewCreateTopicsResponseTopic()
		t.Topic = topic.Topic
		if err != nil {
			switch {
			case errors.Is(err, metadata.ErrTopicExists):
				t.ErrorCode = protocol.TOPIC_ALREADY_EXISTS
			case errors.Is(err, metadata.ErrInvalidTopic):
				t.ErrorCode = protocol.INVALID_TOPIC_EXCEPTION
			default:
				t.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			}
			t.ErrorMessage = kmsg.StringPtr(err.Error())
		}
		results = append(results, t)
	}
	resp := kmsg.NewPtrCreateTopicsResponse()
	resp.Topics = results
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (h *handler) handleDeleteTopics(ctx context.Context, header *protocol.RequestHeader, req *kmsg.DeleteTopicsRequest) ([]byte, error) {
	if header.APIVersion < 0 || header.APIVersion > 2 {
		return nil, fmt.Errorf("delete topics version %d not supported", header.APIVersion)
	}
	results := make([]kmsg.DeleteTopicsResponseTopic, 0, len(req.TopicNames))
	if !h.allowAdminAPIs {
		for _, name := range req.TopicNames {
			t := kmsg.NewDeleteTopicsResponseTopic()
			t.Topic = kmsg.StringPtr(name)
			t.ErrorCode = protocol.TOPIC_AUTHORIZATION_FAILED
			t.ErrorMessage = kmsg.StringPtr("admin APIs disabled")
			results = append(results, t)
		}
		resp := kmsg.NewPtrDeleteTopicsResponse()
		resp.Topics = results
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	}
	if !h.etcdAvailable() {
		for _, name := range req.TopicNames {
			t := kmsg.NewDeleteTopicsResponseTopic()
			t.Topic = kmsg.StringPtr(name)
			t.ErrorCode = protocol.REQUEST_TIMED_OUT
			t.ErrorMessage = kmsg.StringPtr("etcd unavailable")
			results = append(results, t)
		}
		resp := kmsg.NewPtrDeleteTopicsResponse()
		resp.Topics = results
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	}
	for _, name := range req.TopicNames {
		t := kmsg.NewDeleteTopicsResponseTopic()
		t.Topic = kmsg.StringPtr(name)
		if err := h.store.DeleteTopic(ctx, name); err != nil {
			switch {
			case errors.Is(err, metadata.ErrUnknownTopic):
				t.ErrorCode = protocol.UNKNOWN_TOPIC_OR_PARTITION
			default:
				t.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			}
			t.ErrorMessage = kmsg.StringPtr(err.Error())
		}
		results = append(results, t)
	}
	resp := kmsg.NewPtrDeleteTopicsResponse()
	resp.Topics = results
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (h *handler) validateCreateTopic(ctx context.Context, topic kmsg.CreateTopicsRequestTopic) error {
	if topic.Topic == "" || topic.NumPartitions <= 0 {
		return metadata.ErrInvalidTopic
	}
	replicationFactor := topic.ReplicationFactor
	if replicationFactor <= 0 {
		replicationFactor = 1
	}
	meta, err := h.store.Metadata(ctx, nil)
	if err != nil {
		return err
	}
	for _, existing := range meta.Topics {
		if *existing.Topic == topic.Topic {
			return metadata.ErrTopicExists
		}
	}
	if int(replicationFactor) > len(meta.Brokers) {
		return metadata.ErrInvalidTopic
	}
	return nil
}

func (h *handler) etcdAvailable() bool {
	if checker, ok := h.store.(etcdAvailability); ok {
		return checker.Available()
	}
	return true
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

func (h *handler) handleOffsetForLeaderEpoch(ctx context.Context, header *protocol.RequestHeader, req *kmsg.OffsetForLeaderEpochRequest) ([]byte, error) {
	topics := make([]string, 0, len(req.Topics))
	for _, topic := range req.Topics {
		topics = append(topics, topic.Topic)
	}
	meta, err := h.store.Metadata(ctx, topics)
	if err != nil {
		return nil, err
	}
	metaIndex := make(map[string]protocol.MetadataTopic, len(meta.Topics))
	for _, topic := range meta.Topics {
		metaIndex[*topic.Topic] = topic
	}
	respTopics := make([]kmsg.OffsetForLeaderEpochResponseTopic, 0, len(req.Topics))
	for _, topic := range req.Topics {
		metaTopic, ok := metaIndex[topic.Topic]
		partIndex := make(map[int32]protocol.MetadataPartition, len(metaTopic.Partitions))
		if ok {
			for _, part := range metaTopic.Partitions {
				partIndex[part.Partition] = part
			}
		}
		partitions := make([]kmsg.OffsetForLeaderEpochResponseTopicPartition, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			if !ok {
				p := kmsg.NewOffsetForLeaderEpochResponseTopicPartition()
				p.Partition = part.Partition
				p.ErrorCode = protocol.UNKNOWN_TOPIC_OR_PARTITION
				partitions = append(partitions, p)
				continue
			}
			metaPart, exists := partIndex[part.Partition]
			if !exists {
				p := kmsg.NewOffsetForLeaderEpochResponseTopicPartition()
				p.Partition = part.Partition
				p.ErrorCode = protocol.UNKNOWN_TOPIC_OR_PARTITION
				partitions = append(partitions, p)
				continue
			}
			nextOffset, err := h.store.NextOffset(ctx, topic.Topic, part.Partition)
			if err != nil {
				p := kmsg.NewOffsetForLeaderEpochResponseTopicPartition()
				p.Partition = part.Partition
				p.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
				partitions = append(partitions, p)
				continue
			}
			p := kmsg.NewOffsetForLeaderEpochResponseTopicPartition()
			p.Partition = part.Partition
			p.ErrorCode = protocol.NONE
			p.LeaderEpoch = metaPart.LeaderEpoch
			p.EndOffset = nextOffset
			partitions = append(partitions, p)
		}
		t := kmsg.NewOffsetForLeaderEpochResponseTopic()
		t.Topic = topic.Topic
		t.Partitions = partitions
		respTopics = append(respTopics, t)
	}
	resp := kmsg.NewPtrOffsetForLeaderEpochResponse()
	resp.Topics = respTopics
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (h *handler) handleDescribeConfigs(ctx context.Context, header *protocol.RequestHeader, req *kmsg.DescribeConfigsRequest) ([]byte, error) {
	resources := make([]kmsg.DescribeConfigsResponseResource, 0, len(req.Resources))
	principal := principalFromContext(ctx, header)
	for _, resource := range req.Resources {
		switch resource.ResourceType {
		case kmsg.ConfigResourceTypeTopic:
			if !h.allowTopic(principal, resource.ResourceName, acl.ActionFetch) {
				h.recordAuthzDeniedWithPrincipal(principal, acl.ActionFetch, acl.ResourceTopic, resource.ResourceName)
				r := kmsg.NewDescribeConfigsResponseResource()
				r.ErrorCode = protocol.TOPIC_AUTHORIZATION_FAILED
				r.ResourceType = resource.ResourceType
				r.ResourceName = resource.ResourceName
				resources = append(resources, r)
				continue
			}
			cfg, err := h.store.FetchTopicConfig(ctx, resource.ResourceName)
			if err != nil {
				r := kmsg.NewDescribeConfigsResponseResource()
				r.ErrorCode = protocol.UNKNOWN_TOPIC_OR_PARTITION
				r.ResourceType = resource.ResourceType
				r.ResourceName = resource.ResourceName
				resources = append(resources, r)
				continue
			}
			configs := h.topicConfigEntries(cfg, resource.ConfigNames)
			r := kmsg.NewDescribeConfigsResponseResource()
			r.ErrorCode = protocol.NONE
			r.ResourceType = resource.ResourceType
			r.ResourceName = resource.ResourceName
			r.Configs = configs
			resources = append(resources, r)
		case kmsg.ConfigResourceTypeBroker:
			if !h.allowAdmin(principal) {
				h.recordAuthzDeniedWithPrincipal(principal, acl.ActionAdmin, acl.ResourceCluster, "cluster")
				r := kmsg.NewDescribeConfigsResponseResource()
				r.ErrorCode = protocol.CLUSTER_AUTHORIZATION_FAILED
				r.ResourceType = resource.ResourceType
				r.ResourceName = resource.ResourceName
				resources = append(resources, r)
				continue
			}
			configs := h.brokerConfigEntries(resource.ConfigNames)
			r := kmsg.NewDescribeConfigsResponseResource()
			r.ErrorCode = protocol.NONE
			r.ResourceType = resource.ResourceType
			r.ResourceName = resource.ResourceName
			r.Configs = configs
			resources = append(resources, r)
		default:
			if !h.allowAdmin(principal) {
				h.recordAuthzDeniedWithPrincipal(principal, acl.ActionAdmin, acl.ResourceCluster, "cluster")
				r := kmsg.NewDescribeConfigsResponseResource()
				r.ErrorCode = protocol.CLUSTER_AUTHORIZATION_FAILED
				r.ResourceType = resource.ResourceType
				r.ResourceName = resource.ResourceName
				resources = append(resources, r)
				continue
			}
			r := kmsg.NewDescribeConfigsResponseResource()
			r.ErrorCode = protocol.INVALID_REQUEST
			r.ResourceType = resource.ResourceType
			r.ResourceName = resource.ResourceName
			resources = append(resources, r)
		}
	}
	resp := kmsg.NewPtrDescribeConfigsResponse()
	resp.Resources = resources
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (h *handler) handleAlterConfigs(ctx context.Context, header *protocol.RequestHeader, req *kmsg.AlterConfigsRequest) ([]byte, error) {
	resources := make([]kmsg.AlterConfigsResponseResource, 0, len(req.Resources))
	if !h.etcdAvailable() {
		for _, resource := range req.Resources {
			r := kmsg.NewAlterConfigsResponseResource()
			r.ErrorCode = protocol.REQUEST_TIMED_OUT
			r.ResourceType = resource.ResourceType
			r.ResourceName = resource.ResourceName
			resources = append(resources, r)
		}
		resp := kmsg.NewPtrAlterConfigsResponse()
		resp.Resources = resources
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	}
	for _, resource := range req.Resources {
		if resource.ResourceType != kmsg.ConfigResourceTypeTopic || resource.ResourceName == "" {
			r := kmsg.NewAlterConfigsResponseResource()
			r.ErrorCode = protocol.INVALID_REQUEST
			r.ResourceType = resource.ResourceType
			r.ResourceName = resource.ResourceName
			resources = append(resources, r)
			continue
		}
		cfg, err := h.store.FetchTopicConfig(ctx, resource.ResourceName)
		if err != nil {
			r := kmsg.NewAlterConfigsResponseResource()
			r.ErrorCode = protocol.UNKNOWN_TOPIC_OR_PARTITION
			r.ResourceType = resource.ResourceType
			r.ResourceName = resource.ResourceName
			resources = append(resources, r)
			continue
		}
		updated := proto.Clone(cfg).(*metadatapb.TopicConfig)
		if updated.Config == nil {
			updated.Config = make(map[string]string)
		}
		errorCode := int16(protocol.NONE)
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
		r := kmsg.NewAlterConfigsResponseResource()
		r.ErrorCode = errorCode
		r.ResourceType = resource.ResourceType
		r.ResourceName = resource.ResourceName
		resources = append(resources, r)
	}
	resp := kmsg.NewPtrAlterConfigsResponse()
	resp.Resources = resources
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (h *handler) handleCreatePartitions(ctx context.Context, header *protocol.RequestHeader, req *kmsg.CreatePartitionsRequest) ([]byte, error) {
	results := make([]kmsg.CreatePartitionsResponseTopic, 0, len(req.Topics))
	seen := make(map[string]struct{}, len(req.Topics))
	if !h.etcdAvailable() {
		for _, topic := range req.Topics {
			t := kmsg.NewCreatePartitionsResponseTopic()
			t.Topic = topic.Topic
			t.ErrorCode = protocol.REQUEST_TIMED_OUT
			t.ErrorMessage = kmsg.StringPtr("etcd unavailable")
			results = append(results, t)
		}
		resp := kmsg.NewPtrCreatePartitionsResponse()
		resp.Topics = results
		return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
	}
	for _, topic := range req.Topics {
		t := kmsg.NewCreatePartitionsResponseTopic()
		t.Topic = topic.Topic
		if strings.TrimSpace(topic.Topic) == "" {
			t.ErrorCode = protocol.INVALID_TOPIC_EXCEPTION
			t.ErrorMessage = kmsg.StringPtr("invalid topic name")
			results = append(results, t)
			continue
		}
		if _, ok := seen[topic.Topic]; ok {
			t.ErrorCode = protocol.INVALID_REQUEST
			t.ErrorMessage = kmsg.StringPtr("duplicate topic in request")
			results = append(results, t)
			continue
		}
		seen[topic.Topic] = struct{}{}
		if topic.Count <= 0 {
			t.ErrorCode = protocol.INVALID_PARTITIONS
			t.ErrorMessage = kmsg.StringPtr("invalid partition count")
			results = append(results, t)
			continue
		}
		if len(topic.Assignment) > 0 {
			t.ErrorCode = protocol.INVALID_REQUEST
			t.ErrorMessage = kmsg.StringPtr("replica assignment not supported")
			results = append(results, t)
			continue
		}
		var err error
		if req.ValidateOnly {
			err = h.validateCreatePartitions(ctx, topic.Topic, topic.Count)
		} else {
			err = h.store.CreatePartitions(ctx, topic.Topic, topic.Count)
		}
		if err != nil {
			switch {
			case errors.Is(err, metadata.ErrUnknownTopic):
				t.ErrorCode = protocol.UNKNOWN_TOPIC_OR_PARTITION
			case errors.Is(err, metadata.ErrInvalidTopic):
				t.ErrorCode = protocol.INVALID_PARTITIONS
			default:
				t.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			}
			t.ErrorMessage = kmsg.StringPtr(err.Error())
		}
		results = append(results, t)
	}
	resp := kmsg.NewPtrCreatePartitionsResponse()
	resp.Topics = results
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
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

func (h *handler) topicConfigEntries(cfg *metadatapb.TopicConfig, requested []string) []kmsg.DescribeConfigsResponseResourceConfig {
	allow := configNameSet(requested)
	entries := make([]kmsg.DescribeConfigsResponseResourceConfig, 0, 3)
	retentionMs, retentionMsDefault := normalizeRetention(cfg.RetentionMs)
	retentionBytes, retentionBytesDefault := normalizeRetention(cfg.RetentionBytes)
	segmentBytes, segmentDefault := normalizeSegmentBytes(cfg.SegmentBytes, int64(h.segmentBytes))

	entries = appendConfigEntry(entries, allow, configRetentionMs, retentionMs, retentionMsDefault, kmsg.ConfigTypeLong, false)
	entries = appendConfigEntry(entries, allow, configRetentionBytes, retentionBytes, retentionBytesDefault, kmsg.ConfigTypeLong, false)
	entries = appendConfigEntry(entries, allow, configSegmentBytes, segmentBytes, segmentDefault, kmsg.ConfigTypeInt, false)
	return entries
}

func (h *handler) brokerConfigEntries(requested []string) []kmsg.DescribeConfigsResponseResourceConfig {
	allow := configNameSet(requested)
	entries := make([]kmsg.DescribeConfigsResponseResourceConfig, 0, 8)
	entries = appendConfigEntry(entries, allow, configBrokerID, fmt.Sprintf("%d", h.brokerInfo.NodeID), true, kmsg.ConfigTypeInt, true)
	entries = appendConfigEntry(entries, allow, configAdvertised, fmt.Sprintf("%s:%d", h.brokerInfo.Host, h.brokerInfo.Port), true, kmsg.ConfigTypeString, true)
	entries = appendConfigEntry(entries, allow, configS3Bucket, os.Getenv("KAFSCALE_S3_BUCKET"), true, kmsg.ConfigTypeString, true)
	entries = appendConfigEntry(entries, allow, configS3Region, os.Getenv("KAFSCALE_S3_REGION"), true, kmsg.ConfigTypeString, true)
	entries = appendConfigEntry(entries, allow, configS3Endpoint, os.Getenv("KAFSCALE_S3_ENDPOINT"), true, kmsg.ConfigTypeString, true)
	entries = appendConfigEntry(entries, allow, configCacheBytes, fmt.Sprintf("%d", h.cacheSize), true, kmsg.ConfigTypeLong, true)
	entries = appendConfigEntry(entries, allow, configReadAhead, fmt.Sprintf("%d", h.readAhead), true, kmsg.ConfigTypeInt, true)
	entries = appendConfigEntry(entries, allow, configSegmentBytesB, fmt.Sprintf("%d", h.segmentBytes), true, kmsg.ConfigTypeInt, true)
	entries = appendConfigEntry(entries, allow, configFlushInterval, fmt.Sprintf("%d", int64(h.flushInterval/time.Millisecond)), true, kmsg.ConfigTypeLong, true)
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

func appendConfigEntry(entries []kmsg.DescribeConfigsResponseResourceConfig, allow map[string]struct{}, name string, value string, isDefault bool, configType kmsg.ConfigType, readOnly bool) []kmsg.DescribeConfigsResponseResourceConfig {
	if allow != nil {
		if _, ok := allow[name]; !ok {
			return entries
		}
	}
	c := kmsg.NewDescribeConfigsResponseResourceConfig()
	c.Name = name
	c.Value = kmsg.StringPtr(value)
	c.ReadOnly = readOnly
	c.IsDefault = isDefault
	c.Source = chooseConfigSource(isDefault, readOnly)
	c.IsSensitive = false
	c.ConfigType = configType
	entries = append(entries, c)
	return entries
}

func chooseConfigSource(isDefault bool, readOnly bool) kmsg.ConfigSource {
	if isDefault {
		return kmsg.ConfigSourceDefaultConfig
	}
	if readOnly {
		return kmsg.ConfigSourceStaticBrokerConfig
	}
	return kmsg.ConfigSourceDynamicTopicConfig
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

func (h *handler) handleListOffsets(ctx context.Context, header *protocol.RequestHeader, req *kmsg.ListOffsetsRequest) ([]byte, error) {
	if header.APIVersion < 0 || header.APIVersion > 4 {
		return nil, fmt.Errorf("list offsets version %d not supported", header.APIVersion)
	}
	if h.traceKafka {
		h.logger.Debug("list offsets request", "api_version", header.APIVersion, "topics", len(req.Topics))
	}
	topicResponses := make([]kmsg.ListOffsetsResponseTopic, 0, len(req.Topics))
	for _, topic := range req.Topics {
		partitions := make([]kmsg.ListOffsetsResponseTopicPartition, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			if h.traceKafka {
				h.logger.Debug("list offsets partition", "topic", topic.Topic, "partition", part.Partition, "timestamp", part.Timestamp, "max_offsets", part.MaxNumOffsets, "leader_epoch", part.CurrentLeaderEpoch)
			}
			p := kmsg.NewListOffsetsResponseTopicPartition()
			p.Partition = part.Partition
			offset, err := func() (int64, error) {
				switch part.Timestamp {
				case -2:
					plog, err := h.getPartitionLog(ctx, topic.Topic, part.Partition)
					if err != nil {
						return 0, err
					}
					return plog.EarliestOffset(), nil
				default:
					return h.store.NextOffset(ctx, topic.Topic, part.Partition)
				}
			}()
			if err != nil {
				p.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			} else {
				p.Timestamp = part.Timestamp
				p.Offset = offset
				if header.APIVersion == 0 {
					max := part.MaxNumOffsets
					if max <= 0 {
						max = 1
					}
					p.OldStyleOffsets = make([]int64, 0, max)
					p.OldStyleOffsets = append(p.OldStyleOffsets, offset)
				}
			}
			partitions = append(partitions, p)
		}
		t := kmsg.NewListOffsetsResponseTopic()
		t.Topic = topic.Topic
		t.Partitions = partitions
		topicResponses = append(topicResponses, t)
	}
	resp := kmsg.NewPtrListOffsetsResponse()
	resp.Topics = topicResponses
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
}

func (h *handler) handleFetch(ctx context.Context, header *protocol.RequestHeader, req *kmsg.FetchRequest) ([]byte, error) {
	if header.APIVersion < 11 || header.APIVersion > 13 {
		return nil, fmt.Errorf("fetch version %d not supported", header.APIVersion)
	}
	topicResponses := make([]kmsg.FetchResponseTopic, 0, len(req.Topics))
	maxWait := time.Duration(req.MaxWaitMillis) * time.Millisecond
	if maxWait < 0 {
		maxWait = 0
	}
	var fetchedMessages int64
	zeroID := [16]byte{}
	idToName := map[[16]byte]string{}
	principal := principalFromContext(ctx, header)
	for _, topic := range req.Topics {
		if topic.TopicID != zeroID {
			meta, err := h.store.Metadata(ctx, nil)
			if err != nil {
				return nil, fmt.Errorf("load metadata: %w", err)
			}
			for _, t := range meta.Topics {
				idToName[t.TopicID] = *t.Topic
			}
			break
		}
	}

	for _, topic := range req.Topics {
		topicName := topic.Topic
		if topicName == "" && topic.TopicID != zeroID {
			if resolved, ok := idToName[topic.TopicID]; ok {
				topicName = resolved
			} else {
				partitionResponses := make([]kmsg.FetchResponseTopicPartition, 0, len(topic.Partitions))
				for _, part := range topic.Partitions {
					p := kmsg.NewFetchResponseTopicPartition()
					p.Partition = part.Partition
					p.ErrorCode = protocol.UNKNOWN_TOPIC_ID
					partitionResponses = append(partitionResponses, p)
				}
				t := kmsg.NewFetchResponseTopic()
				t.Topic = topicName
				t.TopicID = topic.TopicID
				t.Partitions = partitionResponses
				topicResponses = append(topicResponses, t)
				continue
			}
		}
		if !h.allowTopic(principal, topicName, acl.ActionFetch) {
			h.recordAuthzDeniedWithPrincipal(principal, acl.ActionFetch, acl.ResourceTopic, topicName)
			partitionResponses := make([]kmsg.FetchResponseTopicPartition, 0, len(topic.Partitions))
			for _, part := range topic.Partitions {
				p := kmsg.NewFetchResponseTopicPartition()
				p.Partition = part.Partition
				p.ErrorCode = protocol.TOPIC_AUTHORIZATION_FAILED
				partitionResponses = append(partitionResponses, p)
			}
			t := kmsg.NewFetchResponseTopic()
			t.Topic = topicName
			t.TopicID = topic.TopicID
			t.Partitions = partitionResponses
			topicResponses = append(topicResponses, t)
			continue
		}
		partitionResponses := make([]kmsg.FetchResponseTopicPartition, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			if h.traceKafka {
				h.logger.Debug("fetch partition request", "topic", topicName, "partition", part.Partition, "fetch_offset", part.FetchOffset, "max_bytes", part.PartitionMaxBytes)
			}
			switch h.s3Health.State() {
			case broker.S3StateDegraded, broker.S3StateUnavailable:
				p := kmsg.NewFetchResponseTopicPartition()
				p.Partition = part.Partition
				p.ErrorCode = h.backpressureErrorCode()
				partitionResponses = append(partitionResponses, p)
				continue
			}
			plog, err := h.getPartitionLog(ctx, topicName, part.Partition)
			if err != nil {
				h.logger.Error("fetch partition log failed", "topic", topicName, "partition", part.Partition, "error", err, "etcd_available", h.etcdAvailable(), "s3_state", h.s3Health.State())
				errorCode := int16(protocol.UNKNOWN_SERVER_ERROR)
				if !h.etcdAvailable() {
					errorCode = protocol.REQUEST_TIMED_OUT
				}
				p := kmsg.NewFetchResponseTopicPartition()
				p.Partition = part.Partition
				p.ErrorCode = errorCode
				partitionResponses = append(partitionResponses, p)
				continue
			}
			nextOffset, offsetErr := h.waitForFetchData(ctx, topicName, part.Partition, part.FetchOffset, maxWait)
			if offsetErr != nil {
				if errors.Is(offsetErr, metadata.ErrUnknownTopic) {
					p := kmsg.NewFetchResponseTopicPartition()
					p.Partition = part.Partition
					p.ErrorCode = protocol.UNKNOWN_TOPIC_OR_PARTITION
					partitionResponses = append(partitionResponses, p)
					continue
				}
				if errors.Is(offsetErr, context.Canceled) || errors.Is(offsetErr, context.DeadlineExceeded) {
					offsetErr = nil
				} else if !h.etcdAvailable() {
					nextOffset = part.FetchOffset
					offsetErr = nil
				} else {
					h.logger.Error("fetch wait failed", "topic", topicName, "partition", part.Partition, "error", offsetErr, "etcd_available", h.etcdAvailable())
					nextOffset = 0
				}
			}
			errorCode := int16(0)
			var recordSet []byte
			switch {
			case part.FetchOffset > nextOffset:
				errorCode = protocol.OFFSET_OUT_OF_RANGE
			case part.FetchOffset == nextOffset:
				recordSet = nil
			default:
				recordSet, err = plog.Read(ctx, part.FetchOffset, part.PartitionMaxBytes)
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
			p := kmsg.NewFetchResponseTopicPartition()
			p.Partition = part.Partition
			p.ErrorCode = errorCode
			p.HighWatermark = highWatermark
			p.LastStableOffset = highWatermark
			p.LogStartOffset = 0
			p.PreferredReadReplica = -1
			p.RecordBatches = recordSet
			partitionResponses = append(partitionResponses, p)
		}
		t := kmsg.NewFetchResponseTopic()
		t.Topic = topicName
		t.TopicID = topic.TopicID
		t.Partitions = partitionResponses
		topicResponses = append(topicResponses, t)
	}

	if fetchedMessages > 0 {
		h.fetchRate.add(fetchedMessages)
	}

	resp := kmsg.NewPtrFetchResponse()
	resp.Topics = topicResponses
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp), nil
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
	h.logMu.RLock()
	if partitions, ok := h.logs[topic]; ok {
		if plog, ok := partitions[partition]; ok {
			h.logMu.RUnlock()
			return plog, nil
		}
	}
	h.logMu.RUnlock()

	// Requests for other partitions proceed in parallel; only one goroutine
	// per partition does the actual initialization.
	for {
		key := fmt.Sprintf("%s/%d", topic, partition)
		result, err, _ := h.logInit.Do(key, func() (interface{}, error) {
			// Double-check under read lock in case another goroutine just finished.
			h.logMu.RLock()
			if partitions, ok := h.logs[topic]; ok {
				if plog, ok := partitions[partition]; ok {
					h.logMu.RUnlock()
					return plog, nil
				}
			}
			h.logMu.RUnlock()

			// All I/O happens outside the lock.
			nextOffset, err := h.store.NextOffset(ctx, topic, partition)
			if err != nil {
				return nil, err
			}
			plog := storage.NewPartitionLog(h.s3Namespace, topic, partition, nextOffset, h.s3, h.cache, h.logConfig, func(cbCtx context.Context, artifact *storage.SegmentArtifact) {
				if err := h.store.UpdateOffsets(cbCtx, topic, partition, artifact.LastOffset); err != nil {
					h.logger.Error("update offsets failed", "error", err, "topic", topic, "partition", partition)
				}
			}, h.recordS3Op, h.s3sem)
			lastOffset, err := plog.RestoreFromS3(ctx)
			if err != nil {
				h.logger.Error("restore partition log from S3 failed", "topic", topic, "partition", partition, "error", err)
				return nil, err
			}
			if lastOffset >= nextOffset {
				if err := h.store.UpdateOffsets(ctx, topic, partition, lastOffset); err != nil {
					h.logger.Error("sync offsets from S3 failed", "error", err, "topic", topic, "partition", partition)
				}
			}

			h.logMu.Lock()
			if h.logs[topic] == nil {
				h.logs[topic] = make(map[int32]*storage.PartitionLog)
			}
			h.logs[topic][partition] = plog
			h.logMu.Unlock()

			return plog, nil
		})
		if err != nil {
			if errors.Is(err, metadata.ErrUnknownTopic) && h.autoCreateTopics {
				if err := h.ensureTopic(ctx, topic, partition); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		return result.(*storage.PartitionLog), nil
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
	allowAdminAPIs := parseEnvBool("KAFSCALE_ALLOW_ADMIN_APIS", true)
	traceKafka := parseEnvBool("KAFSCALE_TRACE_KAFKA", false)
	throughputWindow := time.Duration(parseEnvInt("KAFSCALE_THROUGHPUT_WINDOW_SEC", 60)) * time.Second
	s3Namespace := envOrDefault("KAFSCALE_S3_NAMESPACE", "default")
	segmentBytes := parseEnvInt("KAFSCALE_SEGMENT_BYTES", 4<<20)
	flushInterval := time.Duration(parseEnvInt("KAFSCALE_FLUSH_INTERVAL_MS", 500)) * time.Millisecond
	flushOnAck := parseEnvBool("KAFSCALE_PRODUCE_SYNC_FLUSH", true)
	// 0 or negative disables the S3 concurrency limit (no semaphore, default HTTP pool).
	s3Concurrency := parseEnvInt("KAFSCALE_S3_CONCURRENCY", defaultS3Concurrency)
	var s3sem *semaphore.Weighted
	if s3Concurrency > 0 {
		s3sem = semaphore.NewWeighted(int64(s3Concurrency))
	}
	produceLatencyBuckets := []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000}
	consumerLagBuckets := []float64{1, 10, 100, 1000, 5000, 10000, 50000, 100000, 500000, 1000000}
	if autoPartitions < 1 {
		autoPartitions = 1
	}
	health := broker.NewS3HealthMonitor(s3HealthConfigFromEnv())
	authorizer := buildAuthorizerFromEnv(logger)
	var leaseManager *metadata.PartitionLeaseManager
	var groupLeaseManager *metadata.GroupLeaseManager
	if etcdStore, ok := store.(*metadata.EtcdStore); ok {
		brokerIDStr := fmt.Sprintf("%d", brokerInfo.NodeID)
		leaseManager = metadata.NewPartitionLeaseManager(etcdStore.EtcdClient(), metadata.PartitionLeaseConfig{
			BrokerID: brokerIDStr,
			Logger:   logger,
		})
		groupLeaseManager = metadata.NewGroupLeaseManager(etcdStore.EtcdClient(), metadata.GroupLeaseConfig{
			BrokerID: brokerIDStr,
			Logger:   logger,
		})
	}
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
			Logger:            logger,
		},
		coordinator:          broker.NewGroupCoordinator(store, brokerInfo, nil),
		leaseManager:         leaseManager,
		groupLeaseManager:    groupLeaseManager,
		s3Health:             health,
		s3Namespace:          s3Namespace,
		brokerInfo:           brokerInfo,
		logger:               logger.With("component", "handler"),
		autoCreateTopics:     autoCreate,
		autoCreatePartitions: autoPartitions,
		allowAdminAPIs:       allowAdminAPIs,
		traceKafka:           traceKafka,
		produceRate:          newThroughputTracker(throughputWindow),
		fetchRate:            newThroughputTracker(throughputWindow),
		produceLatency:       newHistogram(produceLatencyBuckets),
		consumerLag:          newLagMetrics(consumerLagBuckets),
		startTime:            time.Now(),
		cpuTracker:           newCPUTracker(),
		cacheSize:            cacheSize,
		readAhead:            readAhead,
		segmentBytes:         segmentBytes,
		flushInterval:        flushInterval,
		flushOnAck:           flushOnAck,
		adminMetrics:         newAdminMetrics(),
		authorizer:           authorizer,
		authMetrics:          newAuthMetrics(),
		authLogLast:          make(map[string]time.Time),
		s3sem:                s3sem,
	}
}

func (h *handler) runStartupChecks(parent context.Context) error {
	timeout := startupTimeoutFromEnv()
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
	handler.startConsumerLagSampler(ctx)
	metricsAddr := envOrDefault("KAFSCALE_METRICS_ADDR", defaultMetricsAddr)
	controlAddr := envOrDefault("KAFSCALE_CONTROL_ADDR", defaultControlAddr)
	startMetricsServer(ctx, metricsAddr, handler, logger)
	startControlServer(ctx, controlAddr, handler, logger)
	kafkaAddr := envOrDefault("KAFSCALE_BROKER_ADDR", defaultKafkaAddr)
	srv := &broker.Server{
		Addr:            kafkaAddr,
		Handler:         handler,
		ConnContextFunc: buildConnContextFunc(logger),
	}
	if err := srv.ListenAndServe(ctx); err != nil {
		logger.Error("broker server error", "error", err)
		os.Exit(1)
	}
	// Release partition leases immediately on shutdown signal so other brokers
	// can take over while this broker drains in-flight requests. In-flight
	// requests already hold partition logs and don't need the etcd lease.
	if handler.leaseManager != nil {
		handler.leaseManager.ReleaseAll()
	}
	if handler.groupLeaseManager != nil {
		handler.groupLeaseManager.ReleaseAll()
	}
	srv.Wait()
}

func buildS3Client(ctx context.Context, logger *slog.Logger) storage.S3Client {
	writeCfg, readCfg, useMemory, credsProvided, useReadReplica := buildS3ConfigsFromEnv()
	if useMemory {
		logger.Info("using in-memory S3 client", "env", "KAFSCALE_USE_MEMORY_S3=1")
		return storage.NewMemoryS3Client()
	}

	client, err := storage.NewS3Client(ctx, writeCfg)
	if err != nil {
		logger.Error("failed to create S3 client; using in-memory", "error", err, "bucket", writeCfg.Bucket, "region", writeCfg.Region, "endpoint", writeCfg.Endpoint)
		return storage.NewMemoryS3Client()
	}

	if err := client.EnsureBucket(ctx); err != nil {
		logger.Error("failed to ensure S3 bucket", "bucket", writeCfg.Bucket, "error", err)
		os.Exit(1)
	}

	logger.Info("using AWS-compatible S3 client", "bucket", writeCfg.Bucket, "region", writeCfg.Region, "endpoint", writeCfg.Endpoint, "force_path_style", writeCfg.ForcePathStyle, "kms_configured", writeCfg.KMSKeyARN != "", "credentials_provided", credsProvided)

	if useReadReplica {
		readClient, err := storage.NewS3Client(ctx, readCfg)
		if err != nil {
			logger.Error("failed to create read S3 client; using write client", "error", err, "bucket", readCfg.Bucket, "region", readCfg.Region, "endpoint", readCfg.Endpoint)
			return client
		}
		logger.Info("using S3 read replica", "bucket", readCfg.Bucket, "region", readCfg.Region, "endpoint", readCfg.Endpoint)
		return newDualS3Client(client, readClient)
	}

	return client
}

func buildS3ConfigsFromEnv() (storage.S3Config, storage.S3Config, bool, bool, bool) {
	if parseEnvBool("KAFSCALE_USE_MEMORY_S3", false) {
		return storage.S3Config{}, storage.S3Config{}, true, false, false
	}
	writeBucket := os.Getenv("KAFSCALE_S3_BUCKET")
	writeRegion := os.Getenv("KAFSCALE_S3_REGION")
	writeEndpoint := os.Getenv("KAFSCALE_S3_ENDPOINT")
	forcePathStyle := parseEnvBool("KAFSCALE_S3_PATH_STYLE", writeEndpoint != "")
	kmsARN := os.Getenv("KAFSCALE_S3_KMS_ARN")
	accessKey := os.Getenv("KAFSCALE_S3_ACCESS_KEY")
	secretKey := os.Getenv("KAFSCALE_S3_SECRET_KEY")
	sessionToken := os.Getenv("KAFSCALE_S3_SESSION_TOKEN")
	credsProvided := accessKey != "" && secretKey != ""
	s3Concurrency := parseEnvInt("KAFSCALE_S3_CONCURRENCY", defaultS3Concurrency)
	writeCfg := storage.S3Config{
		Bucket:          writeBucket,
		Region:          writeRegion,
		Endpoint:        writeEndpoint,
		ForcePathStyle:  forcePathStyle,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
		KMSKeyARN:       kmsARN,
		MaxConnections:  s3Concurrency,
	}

	readBucket := os.Getenv("KAFSCALE_S3_READ_BUCKET")
	readRegion := os.Getenv("KAFSCALE_S3_READ_REGION")
	readEndpoint := os.Getenv("KAFSCALE_S3_READ_ENDPOINT")
	useReadReplica := readBucket != "" || readRegion != "" || readEndpoint != ""
	if readBucket == "" {
		readBucket = writeBucket
	}
	if readRegion == "" {
		readRegion = writeRegion
	}
	if readEndpoint == "" {
		readEndpoint = writeEndpoint
	}
	readCfg := storage.S3Config{
		Bucket:          readBucket,
		Region:          readRegion,
		Endpoint:        readEndpoint,
		ForcePathStyle:  forcePathStyle,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
		KMSKeyARN:       kmsARN,
		MaxConnections:  s3Concurrency,
	}
	return writeCfg, readCfg, false, credsProvided, useReadReplica
}

func buildConnContextFunc(logger *slog.Logger) broker.ConnContextFunc {
	source := strings.TrimSpace(os.Getenv("KAFSCALE_PRINCIPAL_SOURCE"))
	if source == "" {
		source = "client_id"
	}
	proxyProtocol := parseEnvBool("KAFSCALE_PROXY_PROTOCOL", false)
	if strings.EqualFold(source, "proxy_addr") {
		proxyProtocol = true
	}
	if strings.EqualFold(source, "client_id") && !proxyProtocol {
		if parseEnvBool("KAFSCALE_ACL_ENABLED", false) {
			if logger == nil {
				logger = slog.Default()
			}
			logger.Warn("ACL enabled with client_id principal source; client.id is spoofable without trusted edge auth")
		}
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	if parseEnvBool("KAFSCALE_ACL_ENABLED", false) && strings.EqualFold(source, "client_id") {
		logger.Warn("ACL enabled with client_id principal source; client.id is spoofable without trusted edge auth")
	}
	if proxyProtocol && strings.EqualFold(source, "client_id") {
		logger.Warn("proxy protocol enabled but principal source remains client_id; proxy identity is unused")
	}
	return func(conn net.Conn) (net.Conn, *broker.ConnContext, error) {
		info := &broker.ConnContext{}
		var proxyInfo *broker.ProxyInfo
		if proxyProtocol {
			wrapped, parsed, err := broker.ReadProxyProtocol(conn)
			if err != nil {
				logger.Warn("proxy protocol parse failed", "error", err)
				return conn, nil, err
			}
			if parsed == nil {
				return conn, nil, fmt.Errorf("proxy protocol required but header missing")
			}
			if parsed.Local {
				logger.Warn("proxy protocol local command received; using socket remote addr")
			}
			if wrapped != nil {
				conn = wrapped
			}
			proxyInfo = parsed
			if proxyInfo != nil {
				if !proxyInfo.Local && proxyInfo.SourceAddr != "" {
					info.ProxyAddr = proxyInfo.SourceAddr
					info.RemoteAddr = proxyInfo.SourceAddr
				}
			}
		}
		if info.RemoteAddr == "" {
			info.RemoteAddr = conn.RemoteAddr().String()
		}
		switch strings.ToLower(source) {
		case "remote_addr":
			info.Principal = hostFromAddr(info.RemoteAddr)
		case "proxy_addr":
			if proxyInfo != nil && proxyInfo.SourceAddr != "" {
				info.Principal = hostFromAddr(proxyInfo.SourceAddr)
			} else {
				info.Principal = hostFromAddr(info.RemoteAddr)
			}
		case "client_id":
			// Default; use client.id from the Kafka request header.
		default:
			logger.Warn("unknown principal source; defaulting to client_id", "source", source)
		}
		return conn, info, nil
	}
}

func hostFromAddr(addr string) string {
	if addr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

func buildAuthorizerFromEnv(logger *slog.Logger) *acl.Authorizer {
	if !parseEnvBool("KAFSCALE_ACL_ENABLED", false) {
		return acl.NewAuthorizer(acl.Config{Enabled: false})
	}
	failOpen := parseEnvBool("KAFSCALE_ACL_FAIL_OPEN", false)
	if logger == nil {
		logger = slog.Default()
	}
	var raw []byte
	if inline := strings.TrimSpace(os.Getenv("KAFSCALE_ACL_JSON")); inline != "" {
		raw = []byte(inline)
	} else if path := strings.TrimSpace(os.Getenv("KAFSCALE_ACL_FILE")); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			if failOpen {
				logger.Warn("failed to read ACL file; ACL disabled", "error", err)
				return acl.NewAuthorizer(acl.Config{Enabled: false})
			}
			logger.Warn("failed to read ACL file; default deny enabled", "error", err)
			return acl.NewAuthorizer(acl.Config{Enabled: true, DefaultPolicy: "deny"})
		}
		raw = data
	} else {
		if failOpen {
			logger.Warn("ACL enabled but no config provided; ACL disabled")
			return acl.NewAuthorizer(acl.Config{Enabled: false})
		}
		logger.Warn("ACL enabled but no config provided; default deny enabled")
		return acl.NewAuthorizer(acl.Config{Enabled: true, DefaultPolicy: "deny"})
	}
	var cfg acl.Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		if failOpen {
			logger.Warn("failed to parse ACL config; ACL disabled", "error", err)
			return acl.NewAuthorizer(acl.Config{Enabled: false})
		}
		logger.Warn("failed to parse ACL config; default deny enabled", "error", err)
		return acl.NewAuthorizer(acl.Config{Enabled: true, DefaultPolicy: "deny"})
	}
	cfg.Enabled = true
	return acl.NewAuthorizer(cfg)
}

func s3HealthConfigFromEnv() broker.S3HealthConfig {
	return broker.S3HealthConfig{
		Window:      time.Duration(parseEnvInt("KAFSCALE_S3_HEALTH_WINDOW_SEC", 60)) * time.Second,
		LatencyWarn: time.Duration(parseEnvInt("KAFSCALE_S3_LATENCY_WARN_MS", 500)) * time.Millisecond,
		LatencyCrit: time.Duration(parseEnvInt("KAFSCALE_S3_LATENCY_CRIT_MS", 3000)) * time.Millisecond,
		ErrorWarn:   parseEnvFloat("KAFSCALE_S3_ERROR_RATE_WARN", 0.2),
		ErrorCrit:   parseEnvFloat("KAFSCALE_S3_ERROR_RATE_CRIT", 0.6),
	}
}

func startupTimeoutFromEnv() time.Duration {
	return time.Duration(parseEnvInt("KAFSCALE_STARTUP_TIMEOUT_SEC", 30)) * time.Second
}

func metadataForBroker(b protocol.MetadataBroker) metadata.ClusterMetadata {
	clusterID := "kafscale-cluster"
	return metadata.ClusterMetadata{
		ControllerID: b.NodeID,
		ClusterID:    &clusterID,
		Brokers: []protocol.MetadataBroker{
			b,
		},
		Topics: []protocol.MetadataTopic{
			{
				ErrorCode:  0,
				Topic:      kmsg.StringPtr("orders"),
				TopicID:    metadata.TopicIDForName("orders"),
				IsInternal: false,
				Partitions: []protocol.MetadataPartition{
					{
						ErrorCode: 0,
						Partition: 0,
						Leader:    b.NodeID,
						Replicas:  []int32{b.NodeID},
						ISR:       []int32{b.NodeID},
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
	cfg, ok := etcdConfigFromEnv()
	if !ok {
		return metadata.NewInMemoryStore(meta)
	}
	store, err := metadata.NewEtcdStore(ctx, meta, cfg)
	if err != nil {
		logger.Error("failed to initialize etcd store; using in-memory", "error", err)
		return metadata.NewInMemoryStore(meta)
	}
	logger.Info("using etcd-backed metadata store", "endpoints", cfg.Endpoints)
	return store
}

func etcdConfigFromEnv() (metadata.EtcdStoreConfig, bool) {
	endpoints := strings.TrimSpace(os.Getenv("KAFSCALE_ETCD_ENDPOINTS"))
	if endpoints == "" {
		return metadata.EtcdStoreConfig{}, false
	}
	return metadata.EtcdStoreConfig{
		Endpoints: strings.Split(endpoints, ","),
		Username:  os.Getenv("KAFSCALE_ETCD_USERNAME"),
		Password:  os.Getenv("KAFSCALE_ETCD_PASSWORD"),
	}, true
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
	level := logLevelFromEnv()
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	})
	return slog.New(handler).With("component", "broker")
}

func logLevelFromEnv() slog.Level {
	level := slog.LevelWarn
	switch strings.ToLower(os.Getenv("KAFSCALE_LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	return level
}
func parseEnvInt(name string, fallback int) int {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			return parsed
		}
		warnDefaultEnv(name, val, fmt.Sprintf("%d", fallback))
		return fallback
	}
	warnDefaultEnv(name, "", fmt.Sprintf("%d", fallback))
	return fallback
}

func parseEnvInt32(name string, fallback int32) int32 {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		if parsed, err := strconv.ParseInt(val, 10, 32); err == nil {
			return int32(parsed)
		}
		warnDefaultEnv(name, val, fmt.Sprintf("%d", fallback))
		return fallback
	}
	warnDefaultEnv(name, "", fmt.Sprintf("%d", fallback))
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
	warnDefaultEnv(name, "", fallback)
	return fallback
}

func parseEnvFloat(name string, fallback float64) float64 {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			return parsed
		}
		warnDefaultEnv(name, val, fmt.Sprintf("%g", fallback))
		return fallback
	}
	warnDefaultEnv(name, "", fmt.Sprintf("%g", fallback))
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
		warnDefaultEnv(name, val, fmt.Sprintf("%t", fallback))
		return fallback
	}
	warnDefaultEnv(name, "", fmt.Sprintf("%t", fallback))
	return fallback
}

func warnDefaultEnv(name, value, fallback string) {
	if !isProdEnv() {
		return
	}
	if value == "" {
		slog.Warn("using default for unset env", "env", name, "default", fallback)
		return
	}
	slog.Warn("using default for invalid env", "env", name, "value", value, "default", fallback)
}

func isProdEnv() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("KAFSCALE_ENV")), "prod")
}

type apiVersionSupport struct {
	key                    int16
	minVersion, maxVersion int16
}

func generateApiVersions() []kmsg.ApiVersionsResponseApiKey {
	supported := []apiVersionSupport{
		{key: protocol.APIKeyApiVersion, minVersion: 0, maxVersion: 4},
		{key: protocol.APIKeyMetadata, minVersion: 0, maxVersion: 12},
		{key: protocol.APIKeyProduce, minVersion: 0, maxVersion: 9},
		{key: protocol.APIKeyFetch, minVersion: 11, maxVersion: 13},
		{key: protocol.APIKeyFindCoordinator, minVersion: 3, maxVersion: 3},
		{key: protocol.APIKeyListOffsets, minVersion: 0, maxVersion: 4},
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
		{key: protocol.APIKeyCreateTopics, minVersion: 0, maxVersion: 2},
		{key: protocol.APIKeyDeleteTopics, minVersion: 0, maxVersion: 2},
		{key: protocol.APIKeyDeleteGroups, minVersion: 0, maxVersion: 2},
	}
	unsupported := []int16{
		4, 5, 6, 7,
		21, 22,
		24, 25, 26,
	}

	entries := make([]kmsg.ApiVersionsResponseApiKey, 0, len(supported)+len(unsupported))
	for _, entry := range supported {
		entries = append(entries, kmsg.ApiVersionsResponseApiKey{
			ApiKey:     entry.key,
			MinVersion: entry.minVersion,
			MaxVersion: entry.maxVersion,
		})
	}
	for _, key := range unsupported {
		entries = append(entries, kmsg.ApiVersionsResponseApiKey{
			ApiKey:     key,
			MinVersion: -1,
			MaxVersion: -1,
		})
	}
	return entries
}

func buildBrokerInfo() protocol.MetadataBroker {
	id := resolveBrokerID()
	host := strings.TrimSpace(os.Getenv("KAFSCALE_BROKER_HOST"))
	port := parseEnvInt("KAFSCALE_BROKER_PORT", defaultKafkaPort)
	if addr := strings.TrimSpace(os.Getenv("KAFSCALE_BROKER_ADDR")); addr != "" {
		parsedHost, parsedPort := parseBrokerAddr(addr)
		if parsedHost != "" {
			host = parsedHost
		}
		port = parsedPort
	}
	if host == "" {
		if derived := deriveBrokerHost(); derived != "" {
			host = derived
		}
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

func resolveBrokerID() int32 {
	if val := strings.TrimSpace(os.Getenv("KAFSCALE_BROKER_ID")); val != "" {
		if parsed, err := strconv.ParseInt(val, 10, 32); err == nil {
			return int32(parsed)
		}
	}
	if ordinal, ok := podOrdinal(os.Getenv("POD_NAME")); ok {
		return ordinal
	}
	return 0
}

func deriveBrokerHost() string {
	podName := strings.TrimSpace(os.Getenv("POD_NAME"))
	namespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE"))
	service := strings.TrimSpace(os.Getenv("KAFSCALE_BROKER_SERVICE"))
	if podName == "" || namespace == "" || service == "" {
		return ""
	}
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local", podName, service, namespace)
}

func podOrdinal(name string) (int32, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, false
	}
	idx := strings.LastIndex(name, "-")
	if idx == -1 || idx == len(name)-1 {
		return 0, false
	}
	ord, err := strconv.ParseInt(name[idx+1:], 10, 32)
	if err != nil {
		return 0, false
	}
	return int32(ord), true
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
