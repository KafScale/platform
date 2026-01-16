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

package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	metadatapb "github.com/novatechflow/kafscale/pkg/gen/metadata"
	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/protocol"
)

const (
	toolClusterStatus   = "cluster_status"
	toolClusterMetrics  = "cluster_metrics"
	toolListTopics      = "list_topics"
	toolDescribeTopics  = "describe_topics"
	toolListGroups      = "list_groups"
	toolDescribeGroup   = "describe_group"
	toolFetchOffsets    = "fetch_offsets"
	toolDescribeConfigs = "describe_configs"
)

type emptyInput struct{}

type TopicNameInput struct {
	Names []string `json:"names" jsonschema:"Optional list of topic names to filter"`
}

type GroupInput struct {
	GroupID string `json:"group_id" jsonschema:"Consumer group identifier"`
}

type FetchOffsetsInput struct {
	GroupID string   `json:"group_id" jsonschema:"Consumer group identifier"`
	Topics  []string `json:"topics" jsonschema:"Optional list of topics to fetch offsets for"`
}

type TopicConfigInput struct {
	Topics []string `json:"topics" jsonschema:"Optional list of topics to describe"`
}

type ClusterStatusOutput struct {
	ClusterName  string         `json:"cluster_name,omitempty"`
	ClusterID    string         `json:"cluster_id,omitempty"`
	ControllerID int32          `json:"controller_id"`
	Brokers      []BrokerOutput `json:"brokers"`
	Topics       []TopicSummary `json:"topics"`
	S3           S3Status       `json:"s3"`
	Etcd         ComponentState `json:"etcd"`
	ObservedAt   string         `json:"observed_at"`
}

type BrokerOutput struct {
	NodeID int32  `json:"node_id"`
	Host   string `json:"host"`
	Port   int32  `json:"port"`
}

type TopicSummary struct {
	Name           string `json:"name"`
	PartitionCount int    `json:"partition_count"`
	ErrorCode      int16  `json:"error_code"`
}

type TopicSummaryList struct {
	Topics []TopicSummary `json:"topics"`
}

type TopicDetail struct {
	Name       string             `json:"name"`
	ErrorCode  int16              `json:"error_code"`
	Partitions []PartitionDetails `json:"partitions"`
}

type TopicDetailList struct {
	Topics []TopicDetail `json:"topics"`
}

type PartitionDetails struct {
	Partition       int32   `json:"partition"`
	LeaderID        int32   `json:"leader_id"`
	LeaderEpoch     int32   `json:"leader_epoch"`
	ReplicaNodes    []int32 `json:"replica_nodes"`
	ISRNodes        []int32 `json:"isr_nodes"`
	OfflineReplicas []int32 `json:"offline_replicas"`
	ErrorCode       int16   `json:"error_code"`
}

type ClusterMetricsOutput struct {
	S3State                 string  `json:"s3_state"`
	S3LatencyMS             int     `json:"s3_latency_ms"`
	ProduceRPS              float64 `json:"produce_rps"`
	FetchRPS                float64 `json:"fetch_rps"`
	AdminRequestsTotal      float64 `json:"admin_requests_total"`
	AdminRequestErrorsTotal float64 `json:"admin_request_errors_total"`
	AdminRequestLatencyMS   float64 `json:"admin_request_latency_ms"`
	ObservedAt              string  `json:"observed_at"`
}

type S3Status struct {
	State     string `json:"state"`
	LatencyMS int    `json:"latency_ms"`
}

type ComponentState struct {
	State string `json:"state"`
}

type GroupSummary struct {
	GroupID      string `json:"group_id"`
	State        string `json:"state"`
	ProtocolType string `json:"protocol_type"`
	MemberCount  int    `json:"member_count"`
}

type GroupSummaryList struct {
	Groups []GroupSummary `json:"groups"`
}

type GroupDetails struct {
	GroupID            string          `json:"group_id"`
	State              string          `json:"state"`
	ProtocolType       string          `json:"protocol_type"`
	Protocol           string          `json:"protocol"`
	Leader             string          `json:"leader"`
	GenerationID       int32           `json:"generation_id"`
	RebalanceTimeoutMS int32           `json:"rebalance_timeout_ms"`
	Members            []MemberDetails `json:"members"`
}

type MemberDetails struct {
	MemberID         string           `json:"member_id"`
	ClientID         string           `json:"client_id"`
	ClientHost       string           `json:"client_host"`
	HeartbeatAt      string           `json:"heartbeat_at"`
	Assignments      []AssignmentInfo `json:"assignments"`
	Subscriptions    []string         `json:"subscriptions"`
	SessionTimeoutMS int32            `json:"session_timeout_ms"`
}

type AssignmentInfo struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

type FetchOffsetsOutput struct {
	GroupID string          `json:"group_id"`
	Offsets []OffsetDetails `json:"offsets"`
}

type OffsetDetails struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Metadata  string `json:"metadata"`
}

type TopicConfigOutput struct {
	Name              string            `json:"name"`
	Exists            bool              `json:"exists"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int32             `json:"replication_factor"`
	RetentionMS       int64             `json:"retention_ms"`
	RetentionBytes    int64             `json:"retention_bytes"`
	SegmentBytes      int64             `json:"segment_bytes"`
	CreatedAt         string            `json:"created_at"`
	Config            map[string]string `json:"config"`
}

type TopicConfigList struct {
	Configs []TopicConfigOutput `json:"configs"`
}

func registerTools(server *mcp.Server, opts Options) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        toolClusterStatus,
		Description: "Summarize cluster metadata, brokers, and topic counts",
	}, clusterStatusHandler(opts))

	mcp.AddTool(server, &mcp.Tool{
		Name:        toolClusterMetrics,
		Description: "Return broker metrics snapshot if metrics endpoint is configured",
	}, clusterMetricsHandler(opts))

	mcp.AddTool(server, &mcp.Tool{
		Name:        toolListTopics,
		Description: "List topics and partition counts",
	}, listTopicsHandler(opts))

	mcp.AddTool(server, &mcp.Tool{
		Name:        toolDescribeTopics,
		Description: "Describe topics and their partitions",
	}, describeTopicsHandler(opts))

	mcp.AddTool(server, &mcp.Tool{
		Name:        toolListGroups,
		Description: "List consumer groups",
	}, listGroupsHandler(opts))

	mcp.AddTool(server, &mcp.Tool{
		Name:        toolDescribeGroup,
		Description: "Describe a consumer group",
	}, describeGroupHandler(opts))

	mcp.AddTool(server, &mcp.Tool{
		Name:        toolFetchOffsets,
		Description: "Fetch committed offsets for a consumer group",
	}, fetchOffsetsHandler(opts))

	mcp.AddTool(server, &mcp.Tool{
		Name:        toolDescribeConfigs,
		Description: "Describe topic configs",
	}, describeConfigsHandler(opts))
}

func clusterStatusHandler(opts Options) mcp.ToolHandlerFor[emptyInput, ClusterStatusOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, ClusterStatusOutput, error) {
		store, err := requireStore(opts.Store)
		if err != nil {
			return nil, ClusterStatusOutput{}, err
		}
		meta, err := store.Metadata(ctx, nil)
		if err != nil {
			return nil, ClusterStatusOutput{}, err
		}
		status := ClusterStatusOutput{
			ControllerID: meta.ControllerID,
			Etcd:         ComponentState{State: "connected"},
			ObservedAt:   time.Now().UTC().Format(time.RFC3339),
		}
		if meta.ClusterName != nil {
			status.ClusterName = *meta.ClusterName
		}
		if meta.ClusterID != nil {
			status.ClusterID = *meta.ClusterID
		}
		status.Brokers = make([]BrokerOutput, 0, len(meta.Brokers))
		for _, broker := range meta.Brokers {
			status.Brokers = append(status.Brokers, BrokerOutput{
				NodeID: broker.NodeID,
				Host:   broker.Host,
				Port:   broker.Port,
			})
		}
		status.Topics = summarizeTopics(meta.Topics)

		if opts.Metrics != nil {
			if snap, snapErr := opts.Metrics.Snapshot(ctx); snapErr == nil && snap != nil {
				status.S3.State = snap.S3State
				status.S3.LatencyMS = snap.S3LatencyMS
			}
		}
		return nil, status, nil
	}
}

func clusterMetricsHandler(opts Options) mcp.ToolHandlerFor[emptyInput, ClusterMetricsOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, ClusterMetricsOutput, error) {
		if opts.Metrics == nil {
			return nil, ClusterMetricsOutput{}, errors.New("metrics provider not configured")
		}
		snap, err := opts.Metrics.Snapshot(ctx)
		if err != nil {
			return nil, ClusterMetricsOutput{}, err
		}
		if snap == nil {
			return nil, ClusterMetricsOutput{}, errors.New("metrics snapshot unavailable")
		}
		return nil, ClusterMetricsOutput{
			S3State:                 snap.S3State,
			S3LatencyMS:             snap.S3LatencyMS,
			ProduceRPS:              snap.ProduceRPS,
			FetchRPS:                snap.FetchRPS,
			AdminRequestsTotal:      snap.AdminRequestsTotal,
			AdminRequestErrorsTotal: snap.AdminRequestErrorsTotal,
			AdminRequestLatencyMS:   snap.AdminRequestLatencyMS,
			ObservedAt:              time.Now().UTC().Format(time.RFC3339),
		}, nil
	}
}

func listTopicsHandler(opts Options) mcp.ToolHandlerFor[emptyInput, TopicSummaryList] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, TopicSummaryList, error) {
		store, err := requireStore(opts.Store)
		if err != nil {
			return nil, TopicSummaryList{}, err
		}
		meta, err := store.Metadata(ctx, nil)
		if err != nil {
			return nil, TopicSummaryList{}, err
		}
		return nil, TopicSummaryList{Topics: summarizeTopics(meta.Topics)}, nil
	}
}

func describeTopicsHandler(opts Options) mcp.ToolHandlerFor[TopicNameInput, TopicDetailList] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, input TopicNameInput) (*mcp.CallToolResult, TopicDetailList, error) {
		store, err := requireStore(opts.Store)
		if err != nil {
			return nil, TopicDetailList{}, err
		}
		meta, err := store.Metadata(ctx, input.Names)
		if err != nil {
			return nil, TopicDetailList{}, err
		}
		items := make([]TopicDetail, 0, len(meta.Topics))
		for _, topic := range meta.Topics {
			items = append(items, toTopicDetail(topic))
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
		return nil, TopicDetailList{Topics: items}, nil
	}
}

func listGroupsHandler(opts Options) mcp.ToolHandlerFor[emptyInput, GroupSummaryList] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, GroupSummaryList, error) {
		store, err := requireStore(opts.Store)
		if err != nil {
			return nil, GroupSummaryList{}, err
		}
		groups, err := store.ListConsumerGroups(ctx)
		if err != nil {
			return nil, GroupSummaryList{}, err
		}
		out := make([]GroupSummary, 0, len(groups))
		for _, group := range groups {
			if group == nil {
				continue
			}
			out = append(out, GroupSummary{
				GroupID:      group.GroupId,
				State:        group.State,
				ProtocolType: group.ProtocolType,
				MemberCount:  len(group.Members),
			})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })
		return nil, GroupSummaryList{Groups: out}, nil
	}
}

func describeGroupHandler(opts Options) mcp.ToolHandlerFor[GroupInput, GroupDetails] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, input GroupInput) (*mcp.CallToolResult, GroupDetails, error) {
		if input.GroupID == "" {
			return nil, GroupDetails{}, errors.New("group_id is required")
		}
		store, err := requireStore(opts.Store)
		if err != nil {
			return nil, GroupDetails{}, err
		}
		group, err := store.FetchConsumerGroup(ctx, input.GroupID)
		if err != nil {
			return nil, GroupDetails{}, err
		}
		if group == nil {
			return nil, GroupDetails{}, fmt.Errorf("consumer group %q not found", input.GroupID)
		}
		return nil, toGroupDetails(group), nil
	}
}

func fetchOffsetsHandler(opts Options) mcp.ToolHandlerFor[FetchOffsetsInput, FetchOffsetsOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, input FetchOffsetsInput) (*mcp.CallToolResult, FetchOffsetsOutput, error) {
		if input.GroupID == "" {
			return nil, FetchOffsetsOutput{}, errors.New("group_id is required")
		}
		store, err := requireStore(opts.Store)
		if err != nil {
			return nil, FetchOffsetsOutput{}, err
		}
		meta, err := store.Metadata(ctx, input.Topics)
		if err != nil {
			return nil, FetchOffsetsOutput{}, err
		}
		out := FetchOffsetsOutput{GroupID: input.GroupID}
		for _, topic := range meta.Topics {
			for _, partition := range topic.Partitions {
				offset, metaText, err := store.FetchConsumerOffset(ctx, input.GroupID, topic.Name, partition.PartitionIndex)
				if err != nil {
					return nil, FetchOffsetsOutput{}, err
				}
				out.Offsets = append(out.Offsets, OffsetDetails{
					Topic:     topic.Name,
					Partition: partition.PartitionIndex,
					Offset:    offset,
					Metadata:  metaText,
				})
			}
		}
		sort.Slice(out.Offsets, func(i, j int) bool {
			if out.Offsets[i].Topic == out.Offsets[j].Topic {
				return out.Offsets[i].Partition < out.Offsets[j].Partition
			}
			return out.Offsets[i].Topic < out.Offsets[j].Topic
		})
		return nil, out, nil
	}
}

func describeConfigsHandler(opts Options) mcp.ToolHandlerFor[TopicConfigInput, TopicConfigList] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, input TopicConfigInput) (*mcp.CallToolResult, TopicConfigList, error) {
		store, err := requireStore(opts.Store)
		if err != nil {
			return nil, TopicConfigList{}, err
		}
		topics := input.Topics
		if len(topics) == 0 {
			meta, err := store.Metadata(ctx, nil)
			if err != nil {
				return nil, TopicConfigList{}, err
			}
			topics = make([]string, 0, len(meta.Topics))
			for _, topic := range meta.Topics {
				topics = append(topics, topic.Name)
			}
		}
		out := make([]TopicConfigOutput, 0, len(topics))
		for _, topic := range topics {
			cfg, err := store.FetchTopicConfig(ctx, topic)
			if err != nil {
				return nil, TopicConfigList{}, err
			}
			out = append(out, toTopicConfigOutput(topic, cfg))
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		return nil, TopicConfigList{Configs: out}, nil
	}
}

func requireStore(store metadata.Store) (metadata.Store, error) {
	if store == nil {
		return nil, errors.New("metadata store not configured")
	}
	return store, nil
}

func summarizeTopics(topics []protocol.MetadataTopic) []TopicSummary {
	out := make([]TopicSummary, 0, len(topics))
	for _, topic := range topics {
		out = append(out, TopicSummary{
			Name:           topic.Name,
			PartitionCount: len(topic.Partitions),
			ErrorCode:      topic.ErrorCode,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func toTopicDetail(topic protocol.MetadataTopic) TopicDetail {
	partitions := make([]PartitionDetails, 0, len(topic.Partitions))
	for _, partition := range topic.Partitions {
		partitions = append(partitions, PartitionDetails{
			Partition:       partition.PartitionIndex,
			LeaderID:        partition.LeaderID,
			LeaderEpoch:     partition.LeaderEpoch,
			ReplicaNodes:    copyInt32Slice(partition.ReplicaNodes),
			ISRNodes:        copyInt32Slice(partition.ISRNodes),
			OfflineReplicas: copyInt32Slice(partition.OfflineReplicas),
			ErrorCode:       partition.ErrorCode,
		})
	}
	return TopicDetail{
		Name:       topic.Name,
		ErrorCode:  topic.ErrorCode,
		Partitions: partitions,
	}
}

func copyInt32Slice(values []int32) []int32 {
	if len(values) == 0 {
		return []int32{}
	}
	return append([]int32(nil), values...)
}

func toGroupDetails(group *metadatapb.ConsumerGroup) GroupDetails {
	members := make([]MemberDetails, 0, len(group.Members))
	for memberID, member := range group.Members {
		assignments := make([]AssignmentInfo, 0, len(member.Assignments))
		for _, assignment := range member.Assignments {
			assignments = append(assignments, AssignmentInfo{
				Topic:      assignment.Topic,
				Partitions: append([]int32(nil), assignment.Partitions...),
			})
		}
		members = append(members, MemberDetails{
			MemberID:         memberID,
			ClientID:         member.ClientId,
			ClientHost:       member.ClientHost,
			HeartbeatAt:      member.HeartbeatAt,
			Assignments:      assignments,
			Subscriptions:    append([]string(nil), member.Subscriptions...),
			SessionTimeoutMS: member.SessionTimeoutMs,
		})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].MemberID < members[j].MemberID })
	return GroupDetails{
		GroupID:            group.GroupId,
		State:              group.State,
		ProtocolType:       group.ProtocolType,
		Protocol:           group.Protocol,
		Leader:             group.Leader,
		GenerationID:       group.GenerationId,
		RebalanceTimeoutMS: group.RebalanceTimeoutMs,
		Members:            members,
	}
}

func toTopicConfigOutput(name string, cfg *metadatapb.TopicConfig) TopicConfigOutput {
	if cfg == nil {
		return TopicConfigOutput{Name: name}
	}
	nameValue := cfg.Name
	if nameValue == "" {
		nameValue = name
	}
	return TopicConfigOutput{
		Name:              nameValue,
		Exists:            true,
		Partitions:        cfg.Partitions,
		ReplicationFactor: cfg.ReplicationFactor,
		RetentionMS:       cfg.RetentionMs,
		RetentionBytes:    cfg.RetentionBytes,
		SegmentBytes:      cfg.SegmentBytes,
		CreatedAt:         cfg.CreatedAt,
		Config:            cfg.Config,
	}
}
