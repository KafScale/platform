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

package mcpserver

import (
	"context"
	"testing"

	console "github.com/KafScale/platform/internal/console"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

// mockMetrics implements console.MetricsProvider for testing.
type mockMetrics struct {
	snap *console.MetricsSnapshot
	err  error
}

func (m *mockMetrics) Snapshot(_ context.Context) (*console.MetricsSnapshot, error) {
	return m.snap, m.err
}

func testStore() metadata.Store {
	clusterName := "test-cluster"
	clusterID := "test-id"
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 0, Host: "broker-0", Port: 9092},
			{NodeID: 1, Host: "broker-1", Port: 9092},
		},
		ControllerID: 0,
		ClusterName:  &clusterName,
		ClusterID:    &clusterID,
		Topics: []protocol.MetadataTopic{
			{
				Name: "orders",
				Partitions: []protocol.MetadataPartition{
					{PartitionIndex: 0, LeaderID: 0, ReplicaNodes: []int32{0, 1}, ISRNodes: []int32{0, 1}},
					{PartitionIndex: 1, LeaderID: 1, ReplicaNodes: []int32{0, 1}, ISRNodes: []int32{0, 1}},
				},
			},
			{
				Name: "events",
				Partitions: []protocol.MetadataPartition{
					{PartitionIndex: 0, LeaderID: 0, ReplicaNodes: []int32{0}, ISRNodes: []int32{0}},
				},
			},
		},
	})
	return store
}

// --- NewServer ---

func TestNewServer(t *testing.T) {
	store := testStore()
	server := NewServer(Options{Store: store, Version: "1.0.0"})
	if server == nil {
		t.Fatal("expected non-nil server")
	}
}

func TestNewServerDefaultVersion(t *testing.T) {
	server := NewServer(Options{})
	if server == nil {
		t.Fatal("expected non-nil server")
	}
}

// --- clusterStatusHandler ---

func TestClusterStatusHandlerNoStore(t *testing.T) {
	handler := clusterStatusHandler(Options{})
	_, _, err := handler(context.Background(), nil, emptyInput{})
	if err == nil {
		t.Fatal("expected error for nil store")
	}
}

func TestClusterStatusHandler(t *testing.T) {
	store := testStore()
	metrics := &mockMetrics{snap: &console.MetricsSnapshot{
		S3State:     "healthy",
		S3LatencyMS: 42,
	}}
	handler := clusterStatusHandler(Options{Store: store, Metrics: metrics})
	_, output, err := handler(context.Background(), nil, emptyInput{})
	if err != nil {
		t.Fatalf("clusterStatusHandler: %v", err)
	}
	if output.ClusterName != "test-cluster" {
		t.Fatalf("expected cluster name test-cluster, got %q", output.ClusterName)
	}
	if output.ClusterID != "test-id" {
		t.Fatalf("expected cluster id test-id, got %q", output.ClusterID)
	}
	if len(output.Brokers) != 2 {
		t.Fatalf("expected 2 brokers, got %d", len(output.Brokers))
	}
	if len(output.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(output.Topics))
	}
	if output.S3.State != "healthy" {
		t.Fatalf("expected S3 state healthy, got %q", output.S3.State)
	}
	if output.S3.LatencyMS != 42 {
		t.Fatalf("expected S3 latency 42, got %d", output.S3.LatencyMS)
	}
	if output.ObservedAt == "" {
		t.Fatal("expected observed_at to be set")
	}
}

func TestClusterStatusHandlerNoMetrics(t *testing.T) {
	store := testStore()
	handler := clusterStatusHandler(Options{Store: store})
	_, output, err := handler(context.Background(), nil, emptyInput{})
	if err != nil {
		t.Fatalf("clusterStatusHandler: %v", err)
	}
	if output.S3.State != "" {
		t.Fatalf("expected empty S3 state without metrics, got %q", output.S3.State)
	}
}

// --- clusterMetricsHandler ---

func TestClusterMetricsHandlerNoMetrics(t *testing.T) {
	handler := clusterMetricsHandler(Options{})
	_, _, err := handler(context.Background(), nil, emptyInput{})
	if err == nil {
		t.Fatal("expected error for nil metrics")
	}
}

func TestClusterMetricsHandlerNilSnapshot(t *testing.T) {
	handler := clusterMetricsHandler(Options{Metrics: &mockMetrics{}})
	_, _, err := handler(context.Background(), nil, emptyInput{})
	if err == nil {
		t.Fatal("expected error for nil snapshot")
	}
}

func TestClusterMetricsHandler(t *testing.T) {
	metrics := &mockMetrics{snap: &console.MetricsSnapshot{
		S3State:     "healthy",
		S3LatencyMS: 10,
		ProduceRPS:  100.5,
		FetchRPS:    50.2,
	}}
	handler := clusterMetricsHandler(Options{Metrics: metrics})
	_, output, err := handler(context.Background(), nil, emptyInput{})
	if err != nil {
		t.Fatalf("clusterMetricsHandler: %v", err)
	}
	if output.S3State != "healthy" {
		t.Fatalf("expected healthy, got %q", output.S3State)
	}
	if output.ProduceRPS != 100.5 {
		t.Fatalf("expected 100.5, got %f", output.ProduceRPS)
	}
	if output.ObservedAt == "" {
		t.Fatal("expected observed_at set")
	}
}

// --- listTopicsHandler ---

func TestListTopicsHandlerNoStore(t *testing.T) {
	handler := listTopicsHandler(Options{})
	_, _, err := handler(context.Background(), nil, emptyInput{})
	if err == nil {
		t.Fatal("expected error for nil store")
	}
}

func TestListTopicsHandler(t *testing.T) {
	store := testStore()
	handler := listTopicsHandler(Options{Store: store})
	_, output, err := handler(context.Background(), nil, emptyInput{})
	if err != nil {
		t.Fatalf("listTopicsHandler: %v", err)
	}
	if len(output.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(output.Topics))
	}
	// Should be sorted
	if output.Topics[0].Name != "events" || output.Topics[1].Name != "orders" {
		t.Fatalf("expected sorted topics, got %+v", output.Topics)
	}
}

// --- describeTopicsHandler ---

func TestDescribeTopicsHandlerNoStore(t *testing.T) {
	handler := describeTopicsHandler(Options{})
	_, _, err := handler(context.Background(), nil, TopicNameInput{})
	if err == nil {
		t.Fatal("expected error for nil store")
	}
}

func TestDescribeTopicsHandler(t *testing.T) {
	store := testStore()
	handler := describeTopicsHandler(Options{Store: store})
	_, output, err := handler(context.Background(), nil, TopicNameInput{Names: []string{"orders"}})
	if err != nil {
		t.Fatalf("describeTopicsHandler: %v", err)
	}
	if len(output.Topics) != 1 || output.Topics[0].Name != "orders" {
		t.Fatalf("expected orders topic, got %+v", output.Topics)
	}
	if len(output.Topics[0].Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(output.Topics[0].Partitions))
	}
	if len(output.Topics[0].Partitions[0].ReplicaNodes) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(output.Topics[0].Partitions[0].ReplicaNodes))
	}
}

func TestDescribeTopicsHandlerAll(t *testing.T) {
	store := testStore()
	handler := describeTopicsHandler(Options{Store: store})
	_, output, err := handler(context.Background(), nil, TopicNameInput{})
	if err != nil {
		t.Fatalf("describeTopicsHandler all: %v", err)
	}
	if len(output.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(output.Topics))
	}
}

// --- listGroupsHandler ---

func TestListGroupsHandlerNoStore(t *testing.T) {
	handler := listGroupsHandler(Options{})
	_, _, err := handler(context.Background(), nil, emptyInput{})
	if err == nil {
		t.Fatal("expected error for nil store")
	}
}

func TestListGroupsHandler(t *testing.T) {
	store := testStore()
	// Add a consumer group
	_ = store.(*metadata.InMemoryStore).PutConsumerGroup(context.Background(), &metadatapb.ConsumerGroup{
		GroupId:      "group-1",
		State:        "stable",
		ProtocolType: "consumer",
		Members: map[string]*metadatapb.GroupMember{
			"m1": {Subscriptions: []string{"orders"}},
		},
	})

	handler := listGroupsHandler(Options{Store: store})
	_, output, err := handler(context.Background(), nil, emptyInput{})
	if err != nil {
		t.Fatalf("listGroupsHandler: %v", err)
	}
	if len(output.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(output.Groups))
	}
	if output.Groups[0].GroupID != "group-1" {
		t.Fatalf("expected group-1, got %q", output.Groups[0].GroupID)
	}
	if output.Groups[0].MemberCount != 1 {
		t.Fatalf("expected 1 member, got %d", output.Groups[0].MemberCount)
	}
}

// --- describeGroupHandler ---

func TestDescribeGroupHandlerNoGroupID(t *testing.T) {
	store := testStore()
	handler := describeGroupHandler(Options{Store: store})
	_, _, err := handler(context.Background(), nil, GroupInput{})
	if err == nil {
		t.Fatal("expected error for empty group_id")
	}
}

func TestDescribeGroupHandlerNotFound(t *testing.T) {
	store := testStore()
	handler := describeGroupHandler(Options{Store: store})
	_, _, err := handler(context.Background(), nil, GroupInput{GroupID: "nonexistent"})
	if err == nil {
		t.Fatal("expected error for missing group")
	}
}

func TestDescribeGroupHandler(t *testing.T) {
	store := testStore()
	_ = store.(*metadata.InMemoryStore).PutConsumerGroup(context.Background(), &metadatapb.ConsumerGroup{
		GroupId:      "group-1",
		State:        "stable",
		ProtocolType: "consumer",
		Protocol:     "range",
		Leader:       "m1",
		GenerationId: 5,
		Members: map[string]*metadatapb.GroupMember{
			"m1": {
				ClientId:      "client-1",
				Subscriptions: []string{"orders"},
				Assignments:   []*metadatapb.Assignment{{Topic: "orders", Partitions: []int32{0, 1}}},
			},
		},
	})

	handler := describeGroupHandler(Options{Store: store})
	_, output, err := handler(context.Background(), nil, GroupInput{GroupID: "group-1"})
	if err != nil {
		t.Fatalf("describeGroupHandler: %v", err)
	}
	if output.GroupID != "group-1" {
		t.Fatalf("expected group-1, got %q", output.GroupID)
	}
	if output.GenerationID != 5 {
		t.Fatalf("expected generation 5, got %d", output.GenerationID)
	}
	if len(output.Members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(output.Members))
	}
}

// --- fetchOffsetsHandler ---

func TestFetchOffsetsHandlerNoGroupID(t *testing.T) {
	store := testStore()
	handler := fetchOffsetsHandler(Options{Store: store})
	_, _, err := handler(context.Background(), nil, FetchOffsetsInput{})
	if err == nil {
		t.Fatal("expected error for empty group_id")
	}
}

func TestFetchOffsetsHandler(t *testing.T) {
	store := testStore()
	ctx := context.Background()
	// Commit some offsets
	_ = store.CommitConsumerOffset(ctx, "g1", "orders", 0, 100, "meta-0")
	_ = store.CommitConsumerOffset(ctx, "g1", "orders", 1, 200, "meta-1")

	handler := fetchOffsetsHandler(Options{Store: store})
	_, output, err := handler(ctx, nil, FetchOffsetsInput{GroupID: "g1", Topics: []string{"orders"}})
	if err != nil {
		t.Fatalf("fetchOffsetsHandler: %v", err)
	}
	if output.GroupID != "g1" {
		t.Fatalf("expected g1, got %q", output.GroupID)
	}
	if len(output.Offsets) != 2 {
		t.Fatalf("expected 2 offsets, got %d", len(output.Offsets))
	}
	// Should be sorted by topic then partition
	if output.Offsets[0].Partition != 0 || output.Offsets[1].Partition != 1 {
		t.Fatalf("expected sorted offsets, got %+v", output.Offsets)
	}
	if output.Offsets[0].Offset != 100 {
		t.Fatalf("expected offset 100, got %d", output.Offsets[0].Offset)
	}
}

// --- describeConfigsHandler ---

func TestDescribeConfigsHandlerNoStore(t *testing.T) {
	handler := describeConfigsHandler(Options{})
	_, _, err := handler(context.Background(), nil, TopicConfigInput{})
	if err == nil {
		t.Fatal("expected error for nil store")
	}
}

func TestDescribeConfigsHandler(t *testing.T) {
	store := testStore()
	handler := describeConfigsHandler(Options{Store: store})
	_, output, err := handler(context.Background(), nil, TopicConfigInput{Topics: []string{"orders"}})
	if err != nil {
		t.Fatalf("describeConfigsHandler: %v", err)
	}
	if len(output.Configs) != 1 {
		t.Fatalf("expected 1 config, got %d", len(output.Configs))
	}
	if output.Configs[0].Name != "orders" {
		t.Fatalf("expected orders, got %q", output.Configs[0].Name)
	}
}

func TestDescribeConfigsHandlerAllTopics(t *testing.T) {
	store := testStore()
	handler := describeConfigsHandler(Options{Store: store})
	_, output, err := handler(context.Background(), nil, TopicConfigInput{})
	if err != nil {
		t.Fatalf("describeConfigsHandler all: %v", err)
	}
	if len(output.Configs) != 2 {
		t.Fatalf("expected 2 configs, got %d", len(output.Configs))
	}
}

// --- toTopicDetail ---

func TestToTopicDetail(t *testing.T) {
	topic := protocol.MetadataTopic{
		Name:      "orders",
		ErrorCode: 0,
		Partitions: []protocol.MetadataPartition{
			{
				PartitionIndex:  0,
				LeaderID:        0,
				LeaderEpoch:     5,
				ReplicaNodes:    []int32{0, 1},
				ISRNodes:        []int32{0, 1},
				OfflineReplicas: []int32{},
			},
		},
	}
	detail := toTopicDetail(topic)
	if detail.Name != "orders" {
		t.Fatalf("expected orders, got %q", detail.Name)
	}
	if len(detail.Partitions) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(detail.Partitions))
	}
	if detail.Partitions[0].LeaderEpoch != 5 {
		t.Fatalf("expected epoch 5, got %d", detail.Partitions[0].LeaderEpoch)
	}
}

// --- toTopicConfigOutput ---

func TestToTopicConfigOutputNil(t *testing.T) {
	out := toTopicConfigOutput("orders", nil)
	if out.Name != "orders" {
		t.Fatalf("expected orders, got %q", out.Name)
	}
	if out.Exists {
		t.Fatal("expected exists=false for nil config")
	}
}

func TestToTopicConfigOutput(t *testing.T) {
	cfg := &metadatapb.TopicConfig{
		Name:              "orders",
		Partitions:        3,
		ReplicationFactor: 2,
		RetentionMs:       86400000,
		RetentionBytes:    -1,
		SegmentBytes:      1073741824,
		CreatedAt:         "2025-01-01T00:00:00Z",
		Config:            map[string]string{"cleanup.policy": "delete"},
	}
	out := toTopicConfigOutput("orders", cfg)
	if !out.Exists {
		t.Fatal("expected exists=true")
	}
	if out.Partitions != 3 {
		t.Fatalf("expected 3 partitions, got %d", out.Partitions)
	}
	if out.Config["cleanup.policy"] != "delete" {
		t.Fatal("expected config key")
	}
}

func TestToTopicConfigOutputEmptyName(t *testing.T) {
	cfg := &metadatapb.TopicConfig{
		Name:       "",
		Partitions: 1,
	}
	out := toTopicConfigOutput("fallback", cfg)
	if out.Name != "fallback" {
		t.Fatalf("expected fallback name, got %q", out.Name)
	}
}

// --- copyInt32Slice empty ---

func TestCopyInt32SliceEmpty(t *testing.T) {
	out := copyInt32Slice(nil)
	if out == nil || len(out) != 0 {
		t.Fatalf("expected empty non-nil slice for nil input, got %v", out)
	}
	out2 := copyInt32Slice([]int32{})
	if out2 == nil || len(out2) != 0 {
		t.Fatalf("expected empty non-nil slice for empty input, got %v", out2)
	}
}
