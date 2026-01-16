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

//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/KafScale/platform/internal/mcpserver"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

func TestMCPServer(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "broker-1", Port: 9092},
		},
		ControllerID: 1,
		Topics: []protocol.MetadataTopic{
			{
				Name: "orders",
				Partitions: []protocol.MetadataPartition{
					{PartitionIndex: 0, LeaderID: 1},
					{PartitionIndex: 1, LeaderID: 1},
				},
			},
			{
				Name: "payments",
				Partitions: []protocol.MetadataPartition{
					{PartitionIndex: 0, LeaderID: 1},
				},
			},
		},
		ClusterID: stringPtr("kafscale-test"),
	})

	cfg := &metadatapb.TopicConfig{
		Name:              "orders",
		Partitions:        2,
		ReplicationFactor: 1,
		RetentionMs:       3600000,
		Config: map[string]string{
			"cleanup.policy": "delete",
		},
	}
	if err := store.UpdateTopicConfig(ctx, cfg); err != nil {
		t.Fatalf("UpdateTopicConfig: %v", err)
	}

	group := &metadatapb.ConsumerGroup{
		GroupId:      "group-1",
		State:        "Stable",
		ProtocolType: "consumer",
		Protocol:     "range",
		Leader:       "member-1",
		GenerationId: 1,
		Members: map[string]*metadatapb.GroupMember{
			"member-1": {
				ClientId:    "client-1",
				ClientHost:  "10.0.0.1",
				HeartbeatAt: time.Now().UTC().Format(time.RFC3339),
				Assignments: []*metadatapb.Assignment{
					{Topic: "orders", Partitions: []int32{0, 1}},
				},
				Subscriptions:    []string{"orders"},
				SessionTimeoutMs: 10000,
			},
		},
		RebalanceTimeoutMs: 60000,
	}
	if err := store.PutConsumerGroup(ctx, group); err != nil {
		t.Fatalf("PutConsumerGroup: %v", err)
	}

	if err := store.CommitConsumerOffset(ctx, "group-1", "orders", 0, 42, ""); err != nil {
		t.Fatalf("CommitConsumerOffset: %v", err)
	}
	if err := store.CommitConsumerOffset(ctx, "group-1", "orders", 1, 84, ""); err != nil {
		t.Fatalf("CommitConsumerOffset: %v", err)
	}

	server := mcpserver.NewServer(mcpserver.Options{
		Store:   store,
		Metrics: nil,
		Logger:  log.Default(),
		Version: "test",
	})
	mcpHandler := mcp.NewStreamableHTTPHandler(func(_ *http.Request) *mcp.Server {
		return server
	}, &mcp.StreamableHTTPOptions{})

	mux := http.NewServeMux()
	mux.Handle("/mcp", mcpHandler)
	mux.Handle("/mcp/", mcpHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := mcp.NewClient(&mcp.Implementation{Name: "mcp-test-client", Version: "test"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: srv.URL + "/mcp"}, nil)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer session.Close()

	status := callTool[clusterStatus](t, ctx, session, "cluster_status", nil)
	if status.ClusterID != "kafscale-test" {
		t.Fatalf("cluster_status cluster_id: %q", status.ClusterID)
	}
	if len(status.Brokers) != 1 {
		t.Fatalf("cluster_status brokers: %d", len(status.Brokers))
	}

	topicList := callTool[topicSummaryList](t, ctx, session, "list_topics", nil)
	if len(topicList.Topics) != 2 {
		t.Fatalf("list_topics count: %d", len(topicList.Topics))
	}

	topicDetails := callTool[topicDetailList](t, ctx, session, "describe_topics", map[string]any{
		"names": []string{"orders"},
	})
	if len(topicDetails.Topics) != 1 || topicDetails.Topics[0].Name != "orders" {
		t.Fatalf("describe_topics result: %#v", topicDetails)
	}
	if len(topicDetails.Topics[0].Partitions) != 2 {
		t.Fatalf("describe_topics partitions: %d", len(topicDetails.Topics[0].Partitions))
	}

	groups := callTool[groupSummaryList](t, ctx, session, "list_groups", nil)
	if len(groups.Groups) != 1 || groups.Groups[0].GroupID != "group-1" {
		t.Fatalf("list_groups result: %#v", groups)
	}

	groupDetails := callTool[groupDetails](t, ctx, session, "describe_group", map[string]any{
		"group_id": "group-1",
	})
	if groupDetails.GroupID != "group-1" {
		t.Fatalf("describe_group group_id: %q", groupDetails.GroupID)
	}
	if len(groupDetails.Members) != 1 {
		t.Fatalf("describe_group members: %d", len(groupDetails.Members))
	}

	offsets := callTool[fetchOffsetsOutput](t, ctx, session, "fetch_offsets", map[string]any{
		"group_id": "group-1",
		"topics":   []string{"orders"},
	})
	if len(offsets.Offsets) != 2 {
		t.Fatalf("fetch_offsets count: %d", len(offsets.Offsets))
	}

	configs := callTool[topicConfigList](t, ctx, session, "describe_configs", map[string]any{
		"topics": []string{"orders", "payments"},
	})
	if len(configs.Configs) != 2 {
		t.Fatalf("describe_configs count: %d", len(configs.Configs))
	}
	if !configExists(configs.Configs, "orders") {
		t.Fatalf("describe_configs orders missing")
	}
}

func TestMCPAuth(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	server := mcpserver.NewServer(mcpserver.Options{
		Store:   metadata.NewInMemoryStore(metadata.ClusterMetadata{}),
		Metrics: nil,
		Logger:  log.Default(),
		Version: "test",
	})

	mcpHandler := mcp.NewStreamableHTTPHandler(func(_ *http.Request) *mcp.Server {
		return server
	}, &mcp.StreamableHTTPOptions{})

	protected := mcpserver.RequireBearerToken("secret-token", mcpHandler)

	mux := http.NewServeMux()
	mux.Handle("/mcp", protected)
	mux.Handle("/mcp/", protected)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/mcp", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request without auth: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", resp.StatusCode)
	}

	authReq, err := http.NewRequest(http.MethodPost, srv.URL+"/mcp", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("build auth request: %v", err)
	}
	authReq.Header.Set("Authorization", "Bearer secret-token")
	authResp, err := http.DefaultClient.Do(authReq)
	if err != nil {
		t.Fatalf("request with auth: %v", err)
	}
	authResp.Body.Close()
	if authResp.StatusCode == http.StatusUnauthorized {
		t.Fatalf("expected non-401 with auth, got %d", authResp.StatusCode)
	}
}

type clusterStatus struct {
	ClusterID string         `json:"cluster_id"`
	Brokers   []brokerStatus `json:"brokers"`
}

type brokerStatus struct {
	NodeID int32 `json:"node_id"`
}

type topicSummary struct {
	Name           string `json:"name"`
	PartitionCount int    `json:"partition_count"`
}

type topicSummaryList struct {
	Topics []topicSummary `json:"topics"`
}

type topicDetail struct {
	Name       string             `json:"name"`
	Partitions []partitionDetails `json:"partitions"`
}

type topicDetailList struct {
	Topics []topicDetail `json:"topics"`
}

type partitionDetails struct {
	Partition int32 `json:"partition"`
}

type groupSummary struct {
	GroupID string `json:"group_id"`
}

type groupSummaryList struct {
	Groups []groupSummary `json:"groups"`
}

type groupDetails struct {
	GroupID string         `json:"group_id"`
	Members []memberDetail `json:"members"`
}

type memberDetail struct {
	MemberID string `json:"member_id"`
}

type fetchOffsetsOutput struct {
	Offsets []offsetDetails `json:"offsets"`
}

type offsetDetails struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

type topicConfigOutput struct {
	Name   string `json:"name"`
	Exists bool   `json:"exists"`
}

type topicConfigList struct {
	Configs []topicConfigOutput `json:"configs"`
}

func callTool[T any](t *testing.T, ctx context.Context, session *mcp.ClientSession, name string, args map[string]any) T {
	t.Helper()
	params := &mcp.CallToolParams{Name: name, Arguments: args}
	res, err := session.CallTool(ctx, params)
	if err != nil {
		t.Fatalf("CallTool %s: %v", name, err)
	}
	if res.IsError {
		t.Fatalf("CallTool %s returned error", name)
	}
	return decodeToolOutput[T](t, res)
}

func decodeToolOutput[T any](t *testing.T, res *mcp.CallToolResult) T {
	t.Helper()
	var out T
	if res.StructuredContent != nil {
		payload, err := json.Marshal(res.StructuredContent)
		if err != nil {
			t.Fatalf("marshal output: %v", err)
		}
		if err := json.Unmarshal(payload, &out); err != nil {
			t.Fatalf("unmarshal output: %v", err)
		}
		return out
	}
	if len(res.Content) > 0 {
		if text, ok := res.Content[0].(*mcp.TextContent); ok {
			if err := json.Unmarshal([]byte(text.Text), &out); err != nil {
				t.Fatalf("unmarshal text output: %v", err)
			}
			return out
		}
	}
	t.Fatalf("tool returned no output")
	return out
}

func stringPtr(val string) *string {
	return &val
}

func configExists(configs []topicConfigOutput, name string) bool {
	for _, cfg := range configs {
		if cfg.Name == name {
			return cfg.Exists
		}
	}
	return false
}
