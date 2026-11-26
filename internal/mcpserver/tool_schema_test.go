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
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestToolInputSchemasRequiredFields(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := connectMCPClient(t, ctx, Options{Store: testStore()})
	defer func() { _ = client.Close() }()
	listed, err := client.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantRequired := map[string][]string{
		"describe_topics":  {},
		"describe_configs": {},
		"fetch_offsets":    {"group_id"},
		"describe_group":   {"group_id"},
	}
	seen := map[string]bool{}
	for _, tool := range listed.Tools {
		want, ok := wantRequired[tool.Name]
		if !ok {
			continue
		}
		seen[tool.Name] = true
		got := schemaRequired(t, tool.InputSchema)
		if !slices.Equal(got, want) {
			t.Errorf("%s required = %v, want %v", tool.Name, got, want)
		}
	}
	for name := range wantRequired {
		if !seen[name] {
			t.Errorf("ListTools did not return %s", name)
		}
	}
}

func TestDescribeTopicsThroughMCPListsEveryTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := connectMCPClient(t, ctx, Options{Store: testStore()})
	defer func() { _ = client.Close() }()
	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: toolDescribeTopics, Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("MCP returned tool error: %+v", result)
	}
	raw, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var got TopicDetailList
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, topic := range got.Topics {
		names[topic.Name] = true
	}
	for _, want := range []string{"orders", "events"} {
		if !names[want] {
			t.Errorf("describe_topics without names omitted %s: %s", want, raw)
		}
	}
	if len(got.Topics) != 2 {
		t.Errorf("describe_topics without names returned %d topics, want 2: %s", len(got.Topics), raw)
	}
}

func TestDescribeGroupMissingGroupIDError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := connectMCPClient(t, ctx, Options{Store: testStore()})
	defer func() { _ = client.Close() }()
	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: toolDescribeGroup, Arguments: map[string]any{}})
	if err == nil {
		t.Fatalf("describe_group without group_id succeeded: %+v", result)
	}
	want := `invalid params: validating "arguments": validating root: required: missing properties: ["group_id"]`
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("describe_group without group_id error %q does not contain %q", err, want)
	}
}

func connectMCPClient(t *testing.T, ctx context.Context, opts Options) *mcp.ClientSession {
	t.Helper()
	st, ct := mcp.NewInMemoryTransports()
	server, err := NewServer(opts).Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	client, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func schemaRequired(t *testing.T, schema any) []string {
	t.Helper()
	raw, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	var parsed struct {
		Required []string `json:"required"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.Required == nil {
		return []string{}
	}
	return parsed.Required
}
