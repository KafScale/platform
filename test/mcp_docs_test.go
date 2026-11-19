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

package test

import (
	"strings"
	"testing"
)

func TestMCPToolArgumentsDocumented(t *testing.T) {
	text := readRepoDoc(t, "docs/ops-mcp.md")
	for _, want := range []string{
		"`describe_topics`: optional `names`",
		"`describe_group`: required `group_id`",
		"required `group_id`, optional `topics`",
		"`describe_configs`: optional `topics`",
		`invalid params: validating "arguments": validating root: required: missing properties: ["group_id"]`,
		"metrics provider not configured",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("docs/ops-mcp.md does not document %q", want)
		}
	}
}

func TestMCPOperatorEndpointsDocumented(t *testing.T) {
	text := readRepoDoc(t, "docs/ops-mcp.md")
	for _, want := range []string{
		"<cluster>-etcd-client",
		"<cluster>-broker",
		"demo-etcd-client",
		"demo-broker",
		"@modelcontextprotocol/inspector",
		"--method tools/list",
		"--method tools/call",
		"--tool-name describe_group",
		"--tool-arg 'group_id=<group>'",
		"--config mcp-inspector.json",
		"port-forward svc/kafscale-mcp 8090:80",
		"read every broker registered in etcd",
		"Claude Desktop and IDE examples follow",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("docs/ops-mcp.md does not contain %q", want)
		}
	}
	for _, bad := range []string{"Bearer <token>\" --method", "svc/<mcp-service>", "tag: v1.6.3", "KAFSCALE_MCP_AUTH_TOKEN=\"<"} {
		if strings.Contains(text, bad) {
			t.Errorf("docs/ops-mcp.md still contains %q", bad)
		}
	}
	if strings.Contains(text, "the same way as in the Claude Desktop") {
		t.Error("docs/ops-mcp.md still says Inspector auth works the same way as the Claude Desktop example")
	}
}

func TestMCPEnvVarsInOperationsIndex(t *testing.T) {
	text := readRepoDoc(t, "docs/operations.md")
	start := strings.Index(text, "### MCP server")
	if start < 0 {
		t.Fatal("docs/operations.md has no MCP server section in the environment variable index")
	}
	section := text[start:]
	if end := strings.Index(section[1:], "\n### "); end >= 0 {
		section = section[:end+1]
	}
	for _, env := range []string{
		"KAFSCALE_MCP_HTTP_ADDR",
		"KAFSCALE_MCP_ETCD_ENDPOINTS",
		"KAFSCALE_MCP_ETCD_USERNAME",
		"KAFSCALE_MCP_ETCD_PASSWORD",
		"KAFSCALE_MCP_BROKER_METRICS_URL",
		"KAFSCALE_MCP_SESSION_TIMEOUT",
		"KAFSCALE_MCP_AUTH_TOKEN",
	} {
		if !strings.Contains(section, env) {
			t.Errorf("MCP server section of the environment variable index does not document %s", env)
		}
	}
}
