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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMCPToolArgumentsDocumented(t *testing.T) {
	for _, doc := range []string{"docs/ops-mcp.md", "docs/mcp.md"} {
		text := readMCPDoc(t, doc)
		for _, want := range []string{
			"`describe_topics`: optional `names`",
			"`describe_group`: required `group_id`",
			"required `group_id`, optional `topics`",
			"`describe_configs`: optional `topics`",
			`invalid params: validating "arguments": validating root: required: missing properties: ["group_id"]`,
			"metrics provider not configured",
		} {
			if !strings.Contains(text, want) {
				t.Errorf("%s does not document %q", doc, want)
			}
		}
	}
}

func TestMCPOperatorEndpointsDocumented(t *testing.T) {
	text := readMCPDoc(t, "docs/ops-mcp.md")
	for _, want := range []string{
		"<cluster>-etcd-client",
		"<cluster>-broker",
		"demo-etcd-client",
		"demo-broker",
		"@modelcontextprotocol/inspector",
		"--method tools/list",
		"--method tools/call",
		"--tool-name describe_group",
		"Claude Desktop and IDE examples follow",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("docs/ops-mcp.md does not contain %q", want)
		}
	}
	if strings.Contains(text, "the same way as in the Claude Desktop") {
		t.Error("docs/ops-mcp.md still says Inspector auth works the same way as the Claude Desktop example")
	}
}

func readMCPDoc(t *testing.T, rel string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
