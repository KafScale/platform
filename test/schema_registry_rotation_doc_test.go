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
	"regexp"
	"strings"
	"testing"
)

func TestSchemaRegistryS3RotationRestartDocumented(t *testing.T) {
	root := repoRoot(t)
	chartDir := filepath.Join(root, "deploy", "helm", "kafscale")
	chartYAML, err := os.ReadFile(filepath.Join(chartDir, "Chart.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	match := regexp.MustCompile(`(?m)^name:\s*(\S+)\s*$`).FindStringSubmatch(string(chartYAML))
	if match == nil {
		t.Fatal("chart name not found in Chart.yaml")
	}
	template, err := os.ReadFile(filepath.Join(chartDir, "templates", "schema-registry-deployment.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	tpl := string(template)
	if !strings.Contains(tpl, `"component" "schema-registry"`) {
		t.Fatal("schema registry Deployment no longer uses the schema-registry component name")
	}
	if !strings.Contains(tpl, "secretKeyRef:") || strings.Contains(tpl, "checksum") {
		t.Fatal("schema registry S3 key loading changed; update the rotation docs and this check")
	}
	want := "kubectl rollout restart deployment/" + match[1] + "-schema-registry"
	for _, doc := range []string{"operations.md", "schema-registry.md"} {
		body, err := os.ReadFile(filepath.Join(root, "docs", doc))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), want) {
			t.Errorf("docs/%s does not tell operators to run %q after rotating the S3 secret", doc, want)
		}
	}
}
