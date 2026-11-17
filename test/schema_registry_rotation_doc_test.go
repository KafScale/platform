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

func TestSchemaRegistryS3RotationRestartDocumented(t *testing.T) {
	root := repoRoot(t)
	template, err := os.ReadFile(filepath.Join(root, "deploy", "helm", "kafscale", "templates", "schema-registry-deployment.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	tpl := string(template)
	if !strings.Contains(tpl, "app.kubernetes.io/component: schema-registry") || !strings.Contains(tpl, `"component" "schema-registry"`) {
		t.Fatal("schema registry Deployment no longer carries the schema-registry component label")
	}
	if !strings.Contains(tpl, "secretKeyRef:") || strings.Contains(tpl, "checksum") {
		t.Fatal("schema registry S3 key loading changed; update the rotation docs and this check")
	}
	want := []string{
		"DEPLOY=$(kubectl get deployment -n <namespace> -l app.kubernetes.io/instance=<release>,app.kubernetes.io/component=schema-registry -o name)",
		`kubectl rollout restart "$DEPLOY" -n <namespace>`,
		`kubectl rollout status "$DEPLOY" -n <namespace>`,
	}
	for _, doc := range []string{"operations.md", "schema-registry.md"} {
		body, err := os.ReadFile(filepath.Join(root, "docs", doc))
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		for _, line := range want {
			if !strings.Contains(text, line) {
				t.Errorf("docs/%s does not contain %q", doc, line)
			}
		}
		if strings.Contains(text, "rollout restart deployment/") {
			t.Errorf("docs/%s still restarts the registry by a fixed Deployment name", doc)
		}
	}
}
