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
	want := []string{
		"DEPLOY=$(kubectl get deployment -n '<namespace>' -l 'app.kubernetes.io/instance=<release>,app.kubernetes.io/component=schema-registry' -o name)",
		`kubectl rollout restart "$DEPLOY" -n '<namespace>'`,
		`kubectl rollout status "$DEPLOY" -n '<namespace>'`,
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
		checkRotationWording(t, "docs/"+doc, text)
	}
	notes, err := os.ReadFile(filepath.Join(root, "docs", "releases", "v2.0.0.md"))
	if err != nil {
		t.Fatal(err)
	}
	var note string
	for _, line := range strings.Split(string(notes), "\n") {
		if strings.Contains(line, "schemaRegistry.s3.credentialsSecret") && strings.Contains(line, "(#361)") {
			note = line
		}
	}
	if note == "" {
		t.Fatal("docs/releases/v2.0.0.md has no #361 note about schemaRegistry.s3.credentialsSecret")
	}
	checkRotationWording(t, "docs/releases/v2.0.0.md", note)
}

func checkRotationWording(t *testing.T, doc, text string) {
	t.Helper()
	text = strings.Join(strings.Fields(text), " ")
	if strings.Contains(text, "SignatureDoesNotMatch") && !strings.Contains(text, "for example `InvalidAccessKeyId` or `SignatureDoesNotMatch`") {
		t.Errorf("%s presents SignatureDoesNotMatch as the error that always comes back", doc)
	}
	if !strings.Contains(text, "only then revoke the old keys") && !strings.Contains(text, "Only then revoke the old keys") {
		t.Errorf("%s does not say to revoke the old keys only after the restart", doc)
	}
	for _, early := range []string{"Until the restart, every request", "Until then, every request", "Until the restart, requests"} {
		if strings.Contains(text, early) {
			t.Errorf("%s says S3 requests fail before the old keys are revoked or expire: %q", doc, early)
		}
	}
	if !strings.Contains(text, "keeps using the old keys") || !strings.Contains(text, "revoked") || !strings.Contains(text, "expire") {
		t.Errorf("%s does not say the registry keeps using the old keys until they are revoked or expire", doc)
	}
	if !strings.Contains(text, "no session token") {
		t.Errorf("%s does not say the chart passes no session token to the registry", doc)
	}
}

func TestApicurioImportDocMatchesSubjectLevelRule(t *testing.T) {
	text, err := os.ReadFile(filepath.Join(repoRoot(t), "docs", "schema-registry.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := strings.Join(strings.Fields(string(text)), " ")
	if strings.Contains(doc, "explicit `NONE` override remains unchecked even after you change the global default") {
		t.Error("docs/schema-registry.md says every subject NONE stays unchecked, but a subject level equal to the Apicurio global level is not copied")
	}
	if !strings.Contains(doc, "a subject set to `NONE` while the Apicurio global level is `NONE` follows the new global default") {
		t.Error("docs/schema-registry.md does not explain the Apicurio subject NONE case")
	}
}
