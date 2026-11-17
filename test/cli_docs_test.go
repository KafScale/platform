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

func TestCLIInstallAndCredentialsDocumented(t *testing.T) {
	ops := readDoc(t, "docs/operations.md")
	if !strings.Contains(ops, "go build -o bin/kafscale-cli ./cmd/kafscale-cli") {
		t.Error("docs/operations.md does not say how to build kafscale-cli")
	}
	for _, want := range []string{
		"KAFSCALE_S3_ACCESS_KEY",
		"KAFSCALE_S3_SECRET_KEY",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
	} {
		if !strings.Contains(ops, want) {
			t.Errorf("docs/operations.md does not list credential %s for the CLI", want)
		}
	}
	if !strings.Contains(ops, "RetireTopic") && !strings.Contains(ops, "rolls back") && !strings.Contains(ops, "rolled back") {
		t.Error("docs/operations.md does not say a failed restore leaves a deleted-topic record")
	}
	if !strings.Contains(ops, "operations.md#installing-kafscale-cli") && !strings.Contains(ops, "### Installing kafscale-cli") {
		t.Error("docs/operations.md is missing the Installing kafscale-cli section")
	}
	if !strings.Contains(ops, "If either is unset") {
		t.Error("docs/operations.md does not say the default chain is used if either static key is unset")
	}
	if !strings.Contains(ops, "listing the source segments") || !strings.Contains(ops, "list objects <prefix>:") || !strings.Contains(ops, "get credentials") {
		t.Error("docs/operations.md does not say a restore without credentials fails while listing source segments with list objects <prefix>: including get credentials")
	}
	if !strings.Contains(ops, "with no objects, or only those copied before the failure") {
		t.Error("docs/operations.md does not describe a rolled-back restore prefix accurately")
	}
	if !strings.Contains(ops, "`prune --topic <target> --force` removes the record, and every other deleted incarnation of that name.") {
		t.Error("docs/operations.md does not say prune --topic <target> --force removes every deleted incarnation of that name")
	}
	sr := readDoc(t, "docs/schema-registry.md")
	if !strings.Contains(sr, "go build -o bin/kafscale-cli ./cmd/kafscale-cli") && !strings.Contains(sr, "operations.md#installing-kafscale-cli") {
		t.Error("docs/schema-registry.md does not point at how to get kafscale-cli")
	}
}

func readDoc(t *testing.T, rel string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
