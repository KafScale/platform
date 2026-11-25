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

	"sigs.k8s.io/yaml"
)

func TestCodeQLRequiresAllLanguagesAndOfficialUpload(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoRoot(t), ".github/workflows/codeql.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			If          string
			Permissions map[string]string
			Strategy    struct{ Matrix struct{ Language []string } }
			Steps       []struct {
				Uses, Run, If string
				Continue      bool `json:"continue-on-error"`
				With          map[string]any
			}
		}
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	job := workflow.Jobs["analyze"]
	if job.If != "" || job.Permissions["security-events"] != "write" {
		t.Fatal("security analysis must run and upload")
	}
	if strings.Join(job.Strategy.Matrix.Language, ",") != "go,javascript-typescript,python,actions" {
		t.Fatal("all four languages must be scanned")
	}
	var analyze bool
	for _, step := range job.Steps {
		if step.Continue {
			t.Fatal("security workflow must not ignore failures")
		}
		if strings.HasPrefix(step.Uses, "github/codeql-action/init@") {
			queries, _ := step.With["queries"].(string)
			if !strings.Contains(queries, "security-extended") || !strings.Contains(queries, "security-and-quality") {
				t.Fatal("security query suites must remain enabled")
			}
		}
		if strings.HasPrefix(step.Uses, "github/codeql-action/analyze@") {
			analyze = true
			if step.If != "" || step.With["upload"] != true {
				t.Fatal("official analysis and upload must be required")
			}
		}
		if strings.Contains(step.Run, "upload_codeql_results") {
			t.Fatal("use the official upload action")
		}
	}
	if !analyze {
		t.Fatal("official CodeQL analysis is missing")
	}
}
