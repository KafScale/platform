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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestCodeQLUploadRetriesRemainMandatory(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failures int
		attempts int
		success  bool
	}{{"immediate", 0, 1, true}, {"transient", 2, 3, true}, {"persistent", 9, 3, false}} {
		t.Run(tc.name, func(t *testing.T) {
			work := t.TempDir()
			reportDir := filepath.Join(work, "reports")
			if err := os.Mkdir(reportDir, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(reportDir, "go.sarif"), []byte(`{"version":"2.1.0","runs":[]}`), 0644); err != nil {
				t.Fatal(err)
			}
			fake := `#!/usr/bin/env python3
import json, os, pathlib, sys
capture=pathlib.Path(os.environ["UPLOAD_CAPTURE"])
rows=json.loads(capture.read_text()) if capture.exists() else []
rows.append(sys.argv[1:])
capture.write_text(json.dumps(rows))
sys.exit(1 if len(rows) <= int(os.environ["UPLOAD_FAILURES"]) else 0)
`
			cli := filepath.Join(work, "codeql")
			if err := os.WriteFile(cli, []byte(fake), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(work, "sleep"), []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
				t.Fatal(err)
			}
			capture := filepath.Join(work, "capture.json")
			cmd := exec.Command("bash", "scripts/upload_codeql_results.sh", reportDir)
			cmd.Dir = repoRoot(t)
			cmd.Env = append(os.Environ(), "PATH="+work+string(os.PathListSeparator)+os.Getenv("PATH"), "CODEQL_PATH="+cli, "GITHUB_TOKEN=upload-test-token", "GITHUB_REPOSITORY=KafScale/platform", "GITHUB_REF=refs/pull/435/merge", "GITHUB_SHA=1234567890", "GITHUB_WORKSPACE="+work, "UPLOAD_CAPTURE="+capture, "UPLOAD_FAILURES="+fmt.Sprint(tc.failures))
			output, err := cmd.CombinedOutput()
			if (err == nil) != tc.success {
				t.Fatalf("exit=%v output=%s", err, output)
			}
			if strings.Contains(string(output), "upload-test-token") {
				t.Fatal("token leaked")
			}
			data, err := os.ReadFile(capture)
			if err != nil {
				t.Fatal(err)
			}
			var calls [][]string
			if err := json.Unmarshal(data, &calls); err != nil {
				t.Fatal(err)
			}
			if len(calls) != tc.attempts {
				t.Fatalf("attempts=%d", len(calls))
			}
			for _, args := range calls {
				command := strings.Join(args, " ")
				for _, want := range []string{"github upload-results", "--ref=refs/pull/435/merge", "--commit=1234567890", "--wait-for-processing-timeout=180", "--repository=KafScale/platform"} {
					if !strings.Contains(command, want) {
						t.Fatalf("missing %s in %s", want, command)
					}
				}
				if strings.Contains(command, "--no-wait-for-processing") || strings.Contains(command, "upload-test-token") {
					t.Fatal("processing or token handling bypassed")
				}
			}
		})
	}
}

func TestCodeQLWorkflowKeepsAnalysisAndUploadRequired(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoRoot(t), ".github/workflows/codeql.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				Name, Uses, Run string
				Continue        bool `json:"continue-on-error"`
				With            map[string]any
			}
		}
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	var analyze, upload bool
	for _, step := range workflow.Jobs["analyze"].Steps {
		if step.Continue {
			t.Fatal("security workflow must not ignore failures")
		}
		if strings.HasPrefix(step.Uses, "github/codeql-action/analyze@") {
			analyze = true
			if step.With["upload"] != false {
				t.Fatal("analysis must persist reports before upload")
			}
		}
		if strings.Contains(step.Run, "scripts/upload_codeql_results.sh") {
			upload = true
		}
	}
	if !analyze || !upload {
		t.Fatal("both analysis and validated upload must be required")
	}
}
