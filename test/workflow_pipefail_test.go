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

func TestWorkflowPipedStepsUsePipefail(t *testing.T) {
	dir := filepath.Join(repoRoot(t), ".github", "workflows")
	files, err := filepath.Glob(filepath.Join(dir, "*.yml"))
	if err != nil {
		t.Fatalf("glob workflows: %v", err)
	}
	checked := 0
	for _, file := range files {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, step := range workflowSteps(string(body)) {
			if !strings.Contains(step, "check_coverage.sh") {
				continue
			}
			checked++
			if !strings.Contains(step, "| tee") {
				continue
			}
			if !stepUsesPipefail(step) {
				t.Errorf("%s: coverage step pipes into tee without pipefail, so a failing gate is masked:\n%s", filepath.Base(file), step)
			}
		}
	}
	if checked == 0 {
		t.Fatalf("no workflow step runs hack/check_coverage.sh")
	}
}

func workflowSteps(body string) []string {
	var steps []string
	var current []string
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimLeft(line, " ")
		if strings.HasPrefix(trimmed, "- ") || (len(trimmed) > 0 && len(line)-len(trimmed) <= 2) {
			if len(current) > 0 {
				steps = append(steps, strings.Join(current, "\n"))
			}
			current = nil
		}
		current = append(current, line)
	}
	if len(current) > 0 {
		steps = append(steps, strings.Join(current, "\n"))
	}
	return steps
}

func stepUsesPipefail(step string) bool {
	if strings.Contains(step, "pipefail") {
		return true
	}
	for _, line := range strings.Split(step, "\n") {
		if strings.TrimSpace(line) == "shell: bash" {
			return true
		}
	}
	return false
}
