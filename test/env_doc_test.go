// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

func TestDocumentedEnvVarsImplemented(t *testing.T) {
	root := repoRoot(t)
	docPath := filepath.Join(root, "docs", "operations.md")
	contents, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read operations doc: %v", err)
	}

	envs := extractEnvVars(string(contents))
	if len(envs) == 0 {
		t.Fatalf("no env vars found in %s", docPath)
	}

	implemented := collectEnvVarsFromGo(t, root)
	for _, env := range envs {
		if !implemented[env] {
			t.Fatalf("documented env var %s not found in Go code", env)
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Dir(wd)
}

func extractEnvVars(text string) []string {
	re := regexp.MustCompile(`KAFSCALE_[A-Z0-9_]+`)
	matches := re.FindAllString(text, -1)
	seen := make(map[string]struct{}, len(matches))
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		if _, ok := seen[m]; ok {
			continue
		}
		seen[m] = struct{}{}
		out = append(out, m)
	}
	return out
}

func collectEnvVarsFromGo(t *testing.T, root string) map[string]bool {
	t.Helper()
	re := regexp.MustCompile(`KAFSCALE_[A-Z0-9_]+`)
	found := make(map[string]bool)
	searchRoots := []string{"cmd", "pkg", "internal"}

	for _, rel := range searchRoots {
		base := filepath.Join(root, rel)
		err := filepath.WalkDir(base, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			body, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			for _, match := range re.FindAllString(string(body), -1) {
				found[match] = true
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", base, err)
		}
	}
	return found
}
