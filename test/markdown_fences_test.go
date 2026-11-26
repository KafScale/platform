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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func markdownFenceProblems(text string) []string {
	var problems []string
	open := ""
	openLine := 0
	for i, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		marker := ""
		for _, char := range []string{"`", "~"} {
			run := len(trimmed) - len(strings.TrimLeft(trimmed, char))
			if run >= 3 {
				marker = trimmed[:run]
			}
		}
		if marker == "" {
			continue
		}
		if open == "" {
			if marker[0] == '`' && strings.Contains(trimmed[len(marker):], "`") {
				continue
			}
			open, openLine = marker, i+1
			continue
		}
		if marker[0] != open[0] || len(marker) < len(open) {
			continue
		}
		if rest := strings.TrimSpace(trimmed[len(marker):]); rest != "" {
			problems = append(problems, fmt.Sprintf("line %d: text after the closing fence of the block opened on line %d: %s", i+1, openLine, rest))
		}
		open = ""
	}
	if open != "" {
		problems = append(problems, fmt.Sprintf("line %d: code fence is never closed", openLine))
	}
	return problems
}

func TestMarkdownFenceProblems(t *testing.T) {
	cases := map[string]bool{
		"```bash\necho ok\n```\n":                      false,
		"````md\n```bash\necho\n```\n````\n":           false,
		"inline ```code``` here\n":                     false,
		"~~~\necho\n~~~\n":                             false,
		"```bash\necho ok\n":                           true,
		"```bash\necho ok\n``` For a shared cluster\n": true,
	}
	for text, wantProblem := range cases {
		if got := len(markdownFenceProblems(text)) > 0; got != wantProblem {
			t.Errorf("markdownFenceProblems(%q) reported problems = %v, want %v", text, got, wantProblem)
		}
	}
}

func TestMarkdownCodeFencesAreClosed(t *testing.T) {
	root := repoRoot(t)
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if entry.IsDir() {
			if name := entry.Name(); name == "node_modules" || (strings.HasPrefix(name, ".") && rel != ".") {
				return filepath.SkipDir
			}
			return nil
		}
		inDocs := strings.HasPrefix(filepath.ToSlash(rel), "docs/") && strings.HasSuffix(rel, ".md")
		if inDocs || strings.EqualFold(entry.Name(), "README.md") {
			files = append(files, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no markdown files found")
	}
	for _, rel := range files {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		for _, problem := range markdownFenceProblems(string(raw)) {
			t.Errorf("%s %s", rel, problem)
		}
	}
}
