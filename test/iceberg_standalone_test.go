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
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestIcebergStandalonePlatformSelection(t *testing.T) {
	root := repoRoot(t)
	moduleDir := filepath.Join(root, "addons/processors/iceberg-processor")
	dockerfile, err := os.ReadFile(filepath.Join(moduleDir, "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(dockerfile), "ARG PLATFORM_VERSION\n") || !strings.Contains(string(dockerfile), "sh ./prepare-platform.sh") {
		t.Fatal("Dockerfile must pass PLATFORM_VERSION to the platform preparation script")
	}
	for _, tc := range []struct {
		name, local, version, wantError string
	}{
		{name: "missing version", wantError: "Standalone builds require --build-arg PLATFORM_VERSION="},
		{name: "local checkout", local: "1"},
		{name: "published invalid tag", version: "v2.0.0", wantError: "invalid version: go.mod has non-.../v2 module path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			original, err := os.ReadFile(filepath.Join(moduleDir, "go.mod"))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "go.mod"), original, 0o644); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("sh", filepath.Join(moduleDir, "prepare-platform.sh"))
			cmd.Dir = dir
			cmd.Env = append(os.Environ(),
				"USE_LOCAL_PLATFORM="+tc.local, "PLATFORM_VERSION="+tc.version,
				"GOPROXY=direct", "GOSUMDB=off", "GOPRIVATE=", "GONOPROXY=", "GONOSUMDB=", "GOVCS=",
				"GOWORK=off", "GOFLAGS=", "GOTOOLCHAIN=local", "GOMODCACHE="+filepath.Join(dir, "modcache"),
				"GIT_CONFIG_GLOBAL="+invalidPlatformRepo(t), "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0",
			)
			out, err := cmd.CombinedOutput()
			if tc.wantError == "" {
				if err != nil {
					t.Fatalf("local preparation: %v\n%s", err, out)
				}
			} else if err == nil || !strings.Contains(string(out), tc.wantError) {
				t.Fatalf("preparation error = %v, output = %s; want %q", err, out, tc.wantError)
			}
			after, err := os.ReadFile(filepath.Join(dir, "go.mod"))
			if err != nil {
				t.Fatal(err)
			}
			if string(after) != string(original) {
				t.Fatal("preparation changed go.mod on local build or failed validation")
			}
		})
	}
}

func invalidPlatformRepo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	repo := filepath.Join(root, "platform")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "go.mod"), []byte("module github.com/KafScale/platform\n\ngo 1.22\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(), "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@example.com")
	for _, args := range [][]string{
		{"init", "-q", "-b", "main"},
		{"add", "go.mod"},
		{"commit", "-q", "-m", "v2.0.0"},
		{"tag", "v2.0.0"},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		cmd.Env = env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	config := filepath.Join(root, "gitconfig")
	rewrite := "[url \"" + filepath.ToSlash(repo) + "\"]\n\tinsteadOf = https://github.com/KafScale/platform\n"
	if err := os.WriteFile(config, []byte(rewrite), 0o644); err != nil {
		t.Fatal(err)
	}
	return config
}
