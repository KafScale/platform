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
			if tc.version != "" && testing.Short() {
				t.Skip("downloads the real published module from the Go proxy")
			}
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
				"GOPROXY=https://proxy.golang.org", "GOWORK=off", "GOFLAGS=",
				"GOMODCACHE="+filepath.Join(dir, "modcache"),
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
