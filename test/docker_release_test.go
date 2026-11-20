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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	yamlv3 "gopkg.in/yaml.v3"
	"sigs.k8s.io/yaml"
)

type releaseWorkflow struct {
	Jobs map[string]struct {
		If          string
		Strategy    map[string]any
		Uses        string
		With        map[string]any
		Permissions map[string]string
		Steps       []struct {
			Uses string
			Run  string
			Env  map[string]string
			If   string
			With map[string]any
		}
	}
}

func TestDockerReleaseImageMatrix(t *testing.T) {
	read := func(name string) releaseWorkflow {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(repoRoot(t), ".github/workflows", name))
		if err != nil {
			t.Fatal(err)
		}
		var workflow releaseWorkflow
		if err := yaml.Unmarshal(data, &workflow); err != nil {
			t.Fatal(err)
		}
		return workflow
	}
	workflow := read("docker.yml")
	validate := workflow.Jobs["validate"]
	publish := workflow.Jobs["build-and-push"]
	if validate.Uses != "./.github/workflows/docker-build.yml" || publish.Uses != validate.Uses {
		t.Fatal("PR and release builds must use the same workflow")
	}
	if validate.With["publish"] != false || publish.With["publish"] != true || validate.Permissions["packages"] == "write" {
		t.Fatal("PR builds must not publish or receive a package write grant")
	}
	if !strings.Contains(validate.If, "github.event_name == 'pull_request'") || publish.If != "github.event_name != 'pull_request' && startsWith(github.ref, 'refs/tags/v')" {
		t.Fatal("release publishing must be restricted to version tags")
	}
	buildWorkflow := read("docker-build.yml")
	build := buildWorkflow.Jobs["images"]
	if len(buildWorkflow.Jobs) != 1 || len(build.Strategy) != 0 {
		t.Fatal("image validation must remain a single job without a matrix")
	}
	expected := map[string]bool{}
	for _, name := range []string{"broker", "operator", "console", "proxy", "mcp", "schema-registry", "etcd-tools", "iceberg-processor", "sql-processor", "e72-browser-demo"} {
		expected["kafscale-"+name] = true
	}
	var catalog struct {
		Group  map[string]struct{ Targets []string }
		Target map[string]struct {
			Context    string
			Dockerfile string
			Tags       []string
			Platforms  []string
			Args       map[string]string
		}
	}
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "deploy/docker/docker-bake.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &catalog); err != nil {
		t.Fatal(err)
	}
	if len(catalog.Target) != len(expected) {
		t.Fatal("release image catalog changed")
	}
	for _, name := range catalog.Group["default"].Targets {
		image, ok := catalog.Target[name]
		if !ok || len(image.Tags) != 1 || image.Tags[0] != "ghcr.io/kafscale/kafscale-"+name+":pr-check" || !expected["kafscale-"+name] {
			t.Fatalf("unexpected or duplicate image %s", name)
		}
		delete(expected, "kafscale-"+name)
		if strings.Join(image.Platforms, ",") != "linux/amd64,linux/arm64" {
			t.Fatal("release platforms changed")
		}
		for _, path := range []string{image.Context, filepath.Join(image.Context, image.Dockerfile)} {
			if _, err := os.Stat(filepath.Join(repoRoot(t), path)); err != nil {
				t.Fatal(err)
			}
		}
		if name == "iceberg-processor" && (image.Args["USE_LOCAL_PLATFORM"] != "1" || image.Args["MODULE_DIR"] != "addons/processors/iceberg-processor") {
			t.Fatal("Iceberg release builds must use the checked-out platform module")
		}
	}
	if len(expected) != 0 {
		t.Fatalf("missing release images: %v", expected)
	}
	var foundBuild, foundPlan, foundLogin bool
	for _, step := range build.Steps {
		if strings.HasPrefix(step.Run, "bash scripts/build_release_images.sh --print") {
			foundPlan = true
			if step.Env["PUBLISH_IMAGES"] != "${{ inputs.publish }}" {
				t.Fatal("publishing mode is not forwarded")
			}
		}
		if strings.HasPrefix(step.Uses, "docker/bake-action@") {
			foundBuild = true
			if step.With["push"] != "${{ inputs.publish }}" || step.With["files"] != "${{ runner.temp }}/release-images.json" {
				t.Fatal("Bake must build the shared plan and respect publishing mode")
			}
		}
		if strings.HasPrefix(step.Uses, "docker/login-action@") {
			foundLogin = true
			if step.If != "inputs.publish" {
				t.Fatal("PR builds must not log in to GHCR")
			}
		}
	}
	if !foundBuild || !foundPlan || !foundLogin {
		t.Fatal("missing build or publishing login step")
	}
}

func TestReleaseImageBuildModes(t *testing.T) {
	for _, mode := range []string{"pr", "release", "invalid-publish-ref"} {
		t.Run(mode, func(t *testing.T) {
			work := t.TempDir()
			// Capture the exact Docker arguments and generated Bake override.
			docker := `#!/bin/sh
printf '%s\n' "$@" > "$CAPTURE/args"
previous=
for arg in "$@"; do
    if [ "$previous" = --file ]; then cp "$arg" "$CAPTURE/config.json"; fi
    previous="$arg"
done
`
			if err := os.WriteFile(filepath.Join(work, "docker"), []byte(docker), 0o755); err != nil {
				t.Fatal(err)
			}
			publish, ref := "false", "refs/pull/413/merge"
			if mode != "pr" {
				publish = "true"
			}
			if mode == "release" {
				ref = "refs/tags/v2.0.1"
			}
			cmd := exec.Command("bash", "scripts/build_release_images.sh")
			cmd.Dir = repoRoot(t)
			cmd.Env = append(os.Environ(), "PATH="+work+string(os.PathListSeparator)+os.Getenv("PATH"),
				"CAPTURE="+work, "PUBLISH_IMAGES="+publish, "GITHUB_REF="+ref,
				"RELEASE_TAG=v2.0.1", "SOURCE_REVISION=fixture", "GITHUB_ACTIONS=true")
			out, err := cmd.CombinedOutput()
			if mode == "invalid-publish-ref" {
				if err == nil || !strings.Contains(string(out), "requires a version tag") {
					t.Fatalf("invalid ref: %v %s", err, out)
				}
				if _, err := os.Stat(filepath.Join(work, "args")); !os.IsNotExist(err) {
					t.Fatal("invalid publishing ref reached Docker")
				}
				return
			}
			if err != nil {
				t.Fatalf("build command: %v %s", err, out)
			}
			args, err := os.ReadFile(filepath.Join(work, "args"))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(args), "--push") != (mode == "release") {
				t.Fatalf("unexpected Docker arguments: %s", args)
			}
			config, err := os.ReadFile(filepath.Join(work, "config.json"))
			if err != nil {
				t.Fatal(err)
			}
			var override struct {
				Target map[string]struct {
					Platforms []string
					Output    []string
					Tags      []string
				}
			}
			if err := json.Unmarshal(config, &override); err != nil {
				t.Fatal(err)
			}
			if len(override.Target) != 10 {
				t.Fatal("override omits release images")
			}
			for name, target := range override.Target {
				if mode == "pr" {
					if strings.Join(target.Platforms, ",") != "linux/amd64" || strings.Join(target.Output, ",") != "type=cacheonly" || len(target.Tags) != 0 {
						t.Fatal("PR must build once without image export")
					}
				} else if len(target.Platforms) != 0 || strings.Join(target.Tags, ",") != "ghcr.io/kafscale/kafscale-"+name+":v2.0.1" {
					t.Fatal("release must retain both catalog platforms and publish the version tag")
				}
			}
		})
	}
}

func TestNewBuildWorkflowsSkipUnrelatedPRs(t *testing.T) {
	for _, name := range []string{"docker.yml", "browser-demo.yml"} {
		data, err := os.ReadFile(filepath.Join(repoRoot(t), ".github/workflows", name))
		if err != nil {
			t.Fatal(err)
		}
		var workflow struct {
			On          map[string]struct{ Paths []string }
			Concurrency struct {
				Group            string
				CancelInProgress string `yaml:"cancel-in-progress"`
			}
		}
		if err := yamlv3.Unmarshal(data, &workflow); err != nil {
			t.Fatal(err)
		}
		paths := workflow.On["pull_request"].Paths
		if len(paths) == 0 {
			t.Fatalf("%s must filter PR build inputs", name)
		}
		var excludesDocs bool
		for _, path := range paths {
			if path == "**" {
				t.Fatalf("%s triggers on every file", name)
			}
			if path == "!**/*.md" {
				excludesDocs = true
			}
		}
		if !excludesDocs || workflow.Concurrency.Group == "" || workflow.Concurrency.CancelInProgress != "${{ github.event_name == 'pull_request' }}" {
			t.Fatalf("%s must skip documentation-only PRs and cancel superseded PR runs", name)
		}
	}
}
