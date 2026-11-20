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

type releaseWorkflow struct {
	Jobs map[string]struct {
		If          string
		Uses        string
		With        map[string]any
		Permissions map[string]string
		Strategy    struct {
			Matrix struct {
				Include []struct {
					Name      string
					Image     string
					Context   string
					File      string
					BuildArgs string `json:"build-args"`
				}
			}
		}
		Steps []struct {
			Uses string
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
	build := read("docker-build.yml").Jobs["images"]
	expected := map[string]bool{}
	for _, name := range []string{"broker", "operator", "console", "proxy", "mcp", "schema-registry", "etcd-tools", "iceberg-processor", "sql-processor", "e72-browser-demo"} {
		expected["kafscale-"+name] = true
	}
	for _, image := range build.Strategy.Matrix.Include {
		if !expected[image.Image] {
			t.Fatalf("unexpected or duplicate image %s", image.Image)
		}
		delete(expected, image.Image)
		for _, path := range []string{image.Context, image.File} {
			if _, err := os.Stat(filepath.Join(repoRoot(t), path)); err != nil {
				t.Fatal(err)
			}
		}
		if image.Name == "iceberg-processor" && (!strings.Contains(image.BuildArgs, "USE_LOCAL_PLATFORM=1") || !strings.Contains(image.BuildArgs, "MODULE_DIR=addons/processors/iceberg-processor")) {
			t.Fatal("Iceberg release builds must use the checked-out platform module")
		}
	}
	if len(expected) != 0 {
		t.Fatalf("missing release images: %v", expected)
	}
	var foundBuild, foundLogin bool
	for _, step := range build.Steps {
		if strings.HasPrefix(step.Uses, "docker/build-push-action@") {
			foundBuild = true
			if step.With["push"] != "${{ inputs.publish }}" || step.With["platforms"] != "linux/amd64,linux/arm64" || step.With["build-args"] != "${{ matrix.build-args }}" {
				t.Fatal("PR builds must build both release platforms and pass release build arguments")
			}
		}
		if strings.HasPrefix(step.Uses, "docker/login-action@") {
			foundLogin = true
			if step.If != "inputs.publish" {
				t.Fatal("PR builds must not log in to GHCR")
			}
		}
	}
	if !foundBuild || !foundLogin {
		t.Fatal("missing build or publishing login step")
	}
}
