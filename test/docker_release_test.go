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

type releaseStep struct {
	Uses string
	Run  string
	Env  map[string]string
	If   string
	With map[string]any
}

type releaseWorkflow struct {
	Jobs map[string]struct {
		If          string
		Strategy    map[string]any
		Uses        string
		RunsOn      string `json:"runs-on"`
		Timeout     int    `json:"timeout-minutes"`
		Permissions map[string]string
		Needs       []string
		Steps       []releaseStep
	}
}

type releaseAction struct {
	Inputs map[string]struct{ Required bool }
	Runs   struct {
		Using string
		Steps []releaseStep
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
	if len(workflow.Jobs) != 4 {
		t.Fatal("release workflow jobs changed")
	}
	release := workflow.Jobs["release"]
	if strings.Join(release.Needs, ",") != "build-and-push,assets" || release.Permissions["contents"] != "write" {
		t.Fatal("release must wait for images and native assets")
	}
	var download, attach bool
	for _, step := range release.Steps {
		if strings.HasPrefix(step.Uses, "actions/checkout@") || step.Run != "" {
			t.Fatal("release write job must not execute checked-out source")
		}
		if strings.HasPrefix(step.Uses, "actions/download-artifact@") && step.With["name"] == "release-assets" {
			download = true
		}
		if strings.HasPrefix(step.Uses, "softprops/action-gh-release@") && step.With["files"] == "release-assets/*" && step.With["fail_on_unmatched_files"] == true {
			attach = true
		}
	}
	if !download || !attach {
		t.Fatal("release assets must be downloaded and attached with missing-file failures")
	}
	assets := workflow.Jobs["assets"]
	if assets.RunsOn != "ubuntu-latest" || len(assets.Permissions) != 1 || assets.Permissions["contents"] != "read" {
		t.Fatal("asset builds must use Ubuntu and read-only permissions")
	}
	validate := workflow.Jobs["validate"]
	publish := workflow.Jobs["build-and-push"]
	for id, mode := range map[string]string{"validate": "false", "build-and-push": "true"} {
		job := workflow.Jobs[id]
		if strings.Join(job.Needs, ",") != "assets" {
			t.Fatalf("%s must test the generated release assets", id)
		}
		if job.Uses != "" || job.RunsOn != "ubuntu-latest" || job.Timeout != 60 || len(job.Strategy) != 0 || len(job.Steps) != 2 {
			t.Fatalf("%s must be a single job without a matrix", id)
		}
		if !strings.HasPrefix(job.Steps[0].Uses, "actions/checkout@") || job.Steps[0].With["persist-credentials"] != false {
			t.Fatalf("%s must check out without persisted credentials", id)
		}
		if job.Steps[1].Uses != "./.github/actions/build-release-images" || job.Steps[1].With["publish"] != mode {
			t.Fatal("PR and release builds must use the same build action")
		}
	}
	if validate.Permissions["packages"] == "write" || len(validate.Permissions) != 1 || validate.Permissions["contents"] != "read" {
		t.Fatal("PR builds must not publish or receive a package write grant")
	}
	if len(publish.Permissions) != 2 || publish.Permissions["contents"] != "read" || publish.Permissions["packages"] != "write" {
		t.Fatal("release builds must receive only the package write grant")
	}
	if !strings.Contains(validate.If, "github.event_name == 'pull_request'") || publish.If != "github.event_name != 'pull_request' && startsWith(github.ref, 'refs/tags/v')" {
		t.Fatal("release publishing must be restricted to version tags")
	}
	if _, err := os.Stat(filepath.Join(repoRoot(t), ".github/workflows/docker-build.yml")); !os.IsNotExist(err) {
		t.Fatal("release images must build through the composite action")
	}
	actionData, err := os.ReadFile(filepath.Join(repoRoot(t), ".github/actions/build-release-images/action.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var action releaseAction
	if err := yaml.Unmarshal(actionData, &action); err != nil {
		t.Fatal(err)
	}
	if action.Runs.Using != "composite" || len(action.Inputs) != 1 || !action.Inputs["publish"].Required {
		t.Fatal("build action must be a composite action with a required publish input")
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
	var foundBuild, foundPlan, foundLogin, foundQEMU, foundCompose, foundSmoke bool
	for _, step := range action.Runs.Steps {
		if strings.HasPrefix(step.Uses, "actions/download-artifact@") && step.With["name"] == "release-assets" && step.With["path"] == "dist/release" {
			foundCompose = true
		}
		if step.Run == "bash scripts/test_release_compose.sh dist/release/docker-compose.yaml" {
			if !foundBuild || !foundCompose {
				t.Fatal("smoke test must run after candidate images and generated assets are available")
			}
			foundSmoke = true
		}
		if strings.HasPrefix(step.Run, "bash scripts/build_release_images.sh --print") {
			foundPlan = true
			if step.Env["PUBLISH_IMAGES"] != "${{ inputs.publish }}" {
				t.Fatal("publishing mode is not forwarded")
			}
		}
		if strings.HasPrefix(step.Uses, "docker/bake-action@") {
			foundBuild = true
			if step.With["push"] != "${{ inputs.publish }}" || step.With["files"] != "${{ runner.temp }}/release-images.json" || step.With["source"] != "." {
				t.Fatal("Bake must build the shared plan and respect publishing mode")
			}
		}
		if strings.HasPrefix(step.Uses, "docker/login-action@") {
			foundLogin = true
			if step.If != "inputs.publish == 'true'" || step.With["password"] != "${{ github.token }}" {
				t.Fatal("PR builds must not log in to GHCR")
			}
		}
		if strings.HasPrefix(step.Uses, "docker/setup-qemu-action@") {
			foundQEMU = true
			if step.If != "inputs.publish == 'true'" {
				t.Fatal("PR builds must not set up release emulation")
			}
		}
	}
	if !foundBuild || !foundPlan || !foundLogin || !foundQEMU || !foundCompose || !foundSmoke {
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
					output := "type=cacheonly"
					var tags []string
					if name == "broker" || name == "proxy" || name == "console" {
						output = "type=docker"
						tags = []string{"ghcr.io/kafscale/kafscale-" + name + ":v2.0.1"}
					}
					if strings.Join(target.Platforms, ",") != "linux/amd64" || strings.Join(target.Output, ",") != output || strings.Join(target.Tags, ",") != strings.Join(tags, ",") {
						t.Fatal("PR must load candidate Compose images and cache other native builds")
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

func TestReleaseComposeProvisioning(t *testing.T) {
	for _, failBucket := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "bucket-failure"}[failBucket], func(t *testing.T) {
			work := t.TempDir()
			docker := `#!/bin/sh
printf 'docker %s\n' "$*" >> "$CAPTURE/events"
case "$*" in
 *" -C -b 127.0.0.1:9093 "*) printf 'release smoke\n' ;;
esac
`
			curl := `#!/bin/sh
printf 'curl %s\n' "$*" >> "$CAPTURE/events"
if [ "$FAIL_BUCKET" = true ]; then
 for arg in "$@"; do
  if [ "$arg" = PUT ]; then exit 22; fi
 done
fi
`
			for name, script := range map[string]string{"docker": docker, "curl": curl} {
				if err := os.WriteFile(filepath.Join(work, name), []byte(script), 0755); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command("bash", "scripts/test_release_compose.sh", "dist/release/docker-compose.yaml")
			cmd.Dir = repoRoot(t)
			failure := "false"
			if failBucket {
				failure = "true"
			}
			cmd.Env = append(os.Environ(), "PATH="+work+string(os.PathListSeparator)+os.Getenv("PATH"), "CAPTURE="+work, "FAIL_BUCKET="+failure)
			out, err := cmd.CombinedOutput()
			if (err != nil) != failBucket {
				t.Fatalf("smoke test: %v %s", err, out)
			}
			raw, err := os.ReadFile(filepath.Join(work, "events"))
			if err != nil {
				t.Fatal(err)
			}
			events := string(raw)
			put := strings.Index(events, "-X PUT http://127.0.0.1:9000/release-compose-check")
			create := strings.Index(events, "docker compose -p release-smoke -f dist/release/docker-compose.yaml create")
			up := strings.Index(events, "up -d --wait --wait-timeout 180")
			if put < 0 {
				t.Fatal("bucket provisioning was not attempted")
			}
			if failBucket {
				if create >= 0 || up >= 0 {
					t.Fatal("product containers started after bucket provisioning failed")
				}
			} else if create < put || up < create || !strings.Contains(events, "curl -fsSI --aws-sigv4") {
				t.Fatal("bucket must be created and verified before starting the generated Compose")
			}
			if !failBucket {
				produce := strings.Index(events, "-P -b 127.0.0.1:9093 -t release-smoke")
				consume := strings.Index(events, "-C -b 127.0.0.1:9093 -t release-smoke")
				if produce < up || consume < produce {
					t.Fatal("smoke test must produce and consume through 127.0.0.1:9093 after the stack is up")
				}
			}
			if !strings.Contains(events, "docker rm -f release-smoke-s3") || !strings.Contains(events, "down --volumes") {
				t.Fatal("smoke test did not clean up")
			}
		})
	}
}

func TestReleaseComposeUsesIPv4Loopback(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "deploy/docker-compose/docker-compose.release.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Environment map[string]string `yaml:"environment"`
			Ports       []string          `yaml:"ports"`
		} `yaml:"services"`
	}
	if err := yamlv3.Unmarshal(raw, &compose); err != nil {
		t.Fatal(err)
	}
	published := 0
	for name, service := range compose.Services {
		for _, port := range service.Ports {
			published++
			if !strings.HasPrefix(port, "127.0.0.1:") {
				t.Errorf("service %s publishes %q; release ports must bind 127.0.0.1 only", name, port)
			}
		}
	}
	if published == 0 {
		t.Fatal("release Compose publishes no ports")
	}
	if got := compose.Services["proxy"].Environment["KAFSCALE_PROXY_ADVERTISED_HOST"]; got != "127.0.0.1" {
		t.Fatalf("proxy advertises %q; it must advertise 127.0.0.1 so clients never resolve localhost to ::1", got)
	}
	doc, err := os.ReadFile(filepath.Join(root, "docs/s3-connection.md"))
	if err != nil {
		t.Fatal(err)
	}
	section := string(doc)
	start := strings.Index(section, "## Release Compose with your bucket")
	if start < 0 {
		t.Fatal("docs/s3-connection.md has no release Compose section")
	}
	section = section[start:]
	if end := strings.Index(section[3:], "\n## "); end >= 0 {
		section = section[:end+3]
	}
	for _, stale := range []string{"localhost:9093", "localhost:9094", "localhost:3080"} {
		if strings.Contains(section, stale) {
			t.Errorf("release Compose docs use %s; use 127.0.0.1 to match the published ports", stale)
		}
	}
	for _, want := range []string{"127.0.0.1:9093", "http://127.0.0.1:9094/readyz", "http://127.0.0.1:3080/ui/"} {
		if !strings.Contains(section, want) {
			t.Errorf("release Compose docs do not mention %s", want)
		}
	}
}
