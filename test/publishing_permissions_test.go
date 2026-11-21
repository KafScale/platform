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

	"sigs.k8s.io/yaml"
)

var demoVideos = []string{"admin", "cli", "schema-registry", "mcp", "ops", "iceberg", "sql"}

type publishingWorkflow struct {
	Permissions map[string]string
	Jobs        map[string]struct {
		If          string
		Needs       any
		Permissions map[string]string
		Steps       []struct {
			Name string
			Uses string
			Run  string
			With map[string]string
			Env  map[string]string
		}
	}
}

func readPublishingWorkflow(t *testing.T, name string) publishingWorkflow {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", name))
	if err != nil {
		t.Fatal(err)
	}
	var workflow publishingWorkflow
	if err := yaml.Unmarshal(body, &workflow); err != nil {
		t.Fatal(err)
	}
	return workflow
}

func TestPublishingTokenPermissions(t *testing.T) {
	for _, name := range []string{"ci.yml", "docker.yml", "sync-demo-videos.yml"} {
		workflow := readPublishingWorkflow(t, name)
		if len(workflow.Permissions) != 1 || workflow.Permissions["contents"] != "read" {
			t.Fatalf("%s default permissions=%v", name, workflow.Permissions)
		}
		for id, job := range workflow.Jobs {
			for permission, level := range job.Permissions {
				if level != "write" {
					continue
				}
				allowed := map[string]map[string]bool{
					"ci.yml/coverage-badge":        {"contents": true},
					"docker.yml/build-and-push":    {"packages": true},
					"docker.yml/release":           {"contents": true},
					"sync-demo-videos.yml/publish": {"contents": true, "pull-requests": true},
				}
				if !allowed[name+"/"+id][permission] {
					t.Fatalf("unexpected %s write grant in %s/%s", permission, name, id)
				}
			}
			for _, step := range job.Steps {
				if strings.HasPrefix(step.Uses, "actions/checkout@") && (id == "coverage-badge" || name != "ci.yml") && step.With["persist-credentials"] != "false" {
					t.Fatalf("%s/%s retains checkout credentials", name, id)
				}
			}
			if job.Permissions["contents"] != "write" && job.Permissions["pull-requests"] != "write" {
				continue
			}
			switch name + "/" + id {
			case "ci.yml/coverage-badge":
				if job.If != "github.event_name == 'push' && github.ref == 'refs/heads/main'" || len(job.Permissions) != 1 {
					t.Fatal("badge publishing is not restricted to trusted main pushes")
				}
			case "docker.yml/release":
				if job.If != "startsWith(github.ref, 'refs/tags/v')" || job.Needs != "build-and-push" || len(job.Permissions) != 1 || len(job.Steps) != 1 || !strings.HasPrefix(job.Steps[0].Uses, "softprops/action-gh-release@") {
					t.Fatal("release write token exposed outside the release API step")
				}
			case "sync-demo-videos.yml/publish":
				if job.Needs != "prepare" || len(job.Permissions) != 2 || job.Permissions["pull-requests"] != "write" {
					t.Fatal("video publisher permissions/dependencies changed")
				}
				for _, step := range job.Steps {
					if step.With["ref"] == "${{ inputs.source_ref }}" || strings.Contains(step.Run, "src/") {
						t.Fatal("write job handles the untrusted video source")
					}
					if strings.HasPrefix(step.Uses, "actions/download-artifact@") && (step.With["run-id"] != "" || step.With["repository"] != "") {
						t.Fatal("publisher accepts cross-run artifacts")
					}
				}
			default:
				t.Fatalf("unexpected publishing write token in %s/%s", name, id)
			}
		}
	}
}

func TestEveryWorkflowDefaultsToReadOnlyToken(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join(repoRoot(t), ".github", "workflows", "*.yml"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("workflows=%v err=%v", paths, err)
	}
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var workflow struct {
			Permissions any
		}
		if err := yaml.Unmarshal(body, &workflow); err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(path)
		switch permissions := workflow.Permissions.(type) {
		case string:
			if permissions != "read-all" {
				t.Fatalf("%s default permissions=%s", name, permissions)
			}
		case map[string]any:
			if len(permissions) == 0 {
				t.Fatalf("%s has an empty default permissions block", name)
			}
			for scope, level := range permissions {
				if level != "read" && level != "none" {
					t.Fatalf("%s grants %s %v by default", name, scope, level)
				}
			}
		default:
			t.Fatalf("%s has no top-level permissions block", name)
		}
	}
}

func TestVideoPreparationAcceptsOnlyExpectedRegularFiles(t *testing.T) {
	workflow := readPublishingWorkflow(t, "sync-demo-videos.yml")
	var script string
	for _, step := range workflow.Jobs["prepare"].Steps {
		if step.Name == "Prepare video files" {
			script = step.Run
		}
	}
	if script == "" {
		t.Fatal("missing preparation step")
	}
	for _, mode := range []string{"valid", "symlink-file", "symlink-directory", "empty-file"} {
		t.Run(mode, func(t *testing.T) {
			work := t.TempDir()
			for _, persona := range demoVideos {
				dir := filepath.Join(work, "src", "docs", "media", "videos", persona)
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				for _, name := range []string{persona + ".webm", persona + ".mp4", persona + "-poster.webp"} {
					if err := os.WriteFile(filepath.Join(dir, name), []byte("fixture-"+name), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			target := filepath.Join(work, "src/docs/media/videos/admin/admin.webm")
			switch mode {
			case "symlink-file":
				if err := os.Remove(target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(work, "src/docs/media/videos/cli/cli.webm"), target); err != nil {
					t.Fatal(err)
				}
			case "symlink-directory":
				dir := filepath.Join(work, "src/docs/media/videos")
				if err := os.Rename(dir, filepath.Join(work, "outside-videos")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(work, "outside-videos"), dir); err != nil {
					t.Fatal(err)
				}
			case "empty-file":
				if err := os.WriteFile(target, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command("bash", "-euc", script)
			cmd.Dir = work
			output, err := cmd.CombinedOutput()
			if mode != "valid" {
				if err == nil {
					t.Fatal("unsafe video source accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("prepare videos: %v %s", err, output)
			}
			count := 0
			err = filepath.WalkDir(filepath.Join(work, "staged-videos"), func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					return nil
				}
				count++
				body, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if string(body) != "fixture-"+entry.Name() {
					t.Errorf("file content changed: %s", path)
				}
				return nil
			})
			if err != nil || count != 3*len(demoVideos) {
				t.Fatalf("prepared files=%d err=%v", count, err)
			}
		})
	}
}

func TestVideoPublisherIncludesNewFilesWithoutRunningSource(t *testing.T) {
	workflow := readPublishingWorkflow(t, "sync-demo-videos.yml")
	var copyScript, publishScript string
	for _, step := range workflow.Jobs["publish"].Steps {
		if step.Name == "Copy prepared videos" {
			copyScript = step.Run
		}
		if step.Name == "Open pull request" {
			publishScript = step.Run
		}
	}
	if copyScript == "" || publishScript == "" {
		t.Fatal("publisher steps missing")
	}
	work := t.TempDir()
	site := filepath.Join(work, "site")
	if err := os.Mkdir(site, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, persona := range demoVideos {
		dir := filepath.Join(work, "prepared-videos", persona)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{persona + ".webm", persona + ".mp4", persona + "-poster.webp"} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte("fixture"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Setenv("GIT_CONFIG_GLOBAL", "/dev/null")
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	runGit := func(args ...string) string {
		cmd := exec.Command("git", args...)
		cmd.Dir = site
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, output)
		}
		return string(output)
	}
	runGit("init", "-b", "gh-pages")
	runGit("config", "user.name", "Fixture")
	runGit("config", "user.email", "fixture@example.invalid")
	if err := os.WriteFile(filepath.Join(site, "README.md"), []byte("site fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("add", "README.md")
	runGit("commit", "-m", "fixture")
	cmd := exec.Command("bash", "-euc", copyScript)
	cmd.Dir = work
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("copy: %v %s", err, output)
	}
	calls := filepath.Join(work, "publisher-calls")
	t.Setenv("PUBLISH_CALLS", calls)
	t.Setenv("SOURCE_REF", "main")
	t.Setenv("RUN_ID", "123")
	prefix := `git() { if [[ "$1" == push ]]; then printf 'push\n' >> "$PUBLISH_CALLS"; return; fi; command git "$@"; }
gh() { printf '%s\n' "$*" >> "$PUBLISH_CALLS"; }
`
	cmd = exec.Command("bash", "-euc", prefix+publishScript)
	cmd.Dir = site
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("publish: %v %s", err, output)
	}
	if strings.TrimSpace(runGit("branch", "--show-current")) != "site/sync-demo-videos-123" {
		t.Fatal("publisher skipped the newly added videos")
	}
	if count := len(strings.Fields(runGit("ls-files", "media/demos"))); count != 3*len(demoVideos) {
		t.Fatalf("tracked videos=%d", count)
	}
	if string(runGit("show", "HEAD:README.md")) != "site fixture" {
		t.Fatal("publisher replaced unrelated site content")
	}
	body, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "auth setup-git\npush\npr create") {
		t.Fatalf("publishing did not authenticate immediately before pushing: %s", body)
	}
}

func TestVideoPublisherRejectsDestinationSymlink(t *testing.T) {
	workflow := readPublishingWorkflow(t, "sync-demo-videos.yml")
	var script string
	for _, step := range workflow.Jobs["publish"].Steps {
		if step.Name == "Copy prepared videos" {
			script = step.Run
		}
	}
	if script == "" {
		t.Fatal("missing copy step")
	}
	work := t.TempDir()
	for _, dir := range []string{"prepared-videos/admin", "site/media/demos/admin"} {
		if err := os.MkdirAll(filepath.Join(work, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(work, "prepared-videos/admin/admin.webm"), []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(work, "outside")
	if err := os.WriteFile(outside, []byte("untouched"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(work, "site/media/demos/admin/admin.webm")); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "-euc", script)
	cmd.Dir = work
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("destination symlink accepted: %s", output)
	}
	body, err := os.ReadFile(outside)
	if err != nil || string(body) != "untouched" {
		t.Fatal("publisher overwrote a file outside the video path")
	}
}

func TestVideoSyncCoversEveryRecordedVideo(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join(repoRoot(t), "docs", "media", "videos"))
	if err != nil {
		t.Fatal(err)
	}
	var recorded []string
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != "tapes" {
			recorded = append(recorded, entry.Name())
		}
	}
	want := map[string]bool{}
	for _, name := range demoVideos {
		want[name] = true
	}
	for _, name := range recorded {
		if !want[name] {
			t.Errorf("docs/media/videos/%s is not in demoVideos", name)
		}
	}
	if len(recorded) != len(demoVideos) {
		t.Errorf("recorded videos %v, demoVideos %v", recorded, demoVideos)
	}
	loop := "for p in " + strings.Join(demoVideos, " ") + "; do"
	workflow := readPublishingWorkflow(t, "sync-demo-videos.yml")
	steps := map[string]string{"prepare": "Prepare video files", "publish": "Copy prepared videos"}
	for job, stepName := range steps {
		found := false
		for _, step := range workflow.Jobs[job].Steps {
			if step.Name == stepName {
				found = true
				if !strings.Contains(step.Run, loop) {
					t.Errorf("%s/%s does not copy %v", job, stepName, demoVideos)
				}
			}
		}
		if !found {
			t.Errorf("missing step %s/%s", job, stepName)
		}
	}
}
