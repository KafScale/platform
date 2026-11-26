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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestHomebrewFormulaPRRetries(t *testing.T) {
	for _, scenario := range []string{"open PR", "merged PR", "interrupted PR creation", "existing branch needs update", "query failure", "remote failure", "formula already current"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv("GIT_CONFIG_GLOBAL", "/dev/null")
			t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
			root := t.TempDir()
			remote := filepath.Join(root, "remote.git")
			seed := filepath.Join(root, "seed")
			bin := filepath.Join(root, "bin")
			write := func(path, content string, mode os.FileMode) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(content), mode); err != nil {
					t.Fatal(err)
				}
			}
			git := func(dir string, args ...string) string {
				t.Helper()
				cmd := exec.Command("git", args...)
				cmd.Dir = dir
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("git %v: %v\n%s", args, err, out)
				}
				return strings.TrimSpace(string(out))
			}
			git(root, "init", "--bare", "--initial-branch=main", remote)
			git(root, "init", "--initial-branch=main", seed)
			git(seed, "config", "user.name", "Fixture")
			git(seed, "config", "user.email", "fixture@example.invalid")
			for _, name := range []string{"open_homebrew_formula_pr.sh", "update_homebrew_formula.sh"} {
				data, err := os.ReadFile(filepath.Join(repoRoot(t), "scripts", name))
				if err != nil {
					t.Fatal(err)
				}
				write(filepath.Join(seed, "scripts", name), string(data), 0o755)
			}
			write(filepath.Join(seed, "Formula", "kafscale-cli.rb"), "old formula\n", 0o644)
			sums := filepath.Join(root, "SHA256SUMS")
			var checksums strings.Builder
			for _, platform := range []string{"darwin_amd64", "darwin_arm64", "linux_amd64", "linux_arm64"} {
				fmt.Fprintf(&checksums, "%s  kafscale-cli_v9.8.7_%s.tar.gz\n", strings.Repeat("a", 64), platform)
			}
			write(sums, checksums.String(), 0o644)
			if scenario == "formula already current" {
				cmd := exec.Command("bash", "scripts/update_homebrew_formula.sh", "v9.8.7", sums, "Formula/kafscale-cli.rb")
				cmd.Dir = seed
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("generate: %v\n%s", err, out)
				}
			}
			git(seed, "add", ".")
			git(seed, "commit", "-m", "fixture")
			git(seed, "remote", "add", "origin", remote)
			git(seed, "push", "origin", "main")
			branch := "refs/heads/homebrew/kafscale-cli-9.8.7"
			if scenario == "existing branch needs update" {
				git(seed, "checkout", "-b", "homebrew/kafscale-cli-9.8.7")
				write(filepath.Join(seed, "unrelated.txt"), "preserve remote work\n", 0o644)
				git(seed, "add", "unrelated.txt")
				git(seed, "commit", "-m", "existing branch")
				git(seed, "push", "origin", "HEAD:"+branch)
			}
			// Only GitHub is stubbed: clones, fetches, commits and pushes use real Git.
			calls := filepath.Join(root, "gh-calls")
			pr := filepath.Join(root, "pr")
			write(filepath.Join(bin, "gh"), `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$GH_CALLS"
case "$1 $2" in
 "auth setup-git") ;;
 "pr list")
   if [[ "${QUERY_FAIL:-}" == 1 ]]; then exit 1; fi
   if [[ -f "$PR_FILE" ]]; then cat "$PR_FILE"; fi
   ;;
 "pr create")
   if [[ "${CREATE_FAIL:-}" == 1 ]]; then exit 1; fi
   echo 'https://example.invalid/pull/1' > "$PR_FILE"
   ;;
 *) exit 99 ;;
esac
`, 0o755)
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("GH_TOKEN", "fixture")
			t.Setenv("RELEASE_TAG", "v9.8.7")
			t.Setenv("DEFAULT_BRANCH", "main")
			t.Setenv("SHA256SUMS", sums)
			t.Setenv("GH_CALLS", calls)
			t.Setenv("PR_FILE", pr)
			t.Setenv("QUERY_FAIL", "")
			t.Setenv("CREATE_FAIL", "")
			run := func(n int, wantFailure bool) string {
				t.Helper()
				clone := filepath.Join(root, fmt.Sprintf("run%d", n))
				git(root, "clone", "--branch", "main", remote, clone)
				if scenario == "remote failure" {
					git(clone, "remote", "set-url", "origin", filepath.Join(root, "missing.git"))
				}
				cmd := exec.Command("bash", "scripts/open_homebrew_formula_pr.sh")
				cmd.Dir = clone
				// Distinct dates ensure a broken rerun cannot accidentally recreate the same commit.
				cmd.Env = append(os.Environ(), fmt.Sprintf("GIT_COMMITTER_DATE=2026-01-0%dT00:00:00Z", n))
				out, err := cmd.CombinedOutput()
				if (err != nil) != wantFailure {
					t.Fatalf("run %d: err=%v, wantFailure=%v\n%s", n, err, wantFailure, out)
				}
				return clone
			}
			switch scenario {
			case "query failure", "remote failure":
				if scenario == "query failure" {
					t.Setenv("QUERY_FAIL", "1")
				}
				run(1, true)
				if refs := git(root, "--git-dir", remote, "for-each-ref", "--format=%(refname)", "refs/heads/homebrew"); refs != "" {
					t.Fatalf("failed lookup pushed a branch: %s", refs)
				}
			case "formula already current":
				run(1, false)
			default:
				if scenario == "interrupted PR creation" {
					t.Setenv("CREATE_FAIL", "1")
				}
				first := run(1, scenario == "interrupted PR creation")
				tip := git(root, "--git-dir", remote, "rev-parse", branch)
				if scenario == "existing branch needs update" {
					if got := git(first, "show", "HEAD:unrelated.txt"); got != "preserve remote work" {
						t.Fatal("lost remote work")
					}
				}
				if scenario == "merged PR" {
					git(seed, "checkout", "main")
					git(seed, "fetch", "origin", branch)
					git(seed, "merge", "--ff-only", "FETCH_HEAD")
					git(seed, "push", "origin", "main")
				}
				t.Setenv("CREATE_FAIL", "")
				run(2, false)
				if got := git(root, "--git-dir", remote, "rev-parse", branch); got != tip {
					t.Fatalf("retry changed remote tip: %s != %s", got, tip)
				}
				if _, err := os.Stat(pr); err != nil {
					t.Fatal("retry did not create/reuse PR:", err)
				}
			}
			data, err := os.ReadFile(calls)
			if err != nil {
				t.Fatal(err)
			}
			expectedCreates := 1
			switch scenario {
			case "interrupted PR creation":
				expectedCreates = 2
			case "query failure", "remote failure", "formula already current":
				expectedCreates = 0
			}
			if got := strings.Count(string(data), "pr create "); got != expectedCreates {
				t.Fatalf("PR creation calls=%d, want=%d\n%s", got, expectedCreates, data)
			}
		})
	}
}
