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
)

func TestIcebergStandaloneBuildDropsLocalReplace(t *testing.T) {
	root := repoRoot(t)
	dockerSrc, err := os.ReadFile(filepath.Join(root, "addons/processors/iceberg-processor/Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(dockerSrc)
	if strings.Contains(text, "dropreplace github.com/KafScale/platform ||") || strings.Contains(text, "github.com/KafScale/platform@v1.6.3") {
		t.Fatal("standalone image still names the unsuffixed module")
	}
	var cmds []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(strings.TrimRight(line, "\\"))
		line = strings.TrimSuffix(strings.TrimSpace(line), ";")
		if strings.HasPrefix(line, "go mod ") {
			cmds = append(cmds, line)
		}
	}
	if len(cmds) < 2 || cmds[0] != "go mod edit -dropreplace github.com/KafScale/platform/v2" || cmds[1] != "go mod download github.com/KafScale/platform/v2" {
		t.Fatalf("standalone module commands = %q", cmds)
	}

	modDir := filepath.Join(t.TempDir(), "a", "b", "c", "iceberg")
	copyTree(t, filepath.Join(root, "addons/processors/iceberg-processor"), modDir)
	proxy := filepath.Join(t.TempDir(), "proxy")
	writePlatformProxy(t, root, proxy)

	cache := t.TempDir()
	t.Cleanup(func() {
		_ = filepath.Walk(cache, func(path string, info os.FileInfo, err error) error {
			if err == nil {
				_ = os.Chmod(path, 0o700)
			}
			return nil
		})
	})
	env := append(os.Environ(),
		"GOCACHE="+filepath.Join(cache, "build"),
		"GOMODCACHE="+filepath.Join(cache, "mod"),
		"GOPROXY=file://"+proxy+",https://proxy.golang.org,direct",
		"GONOSUMDB=github.com/KafScale/platform/v2",
		"GOFLAGS=",
	)
	for _, line := range cmds[:2] {
		runGo(t, modDir, env, "sh", "-c", line)
	}
	out := filepath.Join(t.TempDir(), "iceberg-processor")
	runGo(t, modDir, env, "go", "build", "-o", out, "./cmd/processor")
}

func writePlatformProxy(t *testing.T, root, proxy string) {
	t.Helper()
	const modPath = "github.com/KafScale/platform/v2"
	const ver = "v2.0.0"
	dir := filepath.Join(proxy, escapeModulePath(modPath), "@v")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	zipPath := filepath.Join(dir, ver+".zip")
	cmd := exec.Command("git", "archive", "--format=zip", "--prefix", modPath+"@"+ver+"/", "--output", zipPath, "HEAD",
		":(exclude)addons/processors/iceberg-processor",
		":(exclude)addons/processors/skeleton",
		":(exclude)addons/processors/sql-processor")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git archive: %v %s", err, out)
	}
	mod := exec.Command("git", "show", "HEAD:go.mod")
	mod.Dir = root
	modText, err := mod.Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ver+".mod"), modText, 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := json.Marshal(map[string]string{"Version": ver, "Time": "2026-11-20T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ver+".info"), append(info, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "list"), []byte(ver+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		base := filepath.Base(path)
		if info.IsDir() && (base == ".gocache" || base == "bin" || base == "_site" || base == ".git") {
			return filepath.SkipDir
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, body, info.Mode())
	})
	if err != nil {
		t.Fatal(err)
	}
}

func runGo(t *testing.T, dir string, env []string, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
}

func escapeModulePath(path string) string {
	var b strings.Builder
	for _, r := range path {
		if r >= 'A' && r <= 'Z' {
			b.WriteByte('!')
			b.WriteRune(r + ('a' - 'A'))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
