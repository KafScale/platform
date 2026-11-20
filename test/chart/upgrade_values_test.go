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

package chart

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestRequireCurrentValues(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	silent := copyChart(t, chart, true)
	sets := [][]string{
		nil,
		{"--set", "schemaRegistry.enabled=true", "--set", "schemaRegistry.etcdEndpoints[0]=http://etcd:2379"},
		{"--set", "proxy.enabled=false", "--set", "console.enabled=false"},
	}
	for _, extra := range sets {
		got := helmTemplate(t, chart, extra)
		want := helmTemplate(t, silent, extra)
		if !bytes.Equal(got, want) {
			t.Fatalf("values guard changed the render for %v", extra)
		}
	}
	if out, err := exec.Command("helm", "lint", chart, "--strict").CombinedOutput(); err != nil {
		t.Fatalf("helm lint --strict: %v %s", err, out)
	}

	old := copyChart(t, chart, false)
	raw, err := os.ReadFile(filepath.Join(old, "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]any{}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	delete(values, "schemaRegistry")
	encoded, err := yaml.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(old, "values.yaml"), encoded, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("helm", "template", "kafscale", old, "--namespace", "kafscale").CombinedOutput()
	if err == nil || !bytes.Contains(out, []byte("--reset-then-reuse-values")) || bytes.Contains(out, []byte("nil pointer")) {
		t.Fatalf("pre-2.0 values: %v %s", err, out)
	}
}

func helmTemplate(t *testing.T, chart string, extra []string) []byte {
	t.Helper()
	args := append([]string{"template", "kafscale", chart, "--namespace", "kafscale"}, extra...)
	out, err := exec.Command("helm", args...).Output()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			t.Fatalf("helm template %s: %v %s", chart, err, exit.Stderr)
		}
		t.Fatal(err)
	}
	return out
}

func copyChart(t *testing.T, chart string, dropGuard bool) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.Walk(chart, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(chart, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if dropGuard && strings.HasPrefix(rel, "templates"+string(os.PathSeparator)) {
			body = []byte(dropCurrentValuesGuard(string(body)))
		}
		return os.WriteFile(target, body, info.Mode())
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

func dropCurrentValuesGuard(src string) string {
	var kept []string
	skipEnds := 0
	for _, line := range strings.Split(src, "\n") {
		if strings.Contains(line, `define "kafscale.requireCurrentValues"`) {
			skipEnds = 2
			continue
		}
		if skipEnds > 0 {
			if strings.TrimSpace(line) == "{{- end -}}" {
				skipEnds--
			}
			continue
		}
		if strings.Contains(line, "kafscale.requireCurrentValues") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}
