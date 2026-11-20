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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestChartRejectsValuesFromBefore20(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	src := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	dst := filepath.Join(t.TempDir(), "kafscale")
	if output, err := exec.Command("cp", "-r", src, dst).CombinedOutput(); err != nil {
		t.Fatalf("copy chart: %v: %s", err, output)
	}
	if output, err := exec.Command("helm", "template", "guard", dst, "--namespace", "kafscale").CombinedOutput(); err != nil {
		t.Fatalf("default render: %v: %s", err, output)
	}
	raw, err := os.ReadFile(filepath.Join(dst, "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]any{}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	delete(values, "schemaRegistry")
	old, err := yaml.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "values.yaml"), old, 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("helm", "template", "guard", dst, "--namespace", "kafscale").CombinedOutput()
	if err == nil {
		t.Fatal("chart rendered values without schemaRegistry, want a fail-fast error")
	}
	for _, want := range []string{"--reset-then-reuse-values", "not --reuse-values"} {
		if !strings.Contains(string(output), want) {
			t.Fatalf("render error does not mention %q: %s", want, output)
		}
	}
	if strings.Contains(string(output), "nil pointer") {
		t.Fatalf("render failed with a nil pointer instead of the guard: %s", output)
	}
}
