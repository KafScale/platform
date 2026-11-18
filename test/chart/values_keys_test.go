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
	"path/filepath"
	"regexp"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestValuesYamlHasNoUnreadTopLevelKeys(t *testing.T) {
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	demo := filepath.Join(chart, "values-lfs-demo.yaml")
	if _, err := os.Stat(demo); err == nil {
		t.Fatalf("%s still exists", demo)
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(filepath.Join(chart, "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]any{}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	if _, ok := values["lfsDemos"]; ok {
		t.Fatal("values.yaml still has top-level key lfsDemos")
	}

	entries, err := os.ReadDir(filepath.Join(chart, "templates"))
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`\.Values\.([A-Za-z_][A-Za-z0-9_]*)`)
	used := map[string]struct{}{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		body, err := os.ReadFile(filepath.Join(chart, "templates", entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range re.FindAllSubmatch(body, -1) {
			used[string(m[1])] = struct{}{}
		}
	}
	if len(used) == 0 {
		t.Fatal("no .Values references found in chart templates")
	}
	for key := range values {
		if _, ok := used[key]; !ok {
			t.Errorf("values.yaml top-level key %q is not read by any chart template", key)
		}
	}
}
