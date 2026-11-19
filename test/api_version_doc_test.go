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
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestExamplesUseKafscaleV1(t *testing.T) {
	root := repoRoot(t)
	stale := regexp.MustCompile(`apiVersion: kafscale\.io/v1alpha1|--api-version=kafscale\.io/v1alpha1`)
	var checked int
	for _, dir := range []string{"docs", "scripts", "deploy", "examples", "test/e2e", "README.md"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			if rel == "docs/upgrade-compatibility.md" || strings.HasPrefix(rel, "docs/releases/") || strings.Contains(rel, "node_modules") {
				return nil
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			checked++
			if loc := stale.FindIndex(body); loc != nil {
				t.Errorf("%s still uses kafscale.io/v1alpha1: %q", rel, body[loc[0]:loc[1]])
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if checked == 0 {
		t.Fatal("no files checked")
	}
}

func TestUpgradeGuideDocumentsV1Migration(t *testing.T) {
	text := readRepoDoc(t, "docs/upgrade-compatibility.md")
	for _, want := range []string{
		"## CRD API version v1",
		"kubectl apply --server-side --field-manager=kafscale-crd-upgrade -f deploy/helm/kafscale/crds/",
		"kubectl get kafscaleclusters.kafscale.io --all-namespaces -o json | kubectl replace -f -",
		"kubectl get kafscaletopics.kafscale.io --all-namespaces -o json | kubectl replace -f -",
		`'{"status":{"storedVersions":["v1"]}}'`,
		"will be removed in a future release",
		"github.com/KafScale/platform/api/v1",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("docs/upgrade-compatibility.md does not contain %q", want)
		}
	}
}
