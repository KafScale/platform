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
		"kubectl apply --server-side --force-conflicts --field-manager=kafscale-crd-upgrade -f deploy/helm/kafscale/crds/",
		"kubectl get kafscaleclusters.kafscale.io --all-namespaces -o json | kubectl replace -f -",
		"kubectl get kafscaletopics.kafscale.io --all-namespaces -o json | kubectl replace -f -",
		`'{"status":{"storedVersions":["v1"]}}'`,
		"will be removed in a future release",
		"github.com/KafScale/platform/v2/api/v1",
		"go get github.com/KafScale/platform@v2.0.0",
		"has no `/v2` suffix",
		"Do not tag v1.7.0",
		"github.com/KafScale/platform/v2/addons/processors/iceberg-processor",
		"The standalone image drops that replace",
		"github.com/kafscale/platform/v2/addons/processors/sql-processor",
		"rewriting that checkout onto",
		"A chart rollback must keep the 2.0 CRDs.",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("docs/upgrade-compatibility.md does not contain %q", want)
		}
	}
}

func TestGoModulePathIsV2(t *testing.T) {
	root := repoRoot(t)
	for _, rel := range []string{
		"go.mod",
		"addons/processors/iceberg-processor/go.mod",
		"addons/processors/skeleton/go.mod",
	} {
		line, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(string(line), "module github.com/KafScale/platform/v2") {
			t.Fatalf("%s module path is not /v2", rel)
		}
	}
	sql, err := os.ReadFile(filepath.Join(root, "addons/processors/sql-processor/go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(sql), "module github.com/kafscale/platform/v2/addons/processors/sql-processor\n") {
		t.Fatal("sql processor module path is not /v2")
	}
	ice, err := os.ReadFile(filepath.Join(root, "addons/processors/iceberg-processor/go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ice), "github.com/KafScale/platform/v2 v2.0.0") || !strings.Contains(string(ice), "replace github.com/KafScale/platform/v2 => ../../..") {
		t.Fatal("iceberg processor does not require and replace /v2")
	}
	script := readRepoDoc(t, "scripts/check_go_api_compatibility.sh")
	for _, want := range []string{
		"module=github.com/KafScale/platform/v2",
		`line == "module github.com/KafScale/platform"`,
		`"module github.com/KafScale/platform/v2"`,
		`'"github.com/KafScale/platform/v2'`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("compatibility script missing %q", want)
		}
	}
}

func TestUpgradeGuideKeepsNewChartDefaults(t *testing.T) {
	text := readRepoDoc(t, "docs/upgrade-compatibility.md")
	for _, want := range []string{
		"helm upgrade --install kafscale deploy/helm/kafscale --namespace kafscale --reset-then-reuse-values",
		"helm get values kafscale --namespace kafscale -o yaml > kafscale-values.yaml",
		"helm upgrade --install kafscale deploy/helm/kafscale --namespace kafscale -f kafscale-values.yaml",
		"Helm 3.14",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("docs/upgrade-compatibility.md does not contain %q", want)
		}
	}
	if strings.Contains(text, "--namespace kafscale --reuse-values") {
		t.Error("docs/upgrade-compatibility.md upgrades from 1.x with --reuse-values, which drops the 2.0 chart defaults")
	}
	for _, doc := range []string{"docs/quickstart.md", "docs/operations.md", "deploy/helm/README.md", "deploy/helm/kafscale/README.md"} {
		if !strings.Contains(readRepoDoc(t, doc), "- Helm 3.14+") {
			t.Errorf("%s does not require Helm 3.14+", doc)
		}
	}
}
