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
	"regexp"
	"sort"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

type processorChart struct {
	name      string
	dir       string
	guide     string
	kind      string
	image     string
	wholeKeys []string
}

func processorCharts() []processorChart {
	root := filepath.Join("..", "..", "addons", "processors")
	return []processorChart{
		{
			name:      "iceberg-processor",
			dir:       filepath.Join(root, "iceberg-processor", "deploy", "helm", "iceberg-processor"),
			guide:     filepath.Join(root, "iceberg-processor", "user-guide.md"),
			kind:      filepath.Join(root, "iceberg-processor", "deploy", "kind", "values-kind.yaml"),
			image:     "ghcr.io/kafscale/kafscale-iceberg-processor",
			wholeKeys: []string{"config.s3", "config.iceberg", "config.offsets", "config.processor", "config.discovery", "config.etcd", "config.schema"},
		},
		{
			name:  "sql-processor",
			dir:   filepath.Join(root, "sql-processor", "deploy", "helm", "sql-processor"),
			guide: filepath.Join(root, "sql-processor", "user-guide.md"),
			kind:  filepath.Join(root, "sql-processor", "deploy", "kind", "values-kind.yaml"),
			image: "ghcr.io/kafscale/kafscale-sql-processor",
		},
	}
}

func requireHelm(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
}

func helmTemplateProcessor(t *testing.T, chart string, args ...string) []byte {
	t.Helper()
	cmd := append([]string{"template", "demo", chart}, args...)
	data, err := exec.Command("helm", cmd...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template %s: %v\n%s", chart, err, data)
	}
	return data
}

type renderedProcessor struct {
	deployments []appsv1.Deployment
	services    []corev1.Service
}

func decodeProcessor(t *testing.T, data []byte) renderedProcessor {
	t.Helper()
	var out renderedProcessor
	for _, doc := range splitYAML(data) {
		var meta struct {
			Kind string `json:"kind"`
		}
		if err := yaml.Unmarshal(doc, &meta); err != nil {
			t.Fatalf("decode document: %v\n%s", err, doc)
		}
		switch meta.Kind {
		case "Deployment":
			var d appsv1.Deployment
			if err := yaml.Unmarshal(doc, &d); err != nil {
				t.Fatal(err)
			}
			out.deployments = append(out.deployments, d)
		case "Service":
			var s corev1.Service
			if err := yaml.Unmarshal(doc, &s); err != nil {
				t.Fatal(err)
			}
			out.services = append(out.services, s)
		}
	}
	return out
}

func TestProcessorChartsDefaultToReleaseImage(t *testing.T) {
	requireHelm(t)
	for _, c := range processorCharts() {
		t.Run(c.name, func(t *testing.T) {
			want := c.image + ":" + chartAppVersion(t, c.dir)
			r := decodeProcessor(t, helmTemplateProcessor(t, c.dir, "--set", "proxy.enabled=true"))
			if len(r.deployments) == 0 {
				t.Fatal("no Deployment rendered")
			}
			for _, d := range r.deployments {
				for _, ctr := range d.Spec.Template.Spec.Containers {
					if ctr.Image != want {
						t.Errorf("%s container %s image = %q, want %q", d.Name, ctr.Name, ctr.Image, want)
					}
				}
			}
			r = decodeProcessor(t, helmTemplateProcessor(t, c.dir, "--set", "image.tag=v9.9.9"))
			if got := r.deployments[0].Spec.Template.Spec.Containers[0].Image; got != c.image+":v9.9.9" {
				t.Errorf("image with tag override = %q", got)
			}
		})
	}
}

func TestSQLProcessorChartServiceExposesExecutor(t *testing.T) {
	requireHelm(t)
	chart := processorCharts()[1].dir
	r := decodeProcessor(t, helmTemplateProcessor(t, chart))
	if len(r.services) != 1 {
		t.Fatalf("rendered %d Services, want 1", len(r.services))
	}
	svc := r.services[0]
	if svc.Name != "demo-sql-processor" || svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("Service %s type %s", svc.Name, svc.Spec.Type)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 5432 || svc.Spec.Ports[0].TargetPort.String() != "pgwire" {
		t.Fatalf("Service ports = %+v", svc.Spec.Ports)
	}
	dep := r.deployments[0]
	if svc.Spec.Selector["app.kubernetes.io/instance"] != dep.Spec.Template.Labels["app.kubernetes.io/instance"] ||
		svc.Spec.Selector["app.kubernetes.io/name"] != dep.Spec.Template.Labels["app.kubernetes.io/name"] {
		t.Fatalf("Service selector %v does not match pod labels %v", svc.Spec.Selector, dep.Spec.Template.Labels)
	}
	ports := dep.Spec.Template.Spec.Containers[0].Ports
	if len(ports) != 1 || ports[0].Name != "pgwire" || ports[0].ContainerPort != 5432 {
		t.Fatalf("container ports = %+v", ports)
	}

	r = decodeProcessor(t, helmTemplateProcessor(t, chart,
		"--set", "service.type=NodePort", "--set", "service.port=15432", "--set", "service.targetPort=6543",
		"--set", "service.annotations.example\\.com/team=data"))
	svc = r.services[0]
	if svc.Spec.Type != corev1.ServiceTypeNodePort || svc.Spec.Ports[0].Port != 15432 || svc.Annotations["example.com/team"] != "data" {
		t.Fatalf("Service overrides not applied: %+v", svc)
	}
	if got := r.deployments[0].Spec.Template.Spec.Containers[0].Ports[0].ContainerPort; got != 6543 {
		t.Fatalf("containerPort = %d, want 6543", got)
	}

	r = decodeProcessor(t, helmTemplateProcessor(t, chart, "--set", "service.enabled=false"))
	if len(r.services) != 0 {
		t.Fatalf("service.enabled=false rendered %d Services", len(r.services))
	}
}

func flattenValues(prefix string, v any, whole map[string]bool, out map[string]struct{}) {
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 || whole[prefix] {
		out[prefix] = struct{}{}
		return
	}
	for k, child := range m {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		flattenValues(key, child, whole, out)
	}
}

func chartValueKeys(t *testing.T, c processorChart) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(c.dir, "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]any{}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	whole := map[string]bool{}
	for _, k := range c.wholeKeys {
		whole[k] = true
	}
	keys := map[string]struct{}{}
	flattenValues("", values, whole, keys)
	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func chartTemplates(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "templates"))
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, e := range entries {
		body, err := os.ReadFile(filepath.Join(dir, "templates", e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		b.Write(body)
	}
	return b.String()
}

func valueRead(templates, key string) bool {
	parts := strings.Split(key, ".")
	for i := len(parts); i > 0; i-- {
		ref := regexp.QuoteMeta(".Values." + strings.Join(parts[:i], "."))
		if i == len(parts) {
			if regexp.MustCompile(ref + `\b`).MatchString(templates) {
				return true
			}
			continue
		}
		if regexp.MustCompile(ref + `([^.A-Za-z0-9_]|$)`).MatchString(templates) {
			return true
		}
	}
	return false
}

func TestProcessorChartValuesAreReadAndDocumented(t *testing.T) {
	cellKey := regexp.MustCompile("`([A-Za-z][A-Za-z0-9_.]*)`")
	for _, c := range processorCharts() {
		t.Run(c.name, func(t *testing.T) {
			templates := chartTemplates(t, c.dir)
			guideRaw, err := os.ReadFile(c.guide)
			if err != nil {
				t.Fatal(err)
			}
			guide := string(guideRaw)
			keys := chartValueKeys(t, c)
			known := map[string]bool{}
			for _, key := range keys {
				known[key] = true
				if !valueRead(templates, key) {
					t.Errorf("values key %s is not read by any template", key)
				}
				if !strings.Contains(guide, "| `"+key+"`") && !strings.Contains(guide, "`"+key+"`,") && !strings.Contains(guide, ", `"+key+"`") {
					t.Errorf("values key %s is missing from the Helm values table in %s", key, c.guide)
				}
			}
			table := false
			for _, line := range strings.Split(guide, "\n") {
				if strings.HasPrefix(line, "| Key |") {
					table = true
					continue
				}
				if table && !strings.HasPrefix(line, "|") {
					break
				}
				if !table || strings.HasPrefix(line, "| ---") {
					continue
				}
				first := strings.SplitN(strings.TrimPrefix(line, "|"), "|", 2)[0]
				for _, m := range cellKey.FindAllStringSubmatch(first, -1) {
					key := m[1]
					if known[key] {
						continue
					}
					prefixed := false
					for _, w := range c.wholeKeys {
						if strings.HasPrefix(key, w+".") {
							prefixed = true
						}
					}
					if !prefixed {
						t.Errorf("%s documents values key %s that is not in values.yaml", c.guide, key)
					}
				}
			}
			if !table {
				t.Fatalf("%s has no Helm values table", c.guide)
			}
		})
	}
}

func TestProcessorKindValuesRenderWithSecretRefs(t *testing.T) {
	requireHelm(t)
	secrets := map[string]string{"iceberg-processor": "iceberg-s3-credentials", "sql-processor": "sql-s3-credentials"}
	for _, c := range processorCharts() {
		t.Run(c.name, func(t *testing.T) {
			raw, err := os.ReadFile(c.kind)
			if err != nil {
				t.Fatal(err)
			}
			if regexp.MustCompile(`(?i)(minioadmin|AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|password:\s*"[^"]+")`).Match(raw) {
				t.Fatalf("%s contains an inline credential", c.kind)
			}
			r := decodeProcessor(t, helmTemplateProcessor(t, c.dir, "-f", c.kind))
			ctr := r.deployments[0].Spec.Template.Spec.Containers[0]
			if len(ctr.EnvFrom) != 1 || ctr.EnvFrom[0].SecretRef == nil || ctr.EnvFrom[0].SecretRef.Name != secrets[c.name] {
				t.Fatalf("envFrom = %+v, want Secret %s", ctr.EnvFrom, secrets[c.name])
			}
		})
	}
}

func TestProcessorDemoDocsKeepSecretsOffTheCommandLine(t *testing.T) {
	files := []string{
		filepath.Join("..", "..", "scripts", "iceberg-demo.sh"),
		filepath.Join("..", "..", "scripts", "kafsql-demo.sh"),
	}
	for _, c := range processorCharts() {
		files = append(files, c.guide)
	}
	bad := regexp.MustCompile(`--from-literal|--set[ =][^\n]*(?i:password|access_key|secret_key|key_id)=|export AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY)="\$\{MINIO`)
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if m := bad.Find(raw); m != nil {
			t.Errorf("%s passes a credential on the command line: %s", f, m)
		}
	}
}
