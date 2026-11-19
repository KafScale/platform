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
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/yaml"
)

func mcpAuthChart(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	return filepath.Join("..", "..", "deploy", "helm", "kafscale")
}

func helmTemplateMCP(t *testing.T, chart string, sets ...string) []byte {
	t.Helper()
	args := []string{"template", "mcpauth", chart, "--set", "mcp.enabled=true"}
	args = append(args, sets...)
	data, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, data)
	}
	return data
}

func splitYAML(data []byte) [][]byte {
	var docs [][]byte
	for _, part := range bytes.Split(data, []byte("\n---")) {
		part = bytes.TrimSpace(part)
		part = bytes.TrimPrefix(part, []byte("---"))
		part = bytes.TrimSpace(part)
		if len(part) == 0 || bytes.HasPrefix(part, []byte("#")) && !bytes.Contains(part, []byte("\nkind:")) {
			continue
		}
		docs = append(docs, part)
	}
	return docs
}

func mcpDeploymentAndSecrets(t *testing.T, data []byte) (appsv1.Deployment, []corev1.Secret) {
	t.Helper()
	var dep appsv1.Deployment
	var secrets []corev1.Secret
	found := false
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
				t.Fatalf("decode Deployment: %v", err)
			}
			if d.Labels["app.kubernetes.io/component"] != "mcp" {
				continue
			}
			dep = d
			found = true
		case "Secret":
			var s corev1.Secret
			if err := yaml.Unmarshal(doc, &s); err != nil {
				t.Fatalf("decode Secret: %v", err)
			}
			secrets = append(secrets, s)
		}
	}
	if !found {
		t.Fatal("MCP Deployment not found")
	}
	return dep, secrets
}

func mcpAuthEnv(t *testing.T, d appsv1.Deployment) *corev1.EnvVar {
	t.Helper()
	for _, c := range d.Spec.Template.Spec.Containers {
		for i, e := range c.Env {
			if e.Name == "KAFSCALE_MCP_AUTH_TOKEN" {
				return &c.Env[i]
			}
		}
	}
	t.Fatal("KAFSCALE_MCP_AUTH_TOKEN is not set on the MCP Deployment")
	return nil
}

func TestMCPAuthTokenUsesChartSecret(t *testing.T) {
	chart := mcpAuthChart(t)
	const token = "supersecret-mcp-token"
	data := helmTemplateMCP(t, chart, "--set", "mcp.auth.token="+token)
	dep, secrets := mcpDeploymentAndSecrets(t, data)
	if strings.Contains(string(mustMarshalDeployment(t, dep)), token) {
		t.Fatal("MCP Deployment still contains the token in plain text")
	}
	env := mcpAuthEnv(t, dep)
	if env.Value != "" {
		t.Fatalf("KAFSCALE_MCP_AUTH_TOKEN is a plain value %q, want a secretKeyRef", env.Value)
	}
	if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
		t.Fatal("KAFSCALE_MCP_AUTH_TOKEN has no secretKeyRef")
	}
	ref := env.ValueFrom.SecretKeyRef
	if ref.Key != "token" {
		t.Fatalf("secretKeyRef key %q, want token", ref.Key)
	}
	var found *corev1.Secret
	for i := range secrets {
		if secrets[i].Name == ref.Name {
			found = &secrets[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("chart Secret %q not rendered for mcp.auth.token", ref.Name)
	}
	if found.Namespace != "kafscale-mcp" {
		t.Fatalf("Secret namespace %q, want kafscale-mcp", found.Namespace)
	}
	got := string(found.Data["token"])
	if got == "" {
		got = found.StringData["token"]
	}
	if got != token {
		t.Fatalf("Secret token %q, want %q", got, token)
	}
	sum := dep.Spec.Template.Annotations["kafscale.io/mcp-auth-checksum"]
	if sum == "" {
		t.Fatal("pod template has no kafscale.io/mcp-auth-checksum; a token change would not roll the pod")
	}
	data2 := helmTemplateMCP(t, chart, "--set", "mcp.auth.token="+token+"-changed")
	dep2, _ := mcpDeploymentAndSecrets(t, data2)
	if dep2.Spec.Template.Annotations["kafscale.io/mcp-auth-checksum"] == sum {
		t.Fatal("checksum did not change when mcp.auth.token changed")
	}
}

func TestMCPAuthExistingSecret(t *testing.T) {
	chart := mcpAuthChart(t)
	data := helmTemplateMCP(t, chart,
		"--set", "mcp.auth.existingSecret=my-mcp-token",
		"--set", "mcp.auth.existingSecretKey=mcp-token",
		"--set", "mcp.auth.token=should-not-appear",
	)
	dep, secrets := mcpDeploymentAndSecrets(t, data)
	if strings.Contains(string(mustMarshalDeployment(t, dep)), "should-not-appear") {
		t.Fatal("MCP Deployment contains mcp.auth.token in plain text when existingSecret is set")
	}
	for _, s := range secrets {
		if strings.Contains(s.Name, "mcp") && (string(s.Data["token"]) == "should-not-appear" || s.StringData["token"] == "should-not-appear") {
			t.Fatalf("chart still rendered a Secret %q from mcp.auth.token while existingSecret is set", s.Name)
		}
	}
	env := mcpAuthEnv(t, dep)
	if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
		t.Fatal("KAFSCALE_MCP_AUTH_TOKEN has no secretKeyRef")
	}
	ref := env.ValueFrom.SecretKeyRef
	if ref.Name != "my-mcp-token" || ref.Key != "mcp-token" {
		t.Fatalf("secretKeyRef = %s/%s, want my-mcp-token/mcp-token", ref.Name, ref.Key)
	}
}

func TestMCPExistingSecretDocUsesExternalNamespace(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "ops-mcp.md"))
	if err != nil {
		t.Fatal(err)
	}
	var example string
	for _, block := range strings.Split(string(doc), "```yaml\n")[1:] {
		body, _, ok := strings.Cut(block, "```")
		if ok && strings.Contains(body, "existingSecret:") {
			example = body
			break
		}
	}
	if example == "" {
		t.Fatal("existingSecret values example is missing")
	}
	var values struct {
		MCP struct {
			Namespace struct {
				Name   string
				Create *bool
			}
			Auth struct {
				ExistingSecret    string
				ExistingSecretKey string
			}
		}
	}
	if err := yaml.Unmarshal([]byte(example), &values); err != nil {
		t.Fatal(err)
	}
	if values.MCP.Namespace.Create == nil || *values.MCP.Namespace.Create {
		t.Fatal("existingSecret first-install example must disable chart namespace creation")
	}
	if values.MCP.Namespace.Name == "" || values.MCP.Auth.ExistingSecret == "" || values.MCP.Auth.ExistingSecretKey == "" {
		t.Fatal("existingSecret example must specify its namespace and Secret reference")
	}
	chart := mcpAuthChart(t)
	file := filepath.Join(t.TempDir(), "values.yaml")
	if err := os.WriteFile(file, []byte(example), 0o600); err != nil {
		t.Fatal(err)
	}
	rendered := helmTemplateMCP(t, chart, "-f", file)
	dep, secrets := mcpDeploymentAndSecrets(t, rendered)
	for _, document := range splitYAML(rendered) {
		var obj struct {
			Kind     string
			Metadata struct{ Name string }
		}
		if err := yaml.Unmarshal(document, &obj); err != nil {
			t.Fatal(err)
		}
		if obj.Kind == "Namespace" && obj.Metadata.Name == values.MCP.Namespace.Name {
			t.Fatal("documented values render the pre-existing Namespace, causing a Helm ownership conflict")
		}
	}
	if dep.Namespace != values.MCP.Namespace.Name {
		t.Fatalf("MCP namespace %q, want %q", dep.Namespace, values.MCP.Namespace.Name)
	}
	env := mcpAuthEnv(t, dep)
	if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
		t.Fatal("MCP auth token must use the documented existing Secret")
	}
	ref := env.ValueFrom.SecretKeyRef
	if ref.Name != values.MCP.Auth.ExistingSecret || ref.Key != values.MCP.Auth.ExistingSecretKey {
		t.Fatalf("Secret reference = %s/%s, want %s/%s", ref.Name, ref.Key, values.MCP.Auth.ExistingSecret, values.MCP.Auth.ExistingSecretKey)
	}
	for _, secret := range secrets {
		if secret.Name == ref.Name && secret.Namespace == dep.Namespace {
			t.Fatal("chart must not create the documented existing Secret")
		}
	}
}

func mustMarshalDeployment(t *testing.T, d appsv1.Deployment) []byte {
	t.Helper()
	data, err := yaml.Marshal(&d)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestMCPAuthNoToken(t *testing.T) {
	chart := mcpAuthChart(t)
	data := helmTemplateMCP(t, chart)
	dep, secrets := mcpDeploymentAndSecrets(t, data)
	for _, c := range dep.Spec.Template.Spec.Containers {
		for _, e := range c.Env {
			if e.Name == "KAFSCALE_MCP_AUTH_TOKEN" {
				t.Fatal("KAFSCALE_MCP_AUTH_TOKEN is set when no auth is configured")
			}
		}
	}
	for _, s := range secrets {
		if strings.Contains(s.Name, "mcp-auth") {
			t.Fatalf("chart rendered MCP auth Secret %q when no auth is configured", s.Name)
		}
	}
}

func TestMCPAuthExistingSecretKeyRequiresSecret(t *testing.T) {
	chart := mcpAuthChart(t)
	args := []string{"template", "mcpauth", chart, "--set", "mcp.enabled=true", "--set", "mcp.auth.existingSecretKey=mcp-token"}
	data, err := exec.Command("helm", args...).CombinedOutput()
	if err == nil {
		t.Fatalf("helm template succeeded with existingSecretKey and no existingSecret:\n%s", data)
	}
	if !strings.Contains(string(data), "mcp.auth.existingSecretKey requires mcp.auth.existingSecret") {
		t.Fatalf("helm template error does not mention existingSecret:\n%s", data)
	}
}

func TestMCPAuthExistingSecretChecksumChangesWithNameAndKey(t *testing.T) {
	chart := mcpAuthChart(t)
	data := helmTemplateMCP(t, chart, "--set", "mcp.auth.existingSecret=my-mcp-token")
	dep, _ := mcpDeploymentAndSecrets(t, data)
	sum := dep.Spec.Template.Annotations["kafscale.io/mcp-auth-checksum"]
	if sum == "" {
		t.Fatal("pod template has no kafscale.io/mcp-auth-checksum when existingSecret is set")
	}
	dataName := helmTemplateMCP(t, chart, "--set", "mcp.auth.existingSecret=other-mcp-token")
	depName, _ := mcpDeploymentAndSecrets(t, dataName)
	if depName.Spec.Template.Annotations["kafscale.io/mcp-auth-checksum"] == sum {
		t.Fatal("checksum did not change when existingSecret name changed")
	}
	dataKey := helmTemplateMCP(t, chart, "--set", "mcp.auth.existingSecret=my-mcp-token", "--set", "mcp.auth.existingSecretKey=mcp-token")
	depKey, _ := mcpDeploymentAndSecrets(t, dataKey)
	if depKey.Spec.Template.Annotations["kafscale.io/mcp-auth-checksum"] == sum {
		t.Fatal("checksum did not change when existingSecretKey changed")
	}
}

func TestMCPAuthKeepsPodAnnotationsAndChecksum(t *testing.T) {
	chart := mcpAuthChart(t)
	values := filepath.Join(t.TempDir(), "values.yaml")
	body := []byte(`mcp:
  auth:
    token: supersecret-mcp-token
  podAnnotations:
    custom: kept
    kafscale.io/mcp-auth-checksum: user-override
`)
	if err := os.WriteFile(values, body, 0o644); err != nil {
		t.Fatal(err)
	}
	data := helmTemplateMCP(t, chart, "-f", values)
	dep, _ := mcpDeploymentAndSecrets(t, data)
	ann := dep.Spec.Template.Annotations
	if ann["custom"] != "kept" {
		t.Fatalf("pod annotation custom=%q, want kept", ann["custom"])
	}
	sum := ann["kafscale.io/mcp-auth-checksum"]
	if sum == "" || sum == "user-override" {
		t.Fatalf("checksum annotation %q, want the chart checksum rather than a user override", sum)
	}
}

func TestMCPAuthChecksumIgnoresChartVersion(t *testing.T) {
	chart := mcpAuthChart(t)
	copyDir := t.TempDir()
	data, err := exec.Command("cp", "-a", chart+"/.", copyDir).CombinedOutput()
	if err != nil {
		t.Fatalf("copy chart: %v\n%s", err, data)
	}
	sumFor := func(version string) string {
		t.Helper()
		chartYAML := filepath.Join(copyDir, "Chart.yaml")
		body, err := os.ReadFile(chartYAML)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(body), "\n")
		for i, line := range lines {
			if strings.HasPrefix(line, "version:") {
				lines[i] = "version: " + version
			}
		}
		if err := os.WriteFile(chartYAML, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
			t.Fatal(err)
		}
		rendered := helmTemplateMCP(t, copyDir, "--set", "mcp.auth.token=supersecret-mcp-token")
		dep, _ := mcpDeploymentAndSecrets(t, rendered)
		sum := dep.Spec.Template.Annotations["kafscale.io/mcp-auth-checksum"]
		if sum == "" {
			t.Fatal("pod template has no kafscale.io/mcp-auth-checksum")
		}
		return sum
	}
	if sumFor("0.1.0") != sumFor("9.9.9") {
		t.Fatal("checksum changed when only the chart version changed")
	}
}

func TestMCPAuthDocumentedRestartMatchesRenderedDeployment(t *testing.T) {
	chart := mcpAuthChart(t)
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "ops-mcp.md"))
	if err != nil {
		t.Fatal(err)
	}
	var script string
	for _, block := range strings.Split(string(doc), "```bash\n")[1:] {
		body, _, ok := strings.Cut(block, "```")
		if ok && strings.Contains(body, "rollout restart") {
			script = body
			break
		}
	}
	if script == "" {
		t.Fatal("missing token rotation Bash example")
	}
	for _, tc := range []struct {
		name      string
		namespace string
		sets      []string
	}{
		{name: "default", namespace: "kafscale-mcp"},
		{name: "name_override", namespace: "kafscale-mcp", sets: []string{"--set", "nameOverride=custom"}},
		{name: "fullname_override", namespace: "kafscale-mcp", sets: []string{"--set", "fullnameOverride=custom-full"}},
		{name: "both_overrides_custom_namespace", namespace: "custom-mcp", sets: []string{"--set", "nameOverride=custom", "--set", "fullnameOverride=custom-full"}},
		{name: "truncated_name", namespace: "kafscale-mcp", sets: []string{"--set", "fullnameOverride=" + strings.Repeat("a", 63)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sets := append([]string{"--set", "mcp.auth.token=test-token", "--set", "mcp.namespace.name=" + tc.namespace}, tc.sets...)
			dep, secrets := mcpDeploymentAndSecrets(t, helmTemplateMCP(t, chart, sets...))
			env := mcpAuthEnv(t, dep)
			if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
				t.Fatal("MCP auth environment variable has no Secret reference")
			}
			ref := env.ValueFrom.SecretKeyRef
			if ref.Name != dep.Name+"-auth" {
				t.Fatalf("Secret reference %q, want Deployment name plus -auth (%s-auth)", ref.Name, dep.Name)
			}
			found := false
			for _, secret := range secrets {
				if secret.Name == ref.Name && secret.Namespace == dep.Namespace {
					found = true
				}
			}
			if !found {
				t.Fatal("documented chart-managed auth Secret is missing")
			}
			// Fill in the documented values, then execute the example without a cluster.
			example := strings.ReplaceAll(script, "'<release>'", "'mcpauth'")
			example = strings.ReplaceAll(example, "namespace='kafscale-mcp'", "namespace='"+tc.namespace+"'")
			cmd := exec.Command("bash", "-ec", `kubectl() { printf '%s\0' "$@"; }
`+example)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("documented restart failed: %v\n%s", err, output)
			}
			args := strings.Split(strings.TrimSuffix(string(output), "\x00"), "\x00")
			if len(args) != 6 || !slices.Equal(args[:5], []string{"-n", dep.Namespace, "rollout", "restart", "deployment"}) || !strings.HasPrefix(args[5], "--selector=") {
				t.Fatalf("unexpected restart arguments: %q", args)
			}
			selector, err := labels.Parse(strings.TrimPrefix(args[5], "--selector="))
			if err != nil {
				t.Fatal(err)
			}
			if !selector.Matches(labels.Set(dep.Labels)) {
				t.Fatalf("documented selector %s does not match rendered MCP Deployment %s", selector, dep.Name)
			}
			for _, other := range []labels.Set{
				{"app.kubernetes.io/instance": "other-release", "app.kubernetes.io/component": "mcp"},
				{"app.kubernetes.io/instance": "mcpauth", "app.kubernetes.io/component": "console"},
			} {
				if selector.Matches(other) {
					t.Fatalf("documented selector %s also targets unrelated Deployment labels %v", selector, other)
				}
			}
		})
	}
}

func TestMCPRunbookBashSyntax(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "ops-mcp.md"))
	if err != nil {
		t.Fatal(err)
	}
	for i, block := range strings.Split(string(doc), "```bash\n")[1:] {
		script, _, ok := strings.Cut(block, "```")
		if !ok {
			t.Fatalf("Bash block %d has no closing fence", i+1)
		}
		cmd := exec.Command("bash", "-n")
		cmd.Stdin = strings.NewReader(script)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("Bash block %d has invalid syntax: %v\n%s", i+1, err, output)
		}
	}
}
