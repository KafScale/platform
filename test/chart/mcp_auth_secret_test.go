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
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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

func mustMarshalDeployment(t *testing.T, d appsv1.Deployment) []byte {
	t.Helper()
	data, err := yaml.Marshal(&d)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
