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
	"os/exec"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

func TestExistingSecretRendersSecretKeyRefAndOmitsInline(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")

	cases := []struct {
		name      string
		template  string
		enable    []string
		secretSet string
		inline    []string
		secret    string
		want      map[string]string // env name -> secret key
	}{
		{
			name:      "console auth",
			template:  "templates/console-deployment.yaml",
			enable:    []string{"--set", "console.enabled=true"},
			secretSet: "console.auth.existingSecret",
			inline:    []string{"--set", "console.auth.username=inline-user", "--set", "console.auth.password=inline-pass"},
			secret:    "kafscale-console-auth",
			want: map[string]string{
				"KAFSCALE_UI_USERNAME": "username",
				"KAFSCALE_UI_PASSWORD": "password",
			},
		},
		{
			name:      "console etcd",
			template:  "templates/console-deployment.yaml",
			enable:    []string{"--set", "console.enabled=true"},
			secretSet: "console.etcd.existingSecret",
			inline:    []string{"--set", "console.etcd.username=inline-user", "--set", "console.etcd.password=inline-pass"},
			secret:    "console-etcd-auth",
			want: map[string]string{
				"KAFSCALE_CONSOLE_ETCD_USERNAME": "ETCD_USERNAME",
				"KAFSCALE_CONSOLE_ETCD_PASSWORD": "ETCD_PASSWORD",
			},
		},
		{
			name:      "proxy etcd",
			template:  "templates/proxy-deployment.yaml",
			enable:    []string{"--set", "proxy.enabled=true"},
			secretSet: "proxy.etcd.existingSecret",
			inline:    []string{"--set", "proxy.etcd.username=inline-user", "--set", "proxy.etcd.password=inline-pass"},
			secret:    "proxy-etcd-auth",
			want: map[string]string{
				"KAFSCALE_PROXY_ETCD_USERNAME": "ETCD_USERNAME",
				"KAFSCALE_PROXY_ETCD_PASSWORD": "ETCD_PASSWORD",
			},
		},
		{
			name:      "mcp etcd",
			template:  "templates/mcp-deployment.yaml",
			enable:    []string{"--set", "mcp.enabled=true"},
			secretSet: "mcp.etcd.existingSecret",
			inline:    []string{"--set", "mcp.etcd.username=inline-user", "--set", "mcp.etcd.password=inline-pass"},
			secret:    "mcp-etcd-auth",
			want: map[string]string{
				"KAFSCALE_MCP_ETCD_USERNAME": "ETCD_USERNAME",
				"KAFSCALE_MCP_ETCD_PASSWORD": "ETCD_PASSWORD",
			},
		},
		{
			name:      "schema registry etcd",
			template:  "templates/schema-registry-deployment.yaml",
			enable:    []string{"--set", "schemaRegistry.enabled=true", "--set", "schemaRegistry.etcdEndpoints[0]=http://etcd:2379"},
			secretSet: "schemaRegistry.etcd.existingSecret",
			secret:    "sr-etcd-auth",
			want: map[string]string{
				"KAFSCALE_SCHEMA_REGISTRY_ETCD_USERNAME": "ETCD_USERNAME",
				"KAFSCALE_SCHEMA_REGISTRY_ETCD_PASSWORD": "ETCD_PASSWORD",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name+" existingSecret", func(t *testing.T) {
			args := append([]string{"template", "sec", chart, "--show-only", tc.template}, tc.enable...)
			args = append(args, "--set", tc.secretSet+"="+tc.secret)
			args = append(args, tc.inline...)
			assertSecretKeyRefs(t, args, tc.secret, tc.want)
		})
		if len(tc.inline) == 0 {
			continue
		}
		t.Run(tc.name+" inline fallback", func(t *testing.T) {
			args := append([]string{"template", "sec", chart, "--show-only", tc.template}, tc.enable...)
			args = append(args, tc.inline...)
			env := renderEnv(t, args)
			for name := range tc.want {
				got, ok := env[name]
				if !ok {
					t.Errorf("%s is missing when existingSecret is empty", name)
					continue
				}
				if got.ValueFrom != nil && got.ValueFrom.SecretKeyRef != nil {
					t.Errorf("%s used secretKeyRef %q when existingSecret is empty", name, got.ValueFrom.SecretKeyRef.Name)
				}
				if got.Value == "" {
					t.Errorf("%s has no inline value when existingSecret is empty", name)
				}
				if got.Value == "inline-user" || got.Value == "inline-pass" {
					continue
				}
				t.Errorf("%s inline value = %q, want inline-user or inline-pass", name, got.Value)
			}
		})
	}
}

func renderEnv(t *testing.T, args []string) map[string]corev1.EnvVar {
	t.Helper()
	data, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template: %v %s", err, data)
	}
	var d appsv1.Deployment
	if err := yaml.Unmarshal(data, &d); err != nil {
		t.Fatal(err)
	}
	if len(d.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("deployment has no containers")
	}
	env := map[string]corev1.EnvVar{}
	for _, e := range d.Spec.Template.Spec.Containers[0].Env {
		env[e.Name] = e
	}
	return env
}

func assertSecretKeyRefs(t *testing.T, args []string, secret string, want map[string]string) {
	t.Helper()
	env := renderEnv(t, args)
	for name, key := range want {
		got, ok := env[name]
		if !ok {
			t.Errorf("%s is missing", name)
			continue
		}
		if got.Value != "" {
			t.Errorf("%s has inline value %q when existingSecret is set", name, got.Value)
		}
		if got.ValueFrom == nil || got.ValueFrom.SecretKeyRef == nil {
			t.Errorf("%s has no secretKeyRef", name)
			continue
		}
		ref := got.ValueFrom.SecretKeyRef
		if ref.Name != secret {
			t.Errorf("%s reads Secret %q, want %q", name, ref.Name, secret)
		}
		if ref.Key != key {
			t.Errorf("%s maps key %q, want %q", name, ref.Key, key)
		}
	}
}
