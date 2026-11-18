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

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/yaml"
)

func chartAppVersion(t *testing.T, chart string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(chart, "Chart.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "appVersion:") {
			continue
		}
		return strings.Trim(strings.TrimSpace(strings.TrimPrefix(line, "appVersion:")), `"'`)
	}
	t.Fatal("Chart.yaml has no appVersion")
	return ""
}

func renderOperatorDeployment(t *testing.T, chart string, setArgs ...string) appsv1.Deployment {
	t.Helper()
	args := append([]string{"template", "ks", chart, "--namespace", "kafscale", "--show-only", "templates/operator-deployment.yaml"}, setArgs...)
	output, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatal(err, string(output))
	}
	var deployment appsv1.Deployment
	if err := yaml.Unmarshal(output, &deployment); err != nil {
		t.Fatal(err)
	}
	if len(deployment.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("operator deployment has no containers")
	}
	return deployment
}

func envMap(deployment appsv1.Deployment, names ...string) map[string]string {
	want := map[string]struct{}{}
	for _, name := range names {
		want[name] = struct{}{}
	}
	got := map[string]string{}
	for _, env := range deployment.Spec.Template.Spec.Containers[0].Env {
		if _, ok := want[env.Name]; ok {
			got[env.Name] = env.Value
		}
	}
	return got
}

func TestOperatorLfsProxyImageEnv(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	appVersion := chartAppVersion(t, chart)
	for _, tc := range []struct {
		name       string
		args       []string
		image      string
		pullPolicy string
	}{
		{
			name:       "defaults to appVersion",
			image:      "ghcr.io/kafscale/kafscale-proxy:" + appVersion,
			pullPolicy: "IfNotPresent",
		},
		{
			name:       "useLatest",
			args:       []string{"--set", "operator.lfsProxyImage.useLatest=true"},
			image:      "ghcr.io/kafscale/kafscale-proxy:latest",
			pullPolicy: "Always",
		},
		{
			name:       "tag override",
			args:       []string{"--set", "operator.lfsProxyImage.tag=v2.0.0-test"},
			image:      "ghcr.io/kafscale/kafscale-proxy:v2.0.0-test",
			pullPolicy: "IfNotPresent",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := envMap(renderOperatorDeployment(t, chart, tc.args...), "LFS_PROXY_IMAGE", "LFS_PROXY_IMAGE_PULL_POLICY")
			if got["LFS_PROXY_IMAGE"] != tc.image {
				t.Errorf("LFS_PROXY_IMAGE = %q, want %q", got["LFS_PROXY_IMAGE"], tc.image)
			}
			if got["LFS_PROXY_IMAGE_PULL_POLICY"] != tc.pullPolicy {
				t.Errorf("LFS_PROXY_IMAGE_PULL_POLICY = %q, want %q", got["LFS_PROXY_IMAGE_PULL_POLICY"], tc.pullPolicy)
			}
		})
	}
}

func TestOperatorEtcdAndSnapshotImageEnv(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	const (
		defaultEtcd     = "gcr.io/etcd-development/etcd:v3.6.14@sha256:a491baeaa0cb0c9cd89c0062ac44ece53886e3e5bddad18d2daf36678ce665b6"
		defaultSnapshot = "amazon/aws-cli:2.15.0"
	)
	for _, tc := range []struct {
		name     string
		args     []string
		etcd     string
		snapshot string
	}{
		{
			name:     "defaults to pinned upstream images",
			etcd:     defaultEtcd,
			snapshot: defaultSnapshot,
		},
		{
			name:     "tag override",
			args:     []string{"--set", "operator.etcdImage.tag=v3.6.15", "--set", "operator.etcdSnapshotImage.tag=2.16.0"},
			etcd:     "gcr.io/etcd-development/etcd:v3.6.15",
			snapshot: "amazon/aws-cli:2.16.0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := envMap(renderOperatorDeployment(t, chart, tc.args...), "KAFSCALE_OPERATOR_ETCD_IMAGE", "KAFSCALE_OPERATOR_ETCD_SNAPSHOT_IMAGE")
			if got["KAFSCALE_OPERATOR_ETCD_IMAGE"] != tc.etcd {
				t.Errorf("KAFSCALE_OPERATOR_ETCD_IMAGE = %q, want %q", got["KAFSCALE_OPERATOR_ETCD_IMAGE"], tc.etcd)
			}
			if got["KAFSCALE_OPERATOR_ETCD_SNAPSHOT_IMAGE"] != tc.snapshot {
				t.Errorf("KAFSCALE_OPERATOR_ETCD_SNAPSHOT_IMAGE = %q, want %q", got["KAFSCALE_OPERATOR_ETCD_SNAPSHOT_IMAGE"], tc.snapshot)
			}
		})
	}
}
