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
	"sigs.k8s.io/yaml"
)

func TestSchemaRegistryS3CredentialsMatchRotationDocs(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	const secret = "sr-s3-creds"
	data, err := exec.Command("helm", "template", "rot", chart, "--show-only", "templates/schema-registry-deployment.yaml",
		"--set", "schemaRegistry.enabled=true", "--set", "schemaRegistry.etcdEndpoints[0]=http://etcd:2379",
		"--set", "schemaRegistry.s3.bucket=b", "--set", "schemaRegistry.s3.credentialsSecret="+secret).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template: %v %s", err, data)
	}
	var d appsv1.Deployment
	if err := yaml.Unmarshal(data, &d); err != nil {
		t.Fatal(err)
	}
	if d.Kind != "Deployment" || d.Labels["app.kubernetes.io/component"] != "schema-registry" || d.Labels["app.kubernetes.io/instance"] != "rot" {
		t.Fatalf("schema registry Deployment labels changed: kind=%q labels=%v", d.Kind, d.Labels)
	}
	want := map[string]string{"KAFSCALE_S3_ACCESS_KEY": "AWS_ACCESS_KEY_ID", "KAFSCALE_S3_SECRET_KEY": "AWS_SECRET_ACCESS_KEY"}
	got := map[string]string{}
	for _, c := range d.Spec.Template.Spec.Containers {
		for _, e := range c.Env {
			if e.Name == "KAFSCALE_S3_SESSION_TOKEN" || e.Name == "AWS_SESSION_TOKEN" {
				t.Errorf("container %s maps %s; the rotation docs say the chart passes no session token", c.Name, e.Name)
			}
			if _, ok := want[e.Name]; !ok {
				continue
			}
			if e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
				t.Errorf("container %s sets %s without a secretKeyRef", c.Name, e.Name)
				continue
			}
			ref := e.ValueFrom.SecretKeyRef
			if ref.Name != secret {
				t.Errorf("%s reads Secret %q, want %q", e.Name, ref.Name, secret)
			}
			got[e.Name] = ref.Key
		}
		for _, src := range c.EnvFrom {
			if src.SecretRef != nil {
				t.Errorf("container %s loads Secret %q with envFrom; the rotation docs name only two keys", c.Name, src.SecretRef.Name)
			}
		}
	}
	for env, key := range want {
		if got[env] != key {
			t.Errorf("%s maps key %q, want %q from Secret %s", env, got[env], key, secret)
		}
	}
	if v, ok := d.Spec.Template.Annotations["kafscale.io/s3-credentials-checksum"]; ok {
		t.Errorf("pod template has kafscale.io/s3-credentials-checksum=%q; the rotation docs say a key change needs a manual restart", v)
	}
}
