// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

func TestSchemaRegistryPathStyle(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	render := func(flag, value string) ([]byte, error) {
		args := []string{"template", "sr", chart, "--show-only", "templates/schema-registry-deployment.yaml",
			"--set", "schemaRegistry.enabled=true", "--set", "schemaRegistry.etcdEndpoints[0]=http://etcd:2379",
			"--set", "schemaRegistry.s3.bucket=b", "--set", "schemaRegistry.s3.namespace=prod"}
		if flag != "" {
			args = append(args, flag, "schemaRegistry.s3.pathStyle="+value)
		}
		return exec.Command("helm", args...).CombinedOutput()
	}
	cases := []struct {
		flag, value, want string
		set               bool
	}{
		{"", "", "", false},
		{"--set", "true", "true", true},
		{"--set", "false", "false", true},
		{"--set-string", "false", "false", true},
		{"--set-string", "True", "true", true},
		{"--set-string", "", "", false},
	}
	for _, c := range cases {
		data, err := render(c.flag, c.value)
		if err != nil {
			t.Fatalf("%s %q: %v %s", c.flag, c.value, err, data)
		}
		var d appsv1.Deployment
		if err := yaml.Unmarshal(data, &d); err != nil {
			t.Fatal(err)
		}
		got, set := "", false
		for _, e := range d.Spec.Template.Spec.Containers[0].Env {
			if e.Name == "KAFSCALE_S3_PATH_STYLE" {
				got, set = e.Value, true
			}
		}
		if set != c.set || got != c.want {
			t.Fatalf("%s %q gave set %v value %q", c.flag, c.value, set, got)
		}
	}
	for _, bad := range [][2]string{{"--set-string", "yes"}, {"--set", "1"}} {
		if data, err := render(bad[0], bad[1]); err == nil {
			t.Fatalf("pathStyle %q rendered %s", bad[1], data)
		}
	}
}
