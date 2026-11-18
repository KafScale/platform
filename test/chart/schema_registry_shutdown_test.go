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
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/yaml"
)

func TestSchemaRegistryShutdownValues(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	render := func(sets ...string) appsv1.Deployment {
		t.Helper()
		args := []string{"template", "sr", chart, "--show-only", "templates/schema-registry-deployment.yaml",
			"--set", "schemaRegistry.enabled=true", "--set", "schemaRegistry.etcdEndpoints[0]=http://etcd:2379"}
		for _, s := range sets {
			args = append(args, "--set", s)
		}
		data, err := exec.Command("helm", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %v %s", sets, err, data)
		}
		var d appsv1.Deployment
		if err := yaml.Unmarshal(data, &d); err != nil {
			t.Fatal(err)
		}
		return d
	}
	envOf := func(d appsv1.Deployment) map[string]string {
		out := map[string]string{}
		for _, e := range d.Spec.Template.Spec.Containers[0].Env {
			out[e.Name] = e.Value
		}
		return out
	}

	d := render()
	if g := d.Spec.Template.Spec.TerminationGracePeriodSeconds; g == nil || *g != 30 {
		t.Fatalf("default terminationGracePeriodSeconds %v, want 30", g)
	}
	if l := d.Spec.Template.Spec.Containers[0].Lifecycle; l != nil {
		t.Fatalf("default render has a lifecycle hook %+v", l)
	}
	env := envOf(d)
	for _, k := range []string{"KAFSCALE_SCHEMA_REGISTRY_SHUTDOWN_DRAIN", "KAFSCALE_SCHEMA_REGISTRY_SHUTDOWN_TIMEOUT"} {
		if _, ok := env[k]; ok {
			t.Fatalf("default render sets %s", k)
		}
	}

	d = render("schemaRegistry.shutdownDrain=3s", "schemaRegistry.shutdownTimeout=20s",
		"schemaRegistry.preStopSleepSeconds=4", "schemaRegistry.terminationGracePeriodSeconds=45")
	if g := d.Spec.Template.Spec.TerminationGracePeriodSeconds; g == nil || *g != 45 {
		t.Fatalf("terminationGracePeriodSeconds %v, want 45", g)
	}
	l := d.Spec.Template.Spec.Containers[0].Lifecycle
	if l == nil || l.PreStop == nil || l.PreStop.Exec == nil || !slices.Equal(l.PreStop.Exec.Command, []string{"sleep", "4"}) {
		t.Fatalf("preStop %+v, want exec sleep 4", l)
	}
	env = envOf(d)
	if env["KAFSCALE_SCHEMA_REGISTRY_SHUTDOWN_DRAIN"] != "3s" || env["KAFSCALE_SCHEMA_REGISTRY_SHUTDOWN_TIMEOUT"] != "20s" {
		t.Fatalf("shutdown env %v", env)
	}

	d = render("schemaRegistry.shutdownDrain=0", "schemaRegistry.shutdownTimeout=0", "schemaRegistry.terminationGracePeriodSeconds=0")
	env = envOf(d)
	if v, ok := env["KAFSCALE_SCHEMA_REGISTRY_SHUTDOWN_DRAIN"]; !ok || v != "0" {
		t.Fatalf("numeric 0 shutdownDrain rendered %q %v, want \"0\"", v, ok)
	}
	if v, ok := env["KAFSCALE_SCHEMA_REGISTRY_SHUTDOWN_TIMEOUT"]; !ok || v != "0" {
		t.Fatalf("numeric 0 shutdownTimeout rendered %q %v, want \"0\"", v, ok)
	}
	if g := d.Spec.Template.Spec.TerminationGracePeriodSeconds; g == nil || *g != 0 {
		t.Fatalf("terminationGracePeriodSeconds 0 rendered %v", g)
	}
	if d := render("schemaRegistry.shutdownDrain=0s"); envOf(d)["KAFSCALE_SCHEMA_REGISTRY_SHUTDOWN_DRAIN"] != "0s" {
		t.Fatalf("shutdownDrain 0s env %v", envOf(d))
	}

	env = envOf(render("schemaRegistry.shutdownDrain=null", "schemaRegistry.shutdownTimeout=null"))
	for _, k := range []string{"KAFSCALE_SCHEMA_REGISTRY_SHUTDOWN_DRAIN", "KAFSCALE_SCHEMA_REGISTRY_SHUTDOWN_TIMEOUT"} {
		if v, ok := env[k]; ok {
			t.Fatalf("null value rendered %s=%q", k, v)
		}
	}

	d = render("schemaRegistry.terminationGracePeriodSeconds=null", "schemaRegistry.preStopSleepSeconds=null")
	if g := d.Spec.Template.Spec.TerminationGracePeriodSeconds; g != nil {
		t.Fatalf("unset terminationGracePeriodSeconds rendered %d", *g)
	}
	if l := d.Spec.Template.Spec.Containers[0].Lifecycle; l != nil {
		t.Fatalf("unset preStopSleepSeconds rendered %+v", l)
	}
}
