// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").

package chart

import (
	"os/exec"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/yaml"
)

func TestConsoleAndMCPReadinessProbes(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	for _, component := range []string{"console", "mcp"} {
		t.Run(component, func(t *testing.T) {
			output, err := exec.Command("helm", "template", "readiness", chart, "--show-only", "templates/"+component+"-deployment.yaml", "--set", component+".enabled=true", "--set", component+".etcdEndpoints[0]=http://unreachable.invalid:2379").CombinedOutput()
			if err != nil {
				t.Fatal(err, string(output))
			}
			var deployment appsv1.Deployment
			if err := yaml.Unmarshal(output, &deployment); err != nil {
				t.Fatal(err)
			}
			c := deployment.Spec.Template.Spec.Containers[0]
			if c.ReadinessProbe == nil || c.ReadinessProbe.HTTPGet == nil || c.ReadinessProbe.HTTPGet.Path != "/readyz" || c.ReadinessProbe.TimeoutSeconds != 2 {
				t.Fatalf("readiness probe: %+v", c.ReadinessProbe)
			}
			if c.LivenessProbe == nil || c.LivenessProbe.HTTPGet == nil || c.LivenessProbe.HTTPGet.Path != "/healthz" {
				t.Fatalf("liveness probe: %+v", c.LivenessProbe)
			}
		})
	}
}
