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

func TestConsoleS3Namespace(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	for _, tc := range []struct {
		name  string
		args  []string
		want  string
		found bool
	}{
		{"unset", nil, "", false},
		{"from values", []string{"--set", "console.s3Namespace=team-a"}, "team-a", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"template", "ns", chart, "--namespace", "kafscale", "--show-only", "templates/console-deployment.yaml", "--set", "console.enabled=true"}, tc.args...)
			output, err := exec.Command("helm", args...).CombinedOutput()
			if err != nil {
				t.Fatal(err, string(output))
			}
			var deployment appsv1.Deployment
			if err := yaml.Unmarshal(output, &deployment); err != nil {
				t.Fatal(err)
			}
			got, found := "", false
			for _, env := range deployment.Spec.Template.Spec.Containers[0].Env {
				if env.Name == "KAFSCALE_S3_NAMESPACE" {
					got, found = env.Value, true
				}
			}
			if got != tc.want || found != tc.found {
				t.Fatalf("KAFSCALE_S3_NAMESPACE = %q (set %v), want %q (set %v)", got, found, tc.want, tc.found)
			}
		})
	}
}
