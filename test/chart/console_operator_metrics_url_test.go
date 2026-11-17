// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").

package chart

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/yaml"
)

func TestConsoleOperatorMetricsURL(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	override := filepath.Join(t.TempDir(), "values.yaml")
	if err := os.WriteFile(override, []byte("operator:\n  metrics:\n    port: 9100\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"default port", nil, "http://metrics-kafscale-operator-metrics.kafscale.svc.cluster.local:8080/metrics"},
		{"port from values file", []string{"--values", override}, "http://metrics-kafscale-operator-metrics.kafscale.svc.cluster.local:9100/metrics"},
		{"port from set", []string{"--set", "operator.metrics.port=9200"}, "http://metrics-kafscale-operator-metrics.kafscale.svc.cluster.local:9200/metrics"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"template", "metrics", chart, "--namespace", "kafscale", "--show-only", "templates/console-deployment.yaml", "--set", "console.enabled=true"}, tc.args...)
			output, err := exec.Command("helm", args...).CombinedOutput()
			if err != nil {
				t.Fatal(err, string(output))
			}
			var deployment appsv1.Deployment
			if err := yaml.Unmarshal(output, &deployment); err != nil {
				t.Fatal(err)
			}
			got := ""
			for _, env := range deployment.Spec.Template.Spec.Containers[0].Env {
				if env.Name == "KAFSCALE_CONSOLE_OPERATOR_METRICS_URL" {
					got = env.Value
				}
			}
			if got != tc.want {
				t.Fatalf("KAFSCALE_CONSOLE_OPERATOR_METRICS_URL = %q, want %q", got, tc.want)
			}
		})
	}
}
