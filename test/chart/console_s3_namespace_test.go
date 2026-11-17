// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").

package chart

import (
	"os/exec"
	"path/filepath"
	"strings"
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

func TestConsoleS3NamespaceWarning(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	const warning = "console.s3Namespace is empty"
	for _, tc := range []struct {
		name string
		args []string
		warn bool
	}{
		{"default install", nil, false},
		{"etcd without namespace", []string{"--set", "console.etcdEndpoints[0]=http://etcd:2379"}, true},
		{"etcd with namespace", []string{"--set", "console.etcdEndpoints[0]=http://etcd:2379", "--set", "console.s3Namespace=team-a"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"install", "ns", chart, "--dry-run=client", "--namespace", "kafscale"}, tc.args...)
			output, err := exec.Command("helm", args...).CombinedOutput()
			if err != nil {
				t.Fatal(err, string(output))
			}
			_, notes, found := strings.Cut(string(output), "NOTES:")
			if !found {
				t.Fatalf("no NOTES section in output:\n%s", output)
			}
			if got := strings.Contains(notes, warning); got != tc.warn {
				t.Fatalf("NOTES warning present = %v, want %v:\n%s", got, tc.warn, notes)
			}
		})
	}
}
