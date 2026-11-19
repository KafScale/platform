// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").

package chart

import (
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
		{"default port", nil, "http://metrics-kafscale-operator-metrics-headless.kafscale.svc.cluster.local:8080/metrics"},
		{"port from values file", []string{"--values", override}, "http://metrics-kafscale-operator-metrics-headless.kafscale.svc.cluster.local:9100/metrics"},
		{"port from set", []string{"--set", "operator.metrics.port=9200"}, "http://metrics-kafscale-operator-metrics-headless.kafscale.svc.cluster.local:9200/metrics"},
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
				if env.Name == "KAFSCALE_CONSOLE_OPERATOR_METRICS_DISCOVERY_URL" {
					got = env.Value
				}
			}
			if got != tc.want {
				t.Fatalf("KAFSCALE_CONSOLE_OPERATOR_METRICS_DISCOVERY_URL = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestConsoleOperatorDiscoveryServiceContract(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	for _, tc := range []struct {
		name              string
		args              []string
		direct, discovery string
		disabled          bool
	}{
		{name: "default"},
		{name: "name and port overrides", args: []string{"--set", "fullnameOverride=custom", "--set", "nameOverride=custom-name", "--set", "operator.metrics.port=9191"}},
		{name: "long name", args: []string{"--set", "fullnameOverride=" + strings.Repeat("x", 63)}},
		{name: "direct override", args: []string{"--set", "console.metrics.operatorMetricsURL=http://external:8888/metrics"}, direct: "http://external:8888/metrics"},
		{name: "discovery override", args: []string{"--set", "console.metrics.operatorMetricsDiscoveryURL=http://custom:8888/metrics"}, discovery: "http://custom:8888/metrics"},
		{name: "direct takes precedence", args: []string{"--set", "console.metrics.operatorMetricsURL=http://direct/metrics", "--set", "console.metrics.operatorMetricsDiscoveryURL=http://custom/metrics"}, direct: "http://direct/metrics"},
		{name: "disabled", args: []string{"--set", "operator.metrics.enabled=false"}, disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"template", "metrics", chart, "--namespace", "test-namespace", "--set", "console.enabled=true", "--set", "operator.metrics.serviceMonitor.enabled=true"}, tc.args...)
			rendered, err := exec.Command("helm", args...).CombinedOutput()
			if err != nil {
				t.Fatalf("helm template: %v: %s", err, rendered)
			}
			var console appsv1.Deployment
			var services []corev1.Service
			var monitor struct {
				Spec struct {
					Selector struct{ MatchLabels map[string]string }
				}
			}
			for _, doc := range splitYAML(rendered) {
				var meta struct{ Kind string }
				if err := yaml.Unmarshal(doc, &meta); err != nil {
					t.Fatal(err)
				}
				switch meta.Kind {
				case "Deployment":
					var d appsv1.Deployment
					if err := yaml.Unmarshal(doc, &d); err != nil {
						t.Fatal(err)
					}
					if d.Labels["app.kubernetes.io/component"] == "console" {
						console = d
					}
				case "Service":
					var service corev1.Service
					if err := yaml.Unmarshal(doc, &service); err != nil {
						t.Fatal(err)
					}
					services = append(services, service)
				case "ServiceMonitor":
					if err := yaml.Unmarshal(doc, &monitor); err != nil {
						t.Fatal(err)
					}
				}
			}
			if len(console.Spec.Template.Spec.Containers) == 0 {
				t.Fatal("missing console")
			}
			env := map[string]string{}
			for _, v := range console.Spec.Template.Spec.Containers[0].Env {
				env[v.Name] = v.Value
			}
			direct, discovery := env["KAFSCALE_CONSOLE_OPERATOR_METRICS_URL"], env["KAFSCALE_CONSOLE_OPERATOR_METRICS_DISCOVERY_URL"]
			if tc.direct != "" || tc.discovery != "" || tc.disabled {
				if direct != tc.direct || discovery != tc.discovery {
					t.Fatalf("direct=%s discovery=%s", direct, discovery)
				}
			} else {
				if direct != "" || discovery == "" {
					t.Fatalf("direct=%s discovery=%s", direct, discovery)
				}
				endpoint, err := url.Parse(discovery)
				if err != nil {
					t.Fatal(err)
				}
				var found bool
				for _, service := range services {
					if endpoint.Hostname() != service.Name+".test-namespace.svc.cluster.local" {
						continue
					}
					found = true
					if service.Spec.ClusterIP != "None" || service.Spec.PublishNotReadyAddresses || len(service.Name) > 63 {
						t.Fatalf("discovery service=%+v", service)
					}
					if service.Spec.Selector["app.kubernetes.io/component"] != "operator" {
						t.Fatalf("selector=%v", service.Spec.Selector)
					}
					if endpoint.Port() != fmt.Sprint(service.Spec.Ports[0].Port) || service.Spec.Ports[0].TargetPort.StrVal != "metrics" {
						t.Fatal("port mismatch")
					}
				}
				if !found {
					t.Fatal("discovery URL has no matching headless Service")
				}
			}
			if !tc.disabled {
				selector := labels.SelectorFromSet(monitor.Spec.Selector.MatchLabels)
				matched := 0
				for _, service := range services {
					if selector.Matches(labels.Set(service.Labels)) {
						matched++
						if service.Spec.ClusterIP == "None" {
							t.Fatal("ServiceMonitor must not duplicate scrapes through discovery Service")
						}
					}
				}
				if matched != 1 {
					t.Fatalf("ServiceMonitor selects %d Services, want existing metrics Service only", matched)
				}
			}
		})
	}
}
