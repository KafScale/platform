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

func TestProxyTimeoutValues(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	render := func(extra ...string) map[string]string {
		args := []string{"template", "timeouts", chart, "--show-only", "templates/proxy-deployment.yaml", "--set", "proxy.enabled=true"}
		data, err := exec.Command("helm", append(args, extra...)...).CombinedOutput()
		if err != nil {
			t.Fatal(err, string(data))
		}
		var deployment appsv1.Deployment
		if err := yaml.Unmarshal(data, &deployment); err != nil {
			t.Fatal(err)
		}
		env := map[string]string{}
		for _, v := range deployment.Spec.Template.Spec.Containers[0].Env {
			env[v.Name] = v.Value
		}
		return env
	}
	env := render()
	for _, key := range []string{"KAFSCALE_RESPONSE_WRITE_TIMEOUT_MS", "KAFSCALE_LFS_PROXY_METADATA_TIMEOUT_MS"} {
		if _, ok := env[key]; ok {
			t.Fatalf("%s rendered by default", key)
		}
	}
	env = render("--set", "proxy.responseWriteTimeoutMs=60000", "--set", "proxy.lfsMetadataTimeoutMs=5000")
	if env["KAFSCALE_RESPONSE_WRITE_TIMEOUT_MS"] != "60000" || env["KAFSCALE_LFS_PROXY_METADATA_TIMEOUT_MS"] != "5000" {
		t.Fatal(env)
	}
	writeValues := func(body string) string {
		path := filepath.Join(t.TempDir(), "values.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	tls := "  security:\n    tlsSecretRef: proxy-tls\n    identitySecretRef: proxy-identity\n"
	env = render("-f", writeValues("proxy:\n  responseWriteTimeoutMs: 1200000\n  lfsMetadataTimeoutMs: 1200000\n"+tls+"    handshakeTimeoutMs: 60000\n"))
	want := map[string]string{"KAFSCALE_RESPONSE_WRITE_TIMEOUT_MS": "1200000", "KAFSCALE_LFS_PROXY_METADATA_TIMEOUT_MS": "1200000", "KAFSCALE_PROXY_TLS_HANDSHAKE_TIMEOUT_MS": "60000"}
	for key, value := range want {
		if env[key] != value {
			t.Fatalf("%s rendered %q, want %q", key, env[key], value)
		}
	}
	if env = render("-f", writeValues("proxy:\n"+tls)); env["KAFSCALE_PROXY_TLS_HANDSHAKE_TIMEOUT_MS"] != "1000" {
		t.Fatalf("default handshake timeout rendered %q", env["KAFSCALE_PROXY_TLS_HANDSHAKE_TIMEOUT_MS"])
	}
	for _, empty := range []string{`""`, "null"} {
		env = render("-f", writeValues("proxy:\n"+tls+"    handshakeTimeoutMs: "+empty+"\n"))
		if value, ok := env["KAFSCALE_PROXY_TLS_HANDSHAKE_TIMEOUT_MS"]; ok && value != "" {
			t.Fatalf("handshakeTimeoutMs %s rendered %q", empty, value)
		}
	}
}
