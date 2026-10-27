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

func TestProxyMTLSChart(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("Helm is required")
	}
	chart := filepath.Join("..", "..", "deploy", "helm", "kafscale")
	render := func(extra ...string) ([]byte, error) {
		args := []string{"template", "secure", chart, "--show-only", "templates/proxy-deployment.yaml", "--set", "proxy.enabled=true"}
		return exec.Command("helm", append(args, extra...)...).CombinedOutput()
	}
	data, err := render("--set", "proxy.security.tlsSecretRef=client-tls", "--set", "proxy.security.identitySecretRef=identity", "--set", "proxy.security.aclSecretRef=acl")
	if err != nil {
		t.Fatal(err, string(data))
	}
	var deployment appsv1.Deployment
	if err := yaml.Unmarshal(data, &deployment); err != nil {
		t.Fatal(err)
	}
	pod := deployment.Spec.Template.Spec
	container := pod.Containers[0]
	env := map[string]string{}
	for _, v := range container.Env {
		env[v.Name] = v.Value
	}
	for key, suffix := range map[string]string{"KAFSCALE_PROXY_TLS_CLIENT_CA_FILE": "/ca.crt", "KAFSCALE_PROXY_TLS_PRINCIPALS_FILE": "/principals.json", "KAFSCALE_PROXY_IDENTITY_KEY_FILE": "/identity/key", "KAFSCALE_ACL_FILE": "/acl/acl.json"} {
		if !strings.HasSuffix(env[key], suffix) {
			t.Fatal(key, env[key])
		}
	}
	expected := map[string]string{"kafka-tls": "client-tls", "kafka-identity": "identity", "kafka-acl": "acl"}
	for name, secret := range expected {
		found := false
		for _, v := range pod.Volumes {
			if v.Name == name {
				found = v.Secret != nil && v.Secret.SecretName == secret
			}
		}
		if !found {
			t.Fatal("missing secret", name)
		}
		found = false
		for _, m := range container.VolumeMounts {
			if m.Name == name {
				found = m.ReadOnly && m.SubPath == ""
			}
		}
		if !found {
			t.Fatal("Secret cannot rotate or is writable", name)
		}
	}
	if data, err := render("--set", "proxy.security.tlsSecretRef=client-tls"); err == nil {
		t.Fatal("incomplete mTLS configuration accepted", string(data))
	}
	data, err = render()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "KAFSCALE_PROXY_TLS_") || strings.Contains(string(data), "KAFSCALE_PROXY_IDENTITY_KEY_FILE") {
		t.Fatal("legacy chart enabled authentication")
	}
}
