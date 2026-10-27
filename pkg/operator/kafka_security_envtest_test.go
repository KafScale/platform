//go:build e2e

// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").

package operator

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
)

func TestKafkaSecurityAPI(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E_AUTH") != "1" {
		t.Skip("set KAFSCALE_E2E_AUTH=1 with Kubernetes 1.35 envtest assets")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Fatal("KUBEBUILDER_ASSETS is required")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "deploy", "helm", "kafscale", "crds")}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := testScheme(t)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cluster := testCluster("auth-api", nil)
	cluster.Spec.Security = kafscalev1alpha1.KafkaSecuritySpec{IdentitySecretRef: "identity", ACLSecretRef: "acl", TLSSecretRef: "tls", ProxyPodSelector: map[string]string{"app.kubernetes.io/component": "proxy"}}
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	var stored kafscalev1alpha1.KafscaleCluster
	key := client.ObjectKeyFromObject(cluster)
	if err := c.Get(ctx, key, &stored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored.Spec.Security, cluster.Spec.Security) {
		t.Fatal("API pruned security configuration", stored.Spec.Security)
	}
	copied := stored.DeepCopy()
	copied.Spec.Security.ProxyPodSelector["app.kubernetes.io/component"] = "changed"
	if stored.Spec.Security.ProxyPodSelector["app.kubernetes.io/component"] != "proxy" {
		t.Fatal("deep copy shared security selector")
	}
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	for _, err := range []error{r.reconcileKafkaNetworkPolicy(ctx, &stored), r.reconcileBrokerDeployment(ctx, &stored, nil), r.reconcileLfsProxyDeployment(ctx, &stored, nil)} {
		if err != nil {
			t.Fatal(err)
		}
	}
	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, "default", "auth-api-broker")
	if len(sts.Spec.Template.Spec.Volumes) != 2 {
		t.Fatal("broker Secrets not stored")
	}
	deployment := &appsv1.Deployment{}
	assertFound(t, c, deployment, "default", lfsProxyName(&stored))
	if len(deployment.Spec.Template.Spec.Volumes) != 4 {
		t.Fatal("proxy Secrets not stored")
	}
	policy := &networkingv1.NetworkPolicy{}
	assertFound(t, c, policy, "default", "auth-api-broker-kafka")
	if len(policy.Spec.Ingress[0].From) != 2 {
		t.Fatal("proxy policy not stored")
	}
	// Secret contents remain in Secrets, not environment values or the CRD.
	secret := &corev1.Secret{}
	secret.Name = "identity"
	secret.Namespace = "default"
	secret.Data = map[string][]byte{"key": make([]byte, 32)}
	if err := c.Create(ctx, secret); err != nil {
		t.Fatal(err)
	}
	secret.Data["key"][0] = 1
	if err := c.Update(ctx, secret); err != nil {
		t.Fatal(err)
	}
	if envValue(sts.Spec.Template.Spec.Containers[0].Env, "KAFSCALE_PROXY_IDENTITY_KEY_FILE") == "" {
		t.Fatal("missing identity file reference")
	}
}
