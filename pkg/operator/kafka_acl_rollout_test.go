// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").
package operator

import (
	"context"
	"errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"testing"
)

func TestACLSecretRollsBrokerAndProxy(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	cluster := testCluster("secure", nil)
	cluster.Spec.Security.IdentitySecretRef = "identity"
	cluster.Spec.Security.TLSSecretRef = "tls"
	cluster.Spec.Security.ACLSecretRef = "acl"
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "acl", Namespace: "default"}, Data: map[string][]byte{"acl.json": []byte(`{"default_policy":"deny"}`)}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, secret).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	reconcile := func() string {
		t.Helper()
		if err := r.reconcileBrokerDeployment(ctx, cluster, nil); err != nil {
			t.Fatal(err)
		}
		if err := r.reconcileLfsProxyDeployment(ctx, cluster, nil); err != nil {
			t.Fatal(err)
		}
		sts := &appsv1.StatefulSet{}
		assertFound(t, c, sts, "default", "secure-broker")
		deploy := &appsv1.Deployment{}
		assertFound(t, c, deploy, "default", lfsProxyName(cluster))
		checksum := sts.Spec.Template.Annotations[aclChecksumAnnotation]
		if checksum == "" || deploy.Spec.Template.Annotations[aclChecksumAnnotation] != checksum {
			t.Fatal("inconsistent ACL rollout")
		}
		return checksum
	}
	first := reconcile()
	if first != reconcile() {
		t.Fatal("unchanged ACL triggers rollout")
	}
	secret.Data["acl.json"] = []byte(`{"default_policy":"deny","principals":[]}`)
	if err := c.Update(ctx, secret); err != nil {
		t.Fatal(err)
	}
	if first == reconcile() {
		t.Fatal("changed ACL did not change pod template")
	}
	requests := r.clustersForACLSecret(ctx, secret)
	if len(requests) != 1 || requests[0].Name != "secure" {
		t.Fatal(requests)
	}
	unrelated := secret.DeepCopy()
	unrelated.Name = "tls"
	if len(r.clustersForACLSecret(ctx, unrelated)) != 0 {
		t.Fatal("unrelated Secret triggers ACL rollout")
	}
	template := &corev1.PodTemplateSpec{}
	setACLChecksum(template, "")
	setACLChecksum(template, "a")
	setACLChecksum(template, "b")
	setACLChecksum(template, "")
	if len(template.Annotations) != 0 {
		t.Fatal(template.Annotations)
	}
}
func TestACLSecretRolloutFailsClosed(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	cluster := testCluster("secure", nil)
	if checksum, err := r.kafkaACLChecksum(ctx, cluster); err != nil || checksum != "" {
		t.Fatal(checksum, err)
	}
	cluster.Spec.Security.ACLSecretRef = "missing"
	if _, err := r.kafkaACLChecksum(ctx, cluster); err == nil {
		t.Fatal("missing Secret accepted")
	}
	if err := r.reconcileLfsProxyDeployment(ctx, cluster, nil); err == nil {
		t.Fatal("proxy ignored missing ACL")
	}
	cluster.Spec.Security.IdentitySecretRef = "identity"
	if err := r.reconcileBrokerDeployment(ctx, cluster, nil); err == nil {
		t.Fatal("broker ignored missing ACL")
	}
	if err := c.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "missing", Namespace: "default"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.kafkaACLChecksum(ctx, cluster); err == nil {
		t.Fatal("missing ACL key accepted")
	}
	r.Client = fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
		return errors.New("list denied")
	}}).Build()
	if len(r.clustersForACLSecret(ctx, &corev1.Secret{})) != 0 {
		t.Fatal("list failure ignored")
	}
}
func TestProxySecurityReplacesInheritedEnvironment(t *testing.T) {
	cluster := testCluster("secure", nil)
	cluster.Spec.Security.IdentitySecretRef = "identity"
	cluster.Spec.Security.ACLSecretRef = "acl"
	cluster.Spec.Security.TLSSecretRef = "tls"
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Env: []corev1.EnvVar{{Name: "KAFSCALE_PROXY_TLS_CERT_FILE", Value: "wrong"}, {Name: "KAFSCALE_ACL_JSON", Value: `{"default_policy":"allow"}`}, {Name: "OTHER", Value: "preserve"}}}}}
	if err := proxyKafkaSecurity(pod, cluster.Spec.Security); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, entry := range pod.Containers[0].Env {
		counts[entry.Name]++
	}
	if counts["KAFSCALE_PROXY_TLS_CERT_FILE"] != 1 || envValue(pod.Containers[0].Env, "KAFSCALE_ACL_JSON") != "" || envValue(pod.Containers[0].Env, "OTHER") != "preserve" {
		t.Fatal(pod.Containers[0].Env)
	}
}

func TestKafkaHandshakeSettings(t *testing.T) {
	cluster := testCluster("secure", nil)
	cluster.Spec.Security.IdentitySecretRef = "identity"
	cluster.Spec.Security.ACLSecretRef = "acl"
	for _, value := range []int32{-1, 60001} {
		cluster.Spec.Security.MaxHandshakes = value
		if validateKafkaSecurity(cluster.Spec.Security) == nil {
			t.Fatal("invalid limit accepted")
		}
	}
	cluster.Spec.Security.MaxHandshakes = 2
	cluster.Spec.Security.MaxHandshakesPerIP = 1
	cluster.Spec.Security.HandshakeTimeoutMS = 500
	cluster.Spec.Security.HandshakeQueueMS = 20
	if err := validateKafkaSecurity(cluster.Spec.Security); err != nil {
		t.Fatal(err)
	}
	env := kafkaHandshakeEnv(cluster.Spec.Security)
	if envValue(env, "KAFSCALE_PROXY_TLS_MAX_HANDSHAKES") != "2" || envValue(env, "KAFSCALE_PROXY_TLS_HANDSHAKE_TIMEOUT_MS") != "500" {
		t.Fatal(env)
	}
	cluster.Spec.Security.IdentitySecretRef = ""
	cluster.Spec.Security.ACLSecretRef = ""
	if validateKafkaSecurity(cluster.Spec.Security) == nil {
		t.Fatal("limits without authentication accepted")
	}
}
