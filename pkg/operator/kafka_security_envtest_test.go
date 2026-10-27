//go:build e2e

// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").

package operator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
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
	cluster.Spec.Security = kafscalev1alpha1.KafkaSecuritySpec{IdentitySecretRef: "identity", ACLSecretRef: "acl", TLSSecretRef: "tls", MaxHandshakes: 3, MaxHandshakesPerIP: 2, HandshakeTimeoutMS: 500, HandshakeQueueMS: 20, ProxyPodSelector: map[string]string{"app.kubernetes.io/component": "proxy"}}
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
	aclSecret := &corev1.Secret{}
	aclSecret.Name = "acl"
	aclSecret.Namespace = "default"
	aclSecret.Data = map[string][]byte{"acl.json": []byte(`{"default_policy":"deny"}`)}
	if err := c.Create(ctx, aclSecret); err != nil {
		t.Fatal(err)
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
	secret.Annotations = map[string]string{"kubectl.kubernetes.io/last-applied-configuration": `{"data":{"key":"sensitive"}}`}
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
	// Exercise a real Secret watch and the production mapper. The test controller
	// reconciles only workloads, avoiding unrelated S3/etcd provisioning in envtest.
	cacheOptions := SecretCacheOptions()
	cacheOptions.ReaderFailOnMissingInformer = true
	manager, err := ctrl.NewManager(cfg, ctrl.Options{Client: SecretClientOptions(), Cache: cacheOptions, Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0"})
	if err != nil {
		t.Fatal(err)
	}
	if err := ctrl.NewControllerManagedBy(manager).Named("acl-rollout-api-test").For(&kafscalev1alpha1.KafscaleCluster{}).WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.clustersForACLSecret)).Complete(reconcile.Func(func(ctx context.Context, req reconcile.Request) (ctrl.Result, error) {
		var cluster kafscalev1alpha1.KafscaleCluster
		if err := c.Get(ctx, req.NamespacedName, &cluster); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		if err := r.reconcileBrokerDeployment(ctx, &cluster, nil); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.reconcileLfsProxyDeployment(ctx, &cluster, nil)
	})); err != nil {
		t.Fatal(err)
	}
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	managerDone := make(chan error, 1)
	go func() { managerDone <- manager.Start(watchCtx) }()
	t.Cleanup(func() {
		cancelWatch()
		if err := <-managerDone; err != nil {
			t.Error(err)
		}
	})
	if !manager.GetCache().WaitForCacheSync(watchCtx) {
		t.Fatal("cache did not synchronize")
	}
	// Metadata events must not populate a second, full Secret cache, including
	// after a typed read through the manager's client.
	var directSecret corev1.Secret
	if err := manager.GetClient().Get(ctx, client.ObjectKeyFromObject(secret), &directSecret); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(directSecret.Data, secret.Data) {
		t.Fatal("direct Secret read lost data")
	}
	var notCached *cache.ErrResourceNotCached
	if err := manager.GetCache().Get(ctx, client.ObjectKeyFromObject(secret), &corev1.Secret{}); !errors.As(err, &notCached) {
		t.Fatalf("full Secret unexpectedly cached: %v", err)
	}
	metadata := &metav1.PartialObjectMetadata{}
	metadata.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	if err := manager.GetCache().Get(ctx, client.ObjectKeyFromObject(secret), metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.Name != secret.Name || metadata.Namespace != secret.Namespace || metadata.ResourceVersion == "" || len(metadata.Annotations) != 0 {
		t.Fatal("Secret metadata cache retained sensitive annotations or lost routing metadata")
	}
	oldChecksum := sts.Spec.Template.Annotations[aclChecksumAnnotation]
	aclSecret.Data["acl.json"] = []byte(`{"default_policy":"deny","principals":[]}`)
	if err := c.Update(ctx, aclSecret); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; {
		if err := c.Get(ctx, client.ObjectKeyFromObject(sts), sts); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(deployment), deployment); err != nil {
			t.Fatal(err)
		}
		checksum := sts.Spec.Template.Annotations[aclChecksumAnnotation]
		if checksum != "" && checksum != oldChecksum && deployment.Spec.Template.Annotations[aclChecksumAnnotation] == checksum {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ACL Secret watch did not roll both workloads")
		}
		time.Sleep(20 * time.Millisecond)
	}

}
