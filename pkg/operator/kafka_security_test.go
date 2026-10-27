// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"context"
	"errors"
	"testing"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestKafkaSecurityReconciliation(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "false")
	t.Setenv("KAFSCALE_ACL_FAIL_OPEN", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"allow"}`)
	ctx := context.Background()
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	cluster := testCluster("secure", nil)
	cluster.Spec.Security = kafscalev1alpha1.KafkaSecuritySpec{IdentitySecretRef: "identity", ACLSecretRef: "acl", TLSSecretRef: "tls", ProxyPodSelector: map[string]string{"app.kubernetes.io/component": "proxy", "app.kubernetes.io/instance": "secure"}}
	if err := r.reconcileKafkaNetworkPolicy(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileBrokerDeployment(ctx, cluster, nil); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileLfsProxyDeployment(ctx, cluster, nil); err != nil {
		t.Fatal(err)
	}
	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, "default", "secure-broker")
	broker := sts.Spec.Template.Spec.Containers[0]
	if envValue(broker.Env, "KAFSCALE_ACL_ENABLED") != "true" || envValue(broker.Env, "KAFSCALE_ACL_FAIL_OPEN") != "false" || envValue(broker.Env, "KAFSCALE_ACL_JSON") != "" {
		t.Fatal("inherited settings override authentication", broker.Env)
	}
	if len(sts.Spec.Template.Spec.Volumes) != 2 || len(broker.VolumeMounts) != 2 {
		t.Fatal("missing projected secrets")
	}
	for _, mount := range broker.VolumeMounts {
		if !mount.ReadOnly {
			t.Fatal("writable secret")
		}
	}
	deploy := &appsv1.Deployment{}
	assertFound(t, c, deploy, "default", lfsProxyName(cluster))
	proxy := deploy.Spec.Template.Spec.Containers[0]
	if envValue(proxy.Env, "KAFSCALE_PROXY_TLS_CERT_FILE") == "" || envValue(proxy.Env, "KAFSCALE_PROXY_IDENTITY_KEY_FILE") == "" {
		t.Fatal(proxy.Env)
	}
	policy := &networkingv1.NetworkPolicy{}
	assertFound(t, c, policy, "default", "secure-broker-kafka")
	if len(policy.Spec.Ingress) != 2 || len(policy.Spec.Ingress[0].From) != 2 || policy.Spec.Ingress[0].Ports[0].Port.IntVal != 9092 || policy.Spec.Ingress[1].Ports[0].Port.IntVal != 9093 {
		t.Fatal(policy.Spec)
	}
	if policy.Spec.Ingress[0].From[1].NamespaceSelector != nil || policy.Spec.PodSelector.MatchLabels["cluster"] != "secure" {
		t.Fatal("policy crossed namespace or cluster boundary")
	}
	// Reconciliation repairs drift and does not append duplicate mounts.
	sts.Spec.Template.Spec.Containers[0].Env = nil
	if err := c.Update(ctx, sts); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileBrokerDeployment(ctx, cluster, nil); err != nil {
		t.Fatal(err)
	}
	assertFound(t, c, sts, "default", "secure-broker")
	if len(sts.Spec.Template.Spec.Volumes) != 2 {
		t.Fatal("duplicate volumes")
	}
	cluster.Spec.Security = kafscalev1alpha1.KafkaSecuritySpec{}
	if err := r.reconcileKafkaNetworkPolicy(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	assertNotFound(t, c, &networkingv1.NetworkPolicy{}, "default", "secure-broker-kafka")
	if err := r.reconcileKafkaNetworkPolicy(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileBrokerDeployment(ctx, cluster, nil); err != nil {
		t.Fatal(err)
	}
	assertFound(t, c, sts, "default", "secure-broker")
	if len(sts.Spec.Template.Spec.Volumes) != 0 {
		t.Fatal("stale security mounts")
	}
	if err := r.reconcileLfsProxyDeployment(ctx, cluster, nil); err != nil {
		t.Fatal(err)
	}
}
func TestKafkaSecurityValidation(t *testing.T) {
	for _, s := range []kafscalev1alpha1.KafkaSecuritySpec{{TLSSecretRef: "tls"}, {ACLSecretRef: "acl"}, {ProxyPodSelector: map[string]string{"app": "proxy"}}, {IdentitySecretRef: "identity"}} {
		if validateKafkaSecurity(s) == nil {
			t.Fatal("invalid configuration accepted", s)
		}
	}
	ctx := context.Background()
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	cluster := testCluster("secure", nil)
	cluster.Spec.Security.IdentitySecretRef = "key"
	if err := r.reconcileBrokerDeployment(ctx, cluster, nil); err == nil {
		t.Fatal("broker accepted missing ACL secret")
	}
	if err := r.reconcileKafkaNetworkPolicy(ctx, cluster); err == nil {
		t.Fatal("policy accepted missing ACL secret")
	}
	cluster.Spec.Security.ACLSecretRef = "acl"
	if err := r.reconcileLfsProxyDeployment(ctx, cluster, nil); err == nil {
		t.Fatal("managed proxy accepted missing TLS secret")
	}
	if err := r.reconcileKafkaNetworkPolicy(ctx, cluster); err != nil {
		t.Fatal(err)
	}
}
func TestKafkaPolicyErrors(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	owned := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "secure-broker-kafka", Namespace: "default", OwnerReferences: []metav1.OwnerReference{{UID: "", Controller: boolPtr(true)}}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(owned).WithInterceptorFuncs(interceptor.Funcs{Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
		return errors.New("delete denied")
	}}).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	cluster := testCluster("secure", nil)
	if err := r.reconcileKafkaNetworkPolicy(ctx, cluster); err == nil {
		t.Fatal("delete error ignored")
	}
	c = fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
		return errors.New("create denied")
	}}).Build()
	r.Client = c
	cluster.Spec.Security = kafscalev1alpha1.KafkaSecuritySpec{IdentitySecretRef: "key", ACLSecretRef: "acl"}
	if err := r.reconcileKafkaNetworkPolicy(ctx, cluster); err == nil {
		t.Fatal("create error ignored")
	}
	// A conflicting owner must not be overwritten.
	c = fake.NewClientBuilder().WithScheme(scheme).WithObjects(&networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "secure-broker-kafka", Namespace: "default", OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "other", UID: "other", Controller: boolPtr(true)}}}}).Build()
	r.Client = c
	if err := r.reconcileKafkaNetworkPolicy(ctx, cluster); err == nil {
		t.Fatal("owner conflict ignored")
	}
	cluster.Spec.Security = kafscalev1alpha1.KafkaSecuritySpec{}
	if err := r.reconcileKafkaNetworkPolicy(ctx, cluster); err == nil {
		t.Fatal("deleted policy owned by another resource")
	}
	r.Client = fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
		return errors.New("get denied")
	}}).Build()
	if err := r.reconcileKafkaNetworkPolicy(ctx, cluster); err == nil {
		t.Fatal("get error ignored")
	}

}
