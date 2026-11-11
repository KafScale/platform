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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
)

func snapshotDeniedReconciler(t *testing.T) (*ClusterReconciler, client.Client, *kafscalev1alpha1.KafscaleCluster, *corev1.Secret) {
	t.Helper()
	t.Setenv(operatorEtcdEndpointsEnv, "")
	t.Setenv(operatorEtcdSilenceLogsEnv, "true")
	t.Setenv(operatorEtcdSnapshotSkipPreflightEnv, "")
	t.Setenv(operatorEtcdSnapshotCreateBucketEnv, "")
	denied := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`)
	}))
	t.Cleanup(denied.Close)
	t.Setenv(operatorEtcdSnapshotEndpointEnv, denied.URL)

	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "demo-uid"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			S3:       kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1", CredentialsSecretRef: "s3-creds"},
			LfsProxy: kafscalev1alpha1.LfsProxySpec{Enabled: true},
		},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3-creds", Namespace: "default"}, Data: map[string][]byte{
		s3AccessKeyEnv: []byte("old-ak"),
		s3SecretKeyEnv: []byte("old-sk"),
	}}
	scheme := testScheme(t)
	if err := autoscalingv2.AddToScheme(scheme); err != nil {
		t.Fatalf("add autoscaling scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster, secret).Build()
	return &ClusterReconciler{Client: c, Scheme: scheme, Publisher: NewSnapshotPublisher(c)}, c, cluster, secret
}

func workloadS3Checksums(t *testing.T, c client.Client) (string, string) {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, "default", "demo-broker")
	deploy := &appsv1.Deployment{}
	assertFound(t, c, deploy, "default", "demo-lfs-proxy")
	return sts.Spec.Template.Annotations[s3CredentialsChecksumAnnotation], deploy.Spec.Template.Annotations[s3CredentialsChecksumAnnotation]
}

func TestSnapshotAccessFailureStillRollsBrokerAndProxy(t *testing.T) {
	r, c, cluster, secret := snapshotDeniedReconciler(t)
	ctx := context.Background()
	if err := r.reconcileClusterWorkloads(ctx, cluster, nil); err != nil {
		t.Fatalf("seed workloads: %v", err)
	}
	oldBroker, oldProxy := workloadS3Checksums(t, c)
	if oldBroker == "" || oldBroker != oldProxy {
		t.Fatalf("seeded checksums broker %q proxy %q", oldBroker, oldProxy)
	}

	secret.Data[s3AccessKeyEnv] = []byte("new-ak")
	secret.Data[s3SecretKeyEnv] = []byte("new-sk")
	if err := c.Update(ctx, secret); err != nil {
		t.Fatalf("rotate secret: %v", err)
	}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err == nil {
		t.Fatal("Reconcile with a denied snapshot bucket returned no error")
	}

	newBroker, newProxy := workloadS3Checksums(t, c)
	if newBroker == oldBroker || newProxy == oldProxy {
		t.Fatalf("snapshot access failure blocked the credential rollout: broker %q -> %q, proxy %q -> %q", oldBroker, newBroker, oldProxy, newProxy)
	}
	if newBroker != newProxy {
		t.Fatalf("broker checksum %q and proxy checksum %q differ", newBroker, newProxy)
	}

	var stored kafscalev1alpha1.KafscaleCluster
	if err := c.Get(ctx, req.NamespacedName, &stored); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if ready := meta.FindStatusCondition(stored.Status.Conditions, "Ready"); ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "SnapshotAccessFailed" {
		t.Fatalf("Ready = %+v, want False SnapshotAccessFailed", ready)
	}
	if access := meta.FindStatusCondition(stored.Status.Conditions, "EtcdSnapshotAccess"); access == nil || access.Status != metav1.ConditionFalse {
		t.Fatalf("EtcdSnapshotAccess = %+v, want False", access)
	}
	if creds := meta.FindStatusCondition(stored.Status.Conditions, s3CredentialsConditionType); creds == nil || creds.Status != metav1.ConditionTrue {
		t.Fatalf("S3Credentials = %+v, want True", creds)
	}
}

func TestSnapshotAccessFailureDoesNotCreateBrokers(t *testing.T) {
	r, c, _, _ := snapshotDeniedReconciler(t)
	ctx := context.Background()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err == nil {
		t.Fatal("Reconcile with a denied snapshot bucket returned no error")
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "demo-broker", Namespace: "default"}, &appsv1.StatefulSet{}); !apierrors.IsNotFound(err) {
		t.Fatalf("broker StatefulSet get err = %v, want NotFound", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "demo-lfs-proxy", Namespace: "default"}, &appsv1.Deployment{}); !apierrors.IsNotFound(err) {
		t.Fatalf("LFS proxy Deployment get err = %v, want NotFound", err)
	}
	var stored kafscalev1alpha1.KafscaleCluster
	if err := c.Get(ctx, req.NamespacedName, &stored); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if ready := meta.FindStatusCondition(stored.Status.Conditions, "Ready"); ready == nil || ready.Reason != "SnapshotAccessFailed" {
		t.Fatalf("Ready = %+v, want SnapshotAccessFailed", ready)
	}
}
