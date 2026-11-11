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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
	"github.com/KafScale/platform/internal/testutil"
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

func useOperatorEtcdEndpoints(t *testing.T, endpoints []string) {
	t.Helper()
	previous := operatorManagedEtcdEndpoints
	operatorManagedEtcdEndpoints = func(*kafscalev1alpha1.KafscaleCluster) []string { return endpoints }
	t.Cleanup(func() { operatorManagedEtcdEndpoints = previous })
}

func scaleBrokers(t *testing.T, c client.Client, replicas int32) *kafscalev1alpha1.KafscaleCluster {
	t.Helper()
	cluster := &kafscalev1alpha1.KafscaleCluster{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "demo", Namespace: "default"}, cluster); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	cluster.Spec.Brokers.Replicas = &replicas
	if err := c.Update(context.Background(), cluster); err != nil {
		t.Fatalf("scale cluster: %v", err)
	}
	return cluster
}

func TestSnapshotAccessFailurePublishesScaledDownBrokers(t *testing.T) {
	r, c, _, _ := snapshotDeniedReconciler(t)
	endpoints := testutil.StartEmbeddedEtcd(t)
	useOperatorEtcdEndpoints(t, endpoints)
	ctx := context.Background()
	topic := &kafscalev1alpha1.KafscaleTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
		Spec:       kafscalev1alpha1.KafscaleTopicSpec{ClusterRef: "demo", Partitions: 6},
	}
	if err := c.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	cluster := scaleBrokers(t, c, 3)
	if err := r.reconcileClusterWorkloads(ctx, cluster, endpoints); err != nil {
		t.Fatalf("seed workloads: %v", err)
	}
	if err := r.Publisher.Publish(ctx, cluster, endpoints); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	scaleBrokers(t, c, 1)
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}}
	if _, err := r.Reconcile(ctx, req); err == nil {
		t.Fatal("Reconcile with a denied snapshot bucket returned no error")
	}

	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, "default", "demo-broker")
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 1 {
		t.Fatalf("broker StatefulSet replicas = %v, want 1", sts.Spec.Replicas)
	}
	snap, err := readSnapshotFromEtcd(ctx, endpoints)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if len(snap.Brokers) != 1 || snap.Brokers[0].NodeID != 0 {
		t.Fatalf("published brokers = %+v, want only broker 0", snap.Brokers)
	}
	if len(snap.Topics) != 1 {
		t.Fatalf("published topics = %d, want 1", len(snap.Topics))
	}
	for _, partition := range snap.Topics[0].Partitions {
		if partition.Leader != 0 {
			t.Fatalf("partition %d is led by removed broker %d", partition.Partition, partition.Leader)
		}
	}

	var stored kafscalev1alpha1.KafscaleCluster
	if err := c.Get(ctx, req.NamespacedName, &stored); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if ready := meta.FindStatusCondition(stored.Status.Conditions, "Ready"); ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "SnapshotAccessFailed" {
		t.Fatalf("Ready = %+v, want False SnapshotAccessFailed", ready)
	}
	if etcd := meta.FindStatusCondition(stored.Status.Conditions, "EtcdAvailable"); etcd == nil || etcd.Status != metav1.ConditionTrue || etcd.Reason != "EtcdReady" {
		t.Fatalf("EtcdAvailable = %+v, want True EtcdReady", etcd)
	}
}

func TestSnapshotAccessFailureReportsPublishFailure(t *testing.T) {
	r, c, cluster, _ := snapshotDeniedReconciler(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	closed := "http://" + listener.Addr().String()
	_ = listener.Close()
	useOperatorEtcdEndpoints(t, []string{closed})
	ctx := context.Background()
	if err := r.reconcileClusterWorkloads(ctx, cluster, nil); err != nil {
		t.Fatalf("seed workloads: %v", err)
	}

	reconcileCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}}
	_, err = r.Reconcile(reconcileCtx, req)
	if err == nil {
		t.Fatal("Reconcile returned no error")
	}
	if !strings.Contains(err.Error(), "403") || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Reconcile error does not join the snapshot and publish errors: %v", err)
	}

	var stored kafscalev1alpha1.KafscaleCluster
	if err := c.Get(ctx, req.NamespacedName, &stored); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if ready := meta.FindStatusCondition(stored.Status.Conditions, "Ready"); ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "SnapshotAccessFailed" {
		t.Fatalf("Ready = %+v, want False SnapshotAccessFailed", ready)
	}
	if etcd := meta.FindStatusCondition(stored.Status.Conditions, "EtcdAvailable"); etcd == nil || etcd.Status != metav1.ConditionFalse || etcd.Reason != "EtcdUnavailable" {
		t.Fatalf("EtcdAvailable = %+v, want False EtcdUnavailable", etcd)
	}
}
