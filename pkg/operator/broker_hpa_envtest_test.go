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

//go:build e2e

package operator

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
)

func TestBrokerHPAReplicasAPI(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E_AFFINITY") != "1" {
		t.Skip("set KAFSCALE_E2E_AFFINITY=1 with Kubernetes 1.35 envtest assets")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Fatal("KUBEBUILDER_ASSETS must point to Kubernetes 1.35 envtest binaries")
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
	if err := autoscalingv2.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := autoscalingv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cluster := testCluster("hpa-api", nil)
	cluster.Spec.Brokers.Replicas = int32Ptr(2)
	cluster.Spec.Brokers.Resources = kafscalev1alpha1.BrokerResources{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("512Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
	}
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
		t.Fatal(err)
	}
	if cpu := cluster.Spec.Brokers.Resources.Requests[corev1.ResourceCPU]; cpu.String() != "50m" {
		t.Fatalf("API dropped spec.brokers.resources: %+v", cluster.Spec.Brokers.Resources)
	}
	r := &ClusterReconciler{Client: c, Scheme: scheme, Publisher: NewSnapshotPublisher(c)}
	reconcile := func() {
		t.Helper()
		if err := r.reconcileBrokerDeployment(ctx, cluster, []string{"http://etcd:2379"}); err != nil {
			t.Fatal(err)
		}
		if err := r.reconcileBrokerHPA(ctx, cluster); err != nil {
			t.Fatal(err)
		}
	}
	sts := &appsv1.StatefulSet{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: "hpa-api-broker"}
	replicas := func() int32 {
		t.Helper()
		if err := c.Get(ctx, key, sts); err != nil {
			t.Fatal(err)
		}
		return *sts.Spec.Replicas
	}
	scaleTo := func(n int32) {
		t.Helper()
		if err := c.Get(ctx, key, sts); err != nil {
			t.Fatal(err)
		}
		scale := &autoscalingv1.Scale{Spec: autoscalingv1.ScaleSpec{Replicas: n}}
		if err := c.SubResource("scale").Update(ctx, sts, client.WithSubResourceBody(scale)); err != nil {
			t.Fatal(err)
		}
	}

	reconcile()
	if got := replicas(); got != 2 {
		t.Fatalf("created replicas = %d, want 2", got)
	}
	requests := sts.Spec.Template.Spec.Containers[0].Resources.Requests
	if cpu, mem := requests[corev1.ResourceCPU], requests[corev1.ResourceMemory]; cpu.String() != "50m" || mem.String() != "512Mi" {
		t.Fatalf("broker container requests = %v, want cpu 50m and memory 512Mi for the HPA", requests)
	}
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := c.Get(ctx, key, hpa); err != nil {
		t.Fatal(err)
	}
	if *hpa.Spec.MinReplicas != 2 || hpa.Spec.MaxReplicas != 8 || hpa.Spec.Behavior == nil || hpa.Spec.Behavior.ScaleDown == nil || hpa.Spec.Behavior.ScaleUp == nil {
		t.Fatalf("stored HPA spec = %+v", hpa.Spec)
	}
	hpaVersion := hpa.ResourceVersion

	scaleTo(4)
	reconcile()
	if got := replicas(); got != 4 {
		t.Fatalf("replicas after scale subresource update to 4 and reconcile = %d, want 4", got)
	}
	if err := c.Get(ctx, key, hpa); err != nil {
		t.Fatal(err)
	}
	if hpa.ResourceVersion != hpaVersion {
		t.Fatalf("HPA rewritten on an unchanged reconcile: %s -> %s", hpaVersion, hpa.ResourceVersion)
	}
	if got, err := NewSnapshotPublisher(c).liveBrokerCount(ctx, cluster); err != nil || got != 4 {
		t.Fatalf("live broker count = %d, %v; want 4", got, err)
	}

	scaleTo(30)
	reconcile()
	if got := replicas(); got != 8 {
		t.Fatalf("replicas above the HPA max = %d, want 8", got)
	}

	// Exercise admission and persistence, rather than only changing the local
	// object: zero must be accepted by the CRD while the cluster owns its HPA.
	for _, desired := range []int32{0, 2} {
		cluster.Spec.Brokers.Replicas = int32Ptr(desired)
		if err := c.Update(ctx, cluster); err != nil {
			t.Fatalf("API rejected spec.brokers.replicas=%d: %v", desired, err)
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Brokers.Replicas == nil || *cluster.Spec.Brokers.Replicas != desired {
			t.Fatalf("API changed explicit replicas=%d: %v", desired, cluster.Spec.Brokers.Replicas)
		}
		reconcile()
		if got := replicas(); got != desired {
			t.Fatalf("replicas after API update to %d = %d", desired, got)
		}
		if got, err := r.Publisher.liveBrokerCount(ctx, cluster); err != nil || got != desired {
			t.Fatalf("live broker count after API update to %d = %d, %v", desired, got, err)
		}
	}
	for _, tc := range []struct {
		name  string
		value *int32
		want  int32
	}{
		{"hpa-start-stopped", int32Ptr(0), 0},
		{"hpa-default-count", nil, 3},
	} {
		created := testCluster(tc.name, nil)
		created.Spec.Brokers.Replicas = tc.value
		if err := c.Create(ctx, created); err != nil {
			t.Fatalf("create cluster %s: %v", tc.name, err)
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(created), created); err != nil {
			t.Fatal(err)
		}
		if created.Spec.Brokers.Replicas == nil || *created.Spec.Brokers.Replicas != tc.want {
			t.Fatalf("created cluster %s replicas = %v, want %d", tc.name, created.Spec.Brokers.Replicas, tc.want)
		}
	}
	invalid := testCluster("hpa-negative-count", nil)
	invalid.Spec.Brokers.Replicas = int32Ptr(-1)
	if err := c.Create(ctx, invalid); !apierrors.IsInvalid(err) {
		t.Fatalf("negative replicas must fail CRD admission: %v", err)
	}

	if err := c.Delete(ctx, hpa); err != nil {
		t.Fatal(err)
	}
	scaleTo(5)
	if err := r.reconcileBrokerDeployment(ctx, cluster, []string{"http://etcd:2379"}); err != nil {
		t.Fatal(err)
	}
	if got := replicas(); got != 2 {
		t.Fatalf("replicas without the HPA = %d, want spec value 2", got)
	}
}
