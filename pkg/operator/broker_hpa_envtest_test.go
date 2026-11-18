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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
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
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatal(err)
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
	if got := NewSnapshotPublisher(c).liveBrokerCount(ctx, cluster); got != 4 {
		t.Fatalf("live broker count = %d, want 4", got)
	}

	scaleTo(30)
	reconcile()
	if got := replicas(); got != 8 {
		t.Fatalf("replicas above the HPA max = %d, want 8", got)
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
