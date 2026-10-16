// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
)

// Real API storage catches CRD pruning/defaulting that fake clients cannot.
// Envtest has no workload controllers, so these StatefulSets never start pods.
func TestAffinityOverridesAPI(t *testing.T) {
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
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cluster := testCluster("affinity-api", nil)
	cluster.Spec.Brokers.Replicas = int32Ptr(1)
	custom := testAffinityOverride()
	other := custom.DeepCopy()
	other.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values[0] = "etcd-pool"
	cluster.Spec.Brokers.Affinity = custom
	cluster.Spec.Etcd.Affinity = &corev1.Affinity{}
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	var lastBroker, lastEtcd *appsv1.StatefulSet
	for _, tc := range []struct {
		name         string
		broker, etcd *corev1.Affinity
	}{
		{"create", custom, &corev1.Affinity{}},
		{"swap-empty-and-custom", &corev1.Affinity{}, other},
		{"remove-overrides", nil, nil},
		{"independent-overrides", custom, other},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stored kafscalev1alpha1.KafscaleCluster
			key := client.ObjectKeyFromObject(cluster)
			if err := c.Get(ctx, key, &stored); err != nil {
				t.Fatal(err)
			}
			if tc.name != "create" {
				before := stored.DeepCopy()
				stored.Spec.Brokers.Affinity, stored.Spec.Etcd.Affinity = tc.broker, tc.etcd
				if err := c.Patch(ctx, &stored, client.MergeFrom(before)); err != nil {
					t.Fatal(err)
				}
				if err := c.Get(ctx, key, &stored); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(stored.Spec.Brokers.Affinity, tc.broker) || !reflect.DeepEqual(stored.Spec.Etcd.Affinity, tc.etcd) {
				t.Fatalf("API pruned or defaulted affinity: brokers=%+v etcd=%+v", stored.Spec.Brokers.Affinity, stored.Spec.Etcd.Affinity)
			}
			if err := r.reconcileBrokerDeployment(ctx, &stored, []string{"http://etcd:2379"}); err != nil {
				t.Fatal(err)
			}
			if err := reconcileEtcdStatefulSet(ctx, c, scheme, &stored); err != nil {
				t.Fatal(err)
			}
			for _, workload := range []struct {
				name     string
				want     *corev1.Affinity
				previous **appsv1.StatefulSet
			}{
				{"broker", tc.broker, &lastBroker}, {"etcd", tc.etcd, &lastEtcd},
			} {
				var sts appsv1.StatefulSet
				assertFound(t, c, &sts, cluster.Namespace, cluster.Name+"-"+workload.name)
				if workload.want == nil {
					assertSpreadsOwnPods(t, &sts)
				} else if !reflect.DeepEqual(sts.Spec.Template.Spec.Affinity, workload.want) {
					t.Fatalf("%s template lost affinity: got %+v want %+v", workload.name, sts.Spec.Template.Spec.Affinity, workload.want)
				}
				if prev := *workload.previous; prev != nil && (prev.UID != sts.UID || sts.Generation <= prev.Generation) {
					t.Fatalf("%s affinity update did not change the existing pod template: old=%s/%d new=%s/%d", workload.name, prev.UID, prev.Generation, sts.UID, sts.Generation)
				}
				*workload.previous = sts.DeepCopy()
			}
		})
	}
	// The CRD must validate the new fields, rather than preserving arbitrary JSON.
	invalid := &unstructured.Unstructured{}
	invalid.SetAPIVersion("kafscale.io/v1alpha1")
	invalid.SetKind("KafscaleCluster")
	if err := c.Get(ctx, client.ObjectKeyFromObject(cluster), invalid); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(invalid.Object, "not-an-object", "spec", "brokers", "affinity"); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(ctx, invalid); !apierrors.IsInvalid(err) {
		t.Fatalf("expected schema rejection, got %v", err)
	}
	t.Run("explicit-null", func(t *testing.T) {
		raw := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "kafscale.io/v1alpha1", "kind": "KafscaleCluster",
			"metadata": map[string]interface{}{"name": "affinity-null", "namespace": "default"},
			"spec": map[string]interface{}{
				"brokers": map[string]interface{}{"affinity": nil},
				"etcd":    map[string]interface{}{"affinity": nil},
				"s3":      map[string]interface{}{"bucket": "bucket", "region": "us-east-1"},
			},
		}}
		if err := c.Create(ctx, raw); err != nil {
			t.Fatal(err)
		}
		var stored kafscalev1alpha1.KafscaleCluster
		if err := c.Get(ctx, client.ObjectKeyFromObject(raw), &stored); err != nil {
			t.Fatal(err)
		}
		if stored.Spec.Brokers.Affinity != nil || stored.Spec.Etcd.Affinity != nil {
			t.Fatal("explicit null must select default placement")
		}
	})

}
