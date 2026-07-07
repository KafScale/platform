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

package operator

import (
	"context"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// assertSpreadsOwnPods asserts that the StatefulSet carries a single soft
// (preferred) podAntiAffinity term whose label selector EQUALS the pod-template
// labels and whose topology key is the node hostname. That equality is the
// property that makes the rule work: the selector matches the very pods the
// term is meant to spread. A mismatch (the proxy selector-drift class of bug)
// would leave the rule matching nothing and HA silently disabled.
func assertSpreadsOwnPods(t *testing.T, sts *appsv1.StatefulSet) {
	t.Helper()

	aff := sts.Spec.Template.Spec.Affinity
	if aff == nil || aff.PodAntiAffinity == nil {
		t.Fatalf("expected podAntiAffinity on %s, got %+v", sts.Name, aff)
	}
	terms := aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 1 {
		t.Fatalf("expected exactly one preferred term on %s, got %d", sts.Name, len(terms))
	}
	if aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		t.Fatalf("expected soft-only anti-affinity on %s, found a required term", sts.Name)
	}

	term := terms[0].PodAffinityTerm
	if term.LabelSelector == nil {
		t.Fatalf("expected a label selector on %s", sts.Name)
	}
	if term.TopologyKey != "kubernetes.io/hostname" {
		t.Fatalf("expected topologyKey kubernetes.io/hostname on %s, got %q", sts.Name, term.TopologyKey)
	}

	podLabels := sts.Spec.Template.Labels
	if len(podLabels) == 0 {
		t.Fatalf("expected pod-template labels on %s", sts.Name)
	}
	if !reflect.DeepEqual(term.LabelSelector.MatchLabels, podLabels) {
		t.Fatalf("anti-affinity selector on %s does not match its own pod labels:\n selector=%v\n podLabels=%v",
			sts.Name, term.LabelSelector.MatchLabels, podLabels)
	}
}

func TestBrokerStatefulSetAntiAffinitySpreadsOwnPods(t *testing.T) {
	cluster := testCluster("affinity-broker", nil)
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if err := r.reconcileBrokerDeployment(context.Background(), cluster, []string{"http://etcd:2379"}); err != nil {
		t.Fatalf("reconcileBrokerDeployment: %v", err)
	}

	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, cluster.Namespace, cluster.Name+"-broker")
	assertSpreadsOwnPods(t, sts)
}

func TestEtcdStatefulSetAntiAffinitySpreadsOwnPods(t *testing.T) {
	t.Setenv(operatorEtcdEndpointsEnv, "")
	cluster := testCluster("affinity-etcd", nil)
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	res, err := EnsureEtcd(context.Background(), c, scheme, cluster)
	if err != nil {
		t.Fatalf("EnsureEtcd: %v", err)
	}
	if !res.Managed {
		t.Fatalf("expected managed etcd so a StatefulSet is created")
	}

	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, cluster.Namespace, cluster.Name+"-etcd")
	assertSpreadsOwnPods(t, sts)
}
