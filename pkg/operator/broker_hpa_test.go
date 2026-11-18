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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
	"github.com/KafScale/platform/internal/testutil"
)

func hpaTestReconciler(t *testing.T, replicas *int32) (*ClusterReconciler, client.Client, *kafscalev1alpha1.KafscaleCluster) {
	t.Helper()
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "demo-uid"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{Replicas: replicas},
			S3:      kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1"},
		},
	}
	scheme := testScheme(t)
	if err := autoscalingv2.AddToScheme(scheme); err != nil {
		t.Fatalf("add autoscaling scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	return &ClusterReconciler{Client: c, Scheme: scheme, Publisher: NewSnapshotPublisher(c)}, c, cluster
}

func setBrokerStatefulSetReplicas(t *testing.T, c client.Client, replicas int32) {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, "default", "demo-broker")
	sts.Spec.Replicas = &replicas
	if err := c.Update(context.Background(), sts); err != nil {
		t.Fatalf("update broker StatefulSet: %v", err)
	}
}

func brokerStatefulSetReplicas(t *testing.T, c client.Client) int32 {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, "default", "demo-broker")
	if sts.Spec.Replicas == nil {
		t.Fatal("broker StatefulSet replicas is nil")
	}
	return *sts.Spec.Replicas
}

func reconcileBrokerAndHPA(t *testing.T, r *ClusterReconciler, cluster *kafscalev1alpha1.KafscaleCluster) {
	t.Helper()
	ctx := context.Background()
	if err := r.reconcileBrokerDeployment(ctx, cluster, []string{"http://etcd:2379"}); err != nil {
		t.Fatalf("reconcileBrokerDeployment: %v", err)
	}
	if err := r.reconcileBrokerHPA(ctx, cluster); err != nil {
		t.Fatalf("reconcileBrokerHPA: %v", err)
	}
}

func TestBrokerHPAScaleUpIsNotReverted(t *testing.T) {
	r, c, cluster := hpaTestReconciler(t, int32Ptr(2))
	reconcileBrokerAndHPA(t, r, cluster)
	if got := brokerStatefulSetReplicas(t, c); got != 2 {
		t.Fatalf("created replicas = %d, want 2", got)
	}
	setBrokerStatefulSetReplicas(t, c, 4)
	reconcileBrokerAndHPA(t, r, cluster)
	if got := brokerStatefulSetReplicas(t, c); got != 4 {
		t.Fatalf("replicas after HPA scale-up and reconcile = %d, want 4", got)
	}
}

func TestBrokerReplicasClampedToHPABounds(t *testing.T) {
	r, c, cluster := hpaTestReconciler(t, int32Ptr(2))
	reconcileBrokerAndHPA(t, r, cluster)
	for _, tc := range []struct {
		live, want int32
	}{
		{live: 20, want: 8},
		{live: 1, want: 2},
		{live: 5, want: 5},
	} {
		setBrokerStatefulSetReplicas(t, c, tc.live)
		reconcileBrokerAndHPA(t, r, cluster)
		if got := brokerStatefulSetReplicas(t, c); got != tc.want {
			t.Fatalf("live %d: replicas = %d, want %d", tc.live, got, tc.want)
		}
	}
	cluster.Spec.Brokers.Replicas = int32Ptr(6)
	reconcileBrokerAndHPA(t, r, cluster)
	if got := brokerStatefulSetReplicas(t, c); got != 6 {
		t.Fatalf("replicas after raising the minimum to 6 = %d, want 6", got)
	}
}

func TestBrokerReplicasFollowSpecWithoutHPA(t *testing.T) {
	r, c, cluster := hpaTestReconciler(t, int32Ptr(2))
	ctx := context.Background()
	if err := r.reconcileBrokerDeployment(ctx, cluster, nil); err != nil {
		t.Fatalf("reconcileBrokerDeployment: %v", err)
	}
	setBrokerStatefulSetReplicas(t, c, 5)
	if err := r.reconcileBrokerDeployment(ctx, cluster, nil); err != nil {
		t.Fatalf("reconcileBrokerDeployment: %v", err)
	}
	if got := brokerStatefulSetReplicas(t, c); got != 2 {
		t.Fatalf("replicas without an HPA = %d, want spec value 2", got)
	}
}

func TestBrokerReplicasZeroStopsBrokersWithHPA(t *testing.T) {
	r, c, cluster := hpaTestReconciler(t, int32Ptr(2))
	reconcileBrokerAndHPA(t, r, cluster)
	setBrokerStatefulSetReplicas(t, c, 4)
	cluster.Spec.Brokers.Replicas = int32Ptr(0)
	reconcileBrokerAndHPA(t, r, cluster)
	if got := brokerStatefulSetReplicas(t, c); got != 0 {
		t.Fatalf("replicas with spec 0 = %d, want 0", got)
	}
}

func TestBrokerHPAIgnoresForeignHPA(t *testing.T) {
	r, c, cluster := hpaTestReconciler(t, int32Ptr(2))
	ctx := context.Background()
	foreign := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-broker", Namespace: "default"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			MaxReplicas:    8,
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "StatefulSet", Name: "demo-broker", APIVersion: "apps/v1"},
		},
	}
	if err := c.Create(ctx, foreign); err != nil {
		t.Fatalf("create HPA: %v", err)
	}
	if err := r.reconcileBrokerDeployment(ctx, cluster, nil); err != nil {
		t.Fatalf("reconcileBrokerDeployment: %v", err)
	}
	setBrokerStatefulSetReplicas(t, c, 5)
	if err := r.reconcileBrokerDeployment(ctx, cluster, nil); err != nil {
		t.Fatalf("reconcileBrokerDeployment: %v", err)
	}
	if got := brokerStatefulSetReplicas(t, c); got != 2 {
		t.Fatalf("replicas with an HPA the cluster does not own = %d, want 2", got)
	}
}

func TestBrokerHPAScaleDownPolicy(t *testing.T) {
	r, c, cluster := hpaTestReconciler(t, int32Ptr(2))
	reconcileBrokerAndHPA(t, r, cluster)
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	assertFound(t, c, hpa, "default", "demo-broker")
	if *hpa.Spec.MinReplicas != 2 || hpa.Spec.MaxReplicas != 8 {
		t.Fatalf("HPA bounds = %d/%d, want 2/8", *hpa.Spec.MinReplicas, hpa.Spec.MaxReplicas)
	}
	if hpa.Spec.Behavior == nil || hpa.Spec.Behavior.ScaleDown == nil {
		t.Fatal("HPA has no scale-down rules")
	}
	down := hpa.Spec.Behavior.ScaleDown
	if down.StabilizationWindowSeconds == nil || *down.StabilizationWindowSeconds != 300 {
		t.Fatalf("scale-down window = %v, want 300", down.StabilizationWindowSeconds)
	}
	if len(down.Policies) != 1 || down.Policies[0].Type != autoscalingv2.PodsScalingPolicy || down.Policies[0].Value != 1 || down.Policies[0].PeriodSeconds != 60 {
		t.Fatalf("scale-down policies = %+v, want one pod per 60 seconds", down.Policies)
	}
	if hpa.Spec.Behavior.ScaleUp != nil {
		t.Fatalf("scale-up rules = %+v, want the Kubernetes default", hpa.Spec.Behavior.ScaleUp)
	}
}

func TestSnapshotFollowsLiveBrokerCount(t *testing.T) {
	r, c, cluster := hpaTestReconciler(t, int32Ptr(2))
	ctx := context.Background()
	topic := &kafscalev1alpha1.KafscaleTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
		Spec:       kafscalev1alpha1.KafscaleTopicSpec{ClusterRef: "demo", Partitions: 8},
	}
	if err := c.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	endpoints := testutil.StartEmbeddedEtcd(t)
	reconcileBrokerAndHPA(t, r, cluster)
	for _, live := range []int32{4, 3, 2} {
		setBrokerStatefulSetReplicas(t, c, live)
		if err := r.Publisher.Publish(ctx, cluster, endpoints); err != nil {
			t.Fatalf("publish: %v", err)
		}
		snap, err := readSnapshotFromEtcd(ctx, endpoints)
		if err != nil {
			t.Fatalf("read snapshot: %v", err)
		}
		if len(snap.Brokers) != int(live) {
			t.Fatalf("live %d: snapshot brokers = %d", live, len(snap.Brokers))
		}
		leaders := make(map[int32]int)
		for _, part := range snap.Topics[0].Partitions {
			leaders[part.Leader]++
			if len(part.Replicas) != int(live) {
				t.Fatalf("live %d: partition %d replicas = %v", live, part.Partition, part.Replicas)
			}
		}
		for id := int32(0); id < live; id++ {
			if leaders[id] == 0 {
				t.Fatalf("live %d: broker %d leads no partition: %v", live, id, leaders)
			}
		}
		if leaders[live] != 0 {
			t.Fatalf("live %d: removed broker %d still leads partitions", live, live)
		}
	}
}

func TestSnapshotUsesSpecWithoutBrokerStatefulSet(t *testing.T) {
	_, c, cluster := hpaTestReconciler(t, int32Ptr(2))
	if got := NewSnapshotPublisher(c).liveBrokerCount(context.Background(), cluster); got != 0 {
		t.Fatalf("live broker count without a StatefulSet = %d, want 0", got)
	}
	meta := BuildClusterMetadataForBrokers(cluster, nil, 0)
	if len(meta.Brokers) != 2 {
		t.Fatalf("brokers = %d, want spec value 2", len(meta.Brokers))
	}
}
