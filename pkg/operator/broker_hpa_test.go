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

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	kafscalev1 "github.com/KafScale/platform/api/v1"
	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func hpaTestReconciler(t *testing.T, replicas *int32) (*ClusterReconciler, client.Client, *kafscalev1.KafscaleCluster) {
	t.Helper()
	cluster := &kafscalev1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "demo-uid"},
		Spec: kafscalev1.KafscaleClusterSpec{
			Brokers: kafscalev1.BrokerSpec{Replicas: replicas},
			S3:      kafscalev1.S3Spec{Bucket: "bucket", Region: "us-east-1"},
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

func reconcileBrokerAndHPA(t *testing.T, r *ClusterReconciler, cluster *kafscalev1.KafscaleCluster) {
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
	up := hpa.Spec.Behavior.ScaleUp
	if up == nil || up.StabilizationWindowSeconds == nil || *up.StabilizationWindowSeconds != 60 {
		t.Fatalf("scale-up rules = %+v, want a 60 second window", up)
	}
	if len(up.Policies) != 2 || up.Policies[0].Type != autoscalingv2.PercentScalingPolicy || up.Policies[0].Value != 100 || up.Policies[1].Type != autoscalingv2.PodsScalingPolicy || up.Policies[1].Value != 4 {
		t.Fatalf("scale-up policies = %+v, want the Kubernetes default policies", up.Policies)
	}
}

func TestSnapshotFollowsLiveBrokerCount(t *testing.T) {
	r, c, cluster := hpaTestReconciler(t, int32Ptr(2))
	ctx := context.Background()
	topic := &kafscalev1.KafscaleTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
		Spec:       kafscalev1.KafscaleTopicSpec{ClusterRef: "demo", Partitions: 8},
	}
	if err := c.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	endpoints := testutil.StartEmbeddedEtcd(t)
	reconcileBrokerAndHPA(t, r, cluster)
	for _, live := range []int32{4, 3, 2, 0} {
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
			if live == 0 && (part.Leader != -1 || part.ErrorCode == 0) {
				t.Fatalf("stopped cluster advertises an available leader: %+v", part)
			}
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

func TestSnapshotReadFailureKeepsPublishedBrokerCount(t *testing.T) {
	r, c, cluster := hpaTestReconciler(t, int32Ptr(2))
	ctx := context.Background()
	endpoints := testutil.StartEmbeddedEtcd(t)
	reconcileBrokerAndHPA(t, r, cluster)
	setBrokerStatefulSetReplicas(t, c, 4)
	if err := r.Publisher.Publish(ctx, cluster, endpoints); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("statefulset API unavailable")
	r.Publisher.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*appsv1.StatefulSet); ok {
				return wantErr
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	if err := r.Publisher.Publish(ctx, cluster, endpoints); !errors.Is(err, wantErr) {
		t.Fatalf("publish = %v, want StatefulSet read error", err)
	}
	snap, err := readSnapshotFromEtcd(ctx, endpoints)
	if err != nil || len(snap.Brokers) != 4 {
		t.Fatalf("API failure changed the published broker count: %d, %v", len(snap.Brokers), err)
	}
}

func TestSnapshotRebalancesKafkaCreatedTopics(t *testing.T) {
	r, c, cluster := hpaTestReconciler(t, int32Ptr(2))
	ctx := context.Background()
	endpoints := testutil.StartEmbeddedEtcd(t)
	reconcileBrokerAndHPA(t, r, cluster)
	if err := r.Publisher.Publish(ctx, cluster, endpoints); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic, err := store.CreateTopic(ctx, metadata.TopicSpec{Name: "kafka-created", NumPartitions: 8, ReplicationFactor: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, live := range []int32{4, 1, 0, 3} {
		setBrokerStatefulSetReplicas(t, c, live)
		if err := r.Publisher.Publish(ctx, cluster, endpoints); err != nil {
			t.Fatal(err)
		}
		snap, err := readSnapshotFromEtcd(ctx, endpoints)
		if err != nil || len(snap.Topics) != 1 || snap.Topics[0].TopicID != topic.TopicID {
			t.Fatalf("Kafka-created topic identity was not preserved: %+v, %v", snap.Topics, err)
		}
		for _, part := range snap.Topics[0].Partitions {
			if live == 0 {
				if part.Leader != -1 || len(part.ISR) != 0 || part.ErrorCode == 0 {
					t.Fatalf("stopped cluster advertises an available partition: %+v", part)
				}
				continue
			}
			if part.Leader != part.Partition%live || part.ErrorCode != 0 {
				t.Fatalf("live %d: stale Kafka-created partition placement: %+v", live, part)
			}
		}
	}
}

func TestSnapshotUsesSpecWithoutBrokerStatefulSet(t *testing.T) {
	_, c, cluster := hpaTestReconciler(t, int32Ptr(2))
	got, err := NewSnapshotPublisher(c).liveBrokerCount(context.Background(), cluster)
	if err != nil || got != 2 {
		t.Fatalf("live broker count without a StatefulSet = %d, %v; want spec value 2", got, err)
	}
	meta := BuildClusterMetadata(cluster, nil)
	if len(meta.Brokers) != 2 {
		t.Fatalf("brokers = %d, want spec value 2", len(meta.Brokers))
	}
}

func TestBrokerCountDefaultsAndHPAReadErrors(t *testing.T) {
	r, c, cluster := hpaTestReconciler(t, nil)
	ctx := context.Background()
	count, err := r.Publisher.liveBrokerCount(ctx, cluster)
	if err != nil || count != 3 {
		t.Fatalf("missing StatefulSet default count = %d, %v", count, err)
	}
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "demo-broker", Namespace: "default"}}
	if err := c.Create(ctx, sts); err != nil {
		t.Fatal(err)
	}
	count, err = r.Publisher.liveBrokerCount(ctx, cluster)
	if err != nil || count != 3 {
		t.Fatalf("unset replica count = %d, %v", count, err)
	}
	wantErr := errors.New("HPA API unavailable")
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*autoscalingv2.HorizontalPodAutoscaler); ok {
				return wantErr
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	if err := r.reconcileBrokerDeployment(ctx, cluster, nil); !errors.Is(err, wantErr) {
		t.Fatalf("broker reconcile swallowed HPA read failure: %v", err)
	}
}

func TestSnapshotPlacementPreservesExistingInputAndPartitionErrors(t *testing.T) {
	existing := metadata.ClusterMetadata{Topics: []protocol.MetadataTopic{
		{},
		{Topic: kmsg.StringPtr("kafka-created"), Partitions: []protocol.MetadataPartition{
			{Partition: 0, Leader: 0, Replicas: []int32{0, 1, 2}},
			{Partition: 1, Leader: 0},
			{Partition: -1, ErrorCode: protocol.UNKNOWN_TOPIC_OR_PARTITION},
			{Partition: 2, ErrorCode: protocol.REQUEST_TIMED_OUT},
		}},
	}}
	next := metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 7}, {NodeID: 9}},
		Topics:  []protocol.MetadataTopic{{}, {Topic: kmsg.StringPtr("")}},
	}
	got := mergeSnapshots(next, existing)
	parts := got.Topics[2].Partitions
	if parts[0].Leader != 7 || len(parts[0].Replicas) != 2 || parts[0].Replicas[1] != 9 || parts[1].Leader != 9 || len(parts[1].Replicas) != 1 {
		t.Fatalf("placement did not follow available broker IDs: %+v", parts)
	}
	if parts[2].Partition != -1 || parts[3].ErrorCode != protocol.REQUEST_TIMED_OUT {
		t.Fatal("invalid or failed partitions were silently repaired")
	}
	if existing.Topics[1].Partitions[0].Leader != 0 || existing.Topics[1].Partitions[0].Replicas[2] != 2 {
		t.Fatal("placement mutated its existing snapshot input")
	}
}
