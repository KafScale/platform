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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
	"github.com/twmb/franz-go/pkg/kmsg"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// --- cluster_controller.go helpers ---

func TestParsePullPolicyAlways(t *testing.T) {
	if got := parsePullPolicy("Always"); got != corev1.PullAlways {
		t.Fatalf("expected Always, got %q", got)
	}
}

func TestParsePullPolicyNever(t *testing.T) {
	if got := parsePullPolicy("Never"); got != corev1.PullNever {
		t.Fatalf("expected Never, got %q", got)
	}
}

func TestParsePullPolicyDefault(t *testing.T) {
	if got := parsePullPolicy(""); got != corev1.PullIfNotPresent {
		t.Fatalf("expected IfNotPresent, got %q", got)
	}
}

func TestCopyStringMapNil(t *testing.T) {
	if got := copyStringMap(nil); got != nil {
		t.Fatalf("expected nil for nil map, got %v", got)
	}
}

func TestCopyStringMapEmpty(t *testing.T) {
	if got := copyStringMap(map[string]string{}); got != nil {
		t.Fatalf("expected nil for empty map, got %v", got)
	}
}

func TestCopyStringMapIsolation(t *testing.T) {
	src := map[string]string{"key": "val"}
	dst := copyStringMap(src)
	if dst["key"] != "val" {
		t.Fatalf("expected val, got %q", dst["key"])
	}
	dst["key"] = "changed"
	if src["key"] != "val" {
		t.Fatal("modifying copy affected source")
	}
}

func TestSetClusterCondition(t *testing.T) {
	conditions := []metav1.Condition{}
	cond := metav1.Condition{
		Type:   "Ready",
		Status: metav1.ConditionTrue,
		Reason: "OK",
	}
	setClusterCondition(&conditions, cond)
	if len(conditions) != 1 || conditions[0].Type != "Ready" {
		t.Fatalf("expected 1 condition, got %d", len(conditions))
	}

	// Update existing
	cond2 := metav1.Condition{
		Type:   "Ready",
		Status: metav1.ConditionFalse,
		Reason: "Failed",
	}
	setClusterCondition(&conditions, cond2)
	if len(conditions) != 1 || conditions[0].Reason != "Failed" {
		t.Fatalf("expected condition updated, got %+v", conditions)
	}
}

func TestCloneResourceListNil(t *testing.T) {
	if got := cloneResourceList(nil); got != nil {
		t.Fatalf("expected nil for nil resource list, got %v", got)
	}
}

func TestCloneResourceListDeepCopy(t *testing.T) {
	src := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("100m"),
		corev1.ResourceMemory: resource.MustParse("256Mi"),
	}
	dst := cloneResourceList(src)
	if dst[corev1.ResourceCPU] != resource.MustParse("100m") {
		t.Fatalf("unexpected CPU: %v", dst[corev1.ResourceCPU])
	}
	if len(dst) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(dst))
	}
}

func TestInt32Ptr(t *testing.T) {
	p := int32Ptr(42)
	if *p != 42 {
		t.Fatalf("expected 42, got %d", *p)
	}
}

func TestBoolPtr(t *testing.T) {
	p := boolPtr(true)
	if !*p {
		t.Fatal("expected true")
	}
}

func TestBrokerHeadlessServiceName(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo"},
	}
	if got := brokerHeadlessServiceName(cluster); got != "demo-broker-headless" {
		t.Fatalf("expected demo-broker-headless, got %q", got)
	}
}

func TestGetEnv(t *testing.T) {
	t.Setenv("TEST_GETENV_VAR", "hello")
	if got := getEnv("TEST_GETENV_VAR", "fallback"); got != "hello" {
		t.Fatalf("expected hello, got %q", got)
	}
	if got := getEnv("TEST_GETENV_MISSING", "fallback"); got != "fallback" {
		t.Fatalf("expected fallback, got %q", got)
	}
}

// --- reconcileBrokerDeployment ---

func TestReconcileBrokerDeployment(t *testing.T) {
	replicas := int32(5)
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{Replicas: &replicas},
			S3:      kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1"},
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if err := r.reconcileBrokerDeployment(context.Background(), cluster, []string{"http://etcd:2379"}); err != nil {
		t.Fatalf("reconcileBrokerDeployment: %v", err)
	}

	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, "default", "demo-broker")
	if *sts.Spec.Replicas != 5 {
		t.Fatalf("expected 5 replicas, got %d", *sts.Spec.Replicas)
	}
}

func TestReconcileBrokerDeploymentDefaultReplicas(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{},
			S3:      kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1"},
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if err := r.reconcileBrokerDeployment(context.Background(), cluster, []string{"http://etcd:2379"}); err != nil {
		t.Fatalf("reconcileBrokerDeployment: %v", err)
	}

	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, "default", "demo-broker")
	if *sts.Spec.Replicas != 3 {
		t.Fatalf("expected default 3 replicas, got %d", *sts.Spec.Replicas)
	}
}

// --- deleteLegacyBrokerDeployment ---

func TestDeleteLegacyBrokerDeployment(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       kafscalev1alpha1.KafscaleClusterSpec{},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	// Should not error even when no legacy deployment exists
	if err := r.deleteLegacyBrokerDeployment(context.Background(), cluster); err != nil {
		t.Fatalf("deleteLegacyBrokerDeployment: %v", err)
	}
}

// --- reconcileBrokerHPA ---

func TestReconcileBrokerHPA(t *testing.T) {
	replicas := int32(3)
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{Replicas: &replicas},
			S3:      kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1"},
		},
	}
	scheme := testScheme(t)
	if err := autoscalingv2.AddToScheme(scheme); err != nil {
		t.Fatalf("add autoscaling scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if err := r.reconcileBrokerHPA(context.Background(), cluster); err != nil {
		t.Fatalf("reconcileBrokerHPA: %v", err)
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	assertFound(t, c, hpa, "default", "demo-broker")
	if *hpa.Spec.MinReplicas != 3 {
		t.Fatalf("expected min replicas 3, got %d", *hpa.Spec.MinReplicas)
	}
	if hpa.Spec.MaxReplicas != 12 {
		t.Fatalf("expected max replicas 12, got %d", hpa.Spec.MaxReplicas)
	}
}

func TestReconcileBrokerHPADefaultReplicas(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{},
			S3:      kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1"},
		},
	}
	scheme := testScheme(t)
	if err := autoscalingv2.AddToScheme(scheme); err != nil {
		t.Fatalf("add autoscaling scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if err := r.reconcileBrokerHPA(context.Background(), cluster); err != nil {
		t.Fatalf("reconcileBrokerHPA: %v", err)
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	assertFound(t, c, hpa, "default", "demo-broker")
	if *hpa.Spec.MinReplicas != 3 {
		t.Fatalf("expected default min replicas 3, got %d", *hpa.Spec.MinReplicas)
	}
}

// --- updateStatus ---

func TestUpdateStatus(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       kafscalev1alpha1.KafscaleClusterSpec{},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if err := r.updateStatus(context.Background(), cluster, metav1.ConditionTrue, "Reconciled", "All resources ready"); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}
	if cluster.Status.Phase != "Reconciled" {
		t.Fatalf("expected phase Reconciled, got %q", cluster.Status.Phase)
	}
}

// --- populateEtcdSnapshotStatus ---

func TestPopulateEtcdSnapshotStatusNotManaged(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	r.populateEtcdSnapshotStatus(context.Background(), cluster, EtcdResolution{Managed: false})

	found := false
	for _, cond := range cluster.Status.Conditions {
		if cond.Type == "EtcdSnapshot" && cond.Reason == "SnapshotNotManaged" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected SnapshotNotManaged condition")
	}
}

func TestPopulateEtcdSnapshotStatusCronMissing(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	r.populateEtcdSnapshotStatus(context.Background(), cluster, EtcdResolution{Managed: true})

	found := false
	for _, cond := range cluster.Status.Conditions {
		if cond.Type == "EtcdSnapshot" && cond.Reason == "SnapshotCronMissing" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected SnapshotCronMissing condition")
	}
}

func TestPopulateEtcdSnapshotStatusNeverSucceeded(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
	}
	cron := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-etcd-snapshot", Namespace: "default"},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster, cron).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	r.populateEtcdSnapshotStatus(context.Background(), cluster, EtcdResolution{Managed: true})

	found := false
	for _, cond := range cluster.Status.Conditions {
		if cond.Type == "EtcdSnapshot" && cond.Reason == "SnapshotNeverSucceeded" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected SnapshotNeverSucceeded condition")
	}
}

func TestPopulateEtcdSnapshotStatusHealthy(t *testing.T) {
	now := metav1.NewTime(time.Now())
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
	}
	cron := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-etcd-snapshot", Namespace: "default"},
		Status: batchv1.CronJobStatus{
			LastSuccessfulTime: &now,
			LastScheduleTime:   &now,
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster, cron).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	r.populateEtcdSnapshotStatus(context.Background(), cluster, EtcdResolution{Managed: true})

	found := false
	for _, cond := range cluster.Status.Conditions {
		if cond.Type == "EtcdSnapshot" && cond.Reason == "SnapshotHealthy" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected SnapshotHealthy condition")
	}
}

func TestPopulateEtcdSnapshotStatusStale(t *testing.T) {
	// Use a time well in the past
	old := metav1.NewTime(time.Now().Add(-30 * 24 * time.Hour))
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
	}
	cron := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-etcd-snapshot", Namespace: "default"},
		Status: batchv1.CronJobStatus{
			LastSuccessfulTime: &old,
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster, cron).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	r.populateEtcdSnapshotStatus(context.Background(), cluster, EtcdResolution{Managed: true})

	found := false
	for _, cond := range cluster.Status.Conditions {
		if cond.Type == "EtcdSnapshot" && cond.Reason == "SnapshotStale" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected SnapshotStale condition")
	}
}

// --- etcd_resources.go helpers ---

func TestSanitizeBucketName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"my-bucket", "my-bucket"},
		{"My_Bucket.123", "my-bucket-123"},
		{"   spaces  ", "spaces"},
		{"", defaultSnapshotBucketPrefix},
		{"!!!!", defaultSnapshotBucketPrefix},
		{"a--b", "a-b"},
		{"-leading-trailing-", "leading-trailing"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := sanitizeBucketName(tc.input)
			if got != tc.want {
				t.Fatalf("sanitizeBucketName(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestDefaultEtcdSnapshotBucket(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
	}{
		{"", ""},
		{"demo", ""},
		{"", "default"},
		{"demo", "default"},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s/%s", tc.namespace, tc.name), func(t *testing.T) {
			cluster := &kafscalev1alpha1.KafscaleCluster{
				ObjectMeta: metav1.ObjectMeta{Name: tc.name, Namespace: tc.namespace},
			}
			got := defaultEtcdSnapshotBucket(cluster)
			if got == "" {
				t.Fatal("expected non-empty bucket name")
			}
		})
	}
}

func TestBuildEtcdInitialCluster(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo"},
	}

	got := buildEtcdInitialCluster(cluster, 3)
	if got == "" {
		t.Fatal("expected non-empty cluster string")
	}

	// Zero replicas => at least 1
	got = buildEtcdInitialCluster(cluster, 0)
	if got == "" {
		t.Fatal("expected non-empty cluster string for 0 replicas")
	}
}

func TestEtcdReplicas(t *testing.T) {
	// Default (no env set)
	t.Setenv(operatorEtcdReplicasEnv, "")
	got := etcdReplicas()
	if got != int32(defaultEtcdReplicas) {
		t.Fatalf("expected default %d, got %d", defaultEtcdReplicas, got)
	}

	// Valid override
	t.Setenv(operatorEtcdReplicasEnv, "5")
	got = etcdReplicas()
	if got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}

	// Invalid value
	t.Setenv(operatorEtcdReplicasEnv, "abc")
	got = etcdReplicas()
	if got != int32(defaultEtcdReplicas) {
		t.Fatalf("expected default for invalid, got %d", got)
	}

	// Zero
	t.Setenv(operatorEtcdReplicasEnv, "0")
	got = etcdReplicas()
	if got != int32(defaultEtcdReplicas) {
		t.Fatalf("expected default for zero, got %d", got)
	}
}

func TestParseBoolEnv(t *testing.T) {
	t.Setenv("TEST_BOOL_ENV", "true")
	if !parseBoolEnv("TEST_BOOL_ENV") {
		t.Fatal("expected true for 'true'")
	}
	t.Setenv("TEST_BOOL_ENV", "1")
	if !parseBoolEnv("TEST_BOOL_ENV") {
		t.Fatal("expected true for '1'")
	}
	t.Setenv("TEST_BOOL_ENV", "yes")
	if !parseBoolEnv("TEST_BOOL_ENV") {
		t.Fatal("expected true for 'yes'")
	}
	t.Setenv("TEST_BOOL_ENV", "on")
	if !parseBoolEnv("TEST_BOOL_ENV") {
		t.Fatal("expected true for 'on'")
	}
	t.Setenv("TEST_BOOL_ENV", "false")
	if parseBoolEnv("TEST_BOOL_ENV") {
		t.Fatal("expected false for 'false'")
	}
	t.Setenv("TEST_BOOL_ENV", "")
	if parseBoolEnv("TEST_BOOL_ENV") {
		t.Fatal("expected false for empty")
	}
}

func TestBoolToString(t *testing.T) {
	if boolToString(true) != "1" {
		t.Fatal("expected '1' for true")
	}
	if boolToString(false) != "0" {
		t.Fatal("expected '0' for false")
	}
}

func TestStringPtrOrNil(t *testing.T) {
	if stringPtrOrNil("") != nil {
		t.Fatal("expected nil for empty")
	}
	if stringPtrOrNil("  ") != nil {
		t.Fatal("expected nil for whitespace")
	}
	p := stringPtrOrNil("hello")
	if p == nil || *p != "hello" {
		t.Fatalf("expected 'hello', got %v", p)
	}
}

func TestCleanEndpoints(t *testing.T) {
	got := cleanEndpoints([]string{"  http://a:2379 ", "http://b:2379", "http://a:2379", ""})
	if len(got) != 2 {
		t.Fatalf("expected 2 unique endpoints, got %d: %v", len(got), got)
	}
}

// --- lfs_proxy_resources.go helpers ---

func TestLfsProxyName(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo"},
	}
	if got := lfsProxyName(cluster); got != "demo-lfs-proxy" {
		t.Fatalf("expected demo-lfs-proxy, got %q", got)
	}
}

func TestLfsProxyMetricsName(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo"},
	}
	if got := lfsProxyMetricsName(cluster); got != "demo-lfs-proxy-metrics" {
		t.Fatalf("expected demo-lfs-proxy-metrics, got %q", got)
	}
}

func TestLfsProxyNamespace(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
		Spec:       kafscalev1alpha1.KafscaleClusterSpec{},
	}
	if got := lfsProxyNamespace(cluster); got != "default" {
		t.Fatalf("expected default, got %q", got)
	}

	cluster.Spec.LfsProxy.S3.Namespace = "custom"
	if got := lfsProxyNamespace(cluster); got != "custom" {
		t.Fatalf("expected custom, got %q", got)
	}
}

func TestLfsProxyPortDefaults(t *testing.T) {
	spec := kafscalev1alpha1.LfsProxySpec{}
	if got := lfsProxyPort(spec); got != defaultLfsProxyPort {
		t.Fatalf("expected default port %d, got %d", defaultLfsProxyPort, got)
	}
	if got := lfsProxyHTTPPort(spec); got != defaultLfsProxyHTTPPort {
		t.Fatalf("expected default http port %d, got %d", defaultLfsProxyHTTPPort, got)
	}
	if got := lfsProxyHealthPort(spec); got != defaultLfsProxyHealthPort {
		t.Fatalf("expected default health port %d, got %d", defaultLfsProxyHealthPort, got)
	}
	if got := lfsProxyMetricsPort(spec); got != defaultLfsProxyMetricsPort {
		t.Fatalf("expected default metrics port %d, got %d", defaultLfsProxyMetricsPort, got)
	}
}

func TestLfsProxyPortCustom(t *testing.T) {
	p1, p2, p3, p4 := int32(11111), int32(22222), int32(33333), int32(44444)
	spec := kafscalev1alpha1.LfsProxySpec{
		Service: kafscalev1alpha1.LfsProxyServiceSpec{Port: &p1},
		HTTP:    kafscalev1alpha1.LfsProxyHTTPSpec{Port: &p2},
		Health:  kafscalev1alpha1.LfsProxyHealthSpec{Port: &p3},
		Metrics: kafscalev1alpha1.LfsProxyMetricsSpec{Port: &p4},
	}
	if got := lfsProxyPort(spec); got != 11111 {
		t.Fatalf("expected 11111, got %d", got)
	}
	if got := lfsProxyHTTPPort(spec); got != 22222 {
		t.Fatalf("expected 22222, got %d", got)
	}
	if got := lfsProxyHealthPort(spec); got != 33333 {
		t.Fatalf("expected 33333, got %d", got)
	}
	if got := lfsProxyMetricsPort(spec); got != 44444 {
		t.Fatalf("expected 44444, got %d", got)
	}
}

func TestLfsProxyEnabledFlags(t *testing.T) {
	trueVal, falseVal := true, false

	spec := kafscalev1alpha1.LfsProxySpec{}
	// Defaults
	if lfsProxyHTTPEnabled(spec) {
		t.Fatal("expected HTTP disabled by default")
	}
	if !lfsProxyMetricsEnabled(spec) {
		t.Fatal("expected metrics enabled by default")
	}
	if !lfsProxyHealthEnabled(spec) {
		t.Fatal("expected health enabled by default")
	}

	// Explicit
	spec.HTTP.Enabled = &trueVal
	spec.Metrics.Enabled = &falseVal
	spec.Health.Enabled = &falseVal
	if !lfsProxyHTTPEnabled(spec) {
		t.Fatal("expected HTTP enabled when set")
	}
	if lfsProxyMetricsEnabled(spec) {
		t.Fatal("expected metrics disabled when set")
	}
	if lfsProxyHealthEnabled(spec) {
		t.Fatal("expected health disabled when set")
	}
}

func TestLfsProxyForcePathStyle(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			S3: kafscalev1alpha1.S3Spec{Endpoint: "http://minio:9000"},
		},
	}
	// When endpoint is set, force path style
	if !lfsProxyForcePathStyle(cluster) {
		t.Fatal("expected force path style with endpoint")
	}
	// No endpoint
	cluster.Spec.S3.Endpoint = ""
	if lfsProxyForcePathStyle(cluster) {
		t.Fatal("expected no force path style without endpoint")
	}
	// Explicit override
	trueVal := true
	cluster.Spec.LfsProxy.S3.ForcePathStyle = &trueVal
	if !lfsProxyForcePathStyle(cluster) {
		t.Fatal("expected force path style when explicitly set")
	}
}

func TestLfsProxyEnsureBucket(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{}
	if lfsProxyEnsureBucket(cluster) {
		t.Fatal("expected false by default")
	}
	trueVal := true
	cluster.Spec.LfsProxy.S3.EnsureBucket = &trueVal
	if !lfsProxyEnsureBucket(cluster) {
		t.Fatal("expected true when set")
	}
}

func TestServicePort(t *testing.T) {
	sp := servicePort("test", 8080)
	if sp.Name != "test" || sp.Port != 8080 || sp.Protocol != corev1.ProtocolTCP {
		t.Fatalf("unexpected service port: %+v", sp)
	}
}

// --- reconcileLfsProxyMetricsService ---

func TestReconcileLfsProxyMetricsService(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       kafscalev1alpha1.KafscaleClusterSpec{},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if err := r.reconcileLfsProxyMetricsService(context.Background(), cluster); err != nil {
		t.Fatalf("reconcileLfsProxyMetricsService: %v", err)
	}

	svc := &corev1.Service{}
	assertFound(t, c, svc, "default", "demo-lfs-proxy-metrics")
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("expected ClusterIP, got %s", svc.Spec.Type)
	}
}

// --- deleteLfsProxyResources ---

func TestDeleteLfsProxyResources(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	// Should not error even when resources don't exist
	if err := r.deleteLfsProxyResources(context.Background(), cluster); err != nil {
		t.Fatalf("deleteLfsProxyResources: %v", err)
	}
}

// --- deleteLfsProxyMetricsService ---

func TestDeleteLfsProxyMetricsService(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if err := r.deleteLfsProxyMetricsService(context.Background(), cluster); err != nil {
		t.Fatalf("deleteLfsProxyMetricsService: %v", err)
	}
}

// --- reconcileLfsProxyResources ---

func TestReconcileLfsProxyResourcesDisabled(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			LfsProxy: kafscalev1alpha1.LfsProxySpec{Enabled: false},
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if err := r.reconcileLfsProxyResources(context.Background(), cluster, []string{"http://etcd:2379"}); err != nil {
		t.Fatalf("reconcileLfsProxyResources disabled: %v", err)
	}
}

func TestReconcileLfsProxyResourcesEnabled(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			S3:       kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1"},
			LfsProxy: kafscalev1alpha1.LfsProxySpec{Enabled: true},
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if err := r.reconcileLfsProxyResources(context.Background(), cluster, []string{"http://etcd:2379"}); err != nil {
		t.Fatalf("reconcileLfsProxyResources enabled: %v", err)
	}

	// Should have created deployment and service
	deploy := &appsv1.Deployment{}
	assertFound(t, c, deploy, "default", "demo-lfs-proxy")
}

// --- snapshot.go ---

func TestMergeSnapshots(t *testing.T) {
	next := metadata.ClusterMetadata{
		Topics: []protocol.MetadataTopic{
			{Topic: kmsg.StringPtr("orders")},
		},
	}
	existing := metadata.ClusterMetadata{
		Topics: []protocol.MetadataTopic{
			{Topic: kmsg.StringPtr("orders")},            // duplicate
			{Topic: kmsg.StringPtr("events")},            // new
			{Topic: kmsg.StringPtr("")},                  // empty name, skip
			{Topic: kmsg.StringPtr("bad"), ErrorCode: 3}, // error, skip
		},
	}

	merged := mergeSnapshots(next, existing)
	if len(merged.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(merged.Topics))
	}
	names := make(map[string]bool)
	for _, topic := range merged.Topics {
		names[*topic.Topic] = true
	}
	if !names["orders"] || !names["events"] {
		t.Fatalf("unexpected topics: %v", merged.Topics)
	}
}

func TestMergeSnapshotsEmptyExisting(t *testing.T) {
	next := metadata.ClusterMetadata{
		Topics: []protocol.MetadataTopic{{Topic: kmsg.StringPtr("orders")}},
	}
	existing := metadata.ClusterMetadata{}

	merged := mergeSnapshots(next, existing)
	if len(merged.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(merged.Topics))
	}
}

func TestBuildReplicaIDsZero(t *testing.T) {
	if got := buildReplicaIDs(0); got != nil {
		t.Fatalf("expected nil for 0 replicas, got %v", got)
	}
}

func TestBuildReplicaIDsNegative(t *testing.T) {
	if got := buildReplicaIDs(-1); got != nil {
		t.Fatalf("expected nil for negative replicas, got %v", got)
	}
}

func TestBuildReplicaIDs(t *testing.T) {
	got := buildReplicaIDs(3)
	if len(got) != 3 {
		t.Fatalf("expected 3 IDs, got %d", len(got))
	}
	for i, id := range got {
		if id != int32(i) {
			t.Fatalf("expected ID %d at index %d, got %d", i, i, id)
		}
	}
}

// --- snapshot_access.go ---

func TestFirstSecretValue(t *testing.T) {
	secret := &corev1.Secret{
		Data: map[string][]byte{
			"KEY1": []byte("val1"),
			"KEY2": []byte("val2"),
		},
	}
	if got := firstSecretValue(secret, "KEY1", "KEY2"); got != "val1" {
		t.Fatalf("expected val1, got %q", got)
	}
	if got := firstSecretValue(secret, "MISSING", "KEY2"); got != "val2" {
		t.Fatalf("expected val2, got %q", got)
	}
	if got := firstSecretValue(secret, "MISSING"); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
	// Empty value should be skipped
	secret.Data["EMPTY"] = []byte("  ")
	if got := firstSecretValue(secret, "EMPTY", "KEY1"); got != "val1" {
		t.Fatalf("expected val1 (skip empty), got %q", got)
	}
}

func TestLoadS3CredentialsNoSecret(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			S3: kafscalev1alpha1.S3Spec{CredentialsSecretRef: ""},
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	cfg := &storage.S3Config{}
	if err := r.loadS3Credentials(context.Background(), cluster, cfg); err != nil {
		t.Fatalf("loadS3Credentials: %v", err)
	}
}

func TestLoadS3CredentialsSecretNotFound(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			S3: kafscalev1alpha1.S3Spec{CredentialsSecretRef: "missing-secret"},
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	cfg := &storage.S3Config{}
	err := r.loadS3Credentials(context.Background(), cluster, cfg)
	if err == nil {
		t.Fatal("expected error for missing secret")
	}
}

func TestLoadS3CredentialsWithSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "default"},
		Data: map[string][]byte{
			s3AccessKeyEnv: []byte("AKID"),
			s3SecretKeyEnv: []byte("SECRET"),
		},
	}
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			S3: kafscalev1alpha1.S3Spec{CredentialsSecretRef: "creds"},
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, secret).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	cfg := &storage.S3Config{}
	if err := r.loadS3Credentials(context.Background(), cluster, cfg); err != nil {
		t.Fatalf("loadS3Credentials: %v", err)
	}
	if cfg.AccessKeyID != "AKID" || cfg.SecretAccessKey != "SECRET" {
		t.Fatalf("unexpected credentials: %+v", cfg)
	}
}

func TestRecordSnapshotAccessFailure(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	r.recordSnapshotAccessFailure(context.Background(), cluster, "default/demo", fmt.Errorf("test error"))

	found := false
	for _, cond := range cluster.Status.Conditions {
		if cond.Type == "EtcdSnapshotAccess" && cond.Reason == "SnapshotAccessFailed" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected SnapshotAccessFailed condition")
	}
}

func TestVerifySnapshotS3AccessNotManaged(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	err := r.verifySnapshotS3Access(context.Background(), cluster, EtcdResolution{Managed: false})
	if err != nil {
		t.Fatalf("expected nil error for non-managed, got %v", err)
	}
}

// --- topic_controller.go ---

func TestSetTopicCondition(t *testing.T) {
	conditions := []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionTrue, Reason: "OK"},
	}
	// Update existing
	setTopicCondition(&conditions, metav1.Condition{
		Type: "Ready", Status: metav1.ConditionFalse, Reason: "Failed",
	})
	if len(conditions) != 1 || conditions[0].Reason != "Failed" {
		t.Fatalf("expected updated condition, got %+v", conditions)
	}

	// Add new
	setTopicCondition(&conditions, metav1.Condition{
		Type: "Published", Status: metav1.ConditionTrue, Reason: "OK",
	})
	if len(conditions) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(conditions))
	}
}

// --- metrics.go ---

func TestRecordClusterCount(t *testing.T) {
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	// Should not panic even with no clusters
	recordClusterCount(context.Background(), c)
}

func TestRecordClusterCountWithClusters(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	recordClusterCount(context.Background(), c)
}

// --- brokerContainer: exercise all conditional branches ---

func TestBrokerContainerAllOptions(t *testing.T) {
	// Set ACL env vars
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"rules":[]}`)
	t.Setenv("KAFSCALE_ACL_FILE", "/etc/kafscale/acl.json")
	t.Setenv("KAFSCALE_ACL_FAIL_OPEN", "false")
	t.Setenv("KAFSCALE_PRINCIPAL_SOURCE", "mtls")
	t.Setenv("KAFSCALE_PROXY_PROTOCOL", "true")
	t.Setenv("KAFSCALE_LOG_LEVEL", "debug")
	t.Setenv("KAFSCALE_TRACE_KAFKA", "1")

	replicas := int32(1) // single replica to exercise advertisedHost branch
	port := int32(19092)
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{
				Replicas:       &replicas,
				AdvertisedHost: "my.broker.host",
				AdvertisedPort: &port,
				Resources: kafscalev1alpha1.BrokerResources{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
					Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
				},
			},
			S3: kafscalev1alpha1.S3Spec{
				Bucket:               "bucket",
				Region:               "us-east-1",
				Endpoint:             "http://minio:9000",
				ReadBucket:           "read-bucket",
				ReadRegion:           "eu-west-1",
				ReadEndpoint:         "http://minio-read:9000",
				CredentialsSecretRef: "s3-creds",
			},
			Config: kafscalev1alpha1.ClusterConfigSpec{
				SegmentBytes:    1048576,
				FlushIntervalMs: 5000,
				CacheSize:       "128Mi",
			},
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	container := r.brokerContainer(cluster, []string{"http://etcd:2379"})

	if container.Name != "broker" {
		t.Fatalf("expected broker, got %q", container.Name)
	}

	envMap := make(map[string]string)
	for _, e := range container.Env {
		envMap[e.Name] = e.Value
	}

	// Single replica with advertised host => host should be set
	if envMap["KAFSCALE_BROKER_HOST"] != "my.broker.host" {
		t.Fatalf("expected KAFSCALE_BROKER_HOST=my.broker.host, got %q", envMap["KAFSCALE_BROKER_HOST"])
	}
	if envMap["KAFSCALE_BROKER_PORT"] != "19092" {
		t.Fatalf("expected port 19092, got %q", envMap["KAFSCALE_BROKER_PORT"])
	}
	if envMap["KAFSCALE_S3_ENDPOINT"] != "http://minio:9000" {
		t.Fatalf("expected S3 endpoint, got %q", envMap["KAFSCALE_S3_ENDPOINT"])
	}
	if envMap["KAFSCALE_S3_PATH_STYLE"] != "true" {
		t.Fatal("expected S3 path style set")
	}
	if envMap["KAFSCALE_S3_READ_BUCKET"] != "read-bucket" {
		t.Fatalf("expected read bucket, got %q", envMap["KAFSCALE_S3_READ_BUCKET"])
	}
	if envMap["KAFSCALE_S3_READ_REGION"] != "eu-west-1" {
		t.Fatalf("expected read region, got %q", envMap["KAFSCALE_S3_READ_REGION"])
	}
	if envMap["KAFSCALE_S3_READ_ENDPOINT"] != "http://minio-read:9000" {
		t.Fatalf("expected read endpoint, got %q", envMap["KAFSCALE_S3_READ_ENDPOINT"])
	}
	if envMap["KAFSCALE_SEGMENT_BYTES"] != "1048576" {
		t.Fatalf("expected segment bytes, got %q", envMap["KAFSCALE_SEGMENT_BYTES"])
	}
	if envMap["KAFSCALE_FLUSH_INTERVAL_MS"] != "5000" {
		t.Fatalf("expected flush interval, got %q", envMap["KAFSCALE_FLUSH_INTERVAL_MS"])
	}
	if envMap["KAFSCALE_CACHE_BYTES"] != "128Mi" {
		t.Fatalf("expected cache bytes, got %q", envMap["KAFSCALE_CACHE_BYTES"])
	}
	if envMap["KAFSCALE_ACL_ENABLED"] != "true" {
		t.Fatal("expected ACL enabled env")
	}
	if envMap["KAFSCALE_ACL_JSON"] == "" {
		t.Fatal("expected ACL JSON env")
	}
	if envMap["KAFSCALE_ACL_FILE"] == "" {
		t.Fatal("expected ACL file env")
	}
	if envMap["KAFSCALE_LOG_LEVEL"] != "debug" {
		t.Fatal("expected log level env")
	}
	if envMap["KAFSCALE_TRACE_KAFKA"] != "1" {
		t.Fatal("expected trace kafka env")
	}

	// CredentialsSecretRef should add envFrom
	if len(container.EnvFrom) != 1 {
		t.Fatalf("expected 1 envFrom, got %d", len(container.EnvFrom))
	}
	if container.EnvFrom[0].SecretRef.Name != "s3-creds" {
		t.Fatalf("expected secretRef s3-creds, got %q", container.EnvFrom[0].SecretRef.Name)
	}

	// Resources should be set
	if container.Resources.Requests.Cpu().String() != "500m" {
		t.Fatalf("expected 500m CPU request, got %s", container.Resources.Requests.Cpu().String())
	}
}

// --- deleteLegacyBrokerDeployment: with existing deployment ---

func TestDeleteLegacyBrokerDeploymentExists(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
	}
	legacy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-broker", Namespace: "default"},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, legacy).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if err := r.deleteLegacyBrokerDeployment(context.Background(), cluster); err != nil {
		t.Fatalf("deleteLegacyBrokerDeployment: %v", err)
	}
	assertNotFound(t, c, &appsv1.Deployment{}, "default", "demo-broker")
}

// --- reconcileLfsProxyResources: enabled with metrics disabled ---

func TestReconcileLfsProxyResourcesMetricsDisabled(t *testing.T) {
	falseVal := false
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			S3: kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1"},
			LfsProxy: kafscalev1alpha1.LfsProxySpec{
				Enabled: true,
				Metrics: kafscalev1alpha1.LfsProxyMetricsSpec{Enabled: &falseVal},
			},
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if err := r.reconcileLfsProxyResources(context.Background(), cluster, []string{"http://etcd:2379"}); err != nil {
		t.Fatalf("reconcileLfsProxyResources: %v", err)
	}
	// Deployment should exist, metrics service should not
	assertFound(t, c, &appsv1.Deployment{}, "default", "demo-lfs-proxy")
	assertNotFound(t, c, &corev1.Service{}, "default", "demo-lfs-proxy-metrics")
}

// --- reconcileEtcdResources: test full pipeline directly ---

func TestReconcileEtcdResourcesFullPipeline(t *testing.T) {
	t.Setenv(operatorEtcdEndpointsEnv, "")
	t.Setenv(operatorEtcdSnapshotBucketEnv, "snap-bucket")
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			S3: kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1"},
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	if err := reconcileEtcdResources(context.Background(), c, scheme, cluster); err != nil {
		t.Fatalf("reconcileEtcdResources: %v", err)
	}

	assertFound(t, c, &appsv1.StatefulSet{}, "default", "demo-etcd")
	assertFound(t, c, &corev1.Service{}, "default", "demo-etcd")
	assertFound(t, c, &corev1.Service{}, "default", "demo-etcd-client")
}

// --- reconcileEtcdStatefulSet: memory storage mode ---

func TestReconcileEtcdStatefulSetMemoryMode(t *testing.T) {
	t.Setenv(operatorEtcdStorageMemoryEnv, "true")
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       kafscalev1alpha1.KafscaleClusterSpec{S3: kafscalev1alpha1.S3Spec{Bucket: "b", Region: "r"}},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	if err := reconcileEtcdStatefulSet(context.Background(), c, scheme, cluster); err != nil {
		t.Fatalf("reconcileEtcdStatefulSet memory mode: %v", err)
	}

	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, "default", "demo-etcd")
	if len(sts.Spec.VolumeClaimTemplates) != 0 {
		t.Fatal("expected no VolumeClaimTemplates in memory mode")
	}
	// Should have an emptyDir volume named "data" with Memory medium
	foundMemData := false
	for _, v := range sts.Spec.Template.Spec.Volumes {
		if v.Name == "data" && v.VolumeSource.EmptyDir != nil && v.VolumeSource.EmptyDir.Medium == corev1.StorageMediumMemory {
			foundMemData = true
		}
	}
	if !foundMemData {
		t.Fatal("expected memory-backed emptyDir volume for data")
	}
}

// --- PublishMetadataSnapshot: with embedded etcd ---

func TestPublishMetadataSnapshotEmptyEndpoints(t *testing.T) {
	err := PublishMetadataSnapshot(context.Background(), nil, metadata.ClusterMetadata{})
	if err == nil || !strings.Contains(err.Error(), "endpoints required") {
		t.Fatalf("expected endpoints required error, got %v", err)
	}
}

func TestPublishMetadataSnapshotHappyPath(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	t.Setenv(operatorEtcdSilenceLogsEnv, "true")

	snap := metadata.ClusterMetadata{
		Brokers:      []protocol.MetadataBroker{{NodeID: 0, Host: "b0", Port: 9092}},
		ControllerID: 0,
		Topics: []protocol.MetadataTopic{
			{Topic: kmsg.StringPtr("orders"), Partitions: []protocol.MetadataPartition{{Partition: 0, Leader: 0}}},
		},
	}

	if err := PublishMetadataSnapshot(context.Background(), endpoints, snap); err != nil {
		t.Fatalf("PublishMetadataSnapshot: %v", err)
	}

	// Verify the snapshot was written
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	defer func() { _ = cli.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := cli.Get(ctx, "/kafscale/metadata/snapshot")
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("snapshot not written to etcd")
	}
	var loaded metadata.ClusterMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &loaded); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if len(loaded.Topics) != 1 || *loaded.Topics[0].Topic != "orders" {
		t.Fatalf("unexpected snapshot: %+v", loaded)
	}
}

func TestPublishMetadataSnapshotMergesExisting(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	t.Setenv(operatorEtcdSilenceLogsEnv, "true")

	// Write an initial snapshot with "events" topic
	initial := metadata.ClusterMetadata{
		Topics: []protocol.MetadataTopic{{Topic: kmsg.StringPtr("events")}},
	}
	if err := PublishMetadataSnapshot(context.Background(), endpoints, initial); err != nil {
		t.Fatalf("initial publish: %v", err)
	}

	// Publish new snapshot with "orders" only; should merge "events" from existing
	next := metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 0, Host: "b0", Port: 9092}},
		Topics:  []protocol.MetadataTopic{{Topic: kmsg.StringPtr("orders")}},
	}
	if err := PublishMetadataSnapshot(context.Background(), endpoints, next); err != nil {
		t.Fatalf("second publish: %v", err)
	}

	// Verify merged result
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	defer func() { _ = cli.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := cli.Get(ctx, "/kafscale/metadata/snapshot")
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	var loaded metadata.ClusterMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &loaded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	names := make(map[string]bool)
	for _, topic := range loaded.Topics {
		names[*topic.Topic] = true
	}
	if !names["orders"] || !names["events"] {
		t.Fatalf("expected orders+events, got %v", loaded.Topics)
	}
}

// --- NewSnapshotPublisher + Publish ---

func TestNewSnapshotPublisher(t *testing.T) {
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	p := NewSnapshotPublisher(c)
	if p == nil || p.Client == nil {
		t.Fatal("expected non-nil publisher")
	}
}

func TestSnapshotPublisherPublish(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	t.Setenv(operatorEtcdSilenceLogsEnv, "true")

	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: types.UID("uid-123")},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{},
			S3:      kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1"},
		},
	}
	topic := &kafscalev1alpha1.KafscaleTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleTopicSpec{
			ClusterRef: "demo",
			Partitions: 3,
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, topic).Build()
	p := NewSnapshotPublisher(c)

	if err := p.Publish(context.Background(), cluster, endpoints); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Verify snapshot was written
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	defer func() { _ = cli.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := cli.Get(ctx, "/kafscale/metadata/snapshot")
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("snapshot not written")
	}
	var loaded metadata.ClusterMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &loaded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(loaded.Topics) != 1 || *loaded.Topics[0].Topic != "orders" {
		t.Fatalf("unexpected topics: %+v", loaded.Topics)
	}
	if len(loaded.Topics[0].Partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(loaded.Topics[0].Partitions))
	}
}

func TestSnapshotPublisherPublishNoMatchingTopics(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	t.Setenv(operatorEtcdSilenceLogsEnv, "true")

	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{},
			S3:      kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1"},
		},
	}
	// Topic belongs to different cluster
	topic := &kafscalev1alpha1.KafscaleTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "events", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleTopicSpec{
			ClusterRef: "other-cluster",
			Partitions: 1,
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, topic).Build()
	p := NewSnapshotPublisher(c)

	if err := p.Publish(context.Background(), cluster, endpoints); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

// --- ClusterReconciler.Reconcile ---

func TestClusterReconcilerReconcile(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	t.Setenv(operatorEtcdEndpointsEnv, endpoints[0])
	t.Setenv(operatorEtcdSnapshotSkipPreflightEnv, "true")
	t.Setenv(operatorEtcdSilenceLogsEnv, "true")

	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{},
			S3:      kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1"},
		},
	}
	scheme := testScheme(t)
	if err := autoscalingv2.AddToScheme(scheme); err != nil {
		t.Fatalf("add autoscaling scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).Build()
	publisher := NewSnapshotPublisher(c)
	r := &ClusterReconciler{Client: c, Scheme: scheme, Publisher: publisher}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got %v", result.RequeueAfter)
	}

	// Verify resources were created
	assertFound(t, c, &appsv1.StatefulSet{}, "default", "demo-broker")
	assertFound(t, c, &corev1.Service{}, "default", "demo-broker-headless")
	assertFound(t, c, &corev1.Service{}, "default", "demo-broker")
}

func TestClusterReconcilerReconcileNotFound(t *testing.T) {
	scheme := testScheme(t)
	if err := autoscalingv2.AddToScheme(scheme); err != nil {
		t.Fatalf("add autoscaling scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	publisher := NewSnapshotPublisher(c)
	r := &ClusterReconciler{Client: c, Scheme: scheme, Publisher: publisher}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile not found: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue for not found, got %v", result.RequeueAfter)
	}
}

// --- TopicReconciler.Reconcile ---

func TestTopicReconcilerReconcile(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	t.Setenv(operatorEtcdEndpointsEnv, endpoints[0])
	t.Setenv(operatorEtcdSilenceLogsEnv, "true")

	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{},
			S3:      kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1"},
		},
	}
	topic := &kafscalev1alpha1.KafscaleTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleTopicSpec{
			ClusterRef: "demo",
			Partitions: 2,
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(topic).WithObjects(cluster, topic).Build()
	publisher := NewSnapshotPublisher(c)
	r := &TopicReconciler{Client: c, Scheme: scheme, Publisher: publisher}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "orders", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got %v", result.RequeueAfter)
	}
}

func TestTopicReconcilerReconcileNotFound(t *testing.T) {
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	publisher := NewSnapshotPublisher(c)
	r := &TopicReconciler{Client: c, Scheme: scheme, Publisher: publisher}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile not found: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got %v", result.RequeueAfter)
	}
}

// --- BuildClusterMetadata with advertised host ---

func TestBuildClusterMetadataSingleReplicaAdvertisedHost(t *testing.T) {
	replicas := int32(1)
	port := int32(19092)
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: types.UID("test-uid")},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{
				Replicas:       &replicas,
				AdvertisedHost: "my.custom.host",
				AdvertisedPort: &port,
			},
		},
	}
	topics := []kafscalev1alpha1.KafscaleTopic{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "orders"},
			Spec:       kafscalev1alpha1.KafscaleTopicSpec{Partitions: 2},
		},
	}
	meta := BuildClusterMetadata(cluster, topics)
	if len(meta.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(meta.Brokers))
	}
	if meta.Brokers[0].Host != "my.custom.host" {
		t.Fatalf("expected custom host, got %q", meta.Brokers[0].Host)
	}
	if meta.Brokers[0].Port != 19092 {
		t.Fatalf("expected port 19092, got %d", meta.Brokers[0].Port)
	}
	if meta.ClusterID == nil || *meta.ClusterID != "test-uid" {
		t.Fatal("expected cluster ID set")
	}
	if meta.ClusterName == nil || *meta.ClusterName != "demo" {
		t.Fatal("expected cluster name set")
	}
}

// --- lfsProxyContainer with all optional branches ---

func TestLfsProxyContainerAllOptions(t *testing.T) {
	trueVal := true
	advPort := int32(19093)
	cacheTTL := int32(600)
	maxBlob := int64(1048576)
	chunkSize := int64(65536)
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			S3: kafscalev1alpha1.S3Spec{
				Bucket:               "bucket",
				Region:               "us-east-1",
				Endpoint:             "http://minio:9000",
				CredentialsSecretRef: "s3-creds",
			},
			LfsProxy: kafscalev1alpha1.LfsProxySpec{
				Enabled:                true,
				Image:                  "custom-lfs:latest",
				ImagePullPolicy:        "Always",
				AdvertisedHost:         "lfs.example.com",
				AdvertisedPort:         &advPort,
				BackendCacheTTLSeconds: &cacheTTL,
				Backends:               []string{"backend1:9092", "backend2:9092"},
				HTTP: kafscalev1alpha1.LfsProxyHTTPSpec{
					Enabled:         &trueVal,
					APIKeySecretRef: "api-key-secret",
					APIKeySecretKey: "MY_KEY",
				},
				S3: kafscalev1alpha1.LfsProxyS3Spec{
					ForcePathStyle: &trueVal,
					EnsureBucket:   &trueVal,
					MaxBlobSize:    &maxBlob,
					ChunkSize:      &chunkSize,
				},
			},
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	container := r.lfsProxyContainer(cluster, []string{"http://etcd:2379"})

	if container.Image != "custom-lfs:latest" {
		t.Fatalf("expected custom image, got %q", container.Image)
	}
	if container.ImagePullPolicy != corev1.PullAlways {
		t.Fatalf("expected Always pull, got %v", container.ImagePullPolicy)
	}

	envMap := make(map[string]string)
	for _, e := range container.Env {
		envMap[e.Name] = e.Value
	}
	if envMap["KAFSCALE_LFS_PROXY_ADVERTISED_HOST"] != "lfs.example.com" {
		t.Fatal("expected advertised host")
	}
	if envMap["KAFSCALE_LFS_PROXY_ADVERTISED_PORT"] != "19093" {
		t.Fatal("expected advertised port")
	}
	if envMap["KAFSCALE_LFS_PROXY_BACKEND_CACHE_TTL_SEC"] != "600" {
		t.Fatal("expected backend cache TTL")
	}
	if envMap["KAFSCALE_LFS_PROXY_BACKENDS"] != "backend1:9092,backend2:9092" {
		t.Fatal("expected backends")
	}
	if envMap["KAFSCALE_LFS_PROXY_S3_ENDPOINT"] != "http://minio:9000" {
		t.Fatal("expected S3 endpoint")
	}
	if envMap["KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE"] != "true" {
		t.Fatal("expected force path style")
	}
	if envMap["KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET"] != "true" {
		t.Fatal("expected ensure bucket")
	}
	if envMap["KAFSCALE_LFS_PROXY_MAX_BLOB_SIZE"] != "1048576" {
		t.Fatal("expected max blob size")
	}
	if envMap["KAFSCALE_LFS_PROXY_CHUNK_SIZE"] != "65536" {
		t.Fatal("expected chunk size")
	}
	// HTTP port should be present
	if envMap["KAFSCALE_LFS_PROXY_HTTP_ADDR"] == "" {
		t.Fatal("expected HTTP addr env")
	}
	// API key should be from secret
	foundAPIKey := false
	for _, e := range container.Env {
		if e.Name == "KAFSCALE_LFS_PROXY_HTTP_API_KEY" && e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
			if e.ValueFrom.SecretKeyRef.Key == "MY_KEY" {
				foundAPIKey = true
			}
		}
	}
	if !foundAPIKey {
		t.Fatal("expected API key secret ref with custom key")
	}
	// S3 credentials should be from secret
	foundS3Key := false
	for _, e := range container.Env {
		if e.Name == "KAFSCALE_LFS_PROXY_S3_ACCESS_KEY" && e.ValueFrom != nil {
			foundS3Key = true
		}
	}
	if !foundS3Key {
		t.Fatal("expected S3 credentials from secret ref")
	}
	// HTTP and metrics ports should exist in container ports
	if len(container.Ports) < 3 {
		t.Fatalf("expected at least 3 ports (kafka, http, health), got %d", len(container.Ports))
	}
}
