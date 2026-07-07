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
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
)

func TestEnsureEtcdUsesSpecEndpoints(t *testing.T) {
	t.Setenv(operatorEtcdEndpointsEnv, "http://env-etcd:2379")
	cluster := testCluster("spec-endpoints", []string{"http://spec-etcd:2379"})
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	res, err := EnsureEtcd(context.Background(), c, scheme, cluster)
	if err != nil {
		t.Fatalf("EnsureEtcd: %v", err)
	}
	if res.Managed {
		t.Fatalf("expected unmanaged etcd resolution")
	}
	if len(res.Endpoints) != 1 || res.Endpoints[0] != "http://spec-etcd:2379" {
		t.Fatalf("unexpected endpoints: %v", res.Endpoints)
	}

	assertNotFound(t, c, &appsv1.StatefulSet{}, cluster.Namespace, cluster.Name+"-etcd")
}

func TestEnsureEtcdUsesEnvEndpoints(t *testing.T) {
	t.Setenv(operatorEtcdEndpointsEnv, "http://env-etcd:2379")
	cluster := testCluster("env-endpoints", nil)
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	res, err := EnsureEtcd(context.Background(), c, scheme, cluster)
	if err != nil {
		t.Fatalf("EnsureEtcd: %v", err)
	}
	if res.Managed {
		t.Fatalf("expected unmanaged etcd resolution")
	}
	if len(res.Endpoints) != 1 || res.Endpoints[0] != "http://env-etcd:2379" {
		t.Fatalf("unexpected endpoints: %v", res.Endpoints)
	}

	assertNotFound(t, c, &appsv1.StatefulSet{}, cluster.Namespace, cluster.Name+"-etcd")
}

func TestEnsureEtcdCreatesManagedCluster(t *testing.T) {
	t.Setenv(operatorEtcdEndpointsEnv, "")
	cluster := testCluster("managed", nil)
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	res, err := EnsureEtcd(context.Background(), c, scheme, cluster)
	if err != nil {
		t.Fatalf("EnsureEtcd: %v", err)
	}
	if !res.Managed {
		t.Fatalf("expected managed etcd resolution")
	}
	if len(res.Endpoints) != 1 {
		t.Fatalf("unexpected endpoints: %v", res.Endpoints)
	}

	assertFound(t, c, &appsv1.StatefulSet{}, cluster.Namespace, cluster.Name+"-etcd")
	assertFound(t, c, &corev1.Service{}, cluster.Namespace, cluster.Name+"-etcd")
	assertFound(t, c, &corev1.Service{}, cluster.Namespace, cluster.Name+"-etcd-client")
	assertFound(t, c, &policyv1.PodDisruptionBudget{}, cluster.Namespace, cluster.Name+"-etcd")
	assertFound(t, c, &batchv1.CronJob{}, cluster.Namespace, cluster.Name+"-etcd-snapshot")
	assertFound(t, c, &batchv1.CronJob{}, cluster.Namespace, cluster.Name+"-etcd-maintenance")
	assertFound(t, c, &batchv1.CronJob{}, cluster.Namespace, cluster.Name+"-etcd-maintenance-check")

	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, cluster.Namespace, cluster.Name+"-etcd")
	if len(sts.Spec.Template.Spec.InitContainers) == 0 {
		t.Fatalf("expected snapshot restore init containers")
	}
}

func TestEnsureEtcdEnvOverrides(t *testing.T) {
	t.Setenv(operatorEtcdEndpointsEnv, "")
	t.Setenv(operatorEtcdImageEnv, "etcd:test")
	t.Setenv(operatorEtcdStorageEnv, "20Gi")
	t.Setenv(operatorEtcdClassEnv, "fast")
	t.Setenv(operatorEtcdSnapshotBucketEnv, "snap-bucket")
	t.Setenv(operatorEtcdSnapshotPrefixEnv, "snap-prefix")
	t.Setenv(operatorEtcdSnapshotScheduleEnv, "*/5 * * * *")
	t.Setenv(operatorEtcdSnapshotEtcdctlEnv, "etcdctl:test")
	t.Setenv(operatorEtcdSnapshotImageEnv, "awscli:test")
	t.Setenv(operatorEtcdSnapshotEndpointEnv, "http://minio.local")
	t.Setenv(operatorEtcdSnapshotCreateBucketEnv, "1")
	t.Setenv(operatorEtcdSnapshotProtectBucketEnv, "1")

	cluster := testCluster("override", nil)
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	_, err := EnsureEtcd(context.Background(), c, scheme, cluster)
	if err != nil {
		t.Fatalf("EnsureEtcd: %v", err)
	}

	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, cluster.Namespace, cluster.Name+"-etcd")
	if len(sts.Spec.Template.Spec.Containers) == 0 || sts.Spec.Template.Spec.Containers[0].Image != "etcd:test" {
		t.Fatalf("expected etcd image override, got %+v", sts.Spec.Template.Spec.Containers)
	}
	if len(sts.Spec.Template.Spec.InitContainers) < 2 {
		t.Fatalf("expected snapshot restore init containers, got %+v", sts.Spec.Template.Spec.InitContainers)
	}
	if sts.Spec.Template.Spec.InitContainers[0].Image != "awscli:test" {
		t.Fatalf("expected restore download image override, got %+v", sts.Spec.Template.Spec.InitContainers)
	}
	if sts.Spec.Template.Spec.InitContainers[1].Image != "etcdctl:test" {
		t.Fatalf("expected restore etcdctl image override, got %+v", sts.Spec.Template.Spec.InitContainers)
	}
	if len(sts.Spec.VolumeClaimTemplates) == 0 {
		t.Fatalf("expected pvc template")
	}
	if got := sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "20Gi" {
		t.Fatalf("expected storage size 20Gi, got %s", got.String())
	}
	if sts.Spec.VolumeClaimTemplates[0].Spec.StorageClassName == nil || *sts.Spec.VolumeClaimTemplates[0].Spec.StorageClassName != "fast" {
		t.Fatalf("expected storage class fast, got %+v", sts.Spec.VolumeClaimTemplates[0].Spec.StorageClassName)
	}

	cron := &batchv1.CronJob{}
	assertFound(t, c, cron, cluster.Namespace, cluster.Name+"-etcd-snapshot")
	if cron.Spec.Schedule != "*/5 * * * *" {
		t.Fatalf("expected schedule override, got %q", cron.Spec.Schedule)
	}
	if len(cron.Spec.JobTemplate.Spec.Template.Spec.InitContainers) == 0 || cron.Spec.JobTemplate.Spec.Template.Spec.InitContainers[0].Image != "etcdctl:test" {
		t.Fatalf("expected etcdctl image override, got %+v", cron.Spec.JobTemplate.Spec.Template.Spec.InitContainers)
	}
	if len(cron.Spec.JobTemplate.Spec.Template.Spec.Containers) == 0 || cron.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image != "awscli:test" {
		t.Fatalf("expected snapshot image override, got %+v", cron.Spec.JobTemplate.Spec.Template.Spec.Containers)
	}
	env := cron.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env
	if got := envValue(env, "SNAPSHOT_BUCKET"); got != "snap-bucket" {
		t.Fatalf("expected snapshot bucket override, got %q", got)
	}
	if got := envValue(env, "SNAPSHOT_PREFIX"); got != "snap-prefix" {
		t.Fatalf("expected snapshot prefix override, got %q", got)
	}
	if got := envValue(env, "AWS_ENDPOINT_URL"); got != "http://minio.local" {
		t.Fatalf("expected endpoint override, got %q", got)
	}
	if got := envValue(env, "CREATE_BUCKET"); got != "1" {
		t.Fatalf("expected create bucket enabled, got %q", got)
	}
	if got := envValue(env, "PROTECT_BUCKET"); got != "1" {
		t.Fatalf("expected protect bucket enabled, got %q", got)
	}
}

func TestSnapshotStaleAfterEnv(t *testing.T) {
	t.Setenv(operatorEtcdSnapshotStaleAfterEnv, "900")
	if got := snapshotStaleAfterSeconds(); got != 900 {
		t.Fatalf("expected stale after 900, got %d", got)
	}
	t.Setenv(operatorEtcdSnapshotStaleAfterEnv, "0")
	if got := snapshotStaleAfterSeconds(); got != defaultSnapshotStaleAfterSeconds {
		t.Fatalf("expected default stale after, got %d", got)
	}
}

// argValue returns the value of a `--flag=value` argument, or "" if the flag
// is not present in args.
func argValue(args []string, flag string) string {
	prefix := flag + "="
	for _, a := range args {
		if strings.HasPrefix(a, prefix) {
			return strings.TrimPrefix(a, prefix)
		}
	}
	return ""
}

func TestEtcdArgsSpaceManagement(t *testing.T) {
	cluster := testCluster("compaction", nil)

	cases := []struct {
		name          string
		env           map[string]string
		wantMode      string
		wantRetention string
		wantQuota     string
	}{
		{
			name:          "defaults",
			env:           nil,
			wantMode:      "periodic",
			wantRetention: "5m",
			wantQuota:     "4294967296", // 4 GiB
		},
		{
			name: "env overrides",
			env: map[string]string{
				operatorEtcdAutoCompactionModeEnv:      "revision",
				operatorEtcdAutoCompactionRetentionEnv: "10000",
				operatorEtcdQuotaBackendBytesEnv:       "8589934592", // 8 GiB
			},
			wantMode:      "revision",
			wantRetention: "10000",
			wantQuota:     "8589934592",
		},
		{
			name: "garbage falls back to defaults",
			env: map[string]string{
				operatorEtcdAutoCompactionModeEnv:      "bogus",
				operatorEtcdAutoCompactionRetentionEnv: "", // empty -> default
				operatorEtcdQuotaBackendBytesEnv:       "-1",
			},
			wantMode:      "periodic",
			wantRetention: "5m",
			wantQuota:     "4294967296",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			args := etcdArgs(cluster)
			if got := argValue(args, "--auto-compaction-mode"); got != tc.wantMode {
				t.Errorf("auto-compaction-mode = %q, want %q", got, tc.wantMode)
			}
			if got := argValue(args, "--auto-compaction-retention"); got != tc.wantRetention {
				t.Errorf("auto-compaction-retention = %q, want %q", got, tc.wantRetention)
			}
			if got := argValue(args, "--quota-backend-bytes"); got != tc.wantQuota {
				t.Errorf("quota-backend-bytes = %q, want %q", got, tc.wantQuota)
			}
		})
	}
}

func TestEtcdMemoryModeSetsContainerMemoryLimit(t *testing.T) {
	t.Setenv(operatorEtcdEndpointsEnv, "")
	t.Setenv(operatorEtcdStorageMemoryEnv, "true")
	cluster := testCluster("mem-guard", nil)
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	if _, err := EnsureEtcd(context.Background(), c, scheme, cluster); err != nil {
		t.Fatalf("EnsureEtcd: %v", err)
	}

	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, cluster.Namespace, cluster.Name+"-etcd")
	if len(sts.Spec.VolumeClaimTemplates) != 0 {
		t.Fatalf("memory mode should not declare a PVC template, got %d", len(sts.Spec.VolumeClaimTemplates))
	}
	if len(sts.Spec.Template.Spec.Containers) == 0 {
		t.Fatalf("expected etcd container")
	}
	res := sts.Spec.Template.Spec.Containers[0].Resources
	memReq, okReq := res.Requests[corev1.ResourceMemory]
	memLim, okLim := res.Limits[corev1.ResourceMemory]
	if !okReq || !okLim {
		t.Fatalf("memory mode must set memory request and limit, got requests=%v limits=%v", res.Requests, res.Limits)
	}
	// request == quota (4 GiB), limit == quota + 512 MiB headroom.
	if got := memReq.Value(); got != defaultEtcdQuotaBackendBytes {
		t.Errorf("memory request = %d, want %d", got, defaultEtcdQuotaBackendBytes)
	}
	if got := memLim.Value(); got != defaultEtcdQuotaBackendBytes+(512*1024*1024) {
		t.Errorf("memory limit = %d, want %d", got, defaultEtcdQuotaBackendBytes+(512*1024*1024))
	}
}

func TestEtcdMaintenanceCronJobs(t *testing.T) {
	t.Setenv(operatorEtcdEndpointsEnv, "")
	t.Setenv(operatorEtcdMaintenanceScheduleEnv, "15 4 * * *")
	t.Setenv(operatorEtcdMaintenanceCheckScheduleEnv, "*/10 * * * *")
	cluster := testCluster("maintenance", nil)
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	if _, err := EnsureEtcd(context.Background(), c, scheme, cluster); err != nil {
		t.Fatalf("EnsureEtcd: %v", err)
	}

	cron := &batchv1.CronJob{}
	assertFound(t, c, cron, cluster.Namespace, cluster.Name+"-etcd-maintenance")
	if cron.Spec.Schedule != "15 4 * * *" {
		t.Fatalf("maintenance schedule = %q, want %q", cron.Spec.Schedule, "15 4 * * *")
	}
	if got := cron.Labels["kafscale.io/etcd-task"]; got != "maintenance" {
		t.Fatalf("maintenance task label = %q, want maintenance", got)
	}
	maintenance := cron.Spec.JobTemplate.Spec.Template.Spec.Containers[0]
	if got := envValue(maintenance.Env, "CHECK_ONLY"); got != "0" {
		t.Fatalf("CHECK_ONLY = %q, want 0", got)
	}
	script := strings.Join(maintenance.Args, "\n")
	if !strings.Contains(script, "endpoint defrag") || !strings.Contains(script, "alarm disarm") {
		t.Fatalf("maintenance script missing defrag/disarm: %s", script)
	}

	check := &batchv1.CronJob{}
	assertFound(t, c, check, cluster.Namespace, cluster.Name+"-etcd-maintenance-check")
	if check.Spec.Schedule != "*/10 * * * *" {
		t.Fatalf("check schedule = %q, want %q", check.Spec.Schedule, "*/10 * * * *")
	}
	if got := envValue(check.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env, "CHECK_ONLY"); got != "1" {
		t.Fatalf("checker CHECK_ONLY = %q, want 1", got)
	}
}

func TestEtcdMaintenanceDisabled(t *testing.T) {
	t.Setenv(operatorEtcdEndpointsEnv, "")
	t.Setenv(operatorEtcdMaintenanceEnabledEnv, "0")
	cluster := testCluster("maintenance-off", nil)
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	if _, err := EnsureEtcd(context.Background(), c, scheme, cluster); err != nil {
		t.Fatalf("EnsureEtcd: %v", err)
	}

	assertNotFound(t, c, &batchv1.CronJob{}, cluster.Namespace, cluster.Name+"-etcd-maintenance")
	assertNotFound(t, c, &batchv1.CronJob{}, cluster.Namespace, cluster.Name+"-etcd-maintenance-check")
}

func TestEtcdMaintenanceSizeThresholdPct(t *testing.T) {
	t.Setenv(operatorEtcdMaintenanceSizeThresholdPctEnv, "80")
	if got := etcdMaintenanceSizeThresholdPct(); got != 80 {
		t.Fatalf("threshold pct = %d, want 80", got)
	}
	if got := etcdMaintenanceSizeThresholdBytes(1000); got != 800 {
		t.Fatalf("threshold bytes = %d, want 800", got)
	}
	t.Setenv(operatorEtcdMaintenanceSizeThresholdPctEnv, "0")
	if got := etcdMaintenanceSizeThresholdPct(); got != defaultEtcdMaintenanceSizeThresholdPct {
		t.Fatalf("expected default threshold pct, got %d", got)
	}
}

func TestEtcdDiskModeHasNoContainerMemoryLimit(t *testing.T) {
	t.Setenv(operatorEtcdEndpointsEnv, "")
	// storage-memory mode explicitly off (the default); disk-backed PVC path.
	cluster := testCluster("disk-mode", nil)
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()

	if _, err := EnsureEtcd(context.Background(), c, scheme, cluster); err != nil {
		t.Fatalf("EnsureEtcd: %v", err)
	}

	sts := &appsv1.StatefulSet{}
	assertFound(t, c, sts, cluster.Namespace, cluster.Name+"-etcd")
	if len(sts.Spec.VolumeClaimTemplates) == 0 {
		t.Fatalf("disk mode must declare a PVC template")
	}
	res := sts.Spec.Template.Spec.Containers[0].Resources
	if len(res.Requests) != 0 || len(res.Limits) != 0 {
		t.Fatalf("disk mode must not set container resources, got requests=%v limits=%v", res.Requests, res.Limits)
	}
}

func envValue(env []corev1.EnvVar, key string) string {
	for _, entry := range env {
		if entry.Name == key {
			return entry.Value
		}
	}
	return ""
}

func testCluster(name string, endpoints []string) *kafscalev1alpha1.KafscaleCluster {
	return &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{},
			S3: kafscalev1alpha1.S3Spec{
				Bucket: "bucket",
				Region: "us-east-1",
			},
			Etcd: kafscalev1alpha1.EtcdSpec{Endpoints: endpoints},
		},
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := kafscalev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add kafscale scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := policyv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add policy scheme: %v", err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batch scheme: %v", err)
	}
	return scheme
}

func assertFound(t *testing.T, c client.Client, obj client.Object, ns, name string) {
	t.Helper()
	key := client.ObjectKey{Namespace: ns, Name: name}
	if err := c.Get(context.Background(), key, obj); err != nil {
		t.Fatalf("expected %T %s/%s to exist: %v", obj, ns, name, err)
	}
}

func assertNotFound(t *testing.T, c client.Client, obj client.Object, ns, name string) {
	t.Helper()
	key := client.ObjectKey{Namespace: ns, Name: name}
	if err := c.Get(context.Background(), key, obj); err == nil {
		t.Fatalf("expected %T %s/%s to be absent", obj, ns, name)
	}
}
