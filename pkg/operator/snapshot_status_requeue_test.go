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
	"time"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
)

func snapshotStatusFor(t *testing.T, cron *batchv1.CronJob, managed bool) (*kafscalev1alpha1.KafscaleCluster, time.Duration) {
	t.Helper()
	cluster := &kafscalev1alpha1.KafscaleCluster{ObjectMeta: metav1.ObjectMeta{Name: "requeue", Namespace: "default"}}
	scheme := testScheme(t)
	builder := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster)
	if cron != nil {
		builder = builder.WithObjects(cron)
	}
	r := &ClusterReconciler{Client: builder.Build(), Scheme: scheme}
	recheck := r.populateEtcdSnapshotStatus(context.Background(), cluster, EtcdResolution{Managed: managed})
	return cluster, recheck
}

func snapshotCron(lastSuccess *metav1.Time) *batchv1.CronJob {
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "requeue-etcd-snapshot", Namespace: "default"},
		Status:     batchv1.CronJobStatus{LastSuccessfulTime: lastSuccess},
	}
}

func snapshotGauge(t *testing.T, name, cluster string) float64 {
	t.Helper()
	families, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "cluster" && label.GetValue() == cluster {
					return metric.GetGauge().GetValue()
				}
			}
		}
	}
	t.Fatalf("metric %s{cluster=%q} not found", name, cluster)
	return 0
}

func TestEtcdSnapshotStatusRechecksAtStaleDeadline(t *testing.T) {
	t.Setenv(operatorEtcdSnapshotStaleAfterEnv, "600")
	lastSuccess := metav1.NewTime(time.Now().Add(-100 * time.Second))

	cluster, recheck := snapshotStatusFor(t, snapshotCron(&lastSuccess), true)

	cond := meta.FindStatusCondition(cluster.Status.Conditions, "EtcdSnapshot")
	if cond == nil || cond.Reason != "SnapshotHealthy" {
		t.Fatalf("EtcdSnapshot condition = %+v, want SnapshotHealthy", cond)
	}
	if recheck <= 0 || recheck > time.Minute {
		t.Fatalf("recheck = %s, want at most 1m so the age metric stays current", recheck)
	}

	passed := metav1.NewTime(time.Now().Add(-601 * time.Second))
	cluster, recheck = snapshotStatusFor(t, snapshotCron(&passed), true)
	cond = meta.FindStatusCondition(cluster.Status.Conditions, "EtcdSnapshot")
	if cond == nil || cond.Reason != "SnapshotStale" || cond.Status != metav1.ConditionFalse {
		t.Fatalf("EtcdSnapshot condition at the deadline = %+v, want SnapshotStale", cond)
	}
	if got := snapshotGauge(t, "kafscale_operator_etcd_snapshot_stale", "default/requeue"); got != 1 {
		t.Fatalf("kafscale_operator_etcd_snapshot_stale = %v at the deadline, want 1", got)
	}
	if recheck != 0 {
		t.Fatalf("recheck for a stale snapshot = %s, want 0", recheck)
	}
}

func TestEtcdSnapshotStatusNoRecheckWithoutDeadline(t *testing.T) {
	cases := []struct {
		name    string
		cron    *batchv1.CronJob
		managed bool
		reason  string
	}{
		{name: "not managed", managed: false, reason: "SnapshotNotManaged"},
		{name: "cron missing", managed: true, reason: "SnapshotCronMissing"},
		{name: "never succeeded", cron: snapshotCron(nil), managed: true, reason: "SnapshotNeverSucceeded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster, recheck := snapshotStatusFor(t, tc.cron, tc.managed)
			cond := meta.FindStatusCondition(cluster.Status.Conditions, "EtcdSnapshot")
			if cond == nil || cond.Reason != tc.reason {
				t.Fatalf("EtcdSnapshot condition = %+v, want %s", cond, tc.reason)
			}
			if recheck != 0 {
				t.Fatalf("recheck = %s, want 0", recheck)
			}
		})
	}
}

func TestEtcdSnapshotStatusRefreshesAgeWhileHealthy(t *testing.T) {
	t.Setenv(operatorEtcdSnapshotStaleAfterEnv, "600")

	lastSuccess := metav1.NewTime(time.Now().Add(-100 * time.Second))
	_, recheck := snapshotStatusFor(t, snapshotCron(&lastSuccess), true)
	if recheck <= 0 || recheck > time.Minute {
		t.Fatalf("recheck = %s, want at most 1m while the snapshot is healthy", recheck)
	}
	age := snapshotGauge(t, "kafscale_operator_etcd_snapshot_age_seconds", "default/requeue")
	if age < 99 || age > 110 {
		t.Fatalf("age = %v, want about 100s", age)
	}

	nearDeadline := metav1.NewTime(time.Now().Add(-570 * time.Second))
	_, recheck = snapshotStatusFor(t, snapshotCron(&nearDeadline), true)
	if recheck <= 20*time.Second || recheck > 32*time.Second {
		t.Fatalf("recheck = %s, want the remaining time to the stale deadline, about 31s", recheck)
	}
}
