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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// Exercise the actual reconcilers, including updates of existing pod templates,
// rather than testing the helpers in isolation.
func TestOperatorRestrictedWorkloads(t *testing.T) {
	for _, memory := range []string{"false", "true"} {
		t.Run("memory="+memory, func(t *testing.T) {
			t.Setenv(operatorEtcdStorageMemoryEnv, memory)
			t.Setenv(operatorEtcdSnapshotBucketEnv, "snapshots")
			cluster := testCluster("restricted", nil)
			scheme := testScheme(t)
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			r := &ClusterReconciler{Client: c, Scheme: scheme}
			reconcile := func() {
				t.Helper()
				ctx := context.Background()
				for _, err := range []error{
					r.reconcileBrokerDeployment(ctx, cluster, []string{"http://etcd:2379"}),
					reconcileEtcdStatefulSet(ctx, c, scheme, cluster),
					reconcileEtcdSnapshotCronJob(ctx, c, scheme, cluster),
					reconcileEtcdMaintenanceCronJobs(ctx, c, scheme, cluster),
				} {
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			reconcile()
			// Simulate pre-hardening resources. The next reconciliation must repair
			// both regular and init containers, not only newly created workloads.
			var sets appsv1.StatefulSetList
			if err := c.List(context.Background(), &sets); err != nil {
				t.Fatal(err)
			}
			for i := range sets.Items {
				pod := &sets.Items[i].Spec.Template.Spec
				pod.SecurityContext = nil
				for j := range pod.Containers {
					pod.Containers[j].SecurityContext = nil
				}
				for j := range pod.InitContainers {
					pod.InitContainers[j].SecurityContext = nil
				}
				if err := c.Update(context.Background(), &sets.Items[i]); err != nil {
					t.Fatal(err)
				}
			}
			var jobs batchv1.CronJobList
			if err := c.List(context.Background(), &jobs); err != nil {
				t.Fatal(err)
			}
			for i := range jobs.Items {
				pod := &jobs.Items[i].Spec.JobTemplate.Spec.Template.Spec
				pod.SecurityContext = nil
				for j := range pod.Containers {
					pod.Containers[j].SecurityContext = nil
				}
				for j := range pod.InitContainers {
					pod.InitContainers[j].SecurityContext = nil
				}
				if err := c.Update(context.Background(), &jobs.Items[i]); err != nil {
					t.Fatal(err)
				}
			}
			reconcile()
			if err := c.List(context.Background(), &sets); err != nil {
				t.Fatal(err)
			}
			if err := c.List(context.Background(), &jobs); err != nil {
				t.Fatal(err)
			}
			if len(sets.Items) != 2 || len(jobs.Items) != 3 {
				t.Fatalf("missing workloads: %d StatefulSets, %d CronJobs", len(sets.Items), len(jobs.Items))
			}
			for _, sts := range sets.Items {
				assertRestrictedPod(t, sts.Name, sts.Spec.Template.Spec)
			}
			for _, job := range jobs.Items {
				assertRestrictedPod(t, job.Name, job.Spec.JobTemplate.Spec.Template.Spec)
			}
		})
	}
}

func assertRestrictedPod(t *testing.T, name string, pod corev1.PodSpec) {
	t.Helper()
	p := pod.SecurityContext
	if p == nil || p.RunAsNonRoot == nil || !*p.RunAsNonRoot || p.RunAsUser == nil || *p.RunAsUser != 10001 || p.RunAsGroup == nil || *p.RunAsGroup != 10001 || p.SeccompProfile == nil || p.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("%s: invalid pod security context: %+v", name, p)
	}
	for _, v := range pod.Volumes {
		if v.EmptyDir != nil || v.PersistentVolumeClaim != nil {
			if p.FSGroup == nil || *p.FSGroup != 10001 || p.FSGroupChangePolicy == nil || *p.FSGroupChangePolicy != corev1.FSGroupChangeAlways {
				t.Fatalf("%s: missing writable volume ownership", name)
			}
		}
	}
	for _, c := range append(append([]corev1.Container{}, pod.InitContainers...), pod.Containers...) {
		s := c.SecurityContext
		if s == nil || s.AllowPrivilegeEscalation == nil || *s.AllowPrivilegeEscalation || s.ReadOnlyRootFilesystem == nil || !*s.ReadOnlyRootFilesystem || s.Capabilities == nil || len(s.Capabilities.Drop) != 1 || s.Capabilities.Drop[0] != "ALL" || len(s.Capabilities.Add) != 0 {
			t.Fatalf("%s/%s: invalid container security context: %+v", name, c.Name, s)
		}
		for _, mount := range c.VolumeMounts {
			switch mount.MountPath {
			case "/var/lib/etcd", "/snapshots":
			case "/tmp":
				if c.Name != "snapshot-download" && c.Name != "upload" && c.Name != "lfs-proxy" {
					t.Fatalf("%s/%s: unnecessary /tmp mount", name, c.Name)
				}
			default:
				t.Fatalf("%s/%s: unexpected writable path %s", name, c.Name, mount.MountPath)
			}
		}
		if c.Name == "snapshot-download" || c.Name == "upload" {
			found := false
			for _, m := range c.VolumeMounts {
				if m.MountPath == "/tmp" && !m.ReadOnly {
					found = true
				}
			}
			home := false
			for _, e := range c.Env {
				if e.Name == "HOME" && e.Value == "/tmp" {
					home = true
				}
			}
			if !found || !home || c.WorkingDir != "/tmp" {
				t.Fatalf("%s/%s: missing AWS scratch space", name, c.Name)
			}
		}
	}
}

func TestOperatorRestrictedLfsProxy(t *testing.T) {
	cluster := testCluster("restricted-proxy", nil)
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	for i := 0; i < 2; i++ {
		if err := r.reconcileLfsProxyDeployment(context.Background(), cluster, []string{"http://etcd:2379"}); err != nil {
			t.Fatal(err)
		}
		var deploy appsv1.Deployment
		assertFound(t, c, &deploy, cluster.Namespace, lfsProxyName(cluster))
		assertRestrictedPod(t, deploy.Name, deploy.Spec.Template.Spec)
		found := false
		for _, mount := range deploy.Spec.Template.Spec.Containers[0].VolumeMounts {
			if mount.MountPath == "/tmp" && !mount.ReadOnly {
				found = true
			}
		}
		if !found {
			t.Fatal("LFS verification requires writable /tmp")
		}
		deploy.Spec.Template.Spec.SecurityContext = nil
		deploy.Spec.Template.Spec.Containers[0].SecurityContext = nil
		deploy.Spec.Template.Spec.Volumes = nil
		deploy.Spec.Template.Spec.Containers[0].VolumeMounts = nil
		if err := c.Update(context.Background(), &deploy); err != nil {
			t.Fatal(err)
		}
	}
}
