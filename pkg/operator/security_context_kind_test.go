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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestRestrictedWorkloadsKind runs reconciled templates with the shipped images
// on an explicitly selected kind context. It needs broker, etcd-tools and
// e2e-client images loaded into kind (see the Operator Restricted Workloads CI job).
func TestRestrictedWorkloadsKind(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E_KIND_SECURITY") != "1" {
		t.Skip("set KAFSCALE_E2E_KIND_SECURITY=1 to run restricted workload integration")
	}
	kubeContext := os.Getenv("KAFSCALE_SECURITY_CONTEXT")
	if !strings.HasPrefix(kubeContext, "kind-") {
		t.Fatal("KAFSCALE_SECURITY_CONTEXT must explicitly select a kind context")
	}
	namespace := fmt.Sprintf("restricted-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	kubectl := func(input string, args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, "kubectl", append([]string{"--context", kubeContext, "-n", namespace}, args...)...)
		cmd.Stdin = strings.NewReader(input)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("kubectl %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	kubectl("", "create", "namespace", namespace)
	t.Cleanup(func() {
		if t.Failed() {
			for _, args := range [][]string{{"get", "pods", "-o", "wide"}, {"get", "events", "--sort-by=.lastTimestamp"}, {"logs", "sts/security-etcd", "--all-containers=true", "--tail=100"}} {
				out, _ := exec.Command("kubectl", append([]string{"--context", kubeContext, "-n", namespace}, args...)...).CombinedOutput()
				t.Logf("%v\n%s", args, out)
			}
		}
		_ = exec.Command("kubectl", "--context", kubeContext, "delete", "namespace", namespace, "--wait=false").Run()
	})
	apply := func(obj client.Object) {
		t.Helper()
		obj.SetOwnerReferences(nil) // The fixture has no running CR/controller.
		obj.SetResourceVersion("")
		data, err := json.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}
		kubectl(string(data), "apply", "-f", "-")
	}
	replicas := int32(1)
	cluster := testCluster("security", nil)
	cluster.Namespace = namespace
	cluster.Spec.Brokers.Replicas = &replicas
	cluster.Spec.S3.Bucket = "snapshots"
	cluster.Spec.S3.Endpoint = "http://minio:9000"
	cluster.Spec.S3.CredentialsSecretRef = "s3"
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	t.Setenv(operatorEtcdReplicasEnv, "1")
	t.Setenv(operatorEtcdStorageMemoryEnv, "false")
	t.Setenv(operatorEtcdSnapshotBucketEnv, "snapshots")
	t.Setenv(operatorEtcdSnapshotCreateBucketEnv, "1")
	t.Setenv(operatorEtcdSnapshotProtectBucketEnv, "0")
	t.Setenv(operatorEtcdSnapshotEtcdctlEnv, "kafscale-etcd-tools:security")
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	for _, err := range []error{
		reconcileEtcdHeadlessService(ctx, c, scheme, cluster),
		reconcileEtcdClientService(ctx, c, scheme, cluster),
		reconcileEtcdStatefulSet(ctx, c, scheme, cluster),
		reconcileEtcdSnapshotCronJob(ctx, c, scheme, cluster),
		reconcileEtcdMaintenanceCronJobs(ctx, c, scheme, cluster),
		r.reconcileBrokerHeadlessService(ctx, cluster),
		r.reconcileBrokerService(ctx, cluster),
		r.reconcileBrokerDeployment(ctx, cluster, managedEtcdEndpoints(cluster)),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	var services corev1.ServiceList
	if err := c.List(ctx, &services); err != nil {
		t.Fatal(err)
	}
	for i := range services.Items {
		services.Items[i].APIVersion = "v1"
		services.Items[i].Kind = "Service"
		apply(&services.Items[i])
	}
	apply(&corev1.Secret{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}, ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: namespace}, StringData: map[string]string{"KAFSCALE_S3_ACCESS_KEY": "minioadmin", "KAFSCALE_S3_SECRET_KEY": "minioadmin"}})
	// MinIO is test infrastructure, started before enforcing restricted admission.
	apply(&corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Name: "minio", Namespace: namespace, Labels: map[string]string{"app": "minio"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "minio", Image: "quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z", Args: []string{"server", "/data"}, Env: []corev1.EnvVar{{Name: "MINIO_ROOT_USER", Value: "minioadmin"}, {Name: "MINIO_ROOT_PASSWORD", Value: "minioadmin"}}}}}})
	apply(&corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metav1.ObjectMeta{Name: "minio", Namespace: namespace}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "minio"}, Ports: []corev1.ServicePort{{Port: 9000}}}})
	kubectl("", "wait", "--for=condition=Ready", "pod/minio", "--timeout=180s")
	var etcd appsv1.StatefulSet
	assertFound(t, c, &etcd, namespace, "security-etcd")
	etcd.APIVersion, etcd.Kind = "apps/v1", "StatefulSet"
	// kind's default hostPath provisioner cannot apply fsGroup to old files.
	// Request its local-volume mode so kubelet can perform ownership migration.
	etcd.Spec.VolumeClaimTemplates[0].Annotations = map[string]string{"volumeType": "local"}
	legacy := etcd.DeepCopy()
	root := int64(0)
	legacy.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{RunAsUser: &root}
	legacy.Spec.Template.Spec.InitContainers = nil
	legacy.Spec.Template.Spec.Containers[0].SecurityContext = nil
	apply(legacy)
	kubectl("", "rollout", "status", "sts/security-etcd", "--timeout=180s")
	etcdExec := func(args ...string) string {
		t.Helper()
		// The server image contains etcdctl; use it to verify actual persisted data.
		return kubectl("", append([]string{"exec", "security-etcd-0", "-c", "etcd", "--", "etcdctl", "--endpoints=http://127.0.0.1:2379"}, args...)...)
	}
	etcdExec("put", "/security/upgrade", "preserved")
	kubectl("", "label", "namespace", namespace, "pod-security.kubernetes.io/enforce=restricted", "pod-security.kubernetes.io/enforce-version=v1.35", "--overwrite")
	// Prove admission is active, rather than relying on a namespace label alone.
	probe := exec.CommandContext(ctx, "kubectl", "--context", kubeContext, "-n", namespace, "run", "must-be-rejected", "--image=alpine:3.19", "--restart=Never", "--dry-run=server")
	if out, err := probe.CombinedOutput(); err == nil || !strings.Contains(string(out), "violates PodSecurity") {
		t.Fatalf("restricted admission did not reject insecure pod: %v: %s", err, out)
	}
	apply(&etcd)
	kubectl("", "rollout", "status", "sts/security-etcd", "--timeout=180s")
	if out := etcdExec("get", "/security/upgrade", "--print-value-only"); strings.TrimSpace(out) != "preserved" {
		t.Fatalf("upgrade lost root-owned data: %s", out)
	}
	if out := kubectl("", "exec", "security-etcd-0", "-c", "etcd", "--", "id", "-u"); strings.TrimSpace(out) != "10001" {
		t.Fatalf("etcd UID: %s", out)
	}
	var crons batchv1.CronJobList
	if err := c.List(ctx, &crons); err != nil {
		t.Fatal(err)
	}
	for i := range crons.Items {
		crons.Items[i].APIVersion, crons.Items[i].Kind = "batch/v1", "CronJob"
		crons.Items[i].Spec.Suspend = boolPtr(true)
		apply(&crons.Items[i])
	}
	for _, name := range []string{"security-etcd-snapshot", "security-etcd-maintenance", "security-etcd-maintenance-check"} {
		kubectl("", "create", "job", name+"-manual", "--from=cronjob/"+name)
		kubectl("", "wait", "--for=condition=complete", "job/"+name+"-manual", "--timeout=180s")
	}
	var broker appsv1.StatefulSet
	assertFound(t, c, &broker, namespace, "security-broker")
	broker.APIVersion, broker.Kind = "apps/v1", "StatefulSet"
	broker.Spec.Template.Spec.Containers[0].Image = "kafscale-broker:security"
	apply(&broker)
	kubectl("", "rollout", "status", "sts/security-broker", "--timeout=180s")
	runClient := func(mode string) {
		t.Helper()
		pod := &corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Name: mode, Namespace: namespace}, Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, SecurityContext: restrictedPodSecurityContext(false), Containers: []corev1.Container{{Name: "client", Image: "kafscale-e2e-client:security", SecurityContext: restrictedContainerSecurityContext(), Env: []corev1.EnvVar{
			{Name: "KAFSCALE_E2E_MODE", Value: mode}, {Name: "KAFSCALE_E2E_BROKER_ADDR", Value: "security-broker:9092"}, {Name: "KAFSCALE_E2E_TOPIC", Value: "restricted"}, {Name: "KAFSCALE_E2E_COUNT", Value: "3"}, {Name: "KAFSCALE_E2E_PARTITION", Value: "0"}, {Name: "KAFSCALE_E2E_OFFSET", Value: "0"},
		}}}}}
		apply(pod)
		kubectl("", "wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/"+mode, "--timeout=120s")
	}
	runClient("produce")
	runClient("consume")

	// Restore onto a fresh PVC, exercising both hardened init containers and AWS
	// scratch-space writes. Verify the key came from the snapshot, not old data.
	kubectl("", "delete", "sts/security-etcd", "--wait=true")
	kubectl("", "wait", "--for=delete", "pod/security-etcd-0", "--timeout=90s")
	kubectl("", "delete", "pvc/data-security-etcd-0", "--wait=true")
	apply(&etcd)
	kubectl("", "rollout", "status", "sts/security-etcd", "--timeout=180s")
	if out := etcdExec("get", "/security/upgrade", "--print-value-only"); strings.TrimSpace(out) != "preserved" {
		t.Fatalf("snapshot restore lost key: %s", out)
	}
}
