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

package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	kafscalev1alpha1 "github.com/novatechflow/kafscale/api/v1alpha1"
	"github.com/novatechflow/kafscale/pkg/operator"
)

func TestOperatorManagedEtcdResources(t *testing.T) {
	setupTestLogger()
	if !parseBoolEnv("KAFSCALE_E2E") {
		t.Skip("set KAFSCALE_E2E=1 to run operator envtest")
	}
	if !envtestAssetsAvailable() {
		t.Skip("envtest assets missing; set KUBEBUILDER_ASSETS or install setup-envtest")
	}

	t.Setenv("KAFSCALE_OPERATOR_ETCD_ENDPOINTS", "")
	t.Setenv("KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SKIP_PREFLIGHT", "1")
	t.Setenv("KAFSCALE_OPERATOR_ETCD_SILENCE_LOGS", "1")

	crdPath := filepath.Join(repoRoot(t), "deploy", "helm", "kafscale", "crds")
	env := &envtest.Environment{
		CRDDirectoryPaths: []string{crdPath},
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Fatalf("stop envtest: %v", err)
		}
	})

	scheme := k8sruntime.NewScheme()
	utilruntime.Must(kafscalev1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(autoscalingv2.AddToScheme(scheme))
	utilruntime.Must(batchv1.AddToScheme(scheme))
	utilruntime.Must(policyv1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		Controller: config.Controller{
			SkipNameValidation: ptr.To(true),
		},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	publisher := operator.NewSnapshotPublisher(mgr.GetClient())
	if err := operator.NewClusterReconciler(mgr, publisher).SetupWithManager(mgr); err != nil {
		t.Fatalf("cluster reconciler: %v", err)
	}
	if err := operator.NewTopicReconciler(mgr, publisher).SetupWithManager(mgr); err != nil {
		t.Fatalf("topic reconciler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if err := mgr.Start(ctx); err != nil {
			t.Errorf("manager error: %v", err)
		}
	}()

	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "etcd-managed",
			Namespace: "default",
		},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{},
			S3: kafscalev1alpha1.S3Spec{
				Bucket: "snapshots",
				Region: "us-east-1",
			},
			Etcd: kafscalev1alpha1.EtcdSpec{},
		},
	}
	if err := mgr.GetClient().Create(ctx, cluster); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer waitCancel()
	if err := wait.PollUntilContextTimeout(waitCtx, 200*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		if !resourceExists(ctx, mgr.GetClient(), &appsv1.StatefulSet{}, cluster.Namespace, cluster.Name+"-etcd") {
			return false, nil
		}
		if !resourceExists(ctx, mgr.GetClient(), &corev1.Service{}, cluster.Namespace, cluster.Name+"-etcd") {
			return false, nil
		}
		if !resourceExists(ctx, mgr.GetClient(), &corev1.Service{}, cluster.Namespace, cluster.Name+"-etcd-client") {
			return false, nil
		}
		if !resourceExists(ctx, mgr.GetClient(), &policyv1.PodDisruptionBudget{}, cluster.Namespace, cluster.Name+"-etcd") {
			return false, nil
		}
		if !resourceExists(ctx, mgr.GetClient(), &batchv1.CronJob{}, cluster.Namespace, cluster.Name+"-etcd-snapshot") {
			return false, nil
		}
		return true, nil
	}); err != nil {
		t.Fatalf("expected managed etcd resources: %v", err)
	}
}

func envtestAssetsAvailable() bool {
	assets := strings.TrimSpace(os.Getenv("KUBEBUILDER_ASSETS"))
	if assets == "" {
		assets = "/usr/local/kubebuilder/bin"
	}
	etcdPath := filepath.Join(assets, "etcd")
	kubeAPIServerPath := filepath.Join(assets, "kube-apiserver")
	if _, err := os.Stat(etcdPath); err != nil {
		return false
	}
	if _, err := os.Stat(kubeAPIServerPath); err != nil {
		return false
	}
	return true
}

func resourceExists(ctx context.Context, c client.Reader, obj client.Object, ns, name string) bool {
	err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, obj)
	return err == nil || !apierrors.IsNotFound(err)
}
