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
	"fmt"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	kafscalev1alpha1 "github.com/novatechflow/kafscale/api/v1alpha1"
	"github.com/novatechflow/kafscale/pkg/operator"
)

func TestOperatorBrokerExternalAccessConfig(t *testing.T) {
	setupTestLogger()
	if !parseBoolEnv("KAFSCALE_E2E") {
		t.Skip("set KAFSCALE_E2E=1 to run operator envtest")
	}
	if !envtestAssetsAvailable() {
		t.Skip("envtest assets missing; set KUBEBUILDER_ASSETS or install setup-envtest")
	}
	t.Setenv("KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SKIP_PREFLIGHT", "1")
	t.Setenv("KAFSCALE_OPERATOR_ETCD_SILENCE_LOGS", "1")

	advertisedPort := int32(19092)
	kafkaNodePort := int32(30092)
	metricsNodePort := int32(30093)

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

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if err := mgr.Start(ctx); err != nil {
			t.Errorf("manager error: %v", err)
		}
	}()

	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "external-access",
			Namespace: "default",
		},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{
				AdvertisedHost: "kafka.example.com",
				AdvertisedPort: &advertisedPort,
				Replicas:       ptr.To(int32(1)),
				Service: kafscalev1alpha1.BrokerServiceSpec{
					Type:                     string(corev1.ServiceTypeLoadBalancer),
					Annotations:              map[string]string{"cloud.example.com/lb": "external"},
					LoadBalancerIP:           "203.0.113.10",
					LoadBalancerSourceRanges: []string{"203.0.113.0/24"},
					ExternalTrafficPolicy:    string(corev1.ServiceExternalTrafficPolicyTypeLocal),
					KafkaNodePort:            &kafkaNodePort,
					MetricsNodePort:          &metricsNodePort,
				},
			},
			S3: kafscalev1alpha1.S3Spec{
				Bucket:               "snapshots",
				Region:               "us-east-1",
				CredentialsSecretRef: "creds",
			},
			Etcd: kafscalev1alpha1.EtcdSpec{
				Endpoints: []string{"http://etcd:2379"},
			},
		},
	}
	if err := mgr.GetClient().Create(ctx, cluster); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer waitCancel()
	if err := wait.PollUntilContextTimeout(waitCtx, 200*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		service := &corev1.Service{}
		if !resourceExists(ctx, mgr.GetClient(), service, cluster.Namespace, cluster.Name+"-broker") {
			return false, nil
		}
		sts := &appsv1.StatefulSet{}
		if !resourceExists(ctx, mgr.GetClient(), sts, cluster.Namespace, cluster.Name+"-broker") {
			return false, nil
		}

		if service.Spec.Type != corev1.ServiceTypeLoadBalancer {
			return false, nil
		}
		if service.Spec.LoadBalancerIP != "203.0.113.10" {
			return false, nil
		}
		if service.Annotations["cloud.example.com/lb"] != "external" {
			return false, nil
		}
		if service.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyTypeLocal {
			return false, nil
		}
		if len(service.Spec.LoadBalancerSourceRanges) != 1 || service.Spec.LoadBalancerSourceRanges[0] != "203.0.113.0/24" {
			return false, nil
		}
		if len(service.Spec.Ports) != 2 {
			return false, nil
		}
		if portByName(service.Spec.Ports, "kafka").NodePort != kafkaNodePort {
			return false, nil
		}
		if portByName(service.Spec.Ports, "metrics").NodePort != metricsNodePort {
			return false, nil
		}

		if len(sts.Spec.Template.Spec.Containers) == 0 {
			return false, nil
		}
		env := sts.Spec.Template.Spec.Containers[0].Env
		if envValue(env, "KAFSCALE_BROKER_HOST") != "kafka.example.com" {
			return false, nil
		}
		if envValue(env, "KAFSCALE_BROKER_PORT") != fmt.Sprintf("%d", advertisedPort) {
			return false, nil
		}
		return true, nil
	}); err != nil {
		t.Fatalf("expected external access config to reconcile: %v", err)
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

func portByName(ports []corev1.ServicePort, name string) corev1.ServicePort {
	for _, port := range ports {
		if port.Name == name {
			return port
		}
	}
	return corev1.ServicePort{}
}
