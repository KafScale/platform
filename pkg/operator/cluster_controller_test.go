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
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kafscalev1alpha1 "github.com/novatechflow/kafscale/api/v1alpha1"
)

func TestBrokerContainerAdvertisedEndpoint(t *testing.T) {
	port := int32(19092)
	replicas := int32(1)
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{
				AdvertisedHost: "kafka.example.com",
				AdvertisedPort: &port,
				Replicas:       &replicas,
			},
			S3: kafscalev1alpha1.S3Spec{
				Bucket:               "bucket",
				Region:               "us-east-1",
				CredentialsSecretRef: "creds",
			},
		},
	}
	r := &ClusterReconciler{}
	container := r.brokerContainer(cluster, []string{"http://etcd:2379"})
	if got := envValue(container.Env, "KAFSCALE_BROKER_HOST"); got != "kafka.example.com" {
		t.Fatalf("expected advertised host, got %q", got)
	}
	if got := envValue(container.Env, "KAFSCALE_BROKER_PORT"); got != fmt.Sprintf("%d", port) {
		t.Fatalf("expected advertised port, got %q", got)
	}
}

func TestReconcileBrokerHeadlessService(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{},
			S3: kafscalev1alpha1.S3Spec{
				Bucket:               "bucket",
				Region:               "us-east-1",
				CredentialsSecretRef: "creds",
			},
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if err := r.reconcileBrokerHeadlessService(context.Background(), cluster); err != nil {
		t.Fatalf("reconcile broker headless service: %v", err)
	}

	svc := &corev1.Service{}
	assertFound(t, c, svc, cluster.Namespace, brokerHeadlessServiceName(cluster))
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("expected headless service, got ClusterIP %q", svc.Spec.ClusterIP)
	}
}

func TestReconcileBrokerServiceExternalAccess(t *testing.T) {
	portKafka := int32(30092)
	portMetrics := int32(30093)
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{
				Service: kafscalev1alpha1.BrokerServiceSpec{
					Type:                     string(corev1.ServiceTypeLoadBalancer),
					Annotations:              map[string]string{"cloud.example.com/lb": "external"},
					LoadBalancerIP:           "203.0.113.10",
					LoadBalancerSourceRanges: []string{"203.0.113.0/24"},
					ExternalTrafficPolicy:    string(corev1.ServiceExternalTrafficPolicyTypeLocal),
					KafkaNodePort:            &portKafka,
					MetricsNodePort:          &portMetrics,
				},
			},
			S3: kafscalev1alpha1.S3Spec{
				Bucket:               "bucket",
				Region:               "us-east-1",
				CredentialsSecretRef: "creds",
			},
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if err := r.reconcileBrokerService(context.Background(), cluster); err != nil {
		t.Fatalf("reconcile broker service: %v", err)
	}

	svc := &corev1.Service{}
	assertFound(t, c, svc, cluster.Namespace, cluster.Name+"-broker")
	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
		t.Fatalf("expected service type LoadBalancer, got %s", svc.Spec.Type)
	}
	if got := svc.Annotations["cloud.example.com/lb"]; got != "external" {
		t.Fatalf("expected annotation, got %q", got)
	}
	if svc.Spec.LoadBalancerIP != "203.0.113.10" {
		t.Fatalf("expected load balancer IP, got %q", svc.Spec.LoadBalancerIP)
	}
	if len(svc.Spec.LoadBalancerSourceRanges) != 1 || svc.Spec.LoadBalancerSourceRanges[0] != "203.0.113.0/24" {
		t.Fatalf("unexpected load balancer source ranges: %v", svc.Spec.LoadBalancerSourceRanges)
	}
	if svc.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyTypeLocal {
		t.Fatalf("expected external traffic policy Local, got %q", svc.Spec.ExternalTrafficPolicy)
	}
	if len(svc.Spec.Ports) != 2 {
		t.Fatalf("expected 2 service ports, got %d", len(svc.Spec.Ports))
	}
	if svc.Spec.Ports[0].NodePort != portKafka {
		t.Fatalf("expected kafka node port %d, got %d", portKafka, svc.Spec.Ports[0].NodePort)
	}
	if svc.Spec.Ports[1].NodePort != portMetrics {
		t.Fatalf("expected metrics node port %d, got %d", portMetrics, svc.Spec.Ports[1].NodePort)
	}
}

func TestServiceParsingHelpers(t *testing.T) {
	if got := parseServiceType("LoadBalancer"); got != corev1.ServiceTypeLoadBalancer {
		t.Fatalf("expected LoadBalancer, got %q", got)
	}
	if got := parseServiceType("NodePort"); got != corev1.ServiceTypeNodePort {
		t.Fatalf("expected NodePort, got %q", got)
	}
	if got := parseServiceType("ClusterIP"); got != corev1.ServiceTypeClusterIP {
		t.Fatalf("expected ClusterIP, got %q", got)
	}
	if got := parseServiceType("unknown"); got != "" {
		t.Fatalf("expected empty service type, got %q", got)
	}
	if got := parseExternalTrafficPolicy("Local"); got != corev1.ServiceExternalTrafficPolicyTypeLocal {
		t.Fatalf("expected Local, got %q", got)
	}
	if got := parseExternalTrafficPolicy("Cluster"); got != corev1.ServiceExternalTrafficPolicyTypeCluster {
		t.Fatalf("expected Cluster, got %q", got)
	}
	if got := parseExternalTrafficPolicy("unknown"); got != "" {
		t.Fatalf("expected empty external traffic policy, got %q", got)
	}
}
