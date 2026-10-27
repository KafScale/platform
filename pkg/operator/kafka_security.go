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
	"strconv"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const kafkaSecurityPath = "/etc/kafscale/security"

func validateKafkaSecurity(s kafscalev1alpha1.KafkaSecuritySpec) error {
	if s.IdentitySecretRef == "" {
		if s.ACLSecretRef != "" || s.TLSSecretRef != "" || len(s.ProxyPodSelector) != 0 || s.MaxHandshakes != 0 || s.MaxHandshakesPerIP != 0 || s.HandshakeTimeoutMS != 0 || s.HandshakeQueueMS != 0 {
			return errors.New("Kafka security requires identitySecretRef")
		}
		return nil
	}
	if s.ACLSecretRef == "" {
		return errors.New("Kafka security requires aclSecretRef")
	}
	for _, value := range []int32{s.MaxHandshakes, s.MaxHandshakesPerIP, s.HandshakeTimeoutMS, s.HandshakeQueueMS} {
		if value < 0 || value > 60000 {
			return errors.New("Kafka handshake settings must be 1..60000, or omitted")
		}
	}
	return nil
}

func mountSecuritySecret(pod *corev1.PodSpec, container *corev1.Container, name, secret string) {
	pod.Volumes = append(pod.Volumes, corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: secret}}})
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: name, MountPath: kafkaSecurityPath + "/" + name, ReadOnly: true})
}

func brokerKafkaSecurity(pod *corev1.PodSpec, s kafscalev1alpha1.KafkaSecuritySpec) {
	if s.IdentitySecretRef == "" {
		return
	}
	container := &pod.Containers[0]
	mountSecuritySecret(pod, container, "identity", s.IdentitySecretRef)
	mountSecuritySecret(pod, container, "acl", s.ACLSecretRef)
	// Replace inherited operator settings to prevent fail-open or spoofable identity.
	names := map[string]bool{"KAFSCALE_ACL_ENABLED": true, "KAFSCALE_ACL_FAIL_OPEN": true, "KAFSCALE_ACL_JSON": true, "KAFSCALE_ACL_FILE": true, "KAFSCALE_PROXY_IDENTITY_KEY_FILE": true}
	env := container.Env[:0]
	for _, e := range container.Env {
		if !names[e.Name] {
			env = append(env, e)
		}
	}
	container.Env = append(env,
		corev1.EnvVar{Name: "KAFSCALE_PROXY_IDENTITY_KEY_FILE", Value: kafkaSecurityPath + "/identity/key"},
		corev1.EnvVar{Name: "KAFSCALE_ACL_ENABLED", Value: "true"},
		corev1.EnvVar{Name: "KAFSCALE_ACL_FAIL_OPEN", Value: "false"},
		corev1.EnvVar{Name: "KAFSCALE_ACL_FILE", Value: kafkaSecurityPath + "/acl/acl.json"},
	)
}

func proxyKafkaSecurity(pod *corev1.PodSpec, s kafscalev1alpha1.KafkaSecuritySpec) error {
	if s.IdentitySecretRef == "" {
		return nil
	}
	if s.TLSSecretRef == "" {
		return errors.New("managed proxy authentication requires tlsSecretRef")
	}
	container := &pod.Containers[0]
	mountSecuritySecret(pod, container, "identity", s.IdentitySecretRef)
	mountSecuritySecret(pod, container, "tls", s.TLSSecretRef)
	mountSecuritySecret(pod, container, "acl", s.ACLSecretRef)
	replaceSecurityEnv(container,
		corev1.EnvVar{Name: "KAFSCALE_ACL_JSON", Value: ""},
		corev1.EnvVar{Name: "KAFSCALE_PROXY_IDENTITY_KEY_FILE", Value: kafkaSecurityPath + "/identity/key"},
		corev1.EnvVar{Name: "KAFSCALE_ACL_FILE", Value: kafkaSecurityPath + "/acl/acl.json"},
		corev1.EnvVar{Name: "KAFSCALE_PROXY_TLS_CERT_FILE", Value: kafkaSecurityPath + "/tls/tls.crt"},
		corev1.EnvVar{Name: "KAFSCALE_PROXY_TLS_KEY_FILE", Value: kafkaSecurityPath + "/tls/tls.key"},
		corev1.EnvVar{Name: "KAFSCALE_PROXY_TLS_CLIENT_CA_FILE", Value: kafkaSecurityPath + "/tls/ca.crt"},
		corev1.EnvVar{Name: "KAFSCALE_PROXY_TLS_PRINCIPALS_FILE", Value: kafkaSecurityPath + "/tls/principals.json"},
	)
	replaceSecurityEnv(container, kafkaHandshakeEnv(s)...)
	return nil
}

func (r *ClusterReconciler) reconcileKafkaNetworkPolicy(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster) error {
	s := cluster.Spec.Security
	if err := validateKafkaSecurity(s); err != nil {
		return err
	}
	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: cluster.Name + "-broker-kafka", Namespace: cluster.Namespace}}
	if s.IdentitySecretRef == "" {
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(policy), policy); err != nil {
			return client.IgnoreNotFound(err)
		}
		if !metav1.IsControlledBy(policy, cluster) {
			return errors.New("refusing to delete a Kafka NetworkPolicy owned by another resource")
		}
		if err := r.Client.Delete(ctx, policy); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, policy, func() error {
		peers := []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "kafscale-lfs-proxy", "cluster": cluster.Name}}}}
		if len(s.ProxyPodSelector) != 0 {
			peers = append(peers, networkingv1.NetworkPolicyPeer{PodSelector: &metav1.LabelSelector{MatchLabels: s.ProxyPodSelector}})
		}
		policy.Spec = networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "kafscale-broker", "cluster": cluster.Name}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{From: peers, Ports: []networkingv1.NetworkPolicyPort{{Port: ptrPort(9092)}}},
				{Ports: []networkingv1.NetworkPolicyPort{{Port: ptrPort(9093)}}},
			},
		}
		return controllerutil.SetControllerReference(cluster, policy, r.Scheme)
	})
	return err
}

func ptrPort(port int) *intstr.IntOrString { value := intstr.FromInt(port); return &value }

func replaceSecurityEnv(container *corev1.Container, updates ...corev1.EnvVar) {
	names := make(map[string]bool, len(updates))
	for _, entry := range updates {
		names[entry.Name] = true
	}
	env := container.Env[:0]
	for _, entry := range container.Env {
		if !names[entry.Name] {
			env = append(env, entry)
		}
	}
	container.Env = append(env, updates...)
}

func kafkaHandshakeEnv(s kafscalev1alpha1.KafkaSecuritySpec) []corev1.EnvVar {
	var env []corev1.EnvVar
	for _, setting := range []struct {
		name                string
		value, defaultValue int32
	}{
		{"KAFSCALE_PROXY_TLS_MAX_HANDSHAKES", s.MaxHandshakes, 64},
		{"KAFSCALE_PROXY_TLS_MAX_HANDSHAKES_PER_IP", s.MaxHandshakesPerIP, 8},
		{"KAFSCALE_PROXY_TLS_HANDSHAKE_TIMEOUT_MS", s.HandshakeTimeoutMS, 1000},
		{"KAFSCALE_PROXY_TLS_HANDSHAKE_QUEUE_MS", s.HandshakeQueueMS, 100},
	} {
		value := setting.value
		if value == 0 {
			value = setting.defaultValue
		}
		env = append(env, corev1.EnvVar{Name: setting.name, Value: strconv.Itoa(int(value))})
	}
	return env
}
