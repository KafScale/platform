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
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
)

const (
	defaultLfsProxyImage           = "ghcr.io/kafscale/kafscale-lfs-proxy:latest"
	defaultLfsProxyImagePullPolicy = string(corev1.PullIfNotPresent)
	defaultLfsProxyPort            = int32(9092)
	defaultLfsProxyHTTPPort        = int32(8080)
	defaultLfsProxyHealthPort      = int32(9094)
	defaultLfsProxyMetricsPort     = int32(9095)
)

var lfsProxyImage = getEnv("LFS_PROXY_IMAGE", defaultLfsProxyImage)
var lfsProxyImagePullPolicy = getEnv("LFS_PROXY_IMAGE_PULL_POLICY", defaultLfsProxyImagePullPolicy)

func (r *ClusterReconciler) reconcileLfsProxyResources(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster, endpoints []string) error {
	if !cluster.Spec.LfsProxy.Enabled {
		return r.deleteLfsProxyResources(ctx, cluster)
	}
	if err := r.reconcileLfsProxyDeployment(ctx, cluster, endpoints); err != nil {
		return err
	}
	if err := r.reconcileLfsProxyService(ctx, cluster); err != nil {
		return err
	}
	if lfsProxyMetricsEnabled(cluster.Spec.LfsProxy) {
		return r.reconcileLfsProxyMetricsService(ctx, cluster)
	}
	return r.deleteLfsProxyMetricsService(ctx, cluster)
}

func (r *ClusterReconciler) reconcileLfsProxyDeployment(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster, endpoints []string) error {
	deploy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name:      lfsProxyName(cluster),
		Namespace: cluster.Namespace,
	}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		replicas := int32(2)
		if cluster.Spec.LfsProxy.Replicas != nil && *cluster.Spec.LfsProxy.Replicas > 0 {
			replicas = *cluster.Spec.LfsProxy.Replicas
		}
		labels := map[string]string{
			"app":     "kafscale-lfs-proxy",
			"cluster": cluster.Name,
		}
		deploy.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		deploy.Spec.Replicas = &replicas
		deploy.Spec.Template.ObjectMeta.Labels = labels
		deploy.Spec.Template.Spec.Containers = []corev1.Container{
			r.lfsProxyContainer(cluster, endpoints),
		}
		return controllerutil.SetControllerReference(cluster, deploy, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) reconcileLfsProxyService(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      lfsProxyName(cluster),
		Namespace: cluster.Namespace,
	}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		labels := lfsProxyLabels(cluster)
		annotations := copyStringMap(cluster.Spec.LfsProxy.Service.Annotations)
		ports := []corev1.ServicePort{servicePort("kafka", lfsProxyPort(cluster.Spec.LfsProxy))}
		if lfsProxyHTTPEnabled(cluster.Spec.LfsProxy) {
			ports = append(ports, servicePort("http", lfsProxyHTTPPort(cluster.Spec.LfsProxy)))
		}

		svc.Labels = labels
		svc.Spec.Selector = labels
		svc.Spec.Ports = ports
		svc.Spec.Type = parseServiceType(cluster.Spec.LfsProxy.Service.Type)
		svc.Annotations = annotations
		if len(cluster.Spec.LfsProxy.Service.LoadBalancerSourceRanges) > 0 {
			svc.Spec.LoadBalancerSourceRanges = append([]string{}, cluster.Spec.LfsProxy.Service.LoadBalancerSourceRanges...)
		}
		return controllerutil.SetControllerReference(cluster, svc, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) reconcileLfsProxyMetricsService(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      lfsProxyMetricsName(cluster),
		Namespace: cluster.Namespace,
	}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		labels := lfsProxyLabels(cluster)
		ports := []corev1.ServicePort{servicePort("metrics", lfsProxyMetricsPort(cluster.Spec.LfsProxy))}

		svc.Labels = labels
		svc.Spec.Selector = labels
		svc.Spec.Ports = ports
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		return controllerutil.SetControllerReference(cluster, svc, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) deleteLfsProxyResources(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster) error {
	deploy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name:      lfsProxyName(cluster),
		Namespace: cluster.Namespace,
	}}
	if err := r.Client.Delete(ctx, deploy); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := r.deleteLfsProxyMetricsService(ctx, cluster); err != nil {
		return err
	}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      lfsProxyName(cluster),
		Namespace: cluster.Namespace,
	}}
	if err := r.Client.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *ClusterReconciler) deleteLfsProxyMetricsService(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      lfsProxyMetricsName(cluster),
		Namespace: cluster.Namespace,
	}}
	if err := r.Client.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *ClusterReconciler) lfsProxyContainer(cluster *kafscalev1alpha1.KafscaleCluster, endpoints []string) corev1.Container {
	image := lfsProxyImage
	if strings.TrimSpace(cluster.Spec.LfsProxy.Image) != "" {
		image = strings.TrimSpace(cluster.Spec.LfsProxy.Image)
	}
	pullPolicy := parsePullPolicy(lfsProxyImagePullPolicy)
	if strings.TrimSpace(cluster.Spec.LfsProxy.ImagePullPolicy) != "" {
		pullPolicy = parsePullPolicy(cluster.Spec.LfsProxy.ImagePullPolicy)
	}
	portKafka := lfsProxyPort(cluster.Spec.LfsProxy)
	portHTTP := lfsProxyHTTPPort(cluster.Spec.LfsProxy)
	portHealth := lfsProxyHealthPort(cluster.Spec.LfsProxy)
	portMetrics := lfsProxyMetricsPort(cluster.Spec.LfsProxy)

	env := []corev1.EnvVar{
		{Name: "KAFSCALE_LFS_PROXY_ADDR", Value: fmt.Sprintf(":%d", portKafka)},
		{Name: "KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS", Value: strings.Join(endpoints, ",")},
		{Name: "KAFSCALE_LFS_PROXY_S3_BUCKET", Value: cluster.Spec.S3.Bucket},
		{Name: "KAFSCALE_LFS_PROXY_S3_REGION", Value: cluster.Spec.S3.Region},
		{Name: "KAFSCALE_S3_NAMESPACE", Value: lfsProxyNamespace(cluster)},
	}
	if cluster.Spec.LfsProxy.AdvertisedHost != "" {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_LFS_PROXY_ADVERTISED_HOST", Value: cluster.Spec.LfsProxy.AdvertisedHost})
	}
	if cluster.Spec.LfsProxy.AdvertisedPort != nil && *cluster.Spec.LfsProxy.AdvertisedPort > 0 {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_LFS_PROXY_ADVERTISED_PORT", Value: fmt.Sprintf("%d", *cluster.Spec.LfsProxy.AdvertisedPort)})
	}
	if cluster.Spec.LfsProxy.BackendCacheTTLSeconds != nil && *cluster.Spec.LfsProxy.BackendCacheTTLSeconds > 0 {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_LFS_PROXY_BACKEND_CACHE_TTL_SEC", Value: fmt.Sprintf("%d", *cluster.Spec.LfsProxy.BackendCacheTTLSeconds)})
	}
	if len(cluster.Spec.LfsProxy.Backends) > 0 {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_LFS_PROXY_BACKENDS", Value: strings.Join(cluster.Spec.LfsProxy.Backends, ",")})
	}
	if lfsProxyHTTPEnabled(cluster.Spec.LfsProxy) {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_LFS_PROXY_HTTP_ADDR", Value: fmt.Sprintf(":%d", portHTTP)})
		if cluster.Spec.LfsProxy.HTTP.APIKeySecretRef != "" {
			key := strings.TrimSpace(cluster.Spec.LfsProxy.HTTP.APIKeySecretKey)
			if key == "" {
				key = "API_KEY"
			}
			env = append(env, corev1.EnvVar{
				Name: "KAFSCALE_LFS_PROXY_HTTP_API_KEY",
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Spec.LfsProxy.HTTP.APIKeySecretRef},
					Key:                  key,
				}},
			})
		}
	}
	if lfsProxyHealthEnabled(cluster.Spec.LfsProxy) {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_LFS_PROXY_HEALTH_ADDR", Value: fmt.Sprintf(":%d", portHealth)})
	}
	if lfsProxyMetricsEnabled(cluster.Spec.LfsProxy) {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_LFS_PROXY_METRICS_ADDR", Value: fmt.Sprintf(":%d", portMetrics)})
	}
	if strings.TrimSpace(cluster.Spec.S3.Endpoint) != "" {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_LFS_PROXY_S3_ENDPOINT", Value: cluster.Spec.S3.Endpoint})
	}
	if cluster.Spec.S3.CredentialsSecretRef != "" {
		env = append(env,
			corev1.EnvVar{
				Name: "KAFSCALE_LFS_PROXY_S3_ACCESS_KEY",
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Spec.S3.CredentialsSecretRef},
					Key:                  "AWS_ACCESS_KEY_ID",
				}},
			},
			corev1.EnvVar{
				Name: "KAFSCALE_LFS_PROXY_S3_SECRET_KEY",
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Spec.S3.CredentialsSecretRef},
					Key:                  "AWS_SECRET_ACCESS_KEY",
				}},
			},
		)
	}
	if lfsProxyForcePathStyle(cluster) {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE", Value: "true"})
	}
	if lfsProxyEnsureBucket(cluster) {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET", Value: "true"})
	}
	if cluster.Spec.LfsProxy.S3.MaxBlobSize != nil && *cluster.Spec.LfsProxy.S3.MaxBlobSize > 0 {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_LFS_PROXY_MAX_BLOB_SIZE", Value: fmt.Sprintf("%d", *cluster.Spec.LfsProxy.S3.MaxBlobSize)})
	}
	if cluster.Spec.LfsProxy.S3.ChunkSize != nil && *cluster.Spec.LfsProxy.S3.ChunkSize > 0 {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_LFS_PROXY_CHUNK_SIZE", Value: fmt.Sprintf("%d", *cluster.Spec.LfsProxy.S3.ChunkSize)})
	}

	ports := []corev1.ContainerPort{{Name: "kafka", ContainerPort: portKafka}}
	if lfsProxyHTTPEnabled(cluster.Spec.LfsProxy) {
		ports = append(ports, corev1.ContainerPort{Name: "http", ContainerPort: portHTTP})
	}
	if lfsProxyHealthEnabled(cluster.Spec.LfsProxy) {
		ports = append(ports, corev1.ContainerPort{Name: "health", ContainerPort: portHealth})
	}
	if lfsProxyMetricsEnabled(cluster.Spec.LfsProxy) {
		ports = append(ports, corev1.ContainerPort{Name: "metrics", ContainerPort: portMetrics})
	}

	container := corev1.Container{
		Name:            "lfs-proxy",
		Image:           image,
		ImagePullPolicy: pullPolicy,
		Ports:           ports,
		Env:             env,
	}
	if lfsProxyHealthEnabled(cluster.Spec.LfsProxy) {
		container.ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("health")}}, InitialDelaySeconds: 2, PeriodSeconds: 5, FailureThreshold: 6}
		container.LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/livez", Port: intstr.FromString("health")}}, InitialDelaySeconds: 5, PeriodSeconds: 10, FailureThreshold: 3}
	}
	return container
}

func lfsProxyName(cluster *kafscalev1alpha1.KafscaleCluster) string {
	return fmt.Sprintf("%s-lfs-proxy", cluster.Name)
}

func lfsProxyMetricsName(cluster *kafscalev1alpha1.KafscaleCluster) string {
	return fmt.Sprintf("%s-lfs-proxy-metrics", cluster.Name)
}

func lfsProxyLabels(cluster *kafscalev1alpha1.KafscaleCluster) map[string]string {
	return map[string]string{
		"app":     "kafscale-lfs-proxy",
		"cluster": cluster.Name,
	}
}

func lfsProxyNamespace(cluster *kafscalev1alpha1.KafscaleCluster) string {
	if ns := strings.TrimSpace(cluster.Spec.LfsProxy.S3.Namespace); ns != "" {
		return ns
	}
	return cluster.Namespace
}

func lfsProxyPort(spec kafscalev1alpha1.LfsProxySpec) int32 {
	if spec.Service.Port != nil && *spec.Service.Port > 0 {
		return *spec.Service.Port
	}
	return defaultLfsProxyPort
}

func lfsProxyHTTPPort(spec kafscalev1alpha1.LfsProxySpec) int32 {
	if spec.HTTP.Port != nil && *spec.HTTP.Port > 0 {
		return *spec.HTTP.Port
	}
	return defaultLfsProxyHTTPPort
}

func lfsProxyHealthPort(spec kafscalev1alpha1.LfsProxySpec) int32 {
	if spec.Health.Port != nil && *spec.Health.Port > 0 {
		return *spec.Health.Port
	}
	return defaultLfsProxyHealthPort
}

func lfsProxyMetricsPort(spec kafscalev1alpha1.LfsProxySpec) int32 {
	if spec.Metrics.Port != nil && *spec.Metrics.Port > 0 {
		return *spec.Metrics.Port
	}
	return defaultLfsProxyMetricsPort
}

func lfsProxyHTTPEnabled(spec kafscalev1alpha1.LfsProxySpec) bool {
	if spec.HTTP.Enabled != nil {
		return *spec.HTTP.Enabled
	}
	return false
}

func lfsProxyMetricsEnabled(spec kafscalev1alpha1.LfsProxySpec) bool {
	if spec.Metrics.Enabled != nil {
		return *spec.Metrics.Enabled
	}
	return true
}

func lfsProxyHealthEnabled(spec kafscalev1alpha1.LfsProxySpec) bool {
	if spec.Health.Enabled != nil {
		return *spec.Health.Enabled
	}
	return true
}

func lfsProxyForcePathStyle(cluster *kafscalev1alpha1.KafscaleCluster) bool {
	if cluster.Spec.LfsProxy.S3.ForcePathStyle != nil {
		return *cluster.Spec.LfsProxy.S3.ForcePathStyle
	}
	return strings.TrimSpace(cluster.Spec.S3.Endpoint) != ""
}

func lfsProxyEnsureBucket(cluster *kafscalev1alpha1.KafscaleCluster) bool {
	if cluster.Spec.LfsProxy.S3.EnsureBucket != nil {
		return *cluster.Spec.LfsProxy.S3.EnsureBucket
	}
	return false
}

func servicePort(name string, port int32) corev1.ServicePort {
	return corev1.ServicePort{Name: name, Port: port, Protocol: corev1.ProtocolTCP}
}
