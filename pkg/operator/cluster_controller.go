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
	"os"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kafscalev1alpha1 "github.com/novatechflow/kafscale/api/v1alpha1"
)

const (
	defaultBrokerImage           = "ghcr.io/novatechflow/kafscale-broker:latest"
	defaultBrokerImagePullPolicy = string(corev1.PullIfNotPresent)
)

var brokerImage = getEnv("BROKER_IMAGE", defaultBrokerImage)
var brokerImagePullPolicy = getEnv("BROKER_IMAGE_PULL_POLICY", defaultBrokerImagePullPolicy)

// ClusterReconciler reconciles KafscaleCluster resources into Deployments/Services.
type ClusterReconciler struct {
	Client    client.Client
	Scheme    *runtime.Scheme
	Publisher *SnapshotPublisher
}

func NewClusterReconciler(mgr ctrl.Manager, publisher *SnapshotPublisher) *ClusterReconciler {
	return &ClusterReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Publisher: publisher,
	}
}

// Reconcile ensures broker workloads exist for every KafscaleCluster spec.
func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var cluster kafscalev1alpha1.KafscaleCluster
	if err := r.Client.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	etcdResolution, err := EnsureEtcd(ctx, r.Client, r.Scheme, &cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.verifySnapshotS3Access(ctx, &cluster, etcdResolution); err != nil {
		_ = r.updateStatus(ctx, &cluster, metav1.ConditionFalse, "SnapshotAccessFailed", err.Error())
		return ctrl.Result{}, err
	}
	r.populateEtcdSnapshotStatus(ctx, &cluster, etcdResolution)
	if err := r.reconcileBrokerDeployment(ctx, &cluster, etcdResolution.Endpoints); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileBrokerService(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileBrokerHPA(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Publisher.Publish(ctx, &cluster, etcdResolution.Endpoints); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.updateStatus(ctx, &cluster, metav1.ConditionTrue, "Ready", "Reconciled"); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kafscalev1alpha1.KafscaleCluster{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Complete(r)
}

func (r *ClusterReconciler) reconcileBrokerDeployment(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster, endpoints []string) error {
	deploy := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name:      fmt.Sprintf("%s-broker", cluster.Name),
		Namespace: cluster.Namespace,
	}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		replicas := int32(3)
		if cluster.Spec.Brokers.Replicas != nil {
			replicas = *cluster.Spec.Brokers.Replicas
		}
		labels := map[string]string{
			"app":     "kafscale-broker",
			"cluster": cluster.Name,
		}
		deploy.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		deploy.Spec.Replicas = &replicas
		deploy.Spec.Template.ObjectMeta.Labels = labels
		deploy.Spec.Template.Spec.Containers = []corev1.Container{
			r.brokerContainer(cluster, endpoints),
		}
		return controllerutil.SetControllerReference(cluster, deploy, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) brokerContainer(cluster *kafscalev1alpha1.KafscaleCluster, endpoints []string) corev1.Container {
	image := brokerImage
	pullPolicy := parsePullPolicy(brokerImagePullPolicy)
	env := []corev1.EnvVar{
		{Name: "KAFSCALE_S3_BUCKET", Value: cluster.Spec.S3.Bucket},
		{Name: "KAFSCALE_S3_REGION", Value: cluster.Spec.S3.Region},
		{Name: "KAFSCALE_S3_NAMESPACE", Value: cluster.Namespace},
		{Name: "KAFSCALE_ETCD_ENDPOINTS", Value: strings.Join(endpoints, ",")},
		{Name: "KAFSCALE_BROKER_HOST", Value: fmt.Sprintf("%s-broker.%s.svc.cluster.local", cluster.Name, cluster.Namespace)},
		{Name: "KAFSCALE_BROKER_ADDR", Value: ":9092"},
		{Name: "KAFSCALE_METRICS_ADDR", Value: ":9093"},
	}
	if strings.TrimSpace(cluster.Spec.S3.Endpoint) != "" {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_S3_ENDPOINT", Value: cluster.Spec.S3.Endpoint})
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_S3_PATH_STYLE", Value: "true"})
	}
	if strings.TrimSpace(cluster.Spec.S3.ReadBucket) != "" {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_S3_READ_BUCKET", Value: cluster.Spec.S3.ReadBucket})
	}
	if strings.TrimSpace(cluster.Spec.S3.ReadRegion) != "" {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_S3_READ_REGION", Value: cluster.Spec.S3.ReadRegion})
	}
	if strings.TrimSpace(cluster.Spec.S3.ReadEndpoint) != "" {
		env = append(env, corev1.EnvVar{Name: "KAFSCALE_S3_READ_ENDPOINT", Value: cluster.Spec.S3.ReadEndpoint})
	}
	if cluster.Spec.Config.SegmentBytes > 0 {
		env = append(env, corev1.EnvVar{
			Name:  "KAFSCALE_SEGMENT_BYTES",
			Value: fmt.Sprintf("%d", cluster.Spec.Config.SegmentBytes),
		})
	}
	if cluster.Spec.Config.FlushIntervalMs > 0 {
		env = append(env, corev1.EnvVar{
			Name:  "KAFSCALE_FLUSH_INTERVAL_MS",
			Value: fmt.Sprintf("%d", cluster.Spec.Config.FlushIntervalMs),
		})
	}
	if cluster.Spec.Config.CacheSize != "" {
		env = append(env, corev1.EnvVar{
			Name:  "KAFSCALE_CACHE_BYTES",
			Value: cluster.Spec.Config.CacheSize,
		})
	}
	var envFrom []corev1.EnvFromSource
	if cluster.Spec.S3.CredentialsSecretRef != "" {
		envFrom = append(envFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Spec.S3.CredentialsSecretRef},
				Optional:             boolPtr(true),
			},
		})
	}

	resources := corev1.ResourceRequirements{
		Requests: cloneResourceList(cluster.Spec.Brokers.Resources.Requests),
		Limits:   cloneResourceList(cluster.Spec.Brokers.Resources.Limits),
	}

	return corev1.Container{
		Name:            "broker",
		Image:           image,
		ImagePullPolicy: pullPolicy,
		Ports: []corev1.ContainerPort{
			{Name: "kafka", ContainerPort: 9092},
			{Name: "metrics", ContainerPort: 9093},
		},
		Env:       env,
		EnvFrom:   envFrom,
		Resources: resources,
	}
}

func parsePullPolicy(policy string) corev1.PullPolicy {
	switch strings.TrimSpace(policy) {
	case string(corev1.PullAlways):
		return corev1.PullAlways
	case string(corev1.PullNever):
		return corev1.PullNever
	default:
		return corev1.PullIfNotPresent
	}
}

func (r *ClusterReconciler) reconcileBrokerService(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-broker", cluster.Name),
			Namespace: cluster.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Spec.Selector = map[string]string{
			"app":     "kafscale-broker",
			"cluster": cluster.Name,
		}
		svc.Spec.Ports = []corev1.ServicePort{
			{Name: "kafka", Port: 9092, TargetPort: intstr.FromString("kafka")},
			{Name: "metrics", Port: 9093, TargetPort: intstr.FromString("metrics")},
		}
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		return controllerutil.SetControllerReference(cluster, svc, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) reconcileBrokerHPA(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster) error {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-broker", cluster.Name),
			Namespace: cluster.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, hpa, func() error {
		min := int32(3)
		if cluster.Spec.Brokers.Replicas != nil && *cluster.Spec.Brokers.Replicas > 0 {
			min = *cluster.Spec.Brokers.Replicas
		}
		max := min * 4
		hpa.Spec.MinReplicas = &min
		hpa.Spec.MaxReplicas = max
		hpa.Spec.ScaleTargetRef = autoscalingv2.CrossVersionObjectReference{
			Kind:       "Deployment",
			Name:       fmt.Sprintf("%s-broker", cluster.Name),
			APIVersion: "apps/v1",
		}
		hpa.Spec.Metrics = []autoscalingv2.MetricSpec{
			{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name: corev1.ResourceCPU,
					Target: autoscalingv2.MetricTarget{
						Type:               autoscalingv2.UtilizationMetricType,
						AverageUtilization: int32Ptr(70),
					},
				},
			},
			{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name: corev1.ResourceMemory,
					Target: autoscalingv2.MetricTarget{
						Type:               autoscalingv2.UtilizationMetricType,
						AverageUtilization: int32Ptr(80),
					},
				},
			},
		}
		return controllerutil.SetControllerReference(cluster, hpa, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) updateStatus(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster, status metav1.ConditionStatus, reason, message string) error {
	condition := metav1.Condition{
		Type:               "Ready",
		Status:             status,
		LastTransitionTime: metav1.NewTime(time.Now()),
		Reason:             reason,
		Message:            message,
	}
	cluster.Status.Phase = reason
	setClusterCondition(&cluster.Status.Conditions, condition)
	if err := r.Client.Status().Update(ctx, cluster); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	recordClusterCount(ctx, r.Client)
	return nil
}

func (r *ClusterReconciler) populateEtcdSnapshotStatus(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster, resolution EtcdResolution) {
	clusterKey := cluster.Namespace + "/" + cluster.Name
	now := time.Now()
	if !resolution.Managed {
		operatorEtcdSnapshotAge.WithLabelValues(clusterKey).Set(0)
		operatorEtcdSnapshotLastSuccess.WithLabelValues(clusterKey).Set(0)
		operatorEtcdSnapshotLastSchedule.WithLabelValues(clusterKey).Set(0)
		operatorEtcdSnapshotStale.WithLabelValues(clusterKey).Set(0)
		operatorEtcdSnapshotSuccess.WithLabelValues(clusterKey).Set(0)
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               "EtcdSnapshot",
			Status:             metav1.ConditionFalse,
			Reason:             "SnapshotNotManaged",
			Message:            "Etcd snapshots are managed externally.",
			LastTransitionTime: metav1.NewTime(now),
		})
		return
	}

	cron := &batchv1.CronJob{}
	cronKey := client.ObjectKey{Namespace: cluster.Namespace, Name: fmt.Sprintf("%s-etcd-snapshot", cluster.Name)}
	if err := r.Client.Get(ctx, cronKey, cron); err != nil {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               "EtcdSnapshot",
			Status:             metav1.ConditionFalse,
			Reason:             "SnapshotCronMissing",
			Message:            "Etcd snapshot CronJob is missing.",
			LastTransitionTime: metav1.NewTime(now),
		})
		return
	}

	var lastSchedule time.Time
	if cron.Status.LastScheduleTime != nil {
		lastSchedule = cron.Status.LastScheduleTime.Time
		operatorEtcdSnapshotLastSchedule.WithLabelValues(clusterKey).Set(float64(lastSchedule.Unix()))
	} else {
		operatorEtcdSnapshotLastSchedule.WithLabelValues(clusterKey).Set(0)
	}

	if cron.Status.LastSuccessfulTime == nil {
		operatorEtcdSnapshotAge.WithLabelValues(clusterKey).Set(0)
		operatorEtcdSnapshotLastSuccess.WithLabelValues(clusterKey).Set(0)
		operatorEtcdSnapshotStale.WithLabelValues(clusterKey).Set(1)
		operatorEtcdSnapshotSuccess.WithLabelValues(clusterKey).Set(0)
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               "EtcdSnapshot",
			Status:             metav1.ConditionFalse,
			Reason:             "SnapshotNeverSucceeded",
			Message:            "No successful etcd snapshots recorded yet.",
			LastTransitionTime: metav1.NewTime(now),
		})
		return
	}

	lastSuccess := cron.Status.LastSuccessfulTime.Time
	operatorEtcdSnapshotLastSuccess.WithLabelValues(clusterKey).Set(float64(lastSuccess.Unix()))
	age := now.Sub(lastSuccess)
	operatorEtcdSnapshotAge.WithLabelValues(clusterKey).Set(age.Seconds())
	operatorEtcdSnapshotSuccess.WithLabelValues(clusterKey).Set(1)

	staleAfter := time.Duration(snapshotStaleAfterSeconds()) * time.Second
	stale := age > staleAfter
	if stale {
		operatorEtcdSnapshotStale.WithLabelValues(clusterKey).Set(1)
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               "EtcdSnapshot",
			Status:             metav1.ConditionFalse,
			Reason:             "SnapshotStale",
			Message:            fmt.Sprintf("Last snapshot %s ago (threshold %s).", age.Round(time.Second), staleAfter),
			LastTransitionTime: metav1.NewTime(now),
		})
		return
	}

	operatorEtcdSnapshotStale.WithLabelValues(clusterKey).Set(0)
	message := fmt.Sprintf("Last snapshot %s ago.", age.Round(time.Second))
	if !lastSchedule.IsZero() {
		message = fmt.Sprintf("%s Last scheduled %s ago.", message, now.Sub(lastSchedule).Round(time.Second))
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               "EtcdSnapshot",
		Status:             metav1.ConditionTrue,
		Reason:             "SnapshotHealthy",
		Message:            message,
		LastTransitionTime: metav1.NewTime(now),
	})
}

func setClusterCondition(conditions *[]metav1.Condition, condition metav1.Condition) {
	meta.SetStatusCondition(conditions, condition)
}

func int32Ptr(v int32) *int32 {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

func cloneResourceList(in corev1.ResourceList) corev1.ResourceList {
	if len(in) == 0 {
		return nil
	}
	out := corev1.ResourceList{}
	for k, v := range in {
		out[k] = v.DeepCopy()
	}
	return out
}

func getEnv(key, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return fallback
}
