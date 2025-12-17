package operator

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
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
	defaultBrokerImage = "ghcr.io/novatechflow/kafscale-broker:latest"
)

var brokerImage = getEnv("BROKER_IMAGE", defaultBrokerImage)

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

	if err := r.reconcileBrokerDeployment(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileBrokerService(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileBrokerHPA(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Publisher.Publish(ctx, &cluster); err != nil {
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

func (r *ClusterReconciler) reconcileBrokerDeployment(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster) error {
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
			r.brokerContainer(cluster),
		}
		return controllerutil.SetControllerReference(cluster, deploy, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) brokerContainer(cluster *kafscalev1alpha1.KafscaleCluster) corev1.Container {
	image := brokerImage
	env := []corev1.EnvVar{
		{Name: "KAFSCALE_S3_BUCKET", Value: cluster.Spec.S3.Bucket},
		{Name: "KAFSCALE_S3_REGION", Value: cluster.Spec.S3.Region},
		{Name: "KAFSCALE_ETCD_ENDPOINTS", Value: strings.Join(cluster.Spec.Etcd.Endpoints, ",")},
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
			Name:  "KAFSCALE_CACHE_SIZE",
			Value: cluster.Spec.Config.CacheSize,
		})
	}
	var envFrom []corev1.EnvFromSource
	if cluster.Spec.S3.CredentialsSecret != "" {
		envFrom = append(envFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Spec.S3.CredentialsSecret},
				Optional:             boolPtr(true),
			},
		})
	}

	resources := corev1.ResourceRequirements{
		Requests: cloneResourceList(cluster.Spec.Brokers.Resources.Requests),
		Limits:   cloneResourceList(cluster.Spec.Brokers.Resources.Limits),
	}

	return corev1.Container{
		Name:  "broker",
		Image: image,
		Ports: []corev1.ContainerPort{
			{Name: "kafka", ContainerPort: 9092},
			{Name: "metrics", ContainerPort: 9093},
		},
		Env:       env,
		EnvFrom:   envFrom,
		Resources: resources,
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
