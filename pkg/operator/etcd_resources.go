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
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kafscalev1alpha1 "github.com/novatechflow/kafscale/api/v1alpha1"
)

const (
	operatorEtcdEndpointsEnv             = "KAFSCALE_OPERATOR_ETCD_ENDPOINTS"
	operatorEtcdImageEnv                 = "KAFSCALE_OPERATOR_ETCD_IMAGE"
	operatorEtcdReplicasEnv              = "KAFSCALE_OPERATOR_ETCD_REPLICAS"
	operatorEtcdStorageEnv               = "KAFSCALE_OPERATOR_ETCD_STORAGE_SIZE"
	operatorEtcdClassEnv                 = "KAFSCALE_OPERATOR_ETCD_STORAGE_CLASS"
	operatorEtcdSnapshotBucketEnv        = "KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET"
	operatorEtcdSnapshotPrefixEnv        = "KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PREFIX"
	operatorEtcdSnapshotScheduleEnv      = "KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SCHEDULE"
	operatorEtcdSnapshotImageEnv         = "KAFSCALE_OPERATOR_ETCD_SNAPSHOT_IMAGE"
	operatorEtcdSnapshotEtcdctlEnv       = "KAFSCALE_OPERATOR_ETCD_SNAPSHOT_ETCDCTL_IMAGE"
	operatorEtcdSnapshotEndpointEnv      = "KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT"
	operatorEtcdSnapshotStaleAfterEnv    = "KAFSCALE_OPERATOR_ETCD_SNAPSHOT_STALE_AFTER_SEC"
	operatorEtcdSnapshotCreateBucketEnv  = "KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET"
	operatorEtcdSnapshotProtectBucketEnv = "KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PROTECT_BUCKET"
	operatorEtcdStorageMemoryEnv         = "KAFSCALE_OPERATOR_ETCD_STORAGE_MEMORY"

	defaultEtcdImage                 = "kubesphere/etcd:3.6.4-0"
	defaultEtcdctlImage              = "ghcr.io/novatechflow/kafscale-etcd-tools:dev"
	defaultEtcdStorageSize           = "10Gi"
	defaultEtcdReplicas              = 3
	defaultSnapshotBucketPrefix      = "kafscale-etcd"
	defaultSnapshotPrefix            = "etcd-snapshots"
	defaultSnapshotSchedule          = "0 * * * *"
	defaultSnapshotImage             = "amazon/aws-cli:2.15.0"
	defaultSnapshotStaleAfterSeconds = 2 * 60 * 60
)

type EtcdResolution struct {
	Endpoints []string
	Managed   bool
}

func EnsureEtcd(ctx context.Context, c client.Client, scheme *runtime.Scheme, cluster *kafscalev1alpha1.KafscaleCluster) (EtcdResolution, error) {
	if endpoints := cleanEndpoints(cluster.Spec.Etcd.Endpoints); len(endpoints) > 0 {
		return EtcdResolution{Endpoints: endpoints}, nil
	}
	if envEndpoints := parseEnvEndpoints(operatorEtcdEndpointsEnv); len(envEndpoints) > 0 {
		return EtcdResolution{Endpoints: envEndpoints}, nil
	}

	if err := reconcileEtcdResources(ctx, c, scheme, cluster); err != nil {
		return EtcdResolution{}, err
	}
	return EtcdResolution{Endpoints: managedEtcdEndpoints(cluster), Managed: true}, nil
}

func parseEnvEndpoints(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	return cleanEndpoints(strings.Split(raw, ","))
}

func cleanEndpoints(list []string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(list))
	for _, entry := range list {
		val := strings.TrimSpace(entry)
		if val == "" {
			continue
		}
		if _, ok := seen[val]; ok {
			continue
		}
		seen[val] = struct{}{}
		out = append(out, val)
	}
	return out
}

func managedEtcdEndpoints(cluster *kafscalev1alpha1.KafscaleCluster) []string {
	host := fmt.Sprintf("%s-etcd-client.%s.svc.cluster.local:2379", cluster.Name, cluster.Namespace)
	return []string{"http://" + host}
}

func reconcileEtcdResources(ctx context.Context, c client.Client, scheme *runtime.Scheme, cluster *kafscalev1alpha1.KafscaleCluster) error {
	if err := reconcileEtcdHeadlessService(ctx, c, scheme, cluster); err != nil {
		return err
	}
	if err := reconcileEtcdClientService(ctx, c, scheme, cluster); err != nil {
		return err
	}
	if err := reconcileEtcdStatefulSet(ctx, c, scheme, cluster); err != nil {
		return err
	}
	if err := reconcileEtcdPDB(ctx, c, scheme, cluster); err != nil {
		return err
	}
	if err := reconcileEtcdSnapshotCronJob(ctx, c, scheme, cluster); err != nil {
		return err
	}
	return nil
}

func reconcileEtcdHeadlessService(ctx context.Context, c client.Client, scheme *runtime.Scheme, cluster *kafscalev1alpha1.KafscaleCluster) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      fmt.Sprintf("%s-etcd", cluster.Name),
		Namespace: cluster.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, c, svc, func() error {
		labels := etcdLabels(cluster)
		svc.Labels = labels
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		// Need to allow peer DNS before readiness to avoid etcd bootstrap deadlock.
		svc.Spec.PublishNotReadyAddresses = true
		svc.Spec.Selector = labels
		svc.Spec.Ports = []corev1.ServicePort{
			{Name: "client", Port: 2379, TargetPort: intstr.FromInt(2379)},
			{Name: "peer", Port: 2380, TargetPort: intstr.FromInt(2380)},
		}
		return controllerutil.SetControllerReference(cluster, svc, scheme)
	})
	return err
}

func reconcileEtcdClientService(ctx context.Context, c client.Client, scheme *runtime.Scheme, cluster *kafscalev1alpha1.KafscaleCluster) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name:      fmt.Sprintf("%s-etcd-client", cluster.Name),
		Namespace: cluster.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, c, svc, func() error {
		labels := etcdLabels(cluster)
		svc.Labels = labels
		svc.Spec.Selector = labels
		svc.Spec.Ports = []corev1.ServicePort{
			{Name: "client", Port: 2379, TargetPort: intstr.FromInt(2379)},
		}
		return controllerutil.SetControllerReference(cluster, svc, scheme)
	})
	return err
}

func reconcileEtcdStatefulSet(ctx context.Context, c client.Client, scheme *runtime.Scheme, cluster *kafscalev1alpha1.KafscaleCluster) error {
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name:      fmt.Sprintf("%s-etcd", cluster.Name),
		Namespace: cluster.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, c, sts, func() error {
		labels := etcdLabels(cluster)
		replicas := etcdReplicas()
		sts.Labels = labels
		sts.Spec.ServiceName = fmt.Sprintf("%s-etcd", cluster.Name)
		sts.Spec.Replicas = &replicas
		sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		sts.Spec.Template.ObjectMeta.Labels = labels

		useMemory := parseBoolEnv(operatorEtcdStorageMemoryEnv)
		if useMemory {
			sts.Spec.VolumeClaimTemplates = nil
		} else {
			storageSize := getEnv(operatorEtcdStorageEnv, defaultEtcdStorageSize)
			storageClass := strings.TrimSpace(os.Getenv(operatorEtcdClassEnv))
			sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "data"},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(storageSize),
							},
						},
						StorageClassName: stringPtrOrNil(storageClass),
					},
				},
			}
		}

		image := getEnv(operatorEtcdImageEnv, defaultEtcdImage)
		volumes := []corev1.Volume{
			{
				Name: "snapshots",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		}
		if useMemory {
			volumes = append(volumes, corev1.Volume{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory},
				},
			})
		}
		sts.Spec.Template.Spec.Volumes = append([]corev1.Volume{}, volumes...)

		bucket := snapshotBucket(cluster)
		if bucket != "" {
			prefix := snapshotPrefix(cluster)
			endpoint := strings.TrimSpace(os.Getenv(operatorEtcdSnapshotEndpointEnv))
			if endpoint == "" {
				endpoint = strings.TrimSpace(cluster.Spec.S3.Endpoint)
			}
			etcdctlImage := getEnv(operatorEtcdSnapshotEtcdctlEnv, defaultEtcdctlImage)
			backupImage := getEnv(operatorEtcdSnapshotImageEnv, defaultSnapshotImage)

			restoreEnv := []corev1.EnvVar{
				{Name: "AWS_REGION", Value: cluster.Spec.S3.Region},
				{Name: "AWS_DEFAULT_REGION", Value: cluster.Spec.S3.Region},
				{Name: "AWS_EC2_METADATA_DISABLED", Value: "true"},
				{Name: "SNAPSHOT_BUCKET", Value: bucket},
				{Name: "SNAPSHOT_PREFIX", Value: prefix},
			}
			if endpoint != "" {
				restoreEnv = append(restoreEnv, corev1.EnvVar{Name: "AWS_ENDPOINT_URL", Value: endpoint})
			}
			if strings.TrimSpace(cluster.Spec.S3.CredentialsSecretRef) != "" {
				secretRef := corev1.LocalObjectReference{Name: cluster.Spec.S3.CredentialsSecretRef}
				restoreEnv = append(restoreEnv,
					corev1.EnvVar{
						Name: "AWS_ACCESS_KEY_ID",
						ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: secretRef,
								Key:                  "KAFSCALE_S3_ACCESS_KEY",
								Optional:             boolPtr(true),
							},
						},
					},
					corev1.EnvVar{
						Name: "AWS_SECRET_ACCESS_KEY",
						ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: secretRef,
								Key:                  "KAFSCALE_S3_SECRET_KEY",
								Optional:             boolPtr(true),
							},
						},
					},
					corev1.EnvVar{
						Name: "AWS_SESSION_TOKEN",
						ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: secretRef,
								Key:                  "KAFSCALE_S3_SESSION_TOKEN",
								Optional:             boolPtr(true),
							},
						},
					},
				)
			}
			peerSvc := fmt.Sprintf("%s-etcd", cluster.Name)
			initialCluster := buildEtcdInitialCluster(cluster, int(etcdReplicas()))
			restoreScript := "set -euo pipefail\n" +
				"DATA_DIR=/var/lib/etcd\n" +
				"if [ -d \"$DATA_DIR/member\" ] && [ \"$(ls -A \"$DATA_DIR\")\" ]; then\n" +
				"  echo \"etcd data dir not empty; skipping restore\"\n" +
				"  exit 0\n" +
				"fi\n" +
				"if [ ! -f /snapshots/etcd-snapshot.db ]; then\n" +
				"  echo \"no snapshot file; skipping restore\"\n" +
				"  exit 0\n" +
				"fi\n" +
				"if ! command -v etcdutl >/dev/null 2>&1; then\n" +
				"  echo \"etcdutl not found; snapshot restore requires etcdutl in the image\"\n" +
				"  exit 1\n" +
				"fi\n" +
				"INITIAL_CLUSTER=\"" + initialCluster + "\"\n" +
				"PEER_URL=\"http://${POD_NAME}." + peerSvc + ".${POD_NAMESPACE}.svc.cluster.local:2380\"\n" +
				"etcdutl snapshot restore /snapshots/etcd-snapshot.db " +
				"--data-dir \"$DATA_DIR\" " +
				"--name \"$POD_NAME\" " +
				"--initial-cluster \"$INITIAL_CLUSTER\" " +
				"--initial-cluster-token \"" + cluster.Name + "-etcd\" " +
				"--initial-advertise-peer-urls \"$PEER_URL\"\n"

			downloadScript := "set -euo pipefail\n" +
				"DATA_DIR=/var/lib/etcd\n" +
				"ENDPOINT_OPT=\"\"\n" +
				"if [ -n \"$AWS_ENDPOINT_URL\" ]; then ENDPOINT_OPT=\"--endpoint-url $AWS_ENDPOINT_URL\"; fi\n" +
				"if [ -d \"$DATA_DIR/member\" ] && [ \"$(ls -A \"$DATA_DIR\")\" ]; then\n" +
				"  echo \"etcd data dir not empty; skipping snapshot download\"\n" +
				"  exit 0\n" +
				"fi\n" +
				"BASE=\"s3://$SNAPSHOT_BUCKET\"\n" +
				"PREFIX=\"$SNAPSHOT_PREFIX\"\n" +
				"if [ -n \"$PREFIX\" ]; then BASE=\"$BASE/$PREFIX\"; PREFIX=\"$PREFIX/\"; fi\n" +
				"LATEST=\"\"\n" +
				"attempt=0\n" +
				"while [ $attempt -lt 15 ]; do\n" +
				"  LISTING=$(aws $ENDPOINT_OPT s3api list-objects-v2 --bucket \"$SNAPSHOT_BUCKET\" --prefix \"$PREFIX\" --query 'Contents[].Key' --output text 2>&1 || true)\n" +
				"  LATEST=$(aws $ENDPOINT_OPT s3api list-objects-v2 --bucket \"$SNAPSHOT_BUCKET\" --prefix \"$PREFIX\" --query 'Contents[?ends_with(Key, `.db`)] | sort_by(@,&LastModified)[-1].Key' --output text 2>&1 | tr -d '\\r' || true)\n" +
				"  if [ \"$LATEST\" = \"None\" ]; then LATEST=\"\"; fi\n" +
				"  if [ -n \"$LATEST\" ]; then\n" +
				"    break\n" +
				"  fi\n" +
				"  attempt=$((attempt+1))\n" +
				"  echo \"no snapshot found in $BASE (attempt $attempt)\"\n" +
				"  if [ -n \"$LISTING\" ]; then\n" +
				"    echo \"snapshot listing: $LISTING\"\n" +
				"  fi\n" +
				"  sleep 2\n" +
				"done\n" +
				"if [ -z \"$LATEST\" ]; then\n" +
				"  echo \"no snapshot found in $BASE\"\n" +
				"  exit 0\n" +
				"fi\n" +
				"aws $ENDPOINT_OPT s3 cp \"s3://$SNAPSHOT_BUCKET/$LATEST\" /snapshots/etcd-snapshot.db\n"

			sts.Spec.Template.Spec.InitContainers = []corev1.Container{
				{
					Name:    "snapshot-download",
					Image:   backupImage,
					Command: []string{"/bin/sh", "-c", downloadScript},
					Env:     restoreEnv,
					VolumeMounts: []corev1.VolumeMount{
						{Name: "snapshots", MountPath: "/snapshots"},
						{Name: "data", MountPath: "/var/lib/etcd"},
					},
				},
				{
					Name:  "snapshot-restore",
					Image: etcdctlImage,
					Command: []string{
						"/bin/sh",
						"-c",
						restoreScript,
					},
					Env: []corev1.EnvVar{
						{Name: "ETCDCTL_API", Value: "3"},
						{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
						{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "snapshots", MountPath: "/snapshots"},
						{Name: "data", MountPath: "/var/lib/etcd"},
					},
				},
			}
		}
		sts.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:  "etcd",
				Image: image,
				Ports: []corev1.ContainerPort{
					{Name: "client", ContainerPort: 2379},
					{Name: "peer", ContainerPort: 2380},
				},
				Env: []corev1.EnvVar{
					{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
					{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
				},
				Command: []string{"etcd"},
				Args:    etcdArgs(cluster),
				VolumeMounts: []corev1.VolumeMount{
					{Name: "data", MountPath: "/var/lib/etcd"},
				},
			},
		}
		return controllerutil.SetControllerReference(cluster, sts, scheme)
	})
	return err
}

func etcdArgs(cluster *kafscalev1alpha1.KafscaleCluster) []string {
	peerSvc := fmt.Sprintf("%s-etcd", cluster.Name)
	initialCluster := buildEtcdInitialCluster(cluster, int(etcdReplicas()))
	peerURL := fmt.Sprintf("http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2380", peerSvc)
	clientURL := fmt.Sprintf("http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2379", peerSvc)
	return []string{
		"--name=$(POD_NAME)",
		"--data-dir=/var/lib/etcd",
		"--listen-peer-urls=http://0.0.0.0:2380",
		"--listen-client-urls=http://0.0.0.0:2379",
		"--advertise-client-urls=" + clientURL,
		"--initial-advertise-peer-urls=" + peerURL,
		"--initial-cluster=" + initialCluster,
		"--initial-cluster-state=new",
		"--initial-cluster-token=" + cluster.Name + "-etcd",
	}
}

func buildEtcdInitialCluster(cluster *kafscalev1alpha1.KafscaleCluster, replicas int) string {
	peerSvc := fmt.Sprintf("%s-etcd", cluster.Name)
	if replicas < 1 {
		replicas = 1
	}
	members := make([]string, 0, replicas)
	for i := 0; i < replicas; i++ {
		member := fmt.Sprintf(
			"%s-etcd-%d=http://%s-etcd-%d.%s.$(POD_NAMESPACE).svc.cluster.local:2380",
			cluster.Name, i, cluster.Name, i, peerSvc,
		)
		members = append(members, member)
	}
	return strings.Join(members, ",")
}

func reconcileEtcdPDB(ctx context.Context, c client.Client, scheme *runtime.Scheme, cluster *kafscalev1alpha1.KafscaleCluster) error {
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{
		Name:      fmt.Sprintf("%s-etcd", cluster.Name),
		Namespace: cluster.Namespace,
	}}
	_, err := controllerutil.CreateOrUpdate(ctx, c, pdb, func() error {
		labels := etcdLabels(cluster)
		pdb.Labels = labels
		pdb.Spec = policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: intstrPtr(1),
			Selector:       &metav1.LabelSelector{MatchLabels: labels},
		}
		return controllerutil.SetControllerReference(cluster, pdb, scheme)
	})
	return err
}

func reconcileEtcdSnapshotCronJob(ctx context.Context, c client.Client, scheme *runtime.Scheme, cluster *kafscalev1alpha1.KafscaleCluster) error {
	bucket := snapshotBucket(cluster)
	if bucket == "" {
		return nil
	}
	prefix := snapshotPrefix(cluster)
	schedule := getEnv(operatorEtcdSnapshotScheduleEnv, defaultSnapshotSchedule)
	endpoint := strings.TrimSpace(os.Getenv(operatorEtcdSnapshotEndpointEnv))
	if endpoint == "" {
		endpoint = strings.TrimSpace(cluster.Spec.S3.Endpoint)
	}
	etcdctlImage := getEnv(operatorEtcdSnapshotEtcdctlEnv, defaultEtcdctlImage)
	backupImage := getEnv(operatorEtcdSnapshotImageEnv, defaultSnapshotImage)
	createBucket := parseBoolEnv(operatorEtcdSnapshotCreateBucketEnv)
	protectBucket := parseBoolEnv(operatorEtcdSnapshotProtectBucketEnv)

	cron := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{
		Name:      fmt.Sprintf("%s-etcd-snapshot", cluster.Name),
		Namespace: cluster.Namespace,
	}}

	_, err := controllerutil.CreateOrUpdate(ctx, c, cron, func() error {
		labels := etcdLabels(cluster)
		cron.Labels = labels
		cron.Spec.Schedule = schedule
		cron.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
		cron.Spec.SuccessfulJobsHistoryLimit = int32Ptr(3)
		cron.Spec.FailedJobsHistoryLimit = int32Ptr(3)
		cron.Spec.JobTemplate.Spec.Template.ObjectMeta.Labels = labels
		cron.Spec.JobTemplate.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
		cron.Spec.JobTemplate.Spec.Template.Spec.Volumes = []corev1.Volume{
			{
				Name: "snapshots",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		}

		envFrom := []corev1.EnvFromSource{}
		if strings.TrimSpace(cluster.Spec.S3.CredentialsSecretRef) != "" {
			envFrom = append(envFrom, corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Spec.S3.CredentialsSecretRef},
					Optional:             boolPtr(true),
				},
			})
		}

		cron.Spec.JobTemplate.Spec.Template.Spec.InitContainers = []corev1.Container{
			{
				Name:  "snapshot",
				Image: etcdctlImage,
				Env: []corev1.EnvVar{
					{Name: "ETCD_ENDPOINTS", Value: strings.Join(managedEtcdEndpoints(cluster), ",")},
					{Name: "ETCDCTL_API", Value: "3"},
				},
				Command: []string{"etcdctl"},
				Args: []string{
					"--endpoints=$(ETCD_ENDPOINTS)",
					"snapshot",
					"save",
					"/snapshots/etcd-snapshot.db",
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "snapshots", MountPath: "/snapshots"},
				},
			},
		}

		uploadEnv := []corev1.EnvVar{
			{Name: "AWS_REGION", Value: cluster.Spec.S3.Region},
			{Name: "AWS_DEFAULT_REGION", Value: cluster.Spec.S3.Region},
			{Name: "AWS_EC2_METADATA_DISABLED", Value: "true"},
			{Name: "SNAPSHOT_BUCKET", Value: bucket},
			{Name: "SNAPSHOT_PREFIX", Value: strings.Trim(prefix, "/")},
			{Name: "CREATE_BUCKET", Value: boolToString(createBucket)},
			{Name: "PROTECT_BUCKET", Value: boolToString(protectBucket)},
		}
		if endpoint != "" {
			uploadEnv = append(uploadEnv, corev1.EnvVar{Name: "AWS_ENDPOINT_URL", Value: endpoint})
		}
		if strings.TrimSpace(cluster.Spec.S3.CredentialsSecretRef) != "" {
			secretRef := corev1.LocalObjectReference{Name: cluster.Spec.S3.CredentialsSecretRef}
			uploadEnv = append(uploadEnv,
				corev1.EnvVar{
					Name: "AWS_ACCESS_KEY_ID",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: secretRef,
							Key:                  "KAFSCALE_S3_ACCESS_KEY",
							Optional:             boolPtr(true),
						},
					},
				},
				corev1.EnvVar{
					Name: "AWS_SECRET_ACCESS_KEY",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: secretRef,
							Key:                  "KAFSCALE_S3_SECRET_KEY",
							Optional:             boolPtr(true),
						},
					},
				},
				corev1.EnvVar{
					Name: "AWS_SESSION_TOKEN",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: secretRef,
							Key:                  "KAFSCALE_S3_SESSION_TOKEN",
							Optional:             boolPtr(true),
						},
					},
				},
			)
		}

		cron.Spec.JobTemplate.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:  "upload",
				Image: backupImage,
				Command: []string{
					"/bin/sh",
					"-c",
					"set -euo pipefail\n" +
						"TS=$(date -u +%Y%m%d%H%M%S)\n" +
						"SNAPSHOT=/snapshots/etcd-snapshot.db\n" +
						"CHECKSUM=/snapshots/etcd-snapshot.db.sha256\n" +
						"ENDPOINT_OPT=\"\"\n" +
						"if [ -n \"$AWS_ENDPOINT_URL\" ]; then ENDPOINT_OPT=\"--endpoint-url $AWS_ENDPOINT_URL\"; fi\n" +
						"if [ \"$CREATE_BUCKET\" = \"1\" ]; then\n" +
						"  if ! aws $ENDPOINT_OPT s3api head-bucket --bucket \"$SNAPSHOT_BUCKET\" >/dev/null 2>&1; then\n" +
						"    if [ \"$AWS_REGION\" = \"us-east-1\" ]; then\n" +
						"      aws $ENDPOINT_OPT s3api create-bucket --bucket \"$SNAPSHOT_BUCKET\"\n" +
						"    else\n" +
						"      aws $ENDPOINT_OPT s3api create-bucket --bucket \"$SNAPSHOT_BUCKET\" --create-bucket-configuration LocationConstraint=\"$AWS_REGION\"\n" +
						"    fi\n" +
						"  fi\n" +
						"fi\n" +
						"if [ \"$PROTECT_BUCKET\" = \"1\" ]; then\n" +
						"  aws $ENDPOINT_OPT s3api head-bucket --bucket \"$SNAPSHOT_BUCKET\" >/dev/null\n" +
						"  aws $ENDPOINT_OPT s3api put-bucket-versioning --bucket \"$SNAPSHOT_BUCKET\" --versioning-configuration Status=Enabled\n" +
						"  if ! aws $ENDPOINT_OPT s3api put-public-access-block --bucket \"$SNAPSHOT_BUCKET\" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true >/dev/null 2>&1; then\n" +
						"    echo \"public access block unsupported by endpoint; continuing\"\n" +
						"  fi\n" +
						"fi\n" +
						"sha256sum \"$SNAPSHOT\" > \"$CHECKSUM\"\n" +
						"aws $ENDPOINT_OPT s3 cp \"$SNAPSHOT\" \"s3://$SNAPSHOT_BUCKET/$SNAPSHOT_PREFIX/$TS.db\"\n" +
						"aws $ENDPOINT_OPT s3 cp \"$CHECKSUM\" \"s3://$SNAPSHOT_BUCKET/$SNAPSHOT_PREFIX/$TS.db.sha256\"",
				},
				Env:     uploadEnv,
				EnvFrom: envFrom,
				VolumeMounts: []corev1.VolumeMount{
					{Name: "snapshots", MountPath: "/snapshots"},
				},
			},
		}
		return controllerutil.SetControllerReference(cluster, cron, scheme)
	})
	return err
}

func snapshotBucket(cluster *kafscalev1alpha1.KafscaleCluster) string {
	bucket := strings.TrimSpace(os.Getenv(operatorEtcdSnapshotBucketEnv))
	if bucket == "" {
		bucket = defaultEtcdSnapshotBucket(cluster)
	}
	return strings.TrimSpace(bucket)
}

func snapshotPrefix(cluster *kafscalev1alpha1.KafscaleCluster) string {
	prefix := strings.TrimSpace(os.Getenv(operatorEtcdSnapshotPrefixEnv))
	if prefix != "" {
		return strings.Trim(prefix, "/")
	}
	return strings.Trim(defaultSnapshotPrefix, "/")
}

func defaultEtcdSnapshotBucket(cluster *kafscalev1alpha1.KafscaleCluster) string {
	name := strings.TrimSpace(cluster.Name)
	namespace := strings.TrimSpace(cluster.Namespace)
	if name == "" && namespace == "" {
		return defaultSnapshotBucketPrefix
	}
	if namespace == "" {
		return sanitizeBucketName(fmt.Sprintf("%s-%s", defaultSnapshotBucketPrefix, name))
	}
	if name == "" {
		return sanitizeBucketName(fmt.Sprintf("%s-%s", defaultSnapshotBucketPrefix, namespace))
	}
	return sanitizeBucketName(fmt.Sprintf("%s-%s-%s", defaultSnapshotBucketPrefix, namespace, name))
}

func sanitizeBucketName(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return defaultSnapshotBucketPrefix
	}
	var b strings.Builder
	b.Grow(len(raw))
	lastDash := false
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return defaultSnapshotBucketPrefix
	}
	return out
}

func etcdLabels(cluster *kafscalev1alpha1.KafscaleCluster) map[string]string {
	return map[string]string{
		"app":     "kafscale-etcd",
		"cluster": cluster.Name,
	}
}

func intstrPtr(val int) *intstr.IntOrString {
	v := intstr.FromInt(val)
	return &v
}

func snapshotStaleAfterSeconds() int64 {
	val := strings.TrimSpace(os.Getenv(operatorEtcdSnapshotStaleAfterEnv))
	if val == "" {
		return defaultSnapshotStaleAfterSeconds
	}
	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil || parsed <= 0 {
		return defaultSnapshotStaleAfterSeconds
	}
	return parsed
}

func parseBoolEnv(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func boolToString(val bool) string {
	if val {
		return "1"
	}
	return "0"
}

func etcdReplicas() int32 {
	raw := strings.TrimSpace(os.Getenv(operatorEtcdReplicasEnv))
	if raw == "" {
		return int32(defaultEtcdReplicas)
	}
	parsed, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || parsed < int64(defaultEtcdReplicas) {
		return int32(defaultEtcdReplicas)
	}
	return int32(parsed)
}

func stringPtrOrNil(val string) *string {
	if strings.TrimSpace(val) == "" {
		return nil
	}
	return &val
}
