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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
	"github.com/KafScale/platform/pkg/storage"
)

const (
	operatorEtcdSnapshotSkipPreflightEnv = "KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SKIP_PREFLIGHT"
	s3AccessKeyEnv                       = "KAFSCALE_S3_ACCESS_KEY"
	s3SecretKeyEnv                       = "KAFSCALE_S3_SECRET_KEY"
	s3SessionTokenEnv                    = "KAFSCALE_S3_SESSION_TOKEN"
	awsAccessKeyEnv                      = "AWS_ACCESS_KEY_ID"
	awsSecretKeyEnv                      = "AWS_SECRET_ACCESS_KEY"
	awsSessionTokenEnv                   = "AWS_SESSION_TOKEN"
)

func (r *ClusterReconciler) verifySnapshotS3Access(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster, resolution EtcdResolution) error {
	clusterKey := cluster.Namespace + "/" + cluster.Name
	now := time.Now()
	if !resolution.Managed {
		operatorEtcdSnapshotAccessOK.WithLabelValues(clusterKey).Set(0)
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               "EtcdSnapshotAccess",
			Status:             metav1.ConditionFalse,
			Reason:             "SnapshotNotManaged",
			Message:            "Etcd snapshots are managed externally.",
			LastTransitionTime: metav1.NewTime(now),
		})
		_ = r.Client.Status().Update(ctx, cluster)
		return nil
	}
	if parseBoolEnv(operatorEtcdSnapshotSkipPreflightEnv) {
		return nil
	}

	bucket := snapshotBucket(cluster)
	if bucket == "" {
		err := fmt.Errorf("snapshot bucket is not configured")
		r.recordSnapshotAccessFailure(ctx, cluster, clusterKey, err)
		return err
	}

	endpoint := strings.TrimSpace(os.Getenv(operatorEtcdSnapshotEndpointEnv))
	if endpoint == "" {
		endpoint = strings.TrimSpace(cluster.Spec.S3.Endpoint)
	}
	cfg := storage.S3Config{
		Bucket:         bucket,
		Region:         cluster.Spec.S3.Region,
		Endpoint:       endpoint,
		ForcePathStyle: endpoint != "",
	}
	if err := r.loadS3Credentials(ctx, cluster, &cfg); err != nil {
		r.recordSnapshotAccessFailure(ctx, cluster, clusterKey, err)
		return err
	}

	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client, err := storage.NewS3Client(checkCtx, cfg)
	if err != nil {
		r.recordSnapshotAccessFailure(ctx, cluster, clusterKey, err)
		return err
	}
	if parseBoolEnv(operatorEtcdSnapshotCreateBucketEnv) {
		if err := client.EnsureBucket(checkCtx); err != nil {
			r.recordSnapshotAccessFailure(ctx, cluster, clusterKey, err)
			return err
		}
	}

	prefix := snapshotPrefix(cluster)
	key := fmt.Sprintf("%s/health/%s-%d.txt", strings.Trim(prefix, "/"), cluster.Name, now.UnixNano())
	if err := client.UploadSegment(checkCtx, key, []byte("ok")); err != nil {
		r.recordSnapshotAccessFailure(ctx, cluster, clusterKey, err)
		return err
	}

	operatorEtcdSnapshotAccessOK.WithLabelValues(clusterKey).Set(1)
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               "EtcdSnapshotAccess",
		Status:             metav1.ConditionTrue,
		Reason:             "SnapshotAccessOK",
		Message:            "Snapshot bucket is writable.",
		LastTransitionTime: metav1.NewTime(now),
	})
	_ = r.Client.Status().Update(ctx, cluster)
	return nil
}

func (r *ClusterReconciler) loadS3Credentials(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster, cfg *storage.S3Config) error {
	if strings.TrimSpace(cluster.Spec.S3.CredentialsSecretRef) == "" {
		return nil
	}
	secret := &corev1.Secret{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Spec.S3.CredentialsSecretRef}
	if err := r.Client.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("s3 credentials secret %s not found", cluster.Spec.S3.CredentialsSecretRef)
		}
		return err
	}
	cfg.AccessKeyID = firstSecretValue(secret, s3AccessKeyEnv, awsAccessKeyEnv)
	cfg.SecretAccessKey = firstSecretValue(secret, s3SecretKeyEnv, awsSecretKeyEnv)
	cfg.SessionToken = firstSecretValue(secret, s3SessionTokenEnv, awsSessionTokenEnv)
	return nil
}

func firstSecretValue(secret *corev1.Secret, keys ...string) string {
	for _, key := range keys {
		if val, ok := secret.Data[key]; ok {
			trimmed := strings.TrimSpace(string(val))
			if trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func (r *ClusterReconciler) recordSnapshotAccessFailure(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster, clusterKey string, err error) {
	operatorEtcdSnapshotAccessOK.WithLabelValues(clusterKey).Set(0)
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               "EtcdSnapshotAccess",
		Status:             metav1.ConditionFalse,
		Reason:             "SnapshotAccessFailed",
		Message:            fmt.Sprintf("Snapshot bucket access failed: %v", err),
		LastTransitionTime: metav1.NewTime(time.Now()),
	})
	_ = r.Client.Status().Update(ctx, cluster)
}
