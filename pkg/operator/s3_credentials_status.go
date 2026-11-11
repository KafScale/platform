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
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
)

const s3CredentialsConditionType = "S3Credentials"

func (r *ClusterReconciler) populateS3CredentialsStatus(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster) {
	condition := r.s3CredentialsCondition(ctx, cluster)
	previous := meta.FindStatusCondition(cluster.Status.Conditions, s3CredentialsConditionType)
	changed := previous == nil || previous.Status != condition.Status || previous.Reason != condition.Reason
	if changed && condition.Status == metav1.ConditionFalse {
		ctrl.LoggerFrom(ctx).Info("WARNING: S3 credentials secret is not usable or could not be verified",
			"cluster", cluster.Namespace+"/"+cluster.Name,
			"secret", cluster.Spec.S3.CredentialsSecretRef,
			"reason", condition.Reason,
			"message", condition.Message)
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, condition)
}

func (r *ClusterReconciler) s3CredentialsCondition(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster) metav1.Condition {
	condition := metav1.Condition{
		Type:               s3CredentialsConditionType,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(time.Now()),
	}
	name := cluster.Spec.S3.CredentialsSecretRef
	if strings.TrimSpace(name) == "" {
		condition.Reason = "SecretNotReferenced"
		condition.Message = "No spec.s3.credentialsSecretRef is set; workloads use the default AWS credential chain."
		return condition
	}
	secret := &corev1.Secret{}
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: name}, secret); err != nil {
		condition.Status = metav1.ConditionFalse
		if apierrors.IsNotFound(err) {
			condition.Reason = "SecretNotFound"
			condition.Message = fmt.Sprintf("Secret %q does not exist, so workloads get no static keys from it and use the default AWS credential chain if one is available.", name)
		} else {
			condition.Reason = "SecretReadFailed"
			condition.Message = fmt.Sprintf("The operator could not read or verify secret %q: %v. It cannot tell which keys the workloads receive.", name, err)
		}
		return condition
	}
	hasAWS := secretHasKeyPair(secret, awsAccessKeyEnv, awsSecretKeyEnv)
	hasKafscale := secretHasKeyPair(secret, s3AccessKeyEnv, s3SecretKeyEnv)
	switch {
	case hasKafscale && strings.TrimSpace(string(secret.Data[awsSessionTokenEnv])) != "" && len(secretKeysPresent(secret, s3SessionTokenEnv)) == 0:
		condition.Status = metav1.ConditionFalse
		condition.Reason = "SecretKeysMixed"
		condition.Message = fmt.Sprintf("Secret %q holds %q, %q and %q but not %q. The brokers use %q and %q without a session token, but the etcd snapshot jobs and the operator snapshot check use them with %q, which belongs to the other pair. Add %q or remove %q.", name, s3AccessKeyEnv, s3SecretKeyEnv, awsSessionTokenEnv, s3SessionTokenEnv, s3AccessKeyEnv, s3SecretKeyEnv, awsSessionTokenEnv, s3SessionTokenEnv, awsSessionTokenEnv)
	case hasAWS && hasKafscale:
		condition.Reason = "SecretKeysFound"
		condition.Message = fmt.Sprintf("Secret %q holds both pairs. The LFS proxy reads %q and %q; the brokers and the etcd snapshot jobs read %q and %q. Broker and LFS proxy pods restart to pick up changes to the credential keys.", name, awsAccessKeyEnv, awsSecretKeyEnv, s3AccessKeyEnv, s3SecretKeyEnv)
	case hasAWS && len(secretKeysWithValues(secret, s3AccessKeyEnv, s3SecretKeyEnv, s3SessionTokenEnv)) > 0:
		condition.Status = metav1.ConditionFalse
		condition.Reason = "SecretKeysMixed"
		condition.Message = fmt.Sprintf("Secret %q holds %q and %q and also %s without both %q and %q. The brokers, the LFS proxy and the etcd snapshot jobs use the %q and %q pair and ignore those keys. Remove them or complete the %q and %q pair.", name, awsAccessKeyEnv, awsSecretKeyEnv, quotedKeys(secretKeysWithValues(secret, s3AccessKeyEnv, s3SecretKeyEnv, s3SessionTokenEnv)), s3AccessKeyEnv, s3SecretKeyEnv, awsAccessKeyEnv, awsSecretKeyEnv, s3AccessKeyEnv, s3SecretKeyEnv)
	case hasAWS:
		condition.Reason = "SecretKeysFound"
		condition.Message = fmt.Sprintf("Secret %q holds %q and %q for the brokers, the LFS proxy and the etcd snapshot jobs. Broker and LFS proxy pods restart to pick up changes to the credential keys.", name, awsAccessKeyEnv, awsSecretKeyEnv)
	case hasKafscale:
		condition.Reason = "SecretKeysFound"
		condition.Message = fmt.Sprintf("Secret %q holds %q and %q for the brokers, the LFS proxy and the etcd snapshot jobs. Broker and LFS proxy pods restart to pick up changes to the credential keys.", name, s3AccessKeyEnv, s3SecretKeyEnv)
	default:
		condition.Status = metav1.ConditionFalse
		condition.Reason = "SecretKeysMissing"
		if found := secretCredentialKeys(secret); len(found) > 0 {
			condition.Message = fmt.Sprintf("Secret %q holds only part of a key pair (%s) and needs %q and %q or %q and %q. The brokers and the LFS proxy get no complete key pair from it and use the default AWS credential chain if one is available. The etcd snapshot jobs can fail instead, because the AWS CLI rejects an access key without a secret key.", name, quotedKeys(found), awsAccessKeyEnv, awsSecretKeyEnv, s3AccessKeyEnv, s3SecretKeyEnv)
		} else {
			condition.Message = fmt.Sprintf("Secret %q holds neither %q and %q nor %q and %q, so workloads get no static keys from it and use the default AWS credential chain if one is available.", name, awsAccessKeyEnv, awsSecretKeyEnv, s3AccessKeyEnv, s3SecretKeyEnv)
		}
	}
	return condition
}

func secretHasKeyPair(secret *corev1.Secret, accessKey, secretKey string) bool {
	return strings.TrimSpace(string(secret.Data[accessKey])) != "" && strings.TrimSpace(string(secret.Data[secretKey])) != ""
}

func secretCredentialKeys(secret *corev1.Secret) []string {
	return secretKeysWithValues(secret, awsAccessKeyEnv, awsSecretKeyEnv, s3AccessKeyEnv, s3SecretKeyEnv)
}

func secretKeysPresent(secret *corev1.Secret, keys ...string) []string {
	var present []string
	for _, key := range keys {
		if _, ok := secret.Data[key]; ok {
			present = append(present, key)
		}
	}
	return present
}

func secretKeysWithValues(secret *corev1.Secret, keys ...string) []string {
	var found []string
	for _, key := range keys {
		if strings.TrimSpace(string(secret.Data[key])) != "" {
			found = append(found, key)
		}
	}
	return found
}

func quotedKeys(keys []string) string {
	quoted := make([]string, len(keys))
	for i, key := range keys {
		quoted[i] = fmt.Sprintf("%q", key)
	}
	return strings.Join(quoted, ", ")
}
