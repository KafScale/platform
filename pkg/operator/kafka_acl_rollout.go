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
	"crypto/sha256"
	"encoding/hex"
	"errors"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const aclChecksumAnnotation = "kafscale.io/acl-checksum"

func (r *ClusterReconciler) kafkaACLChecksum(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster) (string, error) {
	if cluster.Spec.Security.ACLSecretRef == "" {
		return "", nil
	}
	secret := &corev1.Secret{}
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Spec.Security.ACLSecretRef}, secret); err != nil {
		return "", err
	}
	data, ok := secret.Data["acl.json"]
	if !ok {
		return "", errors.New("ACL Secret requires acl.json")
	}
	checksum := sha256.Sum256(data)
	return hex.EncodeToString(checksum[:]), nil
}

func setACLChecksum(template *corev1.PodTemplateSpec, checksum string) {
	if checksum == "" {
		delete(template.Annotations, aclChecksumAnnotation)
		return
	}
	if template.Annotations == nil {
		template.Annotations = make(map[string]string)
	}
	template.Annotations[aclChecksumAnnotation] = checksum
}

func (r *ClusterReconciler) clustersForACLSecret(ctx context.Context, secret client.Object) []reconcile.Request {
	var clusters kafscalev1alpha1.KafscaleClusterList
	if err := r.Client.List(ctx, &clusters, client.InNamespace(secret.GetNamespace())); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "cannot find clusters using ACL Secret")
		return nil
	}
	var requests []reconcile.Request
	for _, cluster := range clusters.Items {
		if cluster.Spec.Security.ACLSecretRef == secret.GetName() {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&cluster)})
		}
	}
	return requests
}
