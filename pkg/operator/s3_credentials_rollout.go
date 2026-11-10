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
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	s3CredentialsChecksumAnnotation = "kafscale.io/s3-credentials-checksum"
	s3CredentialsChecksumIterations = 100000
)

var s3CredentialKeys = []string{awsAccessKeyEnv, awsSecretKeyEnv, awsSessionTokenEnv, s3AccessKeyEnv, s3SecretKeyEnv, s3SessionTokenEnv}

func (r *ClusterReconciler) s3CredentialsChecksum(ctx context.Context, cluster *kafscalev1alpha1.KafscaleCluster) (string, bool) {
	name := cluster.Spec.S3.CredentialsSecretRef
	if name == "" {
		return "", true
	}
	secret := &corev1.Secret{}
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: name}, secret); err != nil {
		return "", false
	}
	var material strings.Builder
	for _, key := range s3CredentialKeys {
		if val, ok := secret.Data[key]; ok {
			fmt.Fprintf(&material, "%s=%d:%s", key, len(val), val)
		}
	}
	salt := []byte(s3CredentialsChecksumAnnotation + "/" + cluster.Namespace + "/" + cluster.Name)
	derived, err := pbkdf2.Key(sha256.New, material.String(), salt, s3CredentialsChecksumIterations, sha256.Size)
	if err != nil {
		return "", false
	}
	return hex.EncodeToString(derived), true
}

func setS3CredentialsChecksum(template *corev1.PodTemplateSpec, checksum string, known bool) {
	if !known {
		return
	}
	if checksum == "" {
		delete(template.Annotations, s3CredentialsChecksumAnnotation)
		return
	}
	if template.Annotations == nil {
		template.Annotations = make(map[string]string)
	}
	template.Annotations[s3CredentialsChecksumAnnotation] = checksum
}
