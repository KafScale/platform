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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
)

func resolveContainerEnv(t *testing.T, env []corev1.EnvVar, secret map[string]string) map[string]string {
	t.Helper()
	resolved := make(map[string]string)
	for _, e := range env {
		if e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
			resolved[e.Name] = e.Value
			continue
		}
		ref := e.ValueFrom.SecretKeyRef
		val, ok := secret[ref.Key]
		if !ok {
			if ref.Optional != nil && *ref.Optional {
				continue
			}
			t.Fatalf("pod cannot start: env %s needs key %s, which secret %s does not have", e.Name, ref.Key, ref.Name)
		}
		resolved[e.Name] = val
	}
	return resolved
}

func lfsProxyEffectiveS3Credentials(env map[string]string) (string, string) {
	if env["KAFSCALE_LFS_PROXY_S3_ACCESS_KEY"] != "" && env["KAFSCALE_LFS_PROXY_S3_SECRET_KEY"] != "" {
		return env["KAFSCALE_LFS_PROXY_S3_ACCESS_KEY"], env["KAFSCALE_LFS_PROXY_S3_SECRET_KEY"]
	}
	return env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"]
}

func TestLfsProxyContainerResolvesS3CredentialSecretKeys(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			S3: kafscalev1alpha1.S3Spec{
				Bucket:               "bucket",
				Region:               "us-east-1",
				CredentialsSecretRef: "s3-creds",
			},
			LfsProxy: kafscalev1alpha1.LfsProxySpec{Enabled: true},
		},
	}
	r := &ClusterReconciler{}
	container := r.lfsProxyContainer(cluster, []string{"http://etcd:2379"})

	cases := []struct {
		name               string
		secret             map[string]string
		wantKey, wantValue string
	}{
		{
			name:      "kafscale keys",
			secret:    map[string]string{"KAFSCALE_S3_ACCESS_KEY": "kafscale-ak", "KAFSCALE_S3_SECRET_KEY": "kafscale-sk"},
			wantKey:   "kafscale-ak",
			wantValue: "kafscale-sk",
		},
		{
			name:      "aws keys",
			secret:    map[string]string{"AWS_ACCESS_KEY_ID": "aws-ak", "AWS_SECRET_ACCESS_KEY": "aws-sk"},
			wantKey:   "aws-ak",
			wantValue: "aws-sk",
		},
		{
			name: "both keys keep the aws pair",
			secret: map[string]string{
				"AWS_ACCESS_KEY_ID": "aws-ak", "AWS_SECRET_ACCESS_KEY": "aws-sk",
				"KAFSCALE_S3_ACCESS_KEY": "kafscale-ak", "KAFSCALE_S3_SECRET_KEY": "kafscale-sk",
			},
			wantKey:   "aws-ak",
			wantValue: "aws-sk",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := resolveContainerEnv(t, container.Env, tc.secret)
			ak, sk := lfsProxyEffectiveS3Credentials(env)
			if ak != tc.wantKey || sk != tc.wantValue {
				t.Fatalf("effective S3 credentials = %q/%q, want %q/%q", ak, sk, tc.wantKey, tc.wantValue)
			}
		})
	}
}
