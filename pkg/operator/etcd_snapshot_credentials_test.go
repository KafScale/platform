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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/KafScale/platform/pkg/storage"
)

func TestSnapshotContainersAcceptEitherS3KeyPair(t *testing.T) {
	t.Setenv(operatorEtcdEndpointsEnv, "")
	cases := []struct {
		name string
		data map[string]string
	}{
		{name: "aws keys", data: map[string]string{"AWS_ACCESS_KEY_ID": "aws-ak", "AWS_SECRET_ACCESS_KEY": "aws-sk", "AWS_SESSION_TOKEN": "aws-tok"}},
		{name: "kafscale keys", data: map[string]string{"KAFSCALE_S3_ACCESS_KEY": "ks-ak", "KAFSCALE_S3_SECRET_KEY": "ks-sk", "KAFSCALE_S3_SESSION_TOKEN": "ks-tok"}},
		{name: "both pairs", data: map[string]string{"AWS_ACCESS_KEY_ID": "aws-ak", "AWS_SECRET_ACCESS_KEY": "aws-sk", "KAFSCALE_S3_ACCESS_KEY": "ks-ak", "KAFSCALE_S3_SECRET_KEY": "ks-sk"}},
		{name: "empty kafscale session token", data: map[string]string{"KAFSCALE_S3_ACCESS_KEY": "ks-ak", "KAFSCALE_S3_SECRET_KEY": "ks-sk", "KAFSCALE_S3_SESSION_TOKEN": "", "AWS_SESSION_TOKEN": "aws-tok"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := testCluster("snap-creds", nil)
			cluster.Spec.S3.CredentialsSecretRef = "s3-creds"
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3-creds", Namespace: "default"}, Data: map[string][]byte{}}
			for k, v := range tc.data {
				secret.Data[k] = []byte(v)
			}
			scheme := testScheme(t)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, secret).Build()
			if _, err := EnsureEtcd(context.Background(), c, scheme, cluster); err != nil {
				t.Fatalf("EnsureEtcd: %v", err)
			}
			sts := &appsv1.StatefulSet{}
			assertFound(t, c, sts, cluster.Namespace, cluster.Name+"-etcd")
			cron := &batchv1.CronJob{}
			assertFound(t, c, cron, cluster.Namespace, cluster.Name+"-etcd-snapshot")

			containers := map[string]corev1.Container{"upload": cron.Spec.JobTemplate.Spec.Template.Spec.Containers[0]}
			for _, ic := range sts.Spec.Template.Spec.InitContainers {
				if ic.Name == "snapshot-download" {
					containers["snapshot-download"] = ic
				}
			}
			if len(containers) != 2 {
				t.Fatalf("snapshot-download init container missing")
			}
			cfg := &storage.S3Config{}
			r := &ClusterReconciler{Client: c, Scheme: scheme}
			if err := r.loadS3Credentials(context.Background(), cluster, cfg); err != nil {
				t.Fatalf("loadS3Credentials: %v", err)
			}
			want := []struct{ key, value string }{
				{"AWS_ACCESS_KEY_ID", cfg.AccessKeyID},
				{"AWS_SECRET_ACCESS_KEY", cfg.SecretAccessKey},
				{"AWS_SESSION_TOKEN", cfg.SessionToken},
			}
			for _, name := range []string{"snapshot-download", "upload"} {
				env := resolveContainerEnvWithSecret(t, containers[name], secret)
				for _, w := range want {
					if env[w.key] != w.value {
						t.Errorf("%s %s = %q, want %q", name, w.key, env[w.key], w.value)
					}
				}
			}
		})
	}
}

func resolveContainerEnvWithSecret(t *testing.T, container corev1.Container, secret *corev1.Secret) map[string]string {
	t.Helper()
	data := map[string]string{}
	for k, v := range secret.Data {
		data[k] = string(v)
	}
	env := map[string]string{}
	for _, from := range container.EnvFrom {
		if from.SecretRef != nil && from.SecretRef.Name == secret.Name {
			for k, v := range data {
				env[from.Prefix+k] = v
			}
		}
	}
	for k, v := range resolveContainerEnv(t, container.Env, data) {
		env[k] = v
	}
	return env
}
