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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const testS3ChecksumAnnotation = "kafscale.io/s3-credentials-checksum"

func s3ChecksumReconciler(t *testing.T, ref string, data map[string]string) (*ClusterReconciler, *corev1.Secret, func() (string, bool)) {
	t.Helper()
	ctx := context.Background()
	scheme := testScheme(t)
	cluster := testCluster("s3roll", nil)
	cluster.Spec.S3.CredentialsSecretRef = ref
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3-creds", Namespace: "default"}, Data: map[string][]byte{}}
	for k, v := range data {
		secret.Data[k] = []byte(v)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, secret).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	reconcile := func() (string, bool) {
		t.Helper()
		if err := r.reconcileBrokerDeployment(ctx, cluster, nil); err != nil {
			t.Fatalf("reconcileBrokerDeployment: %v", err)
		}
		if err := r.reconcileLfsProxyDeployment(ctx, cluster, nil); err != nil {
			t.Fatalf("reconcileLfsProxyDeployment: %v", err)
		}
		sts := &appsv1.StatefulSet{}
		assertFound(t, c, sts, "default", "s3roll-broker")
		deploy := &appsv1.Deployment{}
		assertFound(t, c, deploy, "default", lfsProxyName(cluster))
		brokerSum, brokerOK := sts.Spec.Template.Annotations[testS3ChecksumAnnotation]
		proxySum, proxyOK := deploy.Spec.Template.Annotations[testS3ChecksumAnnotation]
		if brokerOK != proxyOK || brokerSum != proxySum {
			t.Fatalf("broker checksum %q (%v) and LFS proxy checksum %q (%v) differ", brokerSum, brokerOK, proxySum, proxyOK)
		}
		return brokerSum, brokerOK
	}
	return r, secret, reconcile
}

func TestS3CredentialsSecretRollsBrokerAndProxy(t *testing.T) {
	ctx := context.Background()
	r, secret, reconcile := s3ChecksumReconciler(t, "s3-creds", map[string]string{"KAFSCALE_S3_ACCESS_KEY": "ak", "KAFSCALE_S3_SECRET_KEY": "sk", "unrelated": "a"})
	update := func(mutate func(map[string][]byte)) {
		t.Helper()
		mutate(secret.Data)
		if err := r.Client.Update(ctx, secret); err != nil {
			t.Fatal(err)
		}
	}
	first, ok := reconcile()
	if !ok || first == "" {
		t.Fatalf("broker and LFS proxy pod templates have no %s annotation", testS3ChecksumAnnotation)
	}
	if again, _ := reconcile(); again != first {
		t.Fatalf("unchanged secret changed the checksum from %q to %q", first, again)
	}
	update(func(d map[string][]byte) { d["unrelated"] = []byte("b") })
	if got, _ := reconcile(); got != first {
		t.Fatalf("unrelated key changed the checksum from %q to %q", first, got)
	}
	update(func(d map[string][]byte) { d["KAFSCALE_S3_SECRET_KEY"] = []byte("sk2") })
	rotated, _ := reconcile()
	if rotated == first {
		t.Fatal("rotated KAFSCALE_S3_SECRET_KEY did not change the checksum")
	}
	update(func(d map[string][]byte) { d["AWS_SESSION_TOKEN"] = []byte("tok") })
	withToken, _ := reconcile()
	if withToken == rotated {
		t.Fatal("added AWS_SESSION_TOKEN did not change the checksum")
	}
	if err := r.Client.Delete(ctx, secret); err != nil {
		t.Fatal(err)
	}
	if got, ok := reconcile(); !ok || got != withToken {
		t.Fatalf("deleted secret changed the checksum from %q to %q (%v)", withToken, got, ok)
	}
}

func TestS3CredentialsChecksumOnlyWithSecretRef(t *testing.T) {
	_, _, reconcile := s3ChecksumReconciler(t, "", map[string]string{"KAFSCALE_S3_ACCESS_KEY": "ak", "KAFSCALE_S3_SECRET_KEY": "sk"})
	if got, ok := reconcile(); ok {
		t.Fatalf("cluster without credentialsSecretRef has checksum %q", got)
	}
	_, _, reconcile = s3ChecksumReconciler(t, "missing", nil)
	if got, ok := reconcile(); ok {
		t.Fatalf("cluster with a missing secret has checksum %q", got)
	}
}
