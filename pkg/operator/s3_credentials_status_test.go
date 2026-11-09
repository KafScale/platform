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
	"bytes"
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
	"github.com/KafScale/platform/internal/testutil"
)

func TestClusterReconcilerReportsS3CredentialsSecret(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	t.Setenv(operatorEtcdEndpointsEnv, endpoints[0])
	t.Setenv(operatorEtcdSnapshotSkipPreflightEnv, "true")
	t.Setenv(operatorEtcdSilenceLogsEnv, "true")

	cases := []struct {
		name       string
		secretRef  string
		secret     map[string]string
		wantStatus metav1.ConditionStatus
		wantReason string
		wantWarn   bool
	}{
		{name: "no secret reference", wantStatus: metav1.ConditionTrue, wantReason: "SecretNotReferenced"},
		{name: "missing secret", secretRef: "s3-creds", wantStatus: metav1.ConditionFalse, wantReason: "SecretNotFound", wantWarn: true},
		{name: "secret without a key pair", secretRef: "s3-creds", secret: map[string]string{"AWS_ACCESS_KEY_ID": "ak", "KAFSCALE_S3_SECRET_KEY": "sk"}, wantStatus: metav1.ConditionFalse, wantReason: "SecretKeysMissing", wantWarn: true},
		{name: "kafscale keys", secretRef: "s3-creds", secret: map[string]string{"KAFSCALE_S3_ACCESS_KEY": "ak", "KAFSCALE_S3_SECRET_KEY": "sk"}, wantStatus: metav1.ConditionTrue, wantReason: "SecretKeysFound"},
		{name: "aws keys", secretRef: "s3-creds", secret: map[string]string{"AWS_ACCESS_KEY_ID": "ak", "AWS_SECRET_ACCESS_KEY": "sk"}, wantStatus: metav1.ConditionTrue, wantReason: "SecretKeysFound"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &kafscalev1alpha1.KafscaleCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
				Spec: kafscalev1alpha1.KafscaleClusterSpec{
					S3:       kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1", CredentialsSecretRef: tc.secretRef},
					LfsProxy: kafscalev1alpha1.LfsProxySpec{Enabled: true},
				},
			}
			objects := []client.Object{cluster}
			if tc.secret != nil {
				secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: tc.secretRef, Namespace: "default"}, Data: map[string][]byte{}}
				for k, v := range tc.secret {
					secret.Data[k] = []byte(v)
				}
				objects = append(objects, secret)
			}
			scheme := testScheme(t)
			if err := autoscalingv2.AddToScheme(scheme); err != nil {
				t.Fatalf("add autoscaling scheme: %v", err)
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(objects...).Build()
			r := &ClusterReconciler{Client: c, Scheme: scheme, Publisher: NewSnapshotPublisher(c)}

			var logs bytes.Buffer
			ctx := ctrl.LoggerInto(context.Background(), zap.New(zap.WriteTo(&logs)))
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}}
			for i := 0; i < 2; i++ {
				if _, err := r.Reconcile(ctx, req); err != nil {
					t.Fatalf("Reconcile %d: %v", i, err)
				}
			}

			assertFound(t, c, &appsv1.StatefulSet{}, "default", "demo-broker")
			assertFound(t, c, &appsv1.Deployment{}, "default", "demo-lfs-proxy")
			var got kafscalev1alpha1.KafscaleCluster
			if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
				t.Fatalf("get cluster: %v", err)
			}
			cond := meta.FindStatusCondition(got.Status.Conditions, "S3Credentials")
			if cond == nil || cond.Status != tc.wantStatus || cond.Reason != tc.wantReason {
				t.Fatalf("S3Credentials condition = %+v, want status %s reason %s", cond, tc.wantStatus, tc.wantReason)
			}
			if ready := meta.FindStatusCondition(got.Status.Conditions, "Ready"); ready == nil || ready.Status != metav1.ConditionTrue {
				t.Fatalf("Ready condition = %+v, want True", ready)
			}
			warnings := strings.Count(logs.String(), "S3 credentials secret is not usable")
			if tc.wantWarn && warnings != 1 {
				t.Fatalf("expected one warning over two reconciles, got %d:\n%s", warnings, logs.String())
			}
			if !tc.wantWarn && warnings != 0 {
				t.Fatalf("expected no warning, got %d:\n%s", warnings, logs.String())
			}
			if strings.Contains(logs.String(), `"ak"`) || strings.Contains(logs.String(), `"sk"`) {
				t.Fatalf("log contains secret values:\n%s", logs.String())
			}
		})
	}
}
