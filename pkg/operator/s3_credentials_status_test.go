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
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

	const chain = "use the default AWS credential chain if one is available."
	const partial = " and needs \"AWS_ACCESS_KEY_ID\" and \"AWS_SECRET_ACCESS_KEY\" or \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\". The brokers and the LFS proxy get no complete key pair from it and " + chain + " The etcd snapshot jobs can fail instead, because the AWS CLI rejects an access key without a secret key."
	cases := []struct {
		name        string
		secretRef   string
		secret      map[string]string
		secretName  string
		readErr     bool
		wantStatus  metav1.ConditionStatus
		wantReason  string
		wantMessage string
		wantWarn    bool
	}{
		{name: "no secret reference", wantStatus: metav1.ConditionTrue, wantReason: "SecretNotReferenced", wantMessage: "No spec.s3.credentialsSecretRef is set; workloads use the default AWS credential chain."},
		{name: "missing secret", secretRef: "s3-creds", wantStatus: metav1.ConditionFalse, wantReason: "SecretNotFound", wantWarn: true, wantMessage: "Secret \"s3-creds\" does not exist, so workloads get no static keys from it and " + chain},
		{name: "unreadable secret", secretRef: "s3-creds", readErr: true, wantStatus: metav1.ConditionFalse, wantReason: "SecretReadFailed", wantWarn: true, wantMessage: "The operator could not read or verify secret \"s3-creds\": secret read denied. It cannot tell which keys the workloads receive."},
		{name: "secret without credential keys", secretRef: "s3-creds", secret: map[string]string{"KAFSCALE_S3_SESSION_TOKEN": "tok", "AWS_ACCESS_KEY_ID": "  "}, wantStatus: metav1.ConditionFalse, wantReason: "SecretKeysMissing", wantWarn: true, wantMessage: "Secret \"s3-creds\" holds neither \"AWS_ACCESS_KEY_ID\" and \"AWS_SECRET_ACCESS_KEY\" nor \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\", so workloads get no static keys from it and " + chain},
		{name: "secret with only an access key", secretRef: "s3-creds", secret: map[string]string{"KAFSCALE_S3_ACCESS_KEY": "ak"}, wantStatus: metav1.ConditionFalse, wantReason: "SecretKeysMissing", wantWarn: true, wantMessage: "Secret \"s3-creds\" holds only part of a key pair (\"KAFSCALE_S3_ACCESS_KEY\")" + partial},
		{name: "secret without a key pair", secretRef: "s3-creds", secret: map[string]string{"AWS_ACCESS_KEY_ID": "ak", "KAFSCALE_S3_SECRET_KEY": "sk"}, wantStatus: metav1.ConditionFalse, wantReason: "SecretKeysMissing", wantWarn: true, wantMessage: "Secret \"s3-creds\" holds only part of a key pair (\"AWS_ACCESS_KEY_ID\", \"KAFSCALE_S3_SECRET_KEY\")" + partial},
		{name: "padded secret name", secretRef: " s3-creds ", secret: map[string]string{"KAFSCALE_S3_ACCESS_KEY": "ak", "KAFSCALE_S3_SECRET_KEY": "sk"}, secretName: "s3-creds", wantStatus: metav1.ConditionFalse, wantReason: "SecretNotFound", wantWarn: true, wantMessage: "Secret \" s3-creds \" does not exist, so workloads get no static keys from it and " + chain},
		{name: "whitespace secret name", secretRef: "   ", wantStatus: metav1.ConditionTrue, wantReason: "SecretNotReferenced", wantMessage: "No spec.s3.credentialsSecretRef is set; workloads use the default AWS credential chain."},
		{name: "kafscale keys", secretRef: "s3-creds", secret: map[string]string{"KAFSCALE_S3_ACCESS_KEY": "ak", "KAFSCALE_S3_SECRET_KEY": "sk"}, wantStatus: metav1.ConditionTrue, wantReason: "SecretKeysFound", wantMessage: "Secret \"s3-creds\" holds \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\" for the brokers, the LFS proxy and the etcd snapshot jobs. Broker and LFS proxy pods restart to pick up changes to the credential keys."},
		{name: "aws keys", secretRef: "s3-creds", secret: map[string]string{"AWS_ACCESS_KEY_ID": "ak", "AWS_SECRET_ACCESS_KEY": "sk"}, wantStatus: metav1.ConditionTrue, wantReason: "SecretKeysFound", wantMessage: "Secret \"s3-creds\" holds \"AWS_ACCESS_KEY_ID\" and \"AWS_SECRET_ACCESS_KEY\" for the brokers, the LFS proxy and the etcd snapshot jobs. Broker and LFS proxy pods restart to pick up changes to the credential keys."},
		{name: "aws pair with a partial kafscale pair", secretRef: "s3-creds", secret: map[string]string{"AWS_ACCESS_KEY_ID": "ak", "AWS_SECRET_ACCESS_KEY": "sk", "KAFSCALE_S3_ACCESS_KEY": "ak2"}, wantStatus: metav1.ConditionFalse, wantReason: "SecretKeysMixed", wantWarn: true, wantMessage: "Secret \"s3-creds\" holds \"AWS_ACCESS_KEY_ID\" and \"AWS_SECRET_ACCESS_KEY\" and also \"KAFSCALE_S3_ACCESS_KEY\" without both \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\". The brokers, the LFS proxy and the etcd snapshot jobs use the \"AWS_ACCESS_KEY_ID\" and \"AWS_SECRET_ACCESS_KEY\" pair and ignore those keys. Remove them or complete the \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\" pair."},
		{name: "aws pair with a kafscale session token", secretRef: "s3-creds", secret: map[string]string{"AWS_ACCESS_KEY_ID": "ak", "AWS_SECRET_ACCESS_KEY": "sk", "KAFSCALE_S3_SESSION_TOKEN": "tok"}, wantStatus: metav1.ConditionFalse, wantReason: "SecretKeysMixed", wantWarn: true, wantMessage: "Secret \"s3-creds\" holds \"AWS_ACCESS_KEY_ID\" and \"AWS_SECRET_ACCESS_KEY\" and also \"KAFSCALE_S3_SESSION_TOKEN\" without both \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\". The brokers, the LFS proxy and the etcd snapshot jobs use the \"AWS_ACCESS_KEY_ID\" and \"AWS_SECRET_ACCESS_KEY\" pair and ignore those keys. Remove them or complete the \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\" pair."},
		{name: "aws pair with an empty kafscale secret key", secretRef: "s3-creds", secret: map[string]string{"AWS_ACCESS_KEY_ID": "ak", "AWS_SECRET_ACCESS_KEY": "sk", "KAFSCALE_S3_SECRET_KEY": ""}, wantStatus: metav1.ConditionTrue, wantReason: "SecretKeysFound", wantMessage: "Secret \"s3-creds\" holds \"AWS_ACCESS_KEY_ID\" and \"AWS_SECRET_ACCESS_KEY\" for the brokers, the LFS proxy and the etcd snapshot jobs. Broker and LFS proxy pods restart to pick up changes to the credential keys."},
		{name: "aws pair with whitespace kafscale keys", secretRef: "s3-creds", secret: map[string]string{"AWS_ACCESS_KEY_ID": "ak", "AWS_SECRET_ACCESS_KEY": "sk", "KAFSCALE_S3_ACCESS_KEY": "  ", "KAFSCALE_S3_SECRET_KEY": "\t", "KAFSCALE_S3_SESSION_TOKEN": " "}, wantStatus: metav1.ConditionTrue, wantReason: "SecretKeysFound", wantMessage: "Secret \"s3-creds\" holds \"AWS_ACCESS_KEY_ID\" and \"AWS_SECRET_ACCESS_KEY\" for the brokers, the LFS proxy and the etcd snapshot jobs. Broker and LFS proxy pods restart to pick up changes to the credential keys."},
		{name: "aws pair with a kafscale access key and whitespace secret key", secretRef: "s3-creds", secret: map[string]string{"AWS_ACCESS_KEY_ID": "ak", "AWS_SECRET_ACCESS_KEY": "sk", "KAFSCALE_S3_ACCESS_KEY": "ak2", "KAFSCALE_S3_SECRET_KEY": " "}, wantStatus: metav1.ConditionFalse, wantReason: "SecretKeysMixed", wantWarn: true, wantMessage: "Secret \"s3-creds\" holds \"AWS_ACCESS_KEY_ID\" and \"AWS_SECRET_ACCESS_KEY\" and also \"KAFSCALE_S3_ACCESS_KEY\" without both \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\". The brokers, the LFS proxy and the etcd snapshot jobs use the \"AWS_ACCESS_KEY_ID\" and \"AWS_SECRET_ACCESS_KEY\" pair and ignore those keys. Remove them or complete the \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\" pair."},
		{name: "kafscale pair with an aws session token", secretRef: "s3-creds", secret: map[string]string{"KAFSCALE_S3_ACCESS_KEY": "ak", "KAFSCALE_S3_SECRET_KEY": "sk", "AWS_SESSION_TOKEN": "tok"}, wantStatus: metav1.ConditionFalse, wantReason: "SecretKeysMixed", wantWarn: true, wantMessage: "Secret \"s3-creds\" holds \"KAFSCALE_S3_ACCESS_KEY\", \"KAFSCALE_S3_SECRET_KEY\" and \"AWS_SESSION_TOKEN\" but not \"KAFSCALE_S3_SESSION_TOKEN\". The brokers use \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\" without a session token, but the etcd snapshot jobs and the operator snapshot check use them with \"AWS_SESSION_TOKEN\", which belongs to the other pair. Add \"KAFSCALE_S3_SESSION_TOKEN\" or remove \"AWS_SESSION_TOKEN\"."},
		{name: "both pairs with an aws session token", secretRef: "s3-creds", secret: map[string]string{"AWS_ACCESS_KEY_ID": "ak", "AWS_SECRET_ACCESS_KEY": "sk", "KAFSCALE_S3_ACCESS_KEY": "ak", "KAFSCALE_S3_SECRET_KEY": "sk", "AWS_SESSION_TOKEN": "tok"}, wantStatus: metav1.ConditionFalse, wantReason: "SecretKeysMixed", wantWarn: true, wantMessage: "Secret \"s3-creds\" holds \"KAFSCALE_S3_ACCESS_KEY\", \"KAFSCALE_S3_SECRET_KEY\" and \"AWS_SESSION_TOKEN\" but not \"KAFSCALE_S3_SESSION_TOKEN\". The brokers use \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\" without a session token, but the etcd snapshot jobs and the operator snapshot check use them with \"AWS_SESSION_TOKEN\", which belongs to the other pair. Add \"KAFSCALE_S3_SESSION_TOKEN\" or remove \"AWS_SESSION_TOKEN\"."},
		{name: "kafscale pair with both session tokens", secretRef: "s3-creds", secret: map[string]string{"KAFSCALE_S3_ACCESS_KEY": "ak", "KAFSCALE_S3_SECRET_KEY": "sk", "KAFSCALE_S3_SESSION_TOKEN": "tok", "AWS_SESSION_TOKEN": "tok2"}, wantStatus: metav1.ConditionTrue, wantReason: "SecretKeysFound", wantMessage: "Secret \"s3-creds\" holds \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\" for the brokers, the LFS proxy and the etcd snapshot jobs. Broker and LFS proxy pods restart to pick up changes to the credential keys."},
		{name: "both pairs with a kafscale session token", secretRef: "s3-creds", secret: map[string]string{"AWS_ACCESS_KEY_ID": "ak", "AWS_SECRET_ACCESS_KEY": "sk", "KAFSCALE_S3_ACCESS_KEY": "ak", "KAFSCALE_S3_SECRET_KEY": "sk", "KAFSCALE_S3_SESSION_TOKEN": "tok", "AWS_SESSION_TOKEN": "tok2"}, wantStatus: metav1.ConditionTrue, wantReason: "SecretKeysFound", wantMessage: "Secret \"s3-creds\" holds both pairs. The LFS proxy reads \"AWS_ACCESS_KEY_ID\" and \"AWS_SECRET_ACCESS_KEY\"; the brokers and the etcd snapshot jobs read \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\". Broker and LFS proxy pods restart to pick up changes to the credential keys."},
		{name: "aws pair with no-break space kafscale keys", secretRef: "s3-creds", secret: map[string]string{"AWS_ACCESS_KEY_ID": "ak", "AWS_SECRET_ACCESS_KEY": "sk", "KAFSCALE_S3_ACCESS_KEY": "\u00a0", "KAFSCALE_S3_SECRET_KEY": "\u00a0"}, wantStatus: metav1.ConditionTrue, wantReason: "SecretKeysFound", wantMessage: "Secret \"s3-creds\" holds both pairs. The LFS proxy reads \"AWS_ACCESS_KEY_ID\" and \"AWS_SECRET_ACCESS_KEY\"; the brokers and the etcd snapshot jobs read \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\". Broker and LFS proxy pods restart to pick up changes to the credential keys."},
		{name: "both pairs", secretRef: "s3-creds", secret: map[string]string{"AWS_ACCESS_KEY_ID": "ak", "AWS_SECRET_ACCESS_KEY": "sk", "KAFSCALE_S3_ACCESS_KEY": "ak", "KAFSCALE_S3_SECRET_KEY": "sk"}, wantStatus: metav1.ConditionTrue, wantReason: "SecretKeysFound", wantMessage: "Secret \"s3-creds\" holds both pairs. The LFS proxy reads \"AWS_ACCESS_KEY_ID\" and \"AWS_SECRET_ACCESS_KEY\"; the brokers and the etcd snapshot jobs read \"KAFSCALE_S3_ACCESS_KEY\" and \"KAFSCALE_S3_SECRET_KEY\". Broker and LFS proxy pods restart to pick up changes to the credential keys."},
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
				secretName := tc.secretRef
				if tc.secretName != "" {
					secretName = tc.secretName
				}
				secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: "default"}, Data: map[string][]byte{}}
				for k, v := range tc.secret {
					secret.Data[k] = []byte(v)
				}
				objects = append(objects, secret)
			}
			scheme := testScheme(t)
			if err := autoscalingv2.AddToScheme(scheme); err != nil {
				t.Fatalf("add autoscaling scheme: %v", err)
			}
			builder := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(objects...)
			if tc.readErr {
				builder = builder.WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if _, ok := obj.(*corev1.Secret); ok {
							return errors.New("secret read denied")
						}
						return cl.Get(ctx, key, obj, opts...)
					},
				})
			}
			c := builder.Build()
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
			if cond.Message != tc.wantMessage {
				t.Fatalf("S3Credentials message = %q, want %q", cond.Message, tc.wantMessage)
			}
			if strings.Contains(cond.Message, "fall back") {
				t.Fatalf("S3Credentials message claims a fallback: %q", cond.Message)
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

func TestClusterReconcilerPersistsS3CredentialsOnFailedReconcile(t *testing.T) {
	t.Setenv(operatorEtcdEndpointsEnv, "")
	t.Setenv(operatorEtcdSnapshotSkipPreflightEnv, "true")
	t.Setenv(operatorEtcdSilenceLogsEnv, "true")

	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			S3: kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1", CredentialsSecretRef: "s3-creds"},
		},
	}
	scheme := testScheme(t)
	if err := autoscalingv2.AddToScheme(scheme); err != nil {
		t.Fatalf("add autoscaling scheme: %v", err)
	}
	brokerErr := errors.New("broker statefulset create failed")
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*appsv1.StatefulSet); ok && obj.GetName() == "demo-broker" {
				return brokerErr
			}
			return cl.Create(ctx, obj, opts...)
		},
	}).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme, Publisher: NewSnapshotPublisher(c)}

	var logs bytes.Buffer
	ctx := ctrl.LoggerInto(context.Background(), zap.New(zap.WriteTo(&logs)))
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}}
	reconcileFails := func(step string) {
		t.Helper()
		if _, err := r.Reconcile(ctx, req); !errors.Is(err, brokerErr) {
			t.Fatalf("Reconcile %s: err = %v, want %v", step, err, brokerErr)
		}
	}
	persisted := func() *metav1.Condition {
		t.Helper()
		var got kafscalev1alpha1.KafscaleCluster
		if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
			t.Fatalf("get cluster: %v", err)
		}
		return meta.FindStatusCondition(got.Status.Conditions, "S3Credentials")
	}

	reconcileFails("with a missing secret")
	reconcileFails("again with a missing secret")
	if cond := persisted(); cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "SecretNotFound" {
		t.Fatalf("S3Credentials condition after failed reconciles = %+v, want False SecretNotFound", cond)
	}
	if warnings := strings.Count(logs.String(), "S3 credentials secret is not usable"); warnings != 1 {
		t.Fatalf("expected one warning over two failed reconciles, got %d:\n%s", warnings, logs.String())
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3-creds", Namespace: "default"},
		Data:       map[string][]byte{"KAFSCALE_S3_ACCESS_KEY": []byte("ak"), "KAFSCALE_S3_SECRET_KEY": []byte("sk")},
	}
	if err := c.Create(context.Background(), secret); err != nil {
		t.Fatalf("create secret: %v", err)
	}
	reconcileFails("after fixing the secret")
	if cond := persisted(); cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "SecretKeysFound" {
		t.Fatalf("S3Credentials condition after fixing the secret = %+v, want True SecretKeysFound", cond)
	}
}

func TestClusterReconcilerPersistsS3CredentialsWhenSetupFails(t *testing.T) {
	setupErr := errors.New("setup failed")
	cases := []struct {
		name  string
		funcs interceptor.Funcs
	}{
		{name: "network policy", funcs: interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*networkingv1.NetworkPolicy); ok {
					return setupErr
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		}},
		{name: "managed etcd", funcs: interceptor.Funcs{
			Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
				return setupErr
			},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(operatorEtcdEndpointsEnv, "")
			t.Setenv(operatorEtcdSnapshotSkipPreflightEnv, "true")
			cluster := &kafscalev1alpha1.KafscaleCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
				Spec: kafscalev1alpha1.KafscaleClusterSpec{
					S3: kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1", CredentialsSecretRef: "s3-creds"},
				},
			}
			scheme := testScheme(t)
			if err := autoscalingv2.AddToScheme(scheme); err != nil {
				t.Fatalf("add autoscaling scheme: %v", err)
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).WithInterceptorFuncs(tc.funcs).Build()
			r := &ClusterReconciler{Client: c, Scheme: scheme, Publisher: NewSnapshotPublisher(c)}

			var logs bytes.Buffer
			ctx := ctrl.LoggerInto(context.Background(), zap.New(zap.WriteTo(&logs)))
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}}
			for i := 0; i < 2; i++ {
				if _, err := r.Reconcile(ctx, req); !errors.Is(err, setupErr) {
					t.Fatalf("Reconcile %d: err = %v, want %v", i, err, setupErr)
				}
			}
			var got kafscalev1alpha1.KafscaleCluster
			if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
				t.Fatalf("get cluster: %v", err)
			}
			if cond := meta.FindStatusCondition(got.Status.Conditions, "S3Credentials"); cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "SecretNotFound" {
				t.Fatalf("S3Credentials condition after failed setup = %+v, want False SecretNotFound", cond)
			}
			if warnings := strings.Count(logs.String(), "S3 credentials secret is not usable"); warnings != 1 {
				t.Fatalf("expected one warning over two failed reconciles, got %d:\n%s", warnings, logs.String())
			}
		})
	}
}

func TestClusterReconcilerFailurePathKeepsConcurrentStatusWrite(t *testing.T) {
	t.Setenv(operatorEtcdEndpointsEnv, "")
	t.Setenv(operatorEtcdSnapshotSkipPreflightEnv, "true")
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			S3: kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1", CredentialsSecretRef: "s3-creds"},
		},
	}
	scheme := testScheme(t)
	if err := autoscalingv2.AddToScheme(scheme); err != nil {
		t.Fatalf("add autoscaling scheme: %v", err)
	}
	brokerErr := errors.New("broker statefulset create failed")
	key := types.NamespacedName{Name: "demo", Namespace: "default"}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*appsv1.StatefulSet); !ok || obj.GetName() != "demo-broker" {
				return cl.Create(ctx, obj, opts...)
			}
			var latest kafscalev1alpha1.KafscaleCluster
			if err := cl.Get(ctx, key, &latest); err != nil {
				return err
			}
			meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{Type: "ConcurrentWrite", Status: metav1.ConditionTrue, Reason: "WrittenElsewhere", Message: "written during the reconcile"})
			if err := cl.Status().Update(ctx, &latest); err != nil {
				return err
			}
			return brokerErr
		},
	}).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme, Publisher: NewSnapshotPublisher(c)}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key}); !errors.Is(err, brokerErr) {
		t.Fatalf("Reconcile: err = %v, want %v", err, brokerErr)
	}
	var got kafscalev1alpha1.KafscaleCluster
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, "ConcurrentWrite"); cond == nil || cond.Reason != "WrittenElsewhere" {
		t.Fatalf("status written during the reconcile was overwritten: conditions = %+v", got.Status.Conditions)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, "S3Credentials"); cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "SecretNotFound" {
		t.Fatalf("S3Credentials was not persisted next to the concurrent write: conditions = %+v", got.Status.Conditions)
	}
}
