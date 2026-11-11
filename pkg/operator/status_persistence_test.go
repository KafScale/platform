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
	"errors"
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
)

func TestFailurePathStatusSkipsChangedCluster(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		name := "new spec generation"
		if recreate {
			name = "recreated resource"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv(operatorEtcdEndpointsEnv, "")
			t.Setenv(operatorEtcdSnapshotSkipPreflightEnv, "true")
			cluster := testCluster("stale-status", nil)
			cluster.UID = types.UID("original")
			cluster.Generation = 1
			cluster.Spec.S3.CredentialsSecretRef = "old-missing-secret"
			scheme := testScheme(t)
			key := client.ObjectKeyFromObject(cluster)
			setupErr := errors.New("network policy read failed")
			mutated := false
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, requestedKey client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*networkingv1.NetworkPolicy); !ok {
						return cl.Get(ctx, requestedKey, obj, opts...)
					}
					latest := &kafscalev1alpha1.KafscaleCluster{}
					if err := cl.Get(ctx, key, latest); err != nil {
						return err
					}
					if recreate {
						if err := cl.Delete(ctx, latest); err != nil {
							return err
						}
						latest.ResourceVersion = ""
						latest.UID = types.UID("replacement")
					} else {
						latest.Generation++
					}
					latest.Spec.S3.CredentialsSecretRef = ""
					if recreate {
						if err := cl.Create(ctx, latest); err != nil {
							return err
						}
					} else if err := cl.Update(ctx, latest); err != nil {
						return err
					}
					latest.Status.Conditions = nil
					meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "CurrentSpec", Message: "belongs to the current object"})
					if err := cl.Status().Update(ctx, latest); err != nil {
						return err
					}
					mutated = true
					return setupErr
				},
			}).Build()
			r := &ClusterReconciler{Client: c, Scheme: scheme, Publisher: NewSnapshotPublisher(c)}
			if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key}); !errors.Is(err, setupErr) {
				t.Fatalf("Reconcile = %v, want %v", err, setupErr)
			}
			if !mutated {
				t.Fatal("concurrent resource change was not exercised")
			}
			latest := &kafscalev1alpha1.KafscaleCluster{}
			if err := c.Get(context.Background(), key, latest); err != nil {
				t.Fatal(err)
			}
			if recreate && latest.UID == cluster.UID {
				t.Fatal("resource was not recreated")
			}
			if !recreate && latest.Generation == cluster.Generation {
				t.Fatal("generation was not advanced")
			}
			if cond := meta.FindStatusCondition(latest.Status.Conditions, "S3Credentials"); cond != nil {
				t.Fatalf("stale S3 condition applied to changed cluster: %+v", cond)
			}
			if cond := meta.FindStatusCondition(latest.Status.Conditions, "Ready"); cond == nil || cond.Reason != "CurrentSpec" {
				t.Fatalf("current status was lost: %+v", latest.Status.Conditions)
			}
		})
	}
}

func TestFailurePathStatusRechecksSpecOnConflict(t *testing.T) {
	for _, changeSpec := range []bool{false, true} {
		name := "concurrent status only"
		if changeSpec {
			name = "spec changed during status retry"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cluster := testCluster("status-conflict", nil)
			cluster.UID = types.UID("same-resource")
			cluster.Generation = 1
			cluster.Spec.S3.CredentialsSecretRef = "missing-secret"
			key := client.ObjectKeyFromObject(cluster)
			scheme := testScheme(t)
			writes := 0
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).WithInterceptorFuncs(interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, cl client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if subresource != "status" {
						return cl.SubResource(subresource).Update(ctx, obj, opts...)
					}
					writes++
					if writes == 1 {
						latest := &kafscalev1alpha1.KafscaleCluster{}
						if err := cl.Get(ctx, key, latest); err != nil {
							return err
						}
						if changeSpec {
							latest.Generation++
							latest.Spec.S3.CredentialsSecretRef = ""
							if err := cl.Update(ctx, latest); err != nil {
								return err
							}
						}
						meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{Type: "ConcurrentWrite", Status: metav1.ConditionTrue, Reason: "Preserved", Message: "fresh status"})
						if err := cl.Status().Update(ctx, latest); err != nil {
							return err
						}
						return apierrors.NewConflict(schema.GroupResource{Group: "kafscale.io", Resource: "kafscaleclusters"}, obj.GetName(), errors.New("simulated concurrent write"))
					}
					return cl.Status().Update(ctx, obj, opts...)
				},
			}).Build()
			r := &ClusterReconciler{Client: c, Scheme: scheme}
			original := &kafscalev1alpha1.KafscaleCluster{}
			if err := c.Get(ctx, key, original); err != nil {
				t.Fatal(err)
			}
			initial := append([]metav1.Condition(nil), original.Status.Conditions...)
			r.populateS3CredentialsStatus(ctx, original)
			r.persistStatusAfterFailure(ctx, original, initial)
			latest := &kafscalev1alpha1.KafscaleCluster{}
			if err := c.Get(ctx, key, latest); err != nil {
				t.Fatal(err)
			}
			if condition := meta.FindStatusCondition(latest.Status.Conditions, "ConcurrentWrite"); condition == nil || condition.Reason != "Preserved" {
				t.Fatalf("concurrent status was lost: %+v", latest.Status.Conditions)
			}
			condition := meta.FindStatusCondition(latest.Status.Conditions, "S3Credentials")
			if changeSpec {
				if writes != 1 || condition != nil {
					t.Fatalf("stale retry wrote status: attempts=%d, condition=%+v", writes, condition)
				}
			} else if writes != 2 || condition == nil || condition.Reason != "SecretNotFound" {
				t.Fatalf("status conflict was not retried: attempts=%d, condition=%+v", writes, condition)
			}
		})
	}
}
