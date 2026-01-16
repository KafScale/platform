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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
)

// TopicReconciler ensures topic metadata is reflected in the etcd snapshot.
type TopicReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Publisher *SnapshotPublisher
}

func NewTopicReconciler(mgr ctrl.Manager, publisher *SnapshotPublisher) *TopicReconciler {
	return &TopicReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Publisher: publisher,
	}
}

func (r *TopicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var topic kafscalev1alpha1.KafscaleTopic
	if err := r.Get(ctx, req.NamespacedName, &topic); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	var cluster kafscalev1alpha1.KafscaleCluster
	if err := r.Get(ctx, types.NamespacedName{
		Name:      topic.Spec.ClusterRef,
		Namespace: topic.Namespace,
	}, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	etcdResolution, err := EnsureEtcd(ctx, r.Client, r.Scheme, &cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Publisher.Publish(ctx, &cluster, etcdResolution.Endpoints); err != nil {
		setTopicCondition(&topic.Status.Conditions, metav1.Condition{
			Type:    "Ready",
			Status:  metav1.ConditionFalse,
			Reason:  "EtcdUnavailable",
			Message: "Topic metadata publish failed",
		})
		topic.Status.Phase = "EtcdUnavailable"
		if err := r.Status().Update(ctx, &topic); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: publishRequeueDelay}, nil
	}
	setTopicCondition(&topic.Status.Conditions, metav1.Condition{
		Type:    "Ready",
		Status:  metav1.ConditionTrue,
		Reason:  "Reconciled",
		Message: "Topic metadata published",
	})
	topic.Status.Phase = "Ready"
	if err := r.Status().Update(ctx, &topic); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *TopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kafscalev1alpha1.KafscaleTopic{}).
		Complete(r)
}

func setTopicCondition(conditions *[]metav1.Condition, condition metav1.Condition) {
	for i, existing := range *conditions {
		if existing.Type == condition.Type {
			(*conditions)[i] = condition
			return
		}
	}
	*conditions = append(*conditions, condition)
}
