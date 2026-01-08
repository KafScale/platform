// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package v1alpha1

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBrokerResourcesDeepCopy(t *testing.T) {
	orig := &BrokerResources{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
	copy := orig.DeepCopy()
	if copy == orig {
		t.Fatalf("expected a deep copy")
	}
	copy.Requests[corev1.ResourceCPU] = resource.MustParse("1")
	origCPU := orig.Requests[corev1.ResourceCPU]
	copyCPU := copy.Requests[corev1.ResourceCPU]
	if (&origCPU).Cmp(copyCPU) == 0 {
		t.Fatalf("expected deep copy of Requests")
	}
}

func TestKafscaleClusterDeepCopy(t *testing.T) {
	orig := &KafscaleCluster{
		Spec: KafscaleClusterSpec{
			Brokers: BrokerSpec{AdvertisedHost: "broker.local"},
			S3:      S3Spec{Bucket: "bucket", Region: "us-east-1", CredentialsSecretRef: "secret"},
			Etcd:    EtcdSpec{Endpoints: []string{"http://127.0.0.1:2379"}},
		},
		Status: KafscaleClusterStatus{
			Phase: "Ready",
			Conditions: []metav1.Condition{{
				Type:   "EtcdSnapshotAccess",
				Status: metav1.ConditionTrue,
			}},
		},
	}
	copy := orig.DeepCopy()
	if copy == orig {
		t.Fatalf("expected deep copy")
	}
	copy.Status.Conditions[0].Status = metav1.ConditionFalse
	if orig.Status.Conditions[0].Status == copy.Status.Conditions[0].Status {
		t.Fatalf("expected deep copy of Conditions")
	}
	if copy.Spec.Brokers.AdvertisedHost != orig.Spec.Brokers.AdvertisedHost {
		t.Fatalf("expected broker host to match")
	}
}

func TestKafscaleTopicDeepCopy(t *testing.T) {
	orig := &KafscaleTopic{
		Spec: KafscaleTopicSpec{ClusterRef: "kafscale", Partitions: 3},
		Status: KafscaleTopicStatus{
			Phase: "Ready",
			Partitions: []TopicPartitionStatus{{
				ID:           0,
				Leader:       "broker-0",
				LogEndOffset: 12,
			}},
		},
	}
	copy := orig.DeepCopy()
	if copy == orig {
		t.Fatalf("expected deep copy")
	}
	copy.Status.Partitions[0].Leader = "broker-1"
	if orig.Status.Partitions[0].Leader == copy.Status.Partitions[0].Leader {
		t.Fatalf("expected deep copy of Partitions")
	}
}
