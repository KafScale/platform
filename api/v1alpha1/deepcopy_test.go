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
	"encoding/json"
	"reflect"
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

func TestAffinityDeepCopyIsolation(t *testing.T) {
	affinity := &corev1.Affinity{
		NodeAffinity:    &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"storage"}}}}}}},
		PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: "kubernetes.io/hostname", LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "broker"}}}}},
	}
	original := &KafscaleCluster{Spec: KafscaleClusterSpec{Brokers: BrokerSpec{Affinity: affinity.DeepCopy()}, Etcd: EtcdSpec{Endpoints: []string{"http://etcd:2379"}, Affinity: affinity.DeepCopy()}}}
	snapshot, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	copied := original.DeepCopy()
	for _, a := range []*corev1.Affinity{copied.Spec.Brokers.Affinity, copied.Spec.Etcd.Affinity} {
		a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values[0] = "changed"
		a.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].LabelSelector.MatchLabels["app"] = "changed"
	}
	copied.Spec.Etcd.Endpoints[0] = "http://changed:2379"
	after, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if string(snapshot) != string(after) {
		t.Fatal("deep copy shares affinity or endpoint storage")
	}
	for _, affinity := range []*corev1.Affinity{nil, {}} {
		spec := &KafscaleClusterSpec{Brokers: BrokerSpec{Affinity: affinity}, Etcd: EtcdSpec{Affinity: affinity}}
		if !reflect.DeepEqual(spec, spec.DeepCopy()) {
			t.Fatal("deep copy lost nil/empty affinity distinction")
		}
	}
}

func TestAffinityJSONPresence(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		present       bool
	}{
		{"omitted", `{"brokers":{},"etcd":{}}`, false},
		{"null", `{"brokers":{"affinity":null},"etcd":{"affinity":null}}`, false},
		{"empty", `{"brokers":{"affinity":{}},"etcd":{"affinity":{}}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var spec KafscaleClusterSpec
			if err := json.Unmarshal([]byte(tc.payload), &spec); err != nil {
				t.Fatal(err)
			}
			if (spec.Brokers.Affinity != nil) != tc.present || (spec.Etcd.Affinity != nil) != tc.present {
				t.Fatal("lost affinity presence")
			}
			encoded, err := json.Marshal(spec)
			if err != nil {
				t.Fatal(err)
			}
			var roundTrip KafscaleClusterSpec
			if err := json.Unmarshal(encoded, &roundTrip); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(spec, roundTrip) {
				t.Fatal("JSON round-trip lost nil/empty distinction")
			}
		})
	}
}
