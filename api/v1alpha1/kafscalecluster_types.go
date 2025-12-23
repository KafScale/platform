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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// KafscaleClusterSpec defines the desired state of a Kafscale cluster.
type KafscaleClusterSpec struct {
	Brokers BrokerSpec        `json:"brokers"`
	S3      S3Spec            `json:"s3"`
	Etcd    EtcdSpec          `json:"etcd"`
	Config  ClusterConfigSpec `json:"config,omitempty"`
}

type BrokerSpec struct {
	Replicas  *int32          `json:"replicas,omitempty"`
	Resources BrokerResources `json:"resources,omitempty"`
}

type BrokerResources struct {
	Requests corev1.ResourceList `json:"requests,omitempty"`
	Limits   corev1.ResourceList `json:"limits,omitempty"`
}

type S3Spec struct {
	Bucket               string `json:"bucket"`
	Region               string `json:"region"`
	Endpoint             string `json:"endpoint,omitempty"`
	ReadBucket           string `json:"readBucket,omitempty"`
	ReadRegion           string `json:"readRegion,omitempty"`
	ReadEndpoint         string `json:"readEndpoint,omitempty"`
	KMSKeyARN            string `json:"kmsKeyArn,omitempty"`
	CredentialsSecretRef string `json:"credentialsSecretRef"`
}

type EtcdSpec struct {
	Endpoints []string `json:"endpoints"`
}

type ClusterConfigSpec struct {
	SegmentBytes    int32  `json:"segmentBytes,omitempty"`
	FlushIntervalMs int32  `json:"flushIntervalMs,omitempty"`
	CacheSize       string `json:"cacheSize,omitempty"`
}

// KafscaleClusterStatus captures observed state.
type KafscaleClusterStatus struct {
	Phase      string             `json:"phase,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// KafscaleCluster is the Schema for the kafscaleclusters API.
type KafscaleCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafscaleClusterSpec   `json:"spec,omitempty"`
	Status KafscaleClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// KafscaleClusterList contains a list of KafscaleCluster.
type KafscaleClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafscaleCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KafscaleCluster{}, &KafscaleClusterList{})
}

func (in *BrokerResources) DeepCopyInto(out *BrokerResources) {
	*out = *in
	if in.Requests != nil {
		out.Requests = make(corev1.ResourceList, len(in.Requests))
		for key, val := range in.Requests {
			out.Requests[key] = val.DeepCopy()
		}
	}
	if in.Limits != nil {
		out.Limits = make(corev1.ResourceList, len(in.Limits))
		for key, val := range in.Limits {
			out.Limits[key] = val.DeepCopy()
		}
	}
}

func (in *BrokerResources) DeepCopy() *BrokerResources {
	if in == nil {
		return nil
	}
	out := new(BrokerResources)
	in.DeepCopyInto(out)
	return out
}

func (in *BrokerSpec) DeepCopyInto(out *BrokerSpec) {
	*out = *in
	if in.Replicas != nil {
		out.Replicas = new(int32)
		*out.Replicas = *in.Replicas
	}
	in.Resources.DeepCopyInto(&out.Resources)
}

func (in *BrokerSpec) DeepCopy() *BrokerSpec {
	if in == nil {
		return nil
	}
	out := new(BrokerSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *ClusterConfigSpec) DeepCopyInto(out *ClusterConfigSpec) {
	*out = *in
}

func (in *ClusterConfigSpec) DeepCopy() *ClusterConfigSpec {
	if in == nil {
		return nil
	}
	out := new(ClusterConfigSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *KafscaleClusterSpec) DeepCopyInto(out *KafscaleClusterSpec) {
	*out = *in
	in.Brokers.DeepCopyInto(&out.Brokers)
	out.S3 = in.S3
	out.Etcd = in.Etcd
	out.Config = in.Config
}

func (in *KafscaleClusterSpec) DeepCopy() *KafscaleClusterSpec {
	if in == nil {
		return nil
	}
	out := new(KafscaleClusterSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *KafscaleClusterStatus) DeepCopyInto(out *KafscaleClusterStatus) {
	*out = *in
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i := range in.Conditions {
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}
}

func (in *KafscaleClusterStatus) DeepCopy() *KafscaleClusterStatus {
	if in == nil {
		return nil
	}
	out := new(KafscaleClusterStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *KafscaleCluster) DeepCopyInto(out *KafscaleCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *KafscaleCluster) DeepCopy() *KafscaleCluster {
	if in == nil {
		return nil
	}
	out := new(KafscaleCluster)
	in.DeepCopyInto(out)
	return out
}

func (in *KafscaleCluster) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *KafscaleClusterList) DeepCopyInto(out *KafscaleClusterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]KafscaleCluster, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *KafscaleClusterList) DeepCopy() *KafscaleClusterList {
	if in == nil {
		return nil
	}
	out := new(KafscaleClusterList)
	in.DeepCopyInto(out)
	return out
}

func (in *KafscaleClusterList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}
