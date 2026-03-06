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
	Brokers  BrokerSpec        `json:"brokers"`
	S3       S3Spec            `json:"s3"`
	Etcd     EtcdSpec          `json:"etcd"`
	Config   ClusterConfigSpec `json:"config,omitempty"`
	LfsProxy LfsProxySpec      `json:"lfsProxy,omitempty"`
}

type BrokerSpec struct {
	Replicas       *int32            `json:"replicas,omitempty"`
	Resources      BrokerResources   `json:"resources,omitempty"`
	AdvertisedHost string            `json:"advertisedHost,omitempty"`
	AdvertisedPort *int32            `json:"advertisedPort,omitempty"`
	Service        BrokerServiceSpec `json:"service,omitempty"`
}

type BrokerResources struct {
	Requests corev1.ResourceList `json:"requests,omitempty"`
	Limits   corev1.ResourceList `json:"limits,omitempty"`
}

type BrokerServiceSpec struct {
	Type                     string            `json:"type,omitempty"`
	Annotations              map[string]string `json:"annotations,omitempty"`
	LoadBalancerIP           string            `json:"loadBalancerIP,omitempty"`
	LoadBalancerSourceRanges []string          `json:"loadBalancerSourceRanges,omitempty"`
	ExternalTrafficPolicy    string            `json:"externalTrafficPolicy,omitempty"`
	KafkaNodePort            *int32            `json:"kafkaNodePort,omitempty"`
	MetricsNodePort          *int32            `json:"metricsNodePort,omitempty"`
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

type LfsProxySpec struct {
	Enabled                bool                `json:"enabled,omitempty"`
	Replicas               *int32              `json:"replicas,omitempty"`
	Image                  string              `json:"image,omitempty"`
	ImagePullPolicy        string              `json:"imagePullPolicy,omitempty"`
	Backends               []string            `json:"backends,omitempty"`
	AdvertisedHost         string              `json:"advertisedHost,omitempty"`
	AdvertisedPort         *int32              `json:"advertisedPort,omitempty"`
	BackendCacheTTLSeconds *int32              `json:"backendCacheTTLSeconds,omitempty"`
	Service                LfsProxyServiceSpec `json:"service,omitempty"`
	HTTP                   LfsProxyHTTPSpec    `json:"http,omitempty"`
	Metrics                LfsProxyMetricsSpec `json:"metrics,omitempty"`
	Health                 LfsProxyHealthSpec  `json:"health,omitempty"`
	S3                     LfsProxyS3Spec      `json:"s3,omitempty"`
}

type LfsProxyServiceSpec struct {
	Type                     string            `json:"type,omitempty"`
	Annotations              map[string]string `json:"annotations,omitempty"`
	LoadBalancerSourceRanges []string          `json:"loadBalancerSourceRanges,omitempty"`
	Port                     *int32            `json:"port,omitempty"`
}

type LfsProxyHTTPSpec struct {
	Enabled         *bool  `json:"enabled,omitempty"`
	Port            *int32 `json:"port,omitempty"`
	APIKeySecretRef string `json:"apiKeySecretRef,omitempty"`
	APIKeySecretKey string `json:"apiKeySecretKey,omitempty"`
}

type LfsProxyMetricsSpec struct {
	Enabled *bool  `json:"enabled,omitempty"`
	Port    *int32 `json:"port,omitempty"`
}

type LfsProxyHealthSpec struct {
	Enabled *bool  `json:"enabled,omitempty"`
	Port    *int32 `json:"port,omitempty"`
}

type LfsProxyS3Spec struct {
	Namespace      string `json:"namespace,omitempty"`
	MaxBlobSize    *int64 `json:"maxBlobSize,omitempty"`
	ChunkSize      *int64 `json:"chunkSize,omitempty"`
	ForcePathStyle *bool  `json:"forcePathStyle,omitempty"`
	EnsureBucket   *bool  `json:"ensureBucket,omitempty"`
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

func (in *LfsProxyServiceSpec) DeepCopyInto(out *LfsProxyServiceSpec) {
	*out = *in
	if in.Annotations != nil {
		out.Annotations = make(map[string]string, len(in.Annotations))
		for key, val := range in.Annotations {
			out.Annotations[key] = val
		}
	}
	if in.LoadBalancerSourceRanges != nil {
		out.LoadBalancerSourceRanges = make([]string, len(in.LoadBalancerSourceRanges))
		copy(out.LoadBalancerSourceRanges, in.LoadBalancerSourceRanges)
	}
	if in.Port != nil {
		out.Port = new(int32)
		*out.Port = *in.Port
	}
}

func (in *LfsProxyServiceSpec) DeepCopy() *LfsProxyServiceSpec {
	if in == nil {
		return nil
	}
	out := new(LfsProxyServiceSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *LfsProxyHTTPSpec) DeepCopyInto(out *LfsProxyHTTPSpec) {
	*out = *in
	if in.Enabled != nil {
		out.Enabled = new(bool)
		*out.Enabled = *in.Enabled
	}
	if in.Port != nil {
		out.Port = new(int32)
		*out.Port = *in.Port
	}
}

func (in *LfsProxyHTTPSpec) DeepCopy() *LfsProxyHTTPSpec {
	if in == nil {
		return nil
	}
	out := new(LfsProxyHTTPSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *LfsProxyMetricsSpec) DeepCopyInto(out *LfsProxyMetricsSpec) {
	*out = *in
	if in.Enabled != nil {
		out.Enabled = new(bool)
		*out.Enabled = *in.Enabled
	}
	if in.Port != nil {
		out.Port = new(int32)
		*out.Port = *in.Port
	}
}

func (in *LfsProxyMetricsSpec) DeepCopy() *LfsProxyMetricsSpec {
	if in == nil {
		return nil
	}
	out := new(LfsProxyMetricsSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *LfsProxyHealthSpec) DeepCopyInto(out *LfsProxyHealthSpec) {
	*out = *in
	if in.Enabled != nil {
		out.Enabled = new(bool)
		*out.Enabled = *in.Enabled
	}
	if in.Port != nil {
		out.Port = new(int32)
		*out.Port = *in.Port
	}
}

func (in *LfsProxyHealthSpec) DeepCopy() *LfsProxyHealthSpec {
	if in == nil {
		return nil
	}
	out := new(LfsProxyHealthSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *LfsProxyS3Spec) DeepCopyInto(out *LfsProxyS3Spec) {
	*out = *in
	if in.MaxBlobSize != nil {
		out.MaxBlobSize = new(int64)
		*out.MaxBlobSize = *in.MaxBlobSize
	}
	if in.ChunkSize != nil {
		out.ChunkSize = new(int64)
		*out.ChunkSize = *in.ChunkSize
	}
	if in.ForcePathStyle != nil {
		out.ForcePathStyle = new(bool)
		*out.ForcePathStyle = *in.ForcePathStyle
	}
	if in.EnsureBucket != nil {
		out.EnsureBucket = new(bool)
		*out.EnsureBucket = *in.EnsureBucket
	}
}

func (in *LfsProxyS3Spec) DeepCopy() *LfsProxyS3Spec {
	if in == nil {
		return nil
	}
	out := new(LfsProxyS3Spec)
	in.DeepCopyInto(out)
	return out
}

func (in *LfsProxySpec) DeepCopyInto(out *LfsProxySpec) {
	*out = *in
	if in.Replicas != nil {
		out.Replicas = new(int32)
		*out.Replicas = *in.Replicas
	}
	if in.AdvertisedPort != nil {
		out.AdvertisedPort = new(int32)
		*out.AdvertisedPort = *in.AdvertisedPort
	}
	if in.BackendCacheTTLSeconds != nil {
		out.BackendCacheTTLSeconds = new(int32)
		*out.BackendCacheTTLSeconds = *in.BackendCacheTTLSeconds
	}
	if in.Backends != nil {
		out.Backends = make([]string, len(in.Backends))
		copy(out.Backends, in.Backends)
	}
	in.Service.DeepCopyInto(&out.Service)
	in.HTTP.DeepCopyInto(&out.HTTP)
	in.Metrics.DeepCopyInto(&out.Metrics)
	in.Health.DeepCopyInto(&out.Health)
	in.S3.DeepCopyInto(&out.S3)
}

func (in *LfsProxySpec) DeepCopy() *LfsProxySpec {
	if in == nil {
		return nil
	}
	out := new(LfsProxySpec)
	in.DeepCopyInto(out)
	return out
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
	if in.AdvertisedPort != nil {
		out.AdvertisedPort = new(int32)
		*out.AdvertisedPort = *in.AdvertisedPort
	}
	in.Resources.DeepCopyInto(&out.Resources)
	in.Service.DeepCopyInto(&out.Service)
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

func (in *BrokerServiceSpec) DeepCopyInto(out *BrokerServiceSpec) {
	*out = *in
	if in.Annotations != nil {
		out.Annotations = make(map[string]string, len(in.Annotations))
		for key, val := range in.Annotations {
			out.Annotations[key] = val
		}
	}
	if in.LoadBalancerSourceRanges != nil {
		out.LoadBalancerSourceRanges = make([]string, len(in.LoadBalancerSourceRanges))
		copy(out.LoadBalancerSourceRanges, in.LoadBalancerSourceRanges)
	}
	if in.KafkaNodePort != nil {
		out.KafkaNodePort = new(int32)
		*out.KafkaNodePort = *in.KafkaNodePort
	}
	if in.MetricsNodePort != nil {
		out.MetricsNodePort = new(int32)
		*out.MetricsNodePort = *in.MetricsNodePort
	}
}

func (in *BrokerServiceSpec) DeepCopy() *BrokerServiceSpec {
	if in == nil {
		return nil
	}
	out := new(BrokerServiceSpec)
	in.DeepCopyInto(out)
	return out
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
	in.LfsProxy.DeepCopyInto(&out.LfsProxy)
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
