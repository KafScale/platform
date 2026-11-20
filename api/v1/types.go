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

package v1

import "github.com/KafScale/platform/v2/api/v1alpha1"

type (
	KafscaleTopicSpec     = v1alpha1.KafscaleTopicSpec
	TopicPartitionStatus  = v1alpha1.TopicPartitionStatus
	KafscaleTopicStatus   = v1alpha1.KafscaleTopicStatus
	KafscaleTopic         = v1alpha1.KafscaleTopic
	KafscaleTopicList     = v1alpha1.KafscaleTopicList
	KafkaSecuritySpec     = v1alpha1.KafkaSecuritySpec
	KafscaleClusterSpec   = v1alpha1.KafscaleClusterSpec
	BrokerSpec            = v1alpha1.BrokerSpec
	BrokerImageSpec       = v1alpha1.BrokerImageSpec
	BrokerResources       = v1alpha1.BrokerResources
	BrokerServiceSpec     = v1alpha1.BrokerServiceSpec
	S3Spec                = v1alpha1.S3Spec
	EtcdSpec              = v1alpha1.EtcdSpec
	ClusterConfigSpec     = v1alpha1.ClusterConfigSpec
	LfsProxySpec          = v1alpha1.LfsProxySpec
	LfsProxyServiceSpec   = v1alpha1.LfsProxyServiceSpec
	LfsProxyHTTPSpec      = v1alpha1.LfsProxyHTTPSpec
	LfsProxyMetricsSpec   = v1alpha1.LfsProxyMetricsSpec
	LfsProxyHealthSpec    = v1alpha1.LfsProxyHealthSpec
	LfsProxyS3Spec        = v1alpha1.LfsProxyS3Spec
	KafscaleClusterStatus = v1alpha1.KafscaleClusterStatus
	KafscaleCluster       = v1alpha1.KafscaleCluster
	KafscaleClusterList   = v1alpha1.KafscaleClusterList
)

func init() {
	SchemeBuilder.Register(&KafscaleCluster{}, &KafscaleClusterList{}, &KafscaleTopic{}, &KafscaleTopicList{})
}
