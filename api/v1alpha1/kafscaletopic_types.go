package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// KafscaleTopicSpec captures desired configuration for a topic that the operator
// will reconcile across etcd metadata and S3 layout.
type KafscaleTopicSpec struct {
	ClusterRef     string            `json:"clusterRef"`
	Partitions     int32             `json:"partitions"`
	RetentionMs    *int64            `json:"retentionMs,omitempty"`
	RetentionBytes *int64            `json:"retentionBytes,omitempty"`
	Config         map[string]string `json:"config,omitempty"`
}

// TopicPartitionStatus describes the observed state of a topic partition.
type TopicPartitionStatus struct {
	ID             int32  `json:"id"`
	Leader         string `json:"leader"`
	LogEndOffset   int64  `json:"logEndOffset"`
	LogStartOffset int64  `json:"logStartOffset"`
}

// KafscaleTopicStatus surfaces observed reconciliation details.
type KafscaleTopicStatus struct {
	Phase      string                 `json:"phase,omitempty"`
	Conditions []metav1.Condition     `json:"conditions,omitempty"`
	Partitions []TopicPartitionStatus `json:"partitions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// KafscaleTopic declares a Kafka-compatible topic managed by the operator.
type KafscaleTopic struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafscaleTopicSpec   `json:"spec,omitempty"`
	Status KafscaleTopicStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// KafscaleTopicList contains multiple topics.
type KafscaleTopicList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafscaleTopic `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KafscaleTopic{}, &KafscaleTopicList{})
}

func (in *KafscaleTopicSpec) DeepCopyInto(out *KafscaleTopicSpec) {
	*out = *in
	if in.RetentionMs != nil {
		out.RetentionMs = new(int64)
		*out.RetentionMs = *in.RetentionMs
	}
	if in.RetentionBytes != nil {
		out.RetentionBytes = new(int64)
		*out.RetentionBytes = *in.RetentionBytes
	}
	if in.Config != nil {
		out.Config = make(map[string]string, len(in.Config))
		for k, v := range in.Config {
			out.Config[k] = v
		}
	}
}

func (in *KafscaleTopicSpec) DeepCopy() *KafscaleTopicSpec {
	if in == nil {
		return nil
	}
	out := new(KafscaleTopicSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *TopicPartitionStatus) DeepCopyInto(out *TopicPartitionStatus) {
	*out = *in
}

func (in *TopicPartitionStatus) DeepCopy() *TopicPartitionStatus {
	if in == nil {
		return nil
	}
	out := new(TopicPartitionStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *KafscaleTopicStatus) DeepCopyInto(out *KafscaleTopicStatus) {
	*out = *in
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i := range in.Conditions {
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}
	if in.Partitions != nil {
		out.Partitions = make([]TopicPartitionStatus, len(in.Partitions))
		copy(out.Partitions, in.Partitions)
	}
}

func (in *KafscaleTopicStatus) DeepCopy() *KafscaleTopicStatus {
	if in == nil {
		return nil
	}
	out := new(KafscaleTopicStatus)
	in.DeepCopyInto(out)
	return out
}

func (in *KafscaleTopic) DeepCopyInto(out *KafscaleTopic) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *KafscaleTopic) DeepCopy() *KafscaleTopic {
	if in == nil {
		return nil
	}
	out := new(KafscaleTopic)
	in.DeepCopyInto(out)
	return out
}

func (in *KafscaleTopic) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *KafscaleTopicList) DeepCopyInto(out *KafscaleTopicList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]KafscaleTopic, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *KafscaleTopicList) DeepCopy() *KafscaleTopicList {
	if in == nil {
		return nil
	}
	out := new(KafscaleTopicList)
	in.DeepCopyInto(out)
	return out
}

func (in *KafscaleTopicList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}
