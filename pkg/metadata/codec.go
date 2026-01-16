// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package metadata

import (
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	metadatapb "github.com/novatechflow/kafscale/pkg/gen/metadata"
)

const (
	topicConfigPrefix      = "/kafscale/topics"
	consumerGroupPrefix    = "/kafscale/consumers"
	brokerRegistrationPath = "/kafscale/brokers"
	assignmentPath         = "/kafscale/assignments"
)

// TopicConfigKey returns the etcd key for a topic configuration object.
func TopicConfigKey(topic string) string {
	return fmt.Sprintf("%s/%s/config", topicConfigPrefix, topic)
}

// PartitionStateKey returns the etcd key for a partition state object.
func PartitionStateKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/%s/partitions/%d", topicConfigPrefix, topic, partition)
}

// ConsumerGroupKey returns the etcd key for a consumer group metadata blob.
func ConsumerGroupKey(groupID string) string {
	return fmt.Sprintf("%s/%s/metadata", consumerGroupPrefix, groupID)
}

// ConsumerGroupPrefix returns the etcd prefix for consumer groups.
func ConsumerGroupPrefix() string {
	return consumerGroupPrefix
}

// ParseConsumerGroupID extracts a group ID from a metadata key.
func ParseConsumerGroupID(key string) (string, bool) {
	prefix := consumerGroupPrefix + "/"
	if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, "/metadata") {
		return "", false
	}
	groupID := strings.TrimSuffix(strings.TrimPrefix(key, prefix), "/metadata")
	if groupID == "" || strings.Contains(groupID, "/") {
		return "", false
	}
	return groupID, true
}

// ConsumerOffsetKey returns the etcd key holding the committed offset for a partition.
func ConsumerOffsetKey(groupID, topic string, partition int32) string {
	return fmt.Sprintf("%s/%s/offsets/%s/%d", consumerGroupPrefix, groupID, topic, partition)
}

// ParseConsumerOffsetKey extracts group, topic, and partition from an offset key.
func ParseConsumerOffsetKey(key string) (string, string, int32, bool) {
	prefix := consumerGroupPrefix + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", "", 0, false
	}
	trimmed := strings.TrimPrefix(key, prefix)
	parts := strings.Split(trimmed, "/")
	if len(parts) != 4 {
		return "", "", 0, false
	}
	groupID := parts[0]
	if parts[1] != "offsets" || groupID == "" || parts[2] == "" {
		return "", "", 0, false
	}
	partition, err := strconv.ParseInt(parts[3], 10, 32)
	if err != nil {
		return "", "", 0, false
	}
	return groupID, parts[2], int32(partition), true
}

// BrokerRegistrationKey returns the etcd key for broker liveness data.
func BrokerRegistrationKey(brokerID string) string {
	return fmt.Sprintf("%s/%s", brokerRegistrationPath, brokerID)
}

// PartitionAssignmentKey returns the etcd key for the current leader assignment.
func PartitionAssignmentKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/%s/%d", assignmentPath, topic, partition)
}

func encode(msg proto.Message) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal %T: %w", msg, err)
	}
	return data, nil
}

func decode(data []byte, msg proto.Message) error {
	if err := proto.Unmarshal(data, msg); err != nil {
		return fmt.Errorf("unmarshal %T: %w", msg, err)
	}
	return nil
}

// EncodeTopicConfig serializes a TopicConfig into etcd-ready bytes.
func EncodeTopicConfig(cfg *metadatapb.TopicConfig) ([]byte, error) {
	return encode(cfg)
}

// DecodeTopicConfig parses bytes into a TopicConfig object.
func DecodeTopicConfig(data []byte) (*metadatapb.TopicConfig, error) {
	cfg := &metadatapb.TopicConfig{}
	if err := decode(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// EncodePartitionState serializes a PartitionState into bytes.
func EncodePartitionState(state *metadatapb.PartitionState) ([]byte, error) {
	return encode(state)
}

// DecodePartitionState parses bytes into a PartitionState.
func DecodePartitionState(data []byte) (*metadatapb.PartitionState, error) {
	state := &metadatapb.PartitionState{}
	if err := decode(data, state); err != nil {
		return nil, err
	}
	return state, nil
}

// EncodeConsumerGroup serializes a ConsumerGroup.
func EncodeConsumerGroup(group *metadatapb.ConsumerGroup) ([]byte, error) {
	return encode(group)
}

// DecodeConsumerGroup parses bytes into a ConsumerGroup struct.
func DecodeConsumerGroup(data []byte) (*metadatapb.ConsumerGroup, error) {
	group := &metadatapb.ConsumerGroup{}
	if err := decode(data, group); err != nil {
		return nil, err
	}
	return group, nil
}
