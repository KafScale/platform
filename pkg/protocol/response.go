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

package protocol

import "fmt"

// ApiVersionsResponse describes server capabilities.
type ApiVersionsResponse struct {
	CorrelationID int32
	ErrorCode     int16
	ThrottleMs    int32
	Versions      []ApiVersion
}

// MetadataBroker describes a broker in Metadata response.
type MetadataBroker struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   *string
}

// MetadataTopic describes a topic in Metadata response.
type MetadataTopic struct {
	ErrorCode                 int16
	Name                      string
	TopicID                   [16]byte
	IsInternal                bool
	Partitions                []MetadataPartition
	TopicAuthorizedOperations int32
}

// MetadataPartition describes partition metadata.
type MetadataPartition struct {
	ErrorCode       int16
	PartitionIndex  int32
	LeaderID        int32
	LeaderEpoch     int32
	ReplicaNodes    []int32
	ISRNodes        []int32
	OfflineReplicas []int32
}

// MetadataResponse holds topic + broker info.
type MetadataResponse struct {
	CorrelationID               int32
	ThrottleMs                  int32
	Brokers                     []MetadataBroker
	ClusterID                   *string
	ControllerID                int32
	Topics                      []MetadataTopic
	ClusterAuthorizedOperations int32
}

// ProduceResponse contains per-partition acknowledgement info.
type ProduceResponse struct {
	CorrelationID int32
	Topics        []ProduceTopicResponse
	ThrottleMs    int32
}

type ProduceTopicResponse struct {
	Name       string
	Partitions []ProducePartitionResponse
}

type ProducePartitionResponse struct {
	Partition       int32
	ErrorCode       int16
	BaseOffset      int64
	LogAppendTimeMs int64
	LogStartOffset  int64
}

// FetchResponse represents data returned to consumers.
type FetchResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	ErrorCode     int16
	SessionID     int32
	Topics        []FetchTopicResponse
}

type FetchTopicResponse struct {
	Name       string
	TopicID    [16]byte
	Partitions []FetchPartitionResponse
}

type FetchAbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

type FetchPartitionResponse struct {
	Partition            int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	PreferredReadReplica int32
	RecordSet            []byte
	AbortedTransactions  []FetchAbortedTransaction
}

type CreateTopicResult struct {
	Name         string
	ErrorCode    int16
	ErrorMessage string
}

type CreateTopicsResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	Topics        []CreateTopicResult
}

type DeleteTopicResult struct {
	Name         string
	ErrorCode    int16
	ErrorMessage string
}

type DeleteTopicsResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	Topics        []DeleteTopicResult
}

type ListOffsetsPartitionResponse struct {
	Partition       int32
	ErrorCode       int16
	Timestamp       int64
	Offset          int64
	LeaderEpoch     int32
	OldStyleOffsets []int64
}

type ListOffsetsTopicResponse struct {
	Name       string
	Partitions []ListOffsetsPartitionResponse
}

type ListOffsetsResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	Topics        []ListOffsetsTopicResponse
}

type FindCoordinatorResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	ErrorCode     int16
	ErrorMessage  *string
	NodeID        int32
	Host          string
	Port          int32
}

type JoinGroupMember struct {
	MemberID   string
	InstanceID *string
	Metadata   []byte
}

type JoinGroupResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	ErrorCode     int16
	GenerationID  int32
	ProtocolName  string
	LeaderID      string
	MemberID      string
	Members       []JoinGroupMember
}

type SyncGroupResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	ErrorCode     int16
	ProtocolType  *string
	ProtocolName  *string
	Assignment    []byte
}

type HeartbeatResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	ErrorCode     int16
}

type LeaveGroupResponse struct {
	CorrelationID int32
	ErrorCode     int16
}

type OffsetCommitPartitionResponse struct {
	Partition int32
	ErrorCode int16
}

type OffsetCommitTopicResponse struct {
	Name       string
	Partitions []OffsetCommitPartitionResponse
}

type OffsetCommitResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	Topics        []OffsetCommitTopicResponse
}

type OffsetFetchPartitionResponse struct {
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Metadata    *string
	ErrorCode   int16
}

type OffsetFetchTopicResponse struct {
	Name       string
	Partitions []OffsetFetchPartitionResponse
}

type OffsetFetchResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	Topics        []OffsetFetchTopicResponse
	ErrorCode     int16
}

type OffsetForLeaderEpochPartitionResponse struct {
	Partition   int32
	ErrorCode   int16
	LeaderEpoch int32
	EndOffset   int64
}

type OffsetForLeaderEpochTopicResponse struct {
	Name       string
	Partitions []OffsetForLeaderEpochPartitionResponse
}

type OffsetForLeaderEpochResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	Topics        []OffsetForLeaderEpochTopicResponse
}

type DescribeGroupsResponseGroupMember struct {
	MemberID         string
	InstanceID       *string
	ClientID         string
	ClientHost       string
	ProtocolMetadata []byte
	MemberAssignment []byte
}

type DescribeGroupsResponseGroup struct {
	ErrorCode            int16
	GroupID              string
	State                string
	ProtocolType         string
	Protocol             string
	Members              []DescribeGroupsResponseGroupMember
	AuthorizedOperations int32
}

type DescribeGroupsResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	Groups        []DescribeGroupsResponseGroup
}

type ListGroupsResponseGroup struct {
	GroupID      string
	ProtocolType string
	GroupState   string
	GroupType    string
}

type ListGroupsResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	ErrorCode     int16
	Groups        []ListGroupsResponseGroup
}

type DescribeConfigsResponseConfigSynonym struct {
	Name   string
	Value  *string
	Source int8
}

type DescribeConfigsResponseConfig struct {
	Name          string
	Value         *string
	ReadOnly      bool
	IsDefault     bool
	Source        int8
	IsSensitive   bool
	Synonyms      []DescribeConfigsResponseConfigSynonym
	ConfigType    int8
	Documentation *string
}

type DescribeConfigsResponseResource struct {
	ErrorCode    int16
	ErrorMessage *string
	ResourceType int8
	ResourceName string
	Configs      []DescribeConfigsResponseConfig
}

type DescribeConfigsResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	Resources     []DescribeConfigsResponseResource
}

type AlterConfigsResponseResource struct {
	ErrorCode    int16
	ErrorMessage *string
	ResourceType int8
	ResourceName string
}

type AlterConfigsResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	Resources     []AlterConfigsResponseResource
}

type CreatePartitionsResponseTopic struct {
	Name         string
	ErrorCode    int16
	ErrorMessage *string
}

type CreatePartitionsResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	Topics        []CreatePartitionsResponseTopic
}

type DeleteGroupsResponseGroup struct {
	Group     string
	ErrorCode int16
}

type DeleteGroupsResponse struct {
	CorrelationID int32
	ThrottleMs    int32
	Groups        []DeleteGroupsResponseGroup
}

// EncodeApiVersionsResponse renders bytes ready to send on the wire.
func EncodeApiVersionsResponse(resp *ApiVersionsResponse, version int16) ([]byte, error) {
	if version < 0 || version > 3 {
		return nil, fmt.Errorf("api versions response version %d not supported", version)
	}
	w := newByteWriter(64)
	w.Int32(resp.CorrelationID)
	w.Int16(resp.ErrorCode)
	if version >= 3 {
		w.CompactArrayLen(len(resp.Versions))
	} else {
		w.Int32(int32(len(resp.Versions)))
	}
	for _, v := range resp.Versions {
		w.Int16(v.APIKey)
		w.Int16(v.MinVersion)
		w.Int16(v.MaxVersion)
		if version >= 3 {
			w.WriteTaggedFields(0)
		}
	}
	if version >= 1 {
		w.Int32(resp.ThrottleMs)
	}
	if version >= 3 {
		w.WriteTaggedFields(0)
	}
	return w.Bytes(), nil
}

// EncodeMetadataResponse renders bytes for metadata responses. version should match
// the Metadata request version that triggered this response.
func EncodeMetadataResponse(resp *MetadataResponse, version int16) ([]byte, error) {
	if version < 0 || version > 12 {
		return nil, fmt.Errorf("metadata response version %d not supported", version)
	}
	flexible := version >= 9
	w := newByteWriter(256)
	w.Int32(resp.CorrelationID)
	if flexible {
		w.WriteTaggedFields(0)
	}
	if version >= 3 {
		w.Int32(resp.ThrottleMs)
	}
	if flexible {
		w.CompactArrayLen(len(resp.Brokers))
	} else {
		w.Int32(int32(len(resp.Brokers)))
	}
	for _, b := range resp.Brokers {
		w.Int32(b.NodeID)
		if flexible {
			w.CompactString(b.Host)
		} else {
			w.String(b.Host)
		}
		w.Int32(b.Port)
		if version >= 1 {
			if flexible {
				w.CompactNullableString(b.Rack)
			} else {
				w.NullableString(b.Rack)
			}
		}
		if flexible {
			w.WriteTaggedFields(0)
		}
	}
	if version >= 2 {
		if flexible {
			w.CompactNullableString(resp.ClusterID)
		} else {
			w.NullableString(resp.ClusterID)
		}
	}
	if version >= 1 {
		w.Int32(resp.ControllerID)
	}
	if flexible {
		w.CompactArrayLen(len(resp.Topics))
	} else {
		w.Int32(int32(len(resp.Topics)))
	}
	for _, t := range resp.Topics {
		w.Int16(t.ErrorCode)
		if version >= 10 {
			var namePtr *string
			if t.Name != "" {
				namePtr = &t.Name
			}
			if flexible {
				w.CompactNullableString(namePtr)
			} else {
				w.NullableString(namePtr)
			}
			w.UUID(t.TopicID)
			if version >= 1 {
				w.Bool(t.IsInternal)
			}
		} else {
			if flexible {
				w.CompactString(t.Name)
			} else {
				w.String(t.Name)
			}
			if version >= 1 {
				w.Bool(t.IsInternal)
			}
		}
		if flexible {
			w.CompactArrayLen(len(t.Partitions))
		} else {
			w.Int32(int32(len(t.Partitions)))
		}
		for _, p := range t.Partitions {
			w.Int16(p.ErrorCode)
			w.Int32(p.PartitionIndex)
			w.Int32(p.LeaderID)
			if version >= 7 {
				w.Int32(p.LeaderEpoch)
			}
			if flexible {
				w.CompactArrayLen(len(p.ReplicaNodes))
			} else {
				w.Int32(int32(len(p.ReplicaNodes)))
			}
			for _, replica := range p.ReplicaNodes {
				w.Int32(replica)
			}
			if flexible {
				w.CompactArrayLen(len(p.ISRNodes))
			} else {
				w.Int32(int32(len(p.ISRNodes)))
			}
			for _, isr := range p.ISRNodes {
				w.Int32(isr)
			}
			if version >= 5 {
				if flexible {
					w.CompactArrayLen(len(p.OfflineReplicas))
				} else {
					w.Int32(int32(len(p.OfflineReplicas)))
				}
				for _, offline := range p.OfflineReplicas {
					w.Int32(offline)
				}
			}
			if flexible {
				w.WriteTaggedFields(0)
			}
		}
		if version >= 8 {
			w.Int32(t.TopicAuthorizedOperations)
		}
		if flexible {
			w.WriteTaggedFields(0)
		}
	}
	if version >= 8 {
		w.Int32(resp.ClusterAuthorizedOperations)
	}
	if flexible {
		w.WriteTaggedFields(0)
	}
	return w.Bytes(), nil
}

// EncodeProduceResponse renders bytes for produce responses.
func EncodeProduceResponse(resp *ProduceResponse, version int16) ([]byte, error) {
	w := newByteWriter(128)
	flexible := version >= 9
	w.Int32(resp.CorrelationID)
	if flexible {
		w.WriteTaggedFields(0)
	}
	if flexible {
		w.CompactArrayLen(len(resp.Topics))
	} else {
		w.Int32(int32(len(resp.Topics)))
	}
	for _, topic := range resp.Topics {
		if flexible {
			w.CompactString(topic.Name)
		} else {
			w.String(topic.Name)
		}
		if flexible {
			w.CompactArrayLen(len(topic.Partitions))
		} else {
			w.Int32(int32(len(topic.Partitions)))
		}
		for _, p := range topic.Partitions {
			w.Int32(p.Partition)
			w.Int16(p.ErrorCode)
			w.Int64(p.BaseOffset)
			if version >= 3 {
				w.Int64(p.LogAppendTimeMs)
			}
			if version >= 5 {
				w.Int64(p.LogStartOffset)
			}
			if version >= 8 {
				if flexible {
					w.CompactArrayLen(0) // error_records
					w.CompactNullableString(nil)
				} else {
					w.Int32(0) // error_records
					w.NullableString(nil)
				}
			}
			if flexible {
				w.WriteTaggedFields(0)
			}
		}
		if flexible {
			w.WriteTaggedFields(0)
		}
	}
	if version >= 1 {
		w.Int32(resp.ThrottleMs)
	}
	if flexible {
		w.WriteTaggedFields(0)
	}
	return w.Bytes(), nil
}

// EncodeFetchResponse renders bytes for fetch responses.
func EncodeFetchResponse(resp *FetchResponse, version int16) ([]byte, error) {
	if version < 1 || version > 13 {
		return nil, fmt.Errorf("fetch response version %d not supported", version)
	}
	flexible := version >= 12
	w := newByteWriter(256)
	w.Int32(resp.CorrelationID)
	if flexible {
		w.WriteTaggedFields(0)
	}
	w.Int32(resp.ThrottleMs)
	if version >= 7 {
		w.Int16(resp.ErrorCode)
		w.Int32(resp.SessionID)
	} else {
		if resp.ErrorCode != 0 || resp.SessionID != 0 {
			return nil, fmt.Errorf("fetch version %d cannot include session fields", version)
		}
	}
	if flexible {
		w.CompactArrayLen(len(resp.Topics))
	} else {
		w.Int32(int32(len(resp.Topics)))
	}
	for _, topic := range resp.Topics {
		if flexible {
			w.UUID(topic.TopicID)
		} else {
			w.String(topic.Name)
		}
		if flexible {
			w.CompactArrayLen(len(topic.Partitions))
		} else {
			w.Int32(int32(len(topic.Partitions)))
		}
		for _, part := range topic.Partitions {
			w.Int32(part.Partition)
			w.Int16(part.ErrorCode)
			w.Int64(part.HighWatermark)
			if version >= 4 {
				w.Int64(part.LastStableOffset)
			}
			if version >= 5 {
				w.Int64(part.LogStartOffset)
			}
			if version >= 4 {
				if flexible {
					w.CompactArrayLen(len(part.AbortedTransactions))
				} else {
					w.Int32(int32(len(part.AbortedTransactions)))
				}
				for _, aborted := range part.AbortedTransactions {
					w.Int64(aborted.ProducerID)
					w.Int64(aborted.FirstOffset)
				}
			}
			if version >= 11 {
				w.Int32(part.PreferredReadReplica)
			}
			if flexible {
				w.CompactBytes(part.RecordSet)
				w.WriteTaggedFields(0)
			} else {
				if part.RecordSet == nil {
					w.Int32(0)
				} else {
					w.Int32(int32(len(part.RecordSet)))
					w.write(part.RecordSet)
				}
			}
		}
		if flexible {
			w.WriteTaggedFields(0)
		}
	}
	if flexible {
		w.WriteTaggedFields(0)
	}
	return w.Bytes(), nil
}

func EncodeCreateTopicsResponse(resp *CreateTopicsResponse, version int16) ([]byte, error) {
	if version < 0 || version > 2 {
		return nil, fmt.Errorf("create topics response version %d not supported", version)
	}
	w := newByteWriter(128)
	w.Int32(resp.CorrelationID)
	if version >= 2 {
		w.Int32(resp.ThrottleMs)
	}
	w.Int32(int32(len(resp.Topics)))
	for _, topic := range resp.Topics {
		w.String(topic.Name)
		w.Int16(topic.ErrorCode)
		if version >= 1 {
			w.String(topic.ErrorMessage)
		}
	}
	return w.Bytes(), nil
}

func EncodeDeleteTopicsResponse(resp *DeleteTopicsResponse, version int16) ([]byte, error) {
	if version < 0 || version > 2 {
		return nil, fmt.Errorf("delete topics response version %d not supported", version)
	}
	w := newByteWriter(128)
	w.Int32(resp.CorrelationID)
	if version >= 1 {
		w.Int32(resp.ThrottleMs)
	}
	w.Int32(int32(len(resp.Topics)))
	for _, topic := range resp.Topics {
		w.String(topic.Name)
		w.Int16(topic.ErrorCode)
	}
	return w.Bytes(), nil
}

func EncodeListOffsetsResponse(version int16, resp *ListOffsetsResponse) ([]byte, error) {
	if version < 0 || version > 4 {
		return nil, fmt.Errorf("list offsets response version %d not supported", version)
	}
	w := newByteWriter(256)
	w.Int32(resp.CorrelationID)
	if version >= 2 {
		w.Int32(resp.ThrottleMs)
	}
	w.Int32(int32(len(resp.Topics)))
	for _, topic := range resp.Topics {
		w.String(topic.Name)
		w.Int32(int32(len(topic.Partitions)))
		for _, part := range topic.Partitions {
			w.Int32(part.Partition)
			w.Int16(part.ErrorCode)
			if version == 0 {
				offsets := part.OldStyleOffsets
				if offsets == nil {
					offsets = []int64{}
				}
				w.Int32(int32(len(offsets)))
				for _, off := range offsets {
					w.Int64(off)
				}
				continue
			}
			w.Int64(part.Timestamp)
			w.Int64(part.Offset)
			if version >= 4 {
				w.Int32(part.LeaderEpoch)
			}
		}
	}
	return w.Bytes(), nil
}

func EncodeFindCoordinatorResponse(resp *FindCoordinatorResponse, version int16) ([]byte, error) {
	if version >= 4 {
		return nil, fmt.Errorf("find coordinator version %d not supported", version)
	}
	w := newByteWriter(64)
	flexible := version >= 3
	w.Int32(resp.CorrelationID)
	if flexible {
		w.WriteTaggedFields(0)
	}
	if version >= 1 {
		w.Int32(resp.ThrottleMs)
	}
	w.Int16(resp.ErrorCode)
	if version >= 1 {
		if flexible {
			w.CompactNullableString(resp.ErrorMessage)
		} else {
			w.NullableString(resp.ErrorMessage)
		}
	}
	w.Int32(resp.NodeID)
	if flexible {
		w.CompactString(resp.Host)
	} else {
		w.String(resp.Host)
	}
	w.Int32(resp.Port)
	if flexible {
		w.WriteTaggedFields(0)
	}
	return w.Bytes(), nil
}

func EncodeJoinGroupResponse(resp *JoinGroupResponse, version int16) ([]byte, error) {
	if version >= 6 {
		return nil, fmt.Errorf("join group response version %d not supported", version)
	}
	w := newByteWriter(256)
	w.Int32(resp.CorrelationID)
	if version >= 2 {
		w.Int32(resp.ThrottleMs)
	}
	w.Int16(resp.ErrorCode)
	w.Int32(resp.GenerationID)
	w.String(resp.ProtocolName)
	w.String(resp.LeaderID)
	w.String(resp.MemberID)
	w.Int32(int32(len(resp.Members)))
	for _, member := range resp.Members {
		w.String(member.MemberID)
		if version >= 5 {
			if member.InstanceID == nil {
				w.Int16(-1)
			} else {
				w.String(*member.InstanceID)
			}
		}
		w.BytesWithLength(member.Metadata)
	}
	return w.Bytes(), nil
}

func EncodeSyncGroupResponse(resp *SyncGroupResponse, version int16) ([]byte, error) {
	if version > 5 {
		return nil, fmt.Errorf("sync group response version %d not supported", version)
	}
	flexible := version >= 4
	w := newByteWriter(192)
	w.Int32(resp.CorrelationID)
	if flexible {
		w.WriteTaggedFields(0)
	}
	if version >= 1 {
		w.Int32(resp.ThrottleMs)
	}
	w.Int16(resp.ErrorCode)
	if version >= 5 {
		if flexible {
			w.CompactNullableString(resp.ProtocolType)
			w.CompactNullableString(resp.ProtocolName)
		} else {
			w.NullableString(resp.ProtocolType)
			w.NullableString(resp.ProtocolName)
		}
	}
	if flexible {
		w.CompactBytes(resp.Assignment)
		w.WriteTaggedFields(0)
	} else {
		w.BytesWithLength(resp.Assignment)
	}
	return w.Bytes(), nil
}

func EncodeHeartbeatResponse(resp *HeartbeatResponse, version int16) ([]byte, error) {
	if version > 4 {
		return nil, fmt.Errorf("heartbeat response version %d not supported", version)
	}
	flexible := version >= 4
	w := newByteWriter(64)
	w.Int32(resp.CorrelationID)
	if flexible {
		w.WriteTaggedFields(0)
	}
	if version >= 1 {
		w.Int32(resp.ThrottleMs)
	}
	w.Int16(resp.ErrorCode)
	if flexible {
		w.WriteTaggedFields(0)
	}
	return w.Bytes(), nil
}

func EncodeLeaveGroupResponse(resp *LeaveGroupResponse) ([]byte, error) {
	w := newByteWriter(32)
	w.Int32(resp.CorrelationID)
	w.Int16(resp.ErrorCode)
	return w.Bytes(), nil
}

func EncodeOffsetCommitResponse(resp *OffsetCommitResponse) ([]byte, error) {
	w := newByteWriter(256)
	w.Int32(resp.CorrelationID)
	w.Int32(resp.ThrottleMs)
	w.Int32(int32(len(resp.Topics)))
	for _, topic := range resp.Topics {
		w.String(topic.Name)
		w.Int32(int32(len(topic.Partitions)))
		for _, part := range topic.Partitions {
			w.Int32(part.Partition)
			w.Int16(part.ErrorCode)
		}
	}
	return w.Bytes(), nil
}

func EncodeOffsetFetchResponse(resp *OffsetFetchResponse, version int16) ([]byte, error) {
	if version < 3 || version > 5 {
		return nil, fmt.Errorf("offset fetch response version %d not supported", version)
	}
	w := newByteWriter(256)
	w.Int32(resp.CorrelationID)
	if version >= 3 {
		w.Int32(resp.ThrottleMs)
	}
	w.Int32(int32(len(resp.Topics)))
	for _, topic := range resp.Topics {
		w.String(topic.Name)
		w.Int32(int32(len(topic.Partitions)))
		for _, part := range topic.Partitions {
			w.Int32(part.Partition)
			w.Int64(part.Offset)
			if version >= 5 {
				w.Int32(part.LeaderEpoch)
			}
			w.NullableString(part.Metadata)
			w.Int16(part.ErrorCode)
		}
	}
	if version >= 2 {
		w.Int16(resp.ErrorCode)
	}
	return w.Bytes(), nil
}

// EncodeOffsetForLeaderEpochResponse renders bytes for offset for leader epoch responses.
func EncodeOffsetForLeaderEpochResponse(resp *OffsetForLeaderEpochResponse, version int16) ([]byte, error) {
	if version != 3 {
		return nil, fmt.Errorf("offset for leader epoch response version %d not supported", version)
	}
	w := newByteWriter(256)
	w.Int32(resp.CorrelationID)
	if version >= 2 {
		w.Int32(resp.ThrottleMs)
	}
	w.Int32(int32(len(resp.Topics)))
	for _, topic := range resp.Topics {
		w.String(topic.Name)
		w.Int32(int32(len(topic.Partitions)))
		for _, part := range topic.Partitions {
			w.Int32(part.Partition)
			w.Int16(part.ErrorCode)
			w.Int32(part.LeaderEpoch)
			w.Int64(part.EndOffset)
		}
	}
	return w.Bytes(), nil
}

// EncodeDescribeGroupsResponse renders bytes for describe groups responses.
func EncodeDescribeGroupsResponse(resp *DescribeGroupsResponse, version int16) ([]byte, error) {
	if version != 5 {
		return nil, fmt.Errorf("describe groups response version %d not supported", version)
	}
	flexible := version >= 5
	w := newByteWriter(256)
	w.Int32(resp.CorrelationID)
	if flexible {
		w.WriteTaggedFields(0)
	}
	if version >= 1 {
		w.Int32(resp.ThrottleMs)
	}
	if flexible {
		w.CompactArrayLen(len(resp.Groups))
	} else {
		w.Int32(int32(len(resp.Groups)))
	}
	for _, group := range resp.Groups {
		w.Int16(group.ErrorCode)
		if flexible {
			w.CompactString(group.GroupID)
			w.CompactString(group.State)
			w.CompactString(group.ProtocolType)
			w.CompactString(group.Protocol)
		} else {
			w.String(group.GroupID)
			w.String(group.State)
			w.String(group.ProtocolType)
			w.String(group.Protocol)
		}
		if flexible {
			w.CompactArrayLen(len(group.Members))
		} else {
			w.Int32(int32(len(group.Members)))
		}
		for _, member := range group.Members {
			if flexible {
				w.CompactString(member.MemberID)
				w.CompactNullableString(member.InstanceID)
				w.CompactString(member.ClientID)
				w.CompactString(member.ClientHost)
				w.CompactBytes(member.ProtocolMetadata)
				w.CompactBytes(member.MemberAssignment)
				w.WriteTaggedFields(0)
			} else {
				w.String(member.MemberID)
				w.NullableString(member.InstanceID)
				w.String(member.ClientID)
				w.String(member.ClientHost)
				w.BytesWithLength(member.ProtocolMetadata)
				w.BytesWithLength(member.MemberAssignment)
			}
		}
		if version >= 3 {
			w.Int32(group.AuthorizedOperations)
		}
		if flexible {
			w.WriteTaggedFields(0)
		}
	}
	if flexible {
		w.WriteTaggedFields(0)
	}
	return w.Bytes(), nil
}

// EncodeListGroupsResponse renders bytes for list groups responses.
func EncodeListGroupsResponse(resp *ListGroupsResponse, version int16) ([]byte, error) {
	if version != 5 {
		return nil, fmt.Errorf("list groups response version %d not supported", version)
	}
	flexible := version >= 3
	w := newByteWriter(256)
	w.Int32(resp.CorrelationID)
	if flexible {
		w.WriteTaggedFields(0)
	}
	if version >= 1 {
		w.Int32(resp.ThrottleMs)
	}
	w.Int16(resp.ErrorCode)
	if flexible {
		w.CompactArrayLen(len(resp.Groups))
	} else {
		w.Int32(int32(len(resp.Groups)))
	}
	for _, group := range resp.Groups {
		if flexible {
			w.CompactString(group.GroupID)
			w.CompactString(group.ProtocolType)
			w.CompactString(group.GroupState)
			w.CompactString(group.GroupType)
			w.WriteTaggedFields(0)
		} else {
			w.String(group.GroupID)
			w.String(group.ProtocolType)
			if version >= 4 {
				w.String(group.GroupState)
			}
			if version >= 5 {
				w.String(group.GroupType)
			}
		}
	}
	if flexible {
		w.WriteTaggedFields(0)
	}
	return w.Bytes(), nil
}

// EncodeDescribeConfigsResponse renders bytes for describe configs responses.
func EncodeDescribeConfigsResponse(resp *DescribeConfigsResponse, version int16) ([]byte, error) {
	if version != 4 {
		return nil, fmt.Errorf("describe configs response version %d not supported", version)
	}
	flexible := version >= 4
	w := newByteWriter(256)
	w.Int32(resp.CorrelationID)
	if flexible {
		w.WriteTaggedFields(0)
	}
	w.Int32(resp.ThrottleMs)
	if flexible {
		w.CompactArrayLen(len(resp.Resources))
	} else {
		w.Int32(int32(len(resp.Resources)))
	}
	for _, resource := range resp.Resources {
		w.Int16(resource.ErrorCode)
		if flexible {
			w.CompactNullableString(resource.ErrorMessage)
		} else {
			w.NullableString(resource.ErrorMessage)
		}
		w.Int8(resource.ResourceType)
		if flexible {
			w.CompactString(resource.ResourceName)
		} else {
			w.String(resource.ResourceName)
		}
		if flexible {
			w.CompactArrayLen(len(resource.Configs))
		} else {
			w.Int32(int32(len(resource.Configs)))
		}
		for _, cfg := range resource.Configs {
			if flexible {
				w.CompactString(cfg.Name)
				w.CompactNullableString(cfg.Value)
			} else {
				w.String(cfg.Name)
				w.NullableString(cfg.Value)
			}
			w.Bool(cfg.ReadOnly)
			w.Int8(cfg.Source)
			w.Bool(cfg.IsSensitive)
			if flexible {
				w.CompactArrayLen(len(cfg.Synonyms))
			} else {
				w.Int32(int32(len(cfg.Synonyms)))
			}
			for _, synonym := range cfg.Synonyms {
				if flexible {
					w.CompactString(synonym.Name)
					w.CompactNullableString(synonym.Value)
				} else {
					w.String(synonym.Name)
					w.NullableString(synonym.Value)
				}
				w.Int8(synonym.Source)
				if flexible {
					w.WriteTaggedFields(0)
				}
			}
			w.Int8(cfg.ConfigType)
			if flexible {
				w.CompactNullableString(cfg.Documentation)
				w.WriteTaggedFields(0)
			} else {
				w.NullableString(cfg.Documentation)
			}
		}
		if flexible {
			w.WriteTaggedFields(0)
		}
	}
	if flexible {
		w.WriteTaggedFields(0)
	}
	return w.Bytes(), nil
}

// EncodeAlterConfigsResponse renders bytes for alter configs responses.
func EncodeAlterConfigsResponse(resp *AlterConfigsResponse, version int16) ([]byte, error) {
	if version != 1 {
		return nil, fmt.Errorf("alter configs response version %d not supported", version)
	}
	w := newByteWriter(256)
	w.Int32(resp.CorrelationID)
	w.Int32(resp.ThrottleMs)
	w.Int32(int32(len(resp.Resources)))
	for _, resource := range resp.Resources {
		w.Int16(resource.ErrorCode)
		w.NullableString(resource.ErrorMessage)
		w.Int8(resource.ResourceType)
		w.String(resource.ResourceName)
	}
	return w.Bytes(), nil
}

// EncodeCreatePartitionsResponse renders bytes for create partitions responses.
func EncodeCreatePartitionsResponse(resp *CreatePartitionsResponse, version int16) ([]byte, error) {
	if version < 0 || version > 3 {
		return nil, fmt.Errorf("create partitions response version %d not supported", version)
	}
	flexible := version >= 2
	w := newByteWriter(128)
	w.Int32(resp.CorrelationID)
	if flexible {
		w.WriteTaggedFields(0)
	}
	w.Int32(resp.ThrottleMs)
	if flexible {
		w.CompactArrayLen(len(resp.Topics))
	} else {
		w.Int32(int32(len(resp.Topics)))
	}
	for _, topic := range resp.Topics {
		if flexible {
			w.CompactString(topic.Name)
		} else {
			w.String(topic.Name)
		}
		w.Int16(topic.ErrorCode)
		if flexible {
			w.CompactNullableString(topic.ErrorMessage)
			w.WriteTaggedFields(0)
		} else {
			w.NullableString(topic.ErrorMessage)
		}
	}
	if flexible {
		w.WriteTaggedFields(0)
	}
	return w.Bytes(), nil
}

// EncodeDeleteGroupsResponse renders bytes for delete groups responses.
func EncodeDeleteGroupsResponse(resp *DeleteGroupsResponse, version int16) ([]byte, error) {
	if version < 0 || version > 2 {
		return nil, fmt.Errorf("delete groups response version %d not supported", version)
	}
	flexible := version >= 2
	w := newByteWriter(128)
	w.Int32(resp.CorrelationID)
	if flexible {
		w.WriteTaggedFields(0)
	}
	w.Int32(resp.ThrottleMs)
	if flexible {
		w.CompactArrayLen(len(resp.Groups))
	} else {
		w.Int32(int32(len(resp.Groups)))
	}
	for _, group := range resp.Groups {
		if flexible {
			w.CompactString(group.Group)
		} else {
			w.String(group.Group)
		}
		w.Int16(group.ErrorCode)
		if flexible {
			w.WriteTaggedFields(0)
		}
	}
	if flexible {
		w.WriteTaggedFields(0)
	}
	return w.Bytes(), nil
}

// EncodeResponse wraps a response payload into a Kafka frame.
func EncodeResponse(payload []byte) ([]byte, error) {
	if len(payload) > int(^uint32(0)>>1) {
		return nil, fmt.Errorf("response too large: %d", len(payload))
	}
	w := newByteWriter(len(payload) + 4)
	w.Int32(int32(len(payload)))
	w.write(payload)
	return w.Bytes(), nil
}
