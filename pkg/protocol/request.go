package protocol

import (
	"fmt"
)

// RequestHeader matches Kafka RequestHeader v1 (simplified without tagged fields).
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      *string
}

// Request is implemented by concrete protocol requests.
type Request interface {
	APIKey() int16
}

// ApiVersionsRequest describes the ApiVersions call.
type ApiVersionsRequest struct{}

func (ApiVersionsRequest) APIKey() int16 { return APIKeyApiVersion }

// ProduceRequest is a simplified representation of Kafka ProduceRequest v9.
type ProduceRequest struct {
	Acks      int16
	TimeoutMs int32
	Topics    []ProduceTopic
}

type ProduceTopic struct {
	Name       string
	Partitions []ProducePartition
}

type ProducePartition struct {
	Partition int32
	Records   []byte
}

func (ProduceRequest) APIKey() int16 { return APIKeyProduce }

// FetchRequest represents a subset of Kafka FetchRequest v13.
type FetchRequest struct {
	ReplicaID int32
	Topics    []FetchTopicRequest
}

type FetchTopicRequest struct {
	Name       string
	Partitions []FetchPartitionRequest
}

type FetchPartitionRequest struct {
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

func (FetchRequest) APIKey() int16 { return APIKeyFetch }

// MetadataRequest asks for cluster metadata. Empty Topics means "all".
type MetadataRequest struct {
	Topics []string
}

func (MetadataRequest) APIKey() int16 { return APIKeyMetadata }

type CreateTopicConfig struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
}

type CreateTopicsRequest struct {
	Topics []CreateTopicConfig
}

func (CreateTopicsRequest) APIKey() int16 { return APIKeyCreateTopics }

type DeleteTopicsRequest struct {
	TopicNames []string
}

func (DeleteTopicsRequest) APIKey() int16 { return APIKeyDeleteTopics }

type ListOffsetsPartition struct {
	Partition int32
	Timestamp int64
}

type ListOffsetsTopic struct {
	Name       string
	Partitions []ListOffsetsPartition
}

type ListOffsetsRequest struct {
	ReplicaID int32
	Topics    []ListOffsetsTopic
}

func (ListOffsetsRequest) APIKey() int16 { return APIKeyListOffsets }

// FindCoordinatorRequest targets a group coordinator lookup.
type FindCoordinatorRequest struct {
	KeyType int8
	Key     string
}

func (FindCoordinatorRequest) APIKey() int16 { return APIKeyFindCoordinator }

type JoinGroupProtocol struct {
	Name     string
	Metadata []byte
}

type JoinGroupRequest struct {
	GroupID            string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	MemberID           string
	ProtocolType       string
	Protocols          []JoinGroupProtocol
}

func (JoinGroupRequest) APIKey() int16 { return APIKeyJoinGroup }

type SyncGroupAssignment struct {
	MemberID   string
	Assignment []byte
}

type SyncGroupRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Assignments  []SyncGroupAssignment
}

func (SyncGroupRequest) APIKey() int16 { return APIKeySyncGroup }

type HeartbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
}

func (HeartbeatRequest) APIKey() int16 { return APIKeyHeartbeat }

type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

func (LeaveGroupRequest) APIKey() int16 { return APIKeyLeaveGroup }

type OffsetCommitPartition struct {
	Partition int32
	Offset    int64
	Metadata  string
}

type OffsetCommitTopic struct {
	Name       string
	Partitions []OffsetCommitPartition
}

type OffsetCommitRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Topics       []OffsetCommitTopic
}

func (OffsetCommitRequest) APIKey() int16 { return APIKeyOffsetCommit }

type OffsetFetchPartition struct {
	Partition int32
}

type OffsetFetchTopic struct {
	Name       string
	Partitions []OffsetFetchPartition
}

type OffsetFetchRequest struct {
	GroupID string
	Topics  []OffsetFetchTopic
}

func (OffsetFetchRequest) APIKey() int16 { return APIKeyOffsetFetch }

// ParseRequestHeader decodes the header portion from raw bytes.
func ParseRequestHeader(b []byte) (*RequestHeader, *byteReader, error) {
	reader := newByteReader(b)
	apiKey, err := reader.Int16()
	if err != nil {
		return nil, nil, fmt.Errorf("read api key: %w", err)
	}
	version, err := reader.Int16()
	if err != nil {
		return nil, nil, fmt.Errorf("read api version: %w", err)
	}
	correlationID, err := reader.Int32()
	if err != nil {
		return nil, nil, fmt.Errorf("read correlation id: %w", err)
	}
	clientID, err := reader.NullableString()
	if err != nil {
		return nil, nil, fmt.Errorf("read client id: %w", err)
	}
	return &RequestHeader{
		APIKey:        apiKey,
		APIVersion:    version,
		CorrelationID: correlationID,
		ClientID:      clientID,
	}, reader, nil
}

// ParseRequest decodes a request header and body from bytes.
func ParseRequest(b []byte) (*RequestHeader, Request, error) {
	header, reader, err := ParseRequestHeader(b)
	if err != nil {
		return nil, nil, err
	}

	var req Request
	switch header.APIKey {
	case APIKeyApiVersion:
		req = &ApiVersionsRequest{}
	case APIKeyProduce:
		acks, err := reader.Int16()
		if err != nil {
			return nil, nil, fmt.Errorf("read produce acks: %w", err)
		}
		timeout, err := reader.Int32()
		if err != nil {
			return nil, nil, fmt.Errorf("read produce timeout: %w", err)
		}
		topicCount, err := reader.Int32()
		if err != nil {
			return nil, nil, fmt.Errorf("read produce topic count: %w", err)
		}
		topics := make([]ProduceTopic, 0, topicCount)
		for i := int32(0); i < topicCount; i++ {
			name, err := reader.String()
			if err != nil {
				return nil, nil, fmt.Errorf("read produce topic name: %w", err)
			}
			partitionCount, err := reader.Int32()
			if err != nil {
				return nil, nil, fmt.Errorf("read produce partition count: %w", err)
			}
			partitions := make([]ProducePartition, 0, partitionCount)
			for j := int32(0); j < partitionCount; j++ {
				index, err := reader.Int32()
				if err != nil {
					return nil, nil, fmt.Errorf("read produce partition index: %w", err)
				}
				records, err := reader.Bytes()
				if err != nil {
					return nil, nil, fmt.Errorf("read produce records: %w", err)
				}
				partitions = append(partitions, ProducePartition{
					Partition: index,
					Records:   records,
				})
			}
			topics = append(topics, ProduceTopic{Name: name, Partitions: partitions})
		}
		req = &ProduceRequest{
			Acks:      acks,
			TimeoutMs: timeout,
			Topics:    topics,
		}
	case APIKeyMetadata:
		var topics []string
		count, err := reader.Int32()
		if err != nil {
			return nil, nil, fmt.Errorf("read metadata topic count: %w", err)
		}
		if count >= 0 {
			topics = make([]string, 0, count)
			for i := int32(0); i < count; i++ {
				name, err := reader.String()
				if err != nil {
					return nil, nil, fmt.Errorf("read metadata topic[%d]: %w", i, err)
				}
				topics = append(topics, name)
			}
		}
		req = &MetadataRequest{Topics: topics}
	case APIKeyCreateTopics:
		topicCount, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		configs := make([]CreateTopicConfig, 0, topicCount)
		for i := int32(0); i < topicCount; i++ {
			name, err := reader.String()
			if err != nil {
				return nil, nil, err
			}
			partitions, err := reader.Int32()
			if err != nil {
				return nil, nil, err
			}
			repl, err := reader.Int16()
			if err != nil {
				return nil, nil, err
			}
			// Configs map (ignored)
			cfgCount, err := reader.Int32()
			if err != nil {
				return nil, nil, err
			}
			for j := int32(0); j < cfgCount; j++ {
				if _, err := reader.String(); err != nil {
					return nil, nil, err
				}
				if _, err := reader.String(); err != nil {
					return nil, nil, err
				}
			}
			configs = append(configs, CreateTopicConfig{Name: name, NumPartitions: partitions, ReplicationFactor: repl})
		}
		req = &CreateTopicsRequest{Topics: configs}
	case APIKeyDeleteTopics:
		count, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		names := make([]string, 0, count)
		for i := int32(0); i < count; i++ {
			name, err := reader.String()
			if err != nil {
				return nil, nil, err
			}
			names = append(names, name)
		}
		req = &DeleteTopicsRequest{TopicNames: names}
	case APIKeyListOffsets:
		replicaID, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		topicCount, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		topics := make([]ListOffsetsTopic, 0, topicCount)
		for i := int32(0); i < topicCount; i++ {
			name, err := reader.String()
			if err != nil {
				return nil, nil, err
			}
			partCount, err := reader.Int32()
			if err != nil {
				return nil, nil, err
			}
			parts := make([]ListOffsetsPartition, 0, partCount)
			for j := int32(0); j < partCount; j++ {
				partition, err := reader.Int32()
				if err != nil {
					return nil, nil, err
				}
				timestamp, err := reader.Int64()
				if err != nil {
					return nil, nil, err
				}
				parts = append(parts, ListOffsetsPartition{Partition: partition, Timestamp: timestamp})
			}
			topics = append(topics, ListOffsetsTopic{Name: name, Partitions: parts})
		}
		req = &ListOffsetsRequest{ReplicaID: replicaID, Topics: topics}
	case APIKeyFetch:
		replicaID, err := reader.Int32()
		if err != nil {
			return nil, nil, fmt.Errorf("read fetch replica id: %w", err)
		}
		// Skip max wait, min bytes, max bytes, isolation, session id/epoch.
		if _, err := reader.Int32(); err != nil {
			return nil, nil, err
		}
		if _, err := reader.Int32(); err != nil {
			return nil, nil, err
		}
		if _, err := reader.Int32(); err != nil {
			return nil, nil, err
		}
		if _, err := reader.Int8(); err != nil {
			return nil, nil, err
		}
		if _, err := reader.Int32(); err != nil {
			return nil, nil, err
		}
		if _, err := reader.Int32(); err != nil {
			return nil, nil, err
		}
		topicCount, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		topics := make([]FetchTopicRequest, 0, topicCount)
		for i := int32(0); i < topicCount; i++ {
			name, err := reader.String()
			if err != nil {
				return nil, nil, err
			}
			partCount, err := reader.Int32()
			if err != nil {
				return nil, nil, err
			}
			partitions := make([]FetchPartitionRequest, 0, partCount)
			for j := int32(0); j < partCount; j++ {
				partitionID, err := reader.Int32()
				if err != nil {
					return nil, nil, err
				}
				if _, err := reader.Int32(); err != nil { // leader epoch
					return nil, nil, err
				}
				fetchOffset, err := reader.Int64()
				if err != nil {
					return nil, nil, err
				}
				if _, err := reader.Int64(); err != nil { // last fetched epoch
					return nil, nil, err
				}
				if _, err := reader.Int64(); err != nil { // log start offset
					return nil, nil, err
				}
				maxBytes, err := reader.Int32()
				if err != nil {
					return nil, nil, err
				}
				partitions = append(partitions, FetchPartitionRequest{
					Partition:   partitionID,
					FetchOffset: fetchOffset,
					MaxBytes:    maxBytes,
				})
			}
			topics = append(topics, FetchTopicRequest{
				Name:       name,
				Partitions: partitions,
			})
		}
		req = &FetchRequest{
			ReplicaID: replicaID,
			Topics:    topics,
		}
	case APIKeyFindCoordinator:
		keyType, err := reader.Int8()
		if err != nil {
			return nil, nil, fmt.Errorf("read coordinator key type: %w", err)
		}
		key, err := reader.String()
		if err != nil {
			return nil, nil, fmt.Errorf("read coordinator key: %w", err)
		}
		req = &FindCoordinatorRequest{KeyType: keyType, Key: key}
	case APIKeyJoinGroup:
		groupID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		sessionTimeout, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		rebalanceTimeout, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		memberID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		protocolType, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		protocolCount, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		protocols := make([]JoinGroupProtocol, 0, protocolCount)
		for i := int32(0); i < protocolCount; i++ {
			name, err := reader.String()
			if err != nil {
				return nil, nil, err
			}
			meta, err := reader.Bytes()
			if err != nil {
				return nil, nil, err
			}
			protocols = append(protocols, JoinGroupProtocol{Name: name, Metadata: meta})
		}
		req = &JoinGroupRequest{
			GroupID:            groupID,
			SessionTimeoutMs:   sessionTimeout,
			RebalanceTimeoutMs: rebalanceTimeout,
			MemberID:           memberID,
			ProtocolType:       protocolType,
			Protocols:          protocols,
		}
	case APIKeySyncGroup:
		groupID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		generationID, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		memberID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		assignCount, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		assignments := make([]SyncGroupAssignment, 0, assignCount)
		for i := int32(0); i < assignCount; i++ {
			mid, err := reader.String()
			if err != nil {
				return nil, nil, err
			}
			data, err := reader.Bytes()
			if err != nil {
				return nil, nil, err
			}
			assignments = append(assignments, SyncGroupAssignment{MemberID: mid, Assignment: data})
		}
		req = &SyncGroupRequest{
			GroupID:      groupID,
			GenerationID: generationID,
			MemberID:     memberID,
			Assignments:  assignments,
		}
	case APIKeyHeartbeat:
		groupID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		generationID, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		memberID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		req = &HeartbeatRequest{
			GroupID:      groupID,
			GenerationID: generationID,
			MemberID:     memberID,
		}
	case APIKeyLeaveGroup:
		groupID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		memberID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		req = &LeaveGroupRequest{
			GroupID:  groupID,
			MemberID: memberID,
		}
	case APIKeyOffsetCommit:
		groupID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		generationID, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		memberID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		if _, err := reader.Int64(); err != nil {
			return nil, nil, err
		}
		topicCount, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		topics := make([]OffsetCommitTopic, 0, topicCount)
		for i := int32(0); i < topicCount; i++ {
			name, err := reader.String()
			if err != nil {
				return nil, nil, err
			}
			partCount, err := reader.Int32()
			if err != nil {
				return nil, nil, err
			}
			partitions := make([]OffsetCommitPartition, 0, partCount)
			for j := int32(0); j < partCount; j++ {
				partition, err := reader.Int32()
				if err != nil {
					return nil, nil, err
				}
				offset, err := reader.Int64()
				if err != nil {
					return nil, nil, err
				}
				if _, err := reader.Int64(); err != nil {
					return nil, nil, err
				}
				meta, err := reader.String()
				if err != nil {
					return nil, nil, err
				}
				partitions = append(partitions, OffsetCommitPartition{
					Partition: partition,
					Offset:    offset,
					Metadata:  meta,
				})
			}
			topics = append(topics, OffsetCommitTopic{Name: name, Partitions: partitions})
		}
		req = &OffsetCommitRequest{
			GroupID:      groupID,
			GenerationID: generationID,
			MemberID:     memberID,
			Topics:       topics,
		}
	case APIKeyOffsetFetch:
		groupID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		topicCount, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		topics := make([]OffsetFetchTopic, 0, topicCount)
		for i := int32(0); i < topicCount; i++ {
			name, err := reader.String()
			if err != nil {
				return nil, nil, err
			}
			partCount, err := reader.Int32()
			if err != nil {
				return nil, nil, err
			}
			partitions := make([]OffsetFetchPartition, 0, partCount)
			for j := int32(0); j < partCount; j++ {
				partition, err := reader.Int32()
				if err != nil {
					return nil, nil, err
				}
				partitions = append(partitions, OffsetFetchPartition{Partition: partition})
			}
			topics = append(topics, OffsetFetchTopic{Name: name, Partitions: partitions})
		}
		req = &OffsetFetchRequest{
			GroupID: groupID,
			Topics:  topics,
		}
	default:
		return nil, nil, fmt.Errorf("unsupported api key %d", header.APIKey)
	}

	return header, req, nil
}
