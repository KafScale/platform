package protocol

import "fmt"

// ApiVersionsResponse describes server capabilities.
type ApiVersionsResponse struct {
	CorrelationID int32
	ErrorCode     int16
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
	ErrorCode  int16
	Name       string
	Partitions []MetadataPartition
}

// MetadataPartition describes partition metadata.
type MetadataPartition struct {
	ErrorCode      int16
	PartitionIndex int32
	LeaderID       int32
	ReplicaNodes   []int32
	ISRNodes       []int32
}

// MetadataResponse holds topic + broker info.
type MetadataResponse struct {
	CorrelationID int32
	Brokers       []MetadataBroker
	ClusterID     *string
	ControllerID  int32
	Topics        []MetadataTopic
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
	Topics        []CreateTopicResult
}

type DeleteTopicResult struct {
	Name         string
	ErrorCode    int16
	ErrorMessage string
}

type DeleteTopicsResponse struct {
	CorrelationID int32
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

// EncodeApiVersionsResponse renders bytes ready to send on the wire.
func EncodeApiVersionsResponse(resp *ApiVersionsResponse) ([]byte, error) {
	w := newByteWriter(64)
	w.Int32(resp.CorrelationID)
	w.Int16(resp.ErrorCode)
	w.Int32(int32(len(resp.Versions)))
	for _, v := range resp.Versions {
		w.Int16(v.APIKey)
		w.Int16(v.MinVersion)
		w.Int16(v.MaxVersion)
	}
	return w.Bytes(), nil
}

// EncodeMetadataResponse renders bytes for metadata responses. version should match
// the Metadata request version that triggered this response.
func EncodeMetadataResponse(resp *MetadataResponse, version int16) ([]byte, error) {
	if version < 0 {
		version = 0
	}

	w := newByteWriter(256)
	w.Int32(resp.CorrelationID)
	w.Int32(int32(len(resp.Brokers)))
	for _, b := range resp.Brokers {
		w.Int32(b.NodeID)
		w.String(b.Host)
		w.Int32(b.Port)
		if version >= 1 {
			w.NullableString(b.Rack)
		}
	}
	if version >= 2 {
		w.NullableString(resp.ClusterID)
	}
	if version >= 1 {
		w.Int32(resp.ControllerID)
	}
	w.Int32(int32(len(resp.Topics)))
	for _, t := range resp.Topics {
		w.Int16(t.ErrorCode)
		w.String(t.Name)
		w.Int32(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			w.Int16(p.ErrorCode)
			w.Int32(p.PartitionIndex)
			w.Int32(p.LeaderID)
			w.Int32(int32(len(p.ReplicaNodes)))
			for _, replica := range p.ReplicaNodes {
				w.Int32(replica)
			}
			w.Int32(int32(len(p.ISRNodes)))
			for _, isr := range p.ISRNodes {
				w.Int32(isr)
			}
		}
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
			w.Int64(p.LogAppendTimeMs)
			w.Int64(p.LogStartOffset)
			w.Int32(0) // log_offset_delta (unused for v9)
			if flexible {
				w.WriteTaggedFields(0)
			}
		}
		if flexible {
			w.WriteTaggedFields(0)
		}
	}
	w.Int32(resp.ThrottleMs)
	if flexible {
		w.WriteTaggedFields(0)
	}
	return w.Bytes(), nil
}

// EncodeFetchResponse renders bytes for fetch responses.
func EncodeFetchResponse(resp *FetchResponse, version int16) ([]byte, error) {
	if version < 1 || version > 11 {
		return nil, fmt.Errorf("fetch response version %d not supported", version)
	}
	w := newByteWriter(256)
	w.Int32(resp.CorrelationID)
	w.Int32(resp.ThrottleMs)
	if version >= 7 {
		w.Int16(resp.ErrorCode)
		w.Int32(resp.SessionID)
	} else {
		if resp.ErrorCode != 0 || resp.SessionID != 0 {
			return nil, fmt.Errorf("fetch version %d cannot include session fields", version)
		}
	}
	w.Int32(int32(len(resp.Topics)))
	for _, topic := range resp.Topics {
		w.String(topic.Name)
		w.Int32(int32(len(topic.Partitions)))
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
				w.Int32(int32(len(part.AbortedTransactions)))
				for _, aborted := range part.AbortedTransactions {
					w.Int64(aborted.ProducerID)
					w.Int64(aborted.FirstOffset)
				}
			}
			if version >= 11 {
				w.Int32(part.PreferredReadReplica)
			}
			if part.RecordSet == nil {
				w.Int32(0)
			} else {
				w.Int32(int32(len(part.RecordSet)))
				w.write(part.RecordSet)
			}
		}
	}
	return w.Bytes(), nil
}

func EncodeCreateTopicsResponse(resp *CreateTopicsResponse) ([]byte, error) {
	w := newByteWriter(128)
	w.Int32(resp.CorrelationID)
	w.Int32(int32(len(resp.Topics)))
	for _, topic := range resp.Topics {
		w.String(topic.Name)
		w.Int16(topic.ErrorCode)
		w.String(topic.ErrorMessage)
	}
	return w.Bytes(), nil
}

func EncodeDeleteTopicsResponse(resp *DeleteTopicsResponse) ([]byte, error) {
	w := newByteWriter(128)
	w.Int32(resp.CorrelationID)
	w.Int32(int32(len(resp.Topics)))
	for _, topic := range resp.Topics {
		w.String(topic.Name)
		w.Int16(topic.ErrorCode)
		w.String(topic.ErrorMessage)
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
