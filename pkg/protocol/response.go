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

// EncodeMetadataResponse renders bytes for metadata responses.
func EncodeMetadataResponse(resp *MetadataResponse) ([]byte, error) {
	w := newByteWriter(256)
	w.Int32(resp.CorrelationID)
	w.Int32(int32(len(resp.Brokers)))
	for _, b := range resp.Brokers {
		w.Int32(b.NodeID)
		w.String(b.Host)
		w.Int32(b.Port)
		w.NullableString(b.Rack)
	}
	w.NullableString(resp.ClusterID)
	w.Int32(resp.ControllerID)
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
func EncodeProduceResponse(resp *ProduceResponse) ([]byte, error) {
	w := newByteWriter(128)
	w.Int32(resp.CorrelationID)
	w.Int32(int32(len(resp.Topics)))
	for _, topic := range resp.Topics {
		w.String(topic.Name)
		w.Int32(int32(len(topic.Partitions)))
		for _, p := range topic.Partitions {
			w.Int32(p.Partition)
			w.Int16(p.ErrorCode)
			w.Int64(p.BaseOffset)
			w.Int64(p.LogAppendTimeMs)
			w.Int64(p.LogStartOffset)
			w.Int32(0) // log_offset_delta (unused for v9)
		}
	}
	w.Int32(resp.ThrottleMs)
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
