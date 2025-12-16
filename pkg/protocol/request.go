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

// MetadataRequest asks for cluster metadata. Empty Topics means "all".
type MetadataRequest struct {
	Topics []string
}

func (MetadataRequest) APIKey() int16 { return APIKeyMetadata }

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
	default:
		return nil, nil, fmt.Errorf("unsupported api key %d", header.APIKey)
	}

	return header, req, nil
}
