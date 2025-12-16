package metadata

import (
	"context"
	"errors"
	"sync"

	"github.com/alo/kafscale/pkg/protocol"
)

// Store exposes read-only access to cluster metadata used by Kafka protocol handlers.
type Store interface {
	// Metadata returns brokers, controller ID, and topics. When topics is non-empty,
	// the implementation should filter to that subset and omit missing topics.
	Metadata(ctx context.Context, topics []string) (*ClusterMetadata, error)
}

// ClusterMetadata describes the Kafka-visible cluster state.
type ClusterMetadata struct {
	Brokers      []protocol.MetadataBroker
	ControllerID int32
	Topics       []protocol.MetadataTopic
	ClusterID    *string
}

// InMemoryStore is a simple Store backed by in-process state. Useful for early development and tests.
type InMemoryStore struct {
	mu    sync.RWMutex
	state ClusterMetadata
}

// NewInMemoryStore builds an in-memory metadata store with the provided state.
func NewInMemoryStore(state ClusterMetadata) *InMemoryStore {
	return &InMemoryStore{state: cloneMetadata(state)}
}

// Update swaps the cluster metadata atomically.
func (s *InMemoryStore) Update(state ClusterMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = cloneMetadata(state)
}

// Metadata implements Store.
func (s *InMemoryStore) Metadata(ctx context.Context, topics []string) (*ClusterMetadata, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	state := cloneMetadata(s.state)
	if len(topics) == 0 {
		return &state, nil
	}

	filtered := filterTopics(state.Topics, topics)
	state.Topics = filtered
	return &state, nil
}

func filterTopics(all []protocol.MetadataTopic, requested []string) []protocol.MetadataTopic {
	if len(requested) == 0 {
		return cloneTopics(all)
	}
	index := make(map[string]protocol.MetadataTopic, len(all))
	for _, topic := range all {
		index[topic.Name] = topic
	}
	result := make([]protocol.MetadataTopic, 0, len(requested))
	for _, name := range requested {
		if topic, ok := index[name]; ok {
			result = append(result, topic)
		} else {
			result = append(result, protocol.MetadataTopic{
				ErrorCode: 3, // UNKNOWN_TOPIC_OR_PARTITION
				Name:      name,
			})
		}
	}
	return result
}

func cloneMetadata(src ClusterMetadata) ClusterMetadata {
	return ClusterMetadata{
		Brokers:      cloneBrokers(src.Brokers),
		ControllerID: src.ControllerID,
		Topics:       cloneTopics(src.Topics),
		ClusterID:    cloneStringPtr(src.ClusterID),
	}
}

func cloneBrokers(brokers []protocol.MetadataBroker) []protocol.MetadataBroker {
	if len(brokers) == 0 {
		return nil
	}
	out := make([]protocol.MetadataBroker, len(brokers))
	copy(out, brokers)
	return out
}

func cloneTopics(topics []protocol.MetadataTopic) []protocol.MetadataTopic {
	if len(topics) == 0 {
		return nil
	}
	out := make([]protocol.MetadataTopic, len(topics))
	for i, topic := range topics {
		out[i] = protocol.MetadataTopic{
			ErrorCode:  topic.ErrorCode,
			Name:       topic.Name,
			Partitions: clonePartitions(topic.Partitions),
		}
	}
	return out
}

func clonePartitions(parts []protocol.MetadataPartition) []protocol.MetadataPartition {
	if len(parts) == 0 {
		return nil
	}
	out := make([]protocol.MetadataPartition, len(parts))
	for i, part := range parts {
		out[i] = protocol.MetadataPartition{
			ErrorCode:      part.ErrorCode,
			PartitionIndex: part.PartitionIndex,
			LeaderID:       part.LeaderID,
			ReplicaNodes:   cloneInt32Slice(part.ReplicaNodes),
			ISRNodes:       cloneInt32Slice(part.ISRNodes),
		}
	}
	return out
}

func cloneInt32Slice(src []int32) []int32 {
	if len(src) == 0 {
		return nil
	}
	out := make([]int32, len(src))
	copy(out, src)
	return out
}

func cloneStringPtr(s *string) *string {
	if s == nil {
		return nil
	}
	c := *s
	return &c
}

var (
	// ErrStoreUnavailable is returned when the metadata store cannot be reached.
	ErrStoreUnavailable = errors.New("metadata store unavailable")
)
