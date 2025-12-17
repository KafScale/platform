package metadata

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/novatechflow/kafscale/pkg/protocol"
)

// Store exposes read-only access to cluster metadata used by Kafka protocol handlers.
type Store interface {
	// Metadata returns brokers, controller ID, and topics. When topics is non-empty,
	// the implementation should filter to that subset and omit missing topics.
	Metadata(ctx context.Context, topics []string) (*ClusterMetadata, error)
	// NextOffset returns the next offset to assign for a topic/partition.
	NextOffset(ctx context.Context, topic string, partition int32) (int64, error)
	// UpdateOffsets records the last persisted offset so future appends continue from there.
	UpdateOffsets(ctx context.Context, topic string, partition int32, lastOffset int64) error
	// CommitConsumerOffset persists a consumer group offset.
	CommitConsumerOffset(ctx context.Context, group, topic string, partition int32, offset int64, metadata string) error
	// FetchConsumerOffset retrieves the committed offset for a consumer group partition.
	FetchConsumerOffset(ctx context.Context, group, topic string, partition int32) (int64, string, error)
	// CreateTopic creates a new topic with the provided specification.
	CreateTopic(ctx context.Context, spec TopicSpec) (*protocol.MetadataTopic, error)
	// DeleteTopic removes a topic and associated offsets.
	DeleteTopic(ctx context.Context, name string) error
}

// TopicSpec describes a topic creation request.
type TopicSpec struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
}

var (
	// ErrTopicExists indicates the topic is already present.
	ErrTopicExists = errors.New("topic already exists")
	// ErrInvalidTopic indicates the topic specification is invalid.
	ErrInvalidTopic = errors.New("invalid topic configuration")
	// ErrUnknownTopic indicates the topic does not exist.
	ErrUnknownTopic = errors.New("unknown topic")
)

// ClusterMetadata describes the Kafka-visible cluster state.
type ClusterMetadata struct {
	Brokers      []protocol.MetadataBroker
	ControllerID int32
	Topics       []protocol.MetadataTopic
	ClusterID    *string
}

// InMemoryStore is a simple Store backed by in-process state. Useful for early development and tests.
type InMemoryStore struct {
	mu              sync.RWMutex
	state           ClusterMetadata
	offsets         map[string]int64
	consumerOffsets map[string]int64
	consumerMeta    map[string]string
}

// NewInMemoryStore builds an in-memory metadata store with the provided state.
func NewInMemoryStore(state ClusterMetadata) *InMemoryStore {
	return &InMemoryStore{
		state:           cloneMetadata(state),
		offsets:         make(map[string]int64),
		consumerOffsets: make(map[string]int64),
		consumerMeta:    make(map[string]string),
	}
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

// NextOffset implements Store.NextOffset.
func (s *InMemoryStore) NextOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offsets[partitionKey(topic, partition)], nil
}

// UpdateOffsets implements Store.UpdateOffsets.
func (s *InMemoryStore) UpdateOffsets(ctx context.Context, topic string, partition int32, lastOffset int64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets[partitionKey(topic, partition)] = lastOffset + 1
	return nil
}

func partitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s:%d", topic, partition)
}

func consumerKey(group, topic string, partition int32) string {
	return fmt.Sprintf("%s:%s:%d", group, topic, partition)
}

// CreateTopic implements Store.CreateTopic.
func (s *InMemoryStore) CreateTopic(ctx context.Context, spec TopicSpec) (*protocol.MetadataTopic, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if spec.Name == "" || spec.NumPartitions <= 0 {
		return nil, ErrInvalidTopic
	}
	if spec.ReplicationFactor <= 0 {
		spec.ReplicationFactor = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, topic := range s.state.Topics {
		if topic.Name == spec.Name {
			return nil, ErrTopicExists
		}
	}
	if int(spec.ReplicationFactor) > len(s.state.Brokers) {
		return nil, ErrInvalidTopic
	}
	leaderID := s.defaultLeaderID()
	partitions := make([]protocol.MetadataPartition, spec.NumPartitions)
	for i := range partitions {
		partitions[i] = protocol.MetadataPartition{
			PartitionIndex: int32(i),
			LeaderID:       leaderID,
			ReplicaNodes:   []int32{leaderID},
			ISRNodes:       []int32{leaderID},
		}
	}
	newTopic := protocol.MetadataTopic{
		Name:       spec.Name,
		Partitions: partitions,
	}
	s.state.Topics = append(s.state.Topics, newTopic)
	return &newTopic, nil
}

func (s *InMemoryStore) defaultLeaderID() int32 {
	if len(s.state.Brokers) == 0 {
		return s.state.ControllerID
	}
	return s.state.Brokers[0].NodeID
}

// DeleteTopic implements Store.DeleteTopic.
func (s *InMemoryStore) DeleteTopic(ctx context.Context, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	index := -1
	for i, topic := range s.state.Topics {
		if topic.Name == name {
			index = i
			break
		}
	}
	if index == -1 {
		return ErrUnknownTopic
	}
	s.state.Topics = append(s.state.Topics[:index], s.state.Topics[index+1:]...)
	for key := range s.offsets {
		if strings.HasPrefix(key, name+":") {
			delete(s.offsets, key)
		}
	}
	return nil
}

// CommitConsumerOffset implements Store.CommitConsumerOffset.
func (s *InMemoryStore) CommitConsumerOffset(ctx context.Context, group, topic string, partition int32, offset int64, metadata string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := consumerKey(group, topic, partition)
	s.consumerOffsets[key] = offset
	s.consumerMeta[key] = metadata
	return nil
}

// FetchConsumerOffset implements Store.FetchConsumerOffset.
func (s *InMemoryStore) FetchConsumerOffset(ctx context.Context, group, topic string, partition int32) (int64, string, error) {
	select {
	case <-ctx.Done():
		return 0, "", ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := consumerKey(group, topic, partition)
	return s.consumerOffsets[key], s.consumerMeta[key], nil
}

var (
	// ErrStoreUnavailable is returned when the metadata store cannot be reached.
	ErrStoreUnavailable = errors.New("metadata store unavailable")
)
