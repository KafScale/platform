package broker

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/protocol"
)

const (
	defaultSessionTimeout   = 30 * time.Second
	defaultRebalanceTimeout = 30 * time.Second
)

type groupPhase int

const (
	groupStateEmpty groupPhase = iota
	groupStatePreparingRebalance
	groupStateCompletingRebalance
	groupStateStable
	groupStateDead
)

type GroupCoordinator struct {
	store  metadata.Store
	broker protocol.MetadataBroker
	config CoordinatorConfig
	stopCh chan struct{}
	mu     sync.Mutex
	groups map[string]*groupState
}

type CoordinatorConfig struct {
	CleanupInterval time.Duration
}

var defaultCoordinatorConfig = CoordinatorConfig{
	CleanupInterval: 5 * time.Second,
}

type groupState struct {
	protocolName string
	protocolType string
	generationID int32
	leaderID     string
	state        groupPhase

	members     map[string]*memberState
	assignments map[string][]assignmentTopic

	rebalanceTimeout  time.Duration
	rebalanceDeadline time.Time
}

type memberState struct {
	topics         []string
	sessionTimeout time.Duration
	lastHeartbeat  time.Time
	joinGeneration int32
}

type assignmentTopic struct {
	Name       string
	Partitions []int32
}

func NewGroupCoordinator(store metadata.Store, broker protocol.MetadataBroker, cfg *CoordinatorConfig) *GroupCoordinator {
	config := defaultCoordinatorConfig
	if cfg != nil && cfg.CleanupInterval > 0 {
		config.CleanupInterval = cfg.CleanupInterval
	}
	c := &GroupCoordinator{
		store:  store,
		broker: broker,
		config: config,
		stopCh: make(chan struct{}),
		groups: make(map[string]*groupState),
	}
	go c.cleanupLoop()
	return c
}

func (c *GroupCoordinator) FindCoordinatorResponse(correlationID int32, errorCode int16) *protocol.FindCoordinatorResponse {
	return &protocol.FindCoordinatorResponse{
		CorrelationID: correlationID,
		ErrorCode:     errorCode,
		NodeID:        c.broker.NodeID,
		Host:          c.broker.Host,
		Port:          c.broker.Port,
	}
}

func (c *GroupCoordinator) JoinGroup(ctx context.Context, req *protocol.JoinGroupRequest, correlationID int32) (*protocol.JoinGroupResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.ensureGroup(req.GroupID)
	state.protocolType = req.ProtocolType
	if len(req.Protocols) > 0 {
		state.protocolName = req.Protocols[0].Name
	}

	timeout := time.Duration(req.RebalanceTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultRebalanceTimeout
	}

	memberID := req.MemberID
	member, exists := state.members[memberID]
	if memberID == "" || member == nil {
		memberID = c.newMemberID(req.GroupID)
		member = &memberState{}
		state.members[memberID] = member
		exists = false
	}

	if req.SessionTimeoutMs > 0 {
		member.sessionTimeout = time.Duration(req.SessionTimeoutMs) * time.Millisecond
	} else if member.sessionTimeout == 0 {
		member.sessionTimeout = defaultSessionTimeout
	}
	member.topics = c.parseSubscriptionTopics(req.Protocols)
	member.lastHeartbeat = time.Now()

	if len(state.members) == 1 && state.state == groupStateEmpty {
		state.leaderID = memberID
		state.startRebalance(timeout)
	} else if state.state == groupStateStable && !exists {
		state.startRebalance(timeout)
	} else if state.state == groupStateEmpty {
		state.startRebalance(timeout)
	} else if state.state == groupStatePreparingRebalance || state.state == groupStateCompletingRebalance {
		state.bumpRebalanceDeadline(timeout)
	}

	member.joinGeneration = state.generationID
	if state.leaderID == "" {
		state.ensureLeader()
	}

	ready := state.state == groupStateStable || state.state == groupStateCompletingRebalance
	if !ready {
		ready = state.completeIfReady()
	}

	var members []protocol.JoinGroupMember
	if ready && memberID == state.leaderID {
		members = c.encodeMemberSubscriptions(state)
	} else {
		members = []protocol.JoinGroupMember{}
	}

	resp := &protocol.JoinGroupResponse{
		CorrelationID: correlationID,
		GenerationID:  state.generationID,
		ProtocolName:  state.protocolName,
		LeaderID:      state.leaderID,
		MemberID:      memberID,
		Members:       members,
		ErrorCode:     protocol.REBALANCE_IN_PROGRESS,
	}
	if ready {
		resp.ErrorCode = protocol.NONE
	}
	return resp, nil
}

func (c *GroupCoordinator) SyncGroup(ctx context.Context, req *protocol.SyncGroupRequest, correlationID int32) (*protocol.SyncGroupResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.groups[req.GroupID]
	if state == nil {
		return &protocol.SyncGroupResponse{
			CorrelationID: correlationID,
			ErrorCode:     protocol.UNKNOWN_MEMBER_ID,
		}, nil
	}
	if req.GenerationID != state.generationID {
		return &protocol.SyncGroupResponse{
			CorrelationID: correlationID,
			ErrorCode:     protocol.ILLEGAL_GENERATION,
		}, nil
	}
	if _, ok := state.members[req.MemberID]; !ok {
		return &protocol.SyncGroupResponse{
			CorrelationID: correlationID,
			ErrorCode:     protocol.UNKNOWN_MEMBER_ID,
		}, nil
	}
	if state.state == groupStatePreparingRebalance {
		return &protocol.SyncGroupResponse{
			CorrelationID: correlationID,
			ErrorCode:     protocol.REBALANCE_IN_PROGRESS,
		}, nil
	}

	if state.state == groupStateCompletingRebalance && len(state.assignments) == 0 {
		if req.MemberID != state.leaderID {
			return &protocol.SyncGroupResponse{
				CorrelationID: correlationID,
				ErrorCode:     protocol.REBALANCE_IN_PROGRESS,
			}, nil
		}
		state.assignments = c.assignPartitions(ctx, state)
		state.markStable()
	}

	assignments := state.assignments[req.MemberID]
	if assignments == nil && state.state != groupStateStable {
		return &protocol.SyncGroupResponse{
			CorrelationID: correlationID,
			ErrorCode:     protocol.REBALANCE_IN_PROGRESS,
		}, nil
	}

	return &protocol.SyncGroupResponse{
		CorrelationID: correlationID,
		ErrorCode:     protocol.NONE,
		Assignment:    encodeAssignment(assignments),
	}, nil
}

func (c *GroupCoordinator) Heartbeat(ctx context.Context, req *protocol.HeartbeatRequest, correlationID int32) *protocol.HeartbeatResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.groups[req.GroupID]
	if state == nil {
		return &protocol.HeartbeatResponse{
			CorrelationID: correlationID,
			ErrorCode:     protocol.UNKNOWN_MEMBER_ID,
		}
	}
	member := state.members[req.MemberID]
	if member == nil {
		return &protocol.HeartbeatResponse{
			CorrelationID: correlationID,
			ErrorCode:     protocol.UNKNOWN_MEMBER_ID,
		}
	}
	if req.GenerationID != state.generationID {
		return &protocol.HeartbeatResponse{
			CorrelationID: correlationID,
			ErrorCode:     protocol.ILLEGAL_GENERATION,
		}
	}
	if state.state != groupStateStable {
		return &protocol.HeartbeatResponse{
			CorrelationID: correlationID,
			ErrorCode:     protocol.REBALANCE_IN_PROGRESS,
		}
	}
	member.lastHeartbeat = time.Now()
	return &protocol.HeartbeatResponse{
		CorrelationID: correlationID,
		ErrorCode:     protocol.NONE,
	}
}

func (c *GroupCoordinator) LeaveGroup(ctx context.Context, req *protocol.LeaveGroupRequest, correlationID int32) *protocol.LeaveGroupResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.groups[req.GroupID]
	if state == nil {
		return &protocol.LeaveGroupResponse{
			CorrelationID: correlationID,
			ErrorCode:     protocol.UNKNOWN_MEMBER_ID,
		}
	}
	if _, ok := state.members[req.MemberID]; !ok {
		return &protocol.LeaveGroupResponse{
			CorrelationID: correlationID,
			ErrorCode:     protocol.UNKNOWN_MEMBER_ID,
		}
	}
	delete(state.members, req.MemberID)
	delete(state.assignments, req.MemberID)

	if len(state.members) == 0 {
		delete(c.groups, req.GroupID)
		return &protocol.LeaveGroupResponse{
			CorrelationID: correlationID,
			ErrorCode:     protocol.NONE,
		}
	}
	if state.leaderID == req.MemberID {
		state.leaderID = ""
	}
	state.startRebalance(0)
	return &protocol.LeaveGroupResponse{
		CorrelationID: correlationID,
		ErrorCode:     protocol.NONE,
	}
}

func (c *GroupCoordinator) OffsetCommit(ctx context.Context, req *protocol.OffsetCommitRequest, correlationID int32) (*protocol.OffsetCommitResponse, error) {
	c.mu.Lock()
	state := c.groups[req.GroupID]
	c.mu.Unlock()

	groupErr := int16(protocol.NONE)
	if state == nil {
		groupErr = protocol.UNKNOWN_MEMBER_ID
	} else if _, ok := state.members[req.MemberID]; !ok {
		groupErr = protocol.UNKNOWN_MEMBER_ID
	} else if req.GenerationID != state.generationID {
		groupErr = protocol.ILLEGAL_GENERATION
	}

	results := make([]protocol.OffsetCommitTopicResponse, 0, len(req.Topics))
	for _, topic := range req.Topics {
		partitions := make([]protocol.OffsetCommitPartitionResponse, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			code := groupErr
			if code == protocol.NONE {
				if err := c.store.CommitConsumerOffset(ctx, req.GroupID, topic.Name, part.Partition, part.Offset, part.Metadata); err != nil {
					code = protocol.UNKNOWN_SERVER_ERROR
				}
			}
			partitions = append(partitions, protocol.OffsetCommitPartitionResponse{
				Partition: part.Partition,
				ErrorCode: code,
			})
		}
		results = append(results, protocol.OffsetCommitTopicResponse{
			Name:       topic.Name,
			Partitions: partitions,
		})
	}
	return &protocol.OffsetCommitResponse{
		CorrelationID: correlationID,
		Topics:        results,
	}, nil
}

func (c *GroupCoordinator) OffsetFetch(ctx context.Context, req *protocol.OffsetFetchRequest, correlationID int32) (*protocol.OffsetFetchResponse, error) {
	topicResponses := make([]protocol.OffsetFetchTopicResponse, 0, len(req.Topics))
	for _, topic := range req.Topics {
		partitions := make([]protocol.OffsetFetchPartitionResponse, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			offset, metadataStr, err := c.store.FetchConsumerOffset(ctx, req.GroupID, topic.Name, part.Partition)
			code := protocol.NONE
			if err != nil {
				code = protocol.UNKNOWN_SERVER_ERROR
			}
			partitions = append(partitions, protocol.OffsetFetchPartitionResponse{
				Partition: part.Partition,
				Offset:    offset,
				Metadata:  metadataStr,
				ErrorCode: code,
			})
		}
		topicResponses = append(topicResponses, protocol.OffsetFetchTopicResponse{
			Name:       topic.Name,
			Partitions: partitions,
		})
	}
	return &protocol.OffsetFetchResponse{
		CorrelationID: correlationID,
		Topics:        topicResponses,
		ErrorCode:     protocol.NONE,
	}, nil
}

func (c *GroupCoordinator) ensureGroup(groupID string) *groupState {
	state := c.groups[groupID]
	if state == nil {
		state = &groupState{
			members:          make(map[string]*memberState),
			assignments:      make(map[string][]assignmentTopic),
			state:            groupStateEmpty,
			rebalanceTimeout: defaultRebalanceTimeout,
		}
		c.groups[groupID] = state
	}
	return state
}

func (c *GroupCoordinator) newMemberID(group string) string {
	return fmt.Sprintf("%s-%d", group, rand.Int63())
}

func (c *GroupCoordinator) parseSubscriptionTopics(protocols []protocol.JoinGroupProtocol) []string {
	if len(protocols) == 0 {
		return nil
	}
	data := protocols[0].Metadata
	if len(data) < 2 {
		return nil
	}
	read := 2 // version
	if len(data[read:]) < 4 {
		return nil
	}
	topicCount := binary.BigEndian.Uint32(data[read : read+4])
	read += 4
	topics := make([]string, 0, topicCount)
	for i := uint32(0); i < topicCount && read+2 <= len(data); i++ {
		nameLen := binary.BigEndian.Uint16(data[read : read+2])
		read += 2
		if int(read)+int(nameLen) > len(data) {
			break
		}
		topics = append(topics, string(data[read:read+int(nameLen)]))
		read += int(nameLen)
	}
	return topics
}

func (c *GroupCoordinator) encodeSubscription(topics []string) []byte {
	buf := make([]byte, 0, 6+len(topics)*10)
	writeInt16 := func(v int16) {
		tmp := make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(v))
		buf = append(buf, tmp...)
	}
	writeInt32 := func(v int32) {
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(v))
		buf = append(buf, tmp...)
	}
	writeInt16(0) // version
	writeInt32(int32(len(topics)))
	for _, topic := range topics {
		writeInt16(int16(len(topic)))
		buf = append(buf, []byte(topic)...)
	}
	writeInt32(0) // user data length
	return buf
}

func (c *GroupCoordinator) encodeMemberSubscriptions(state *groupState) []protocol.JoinGroupMember {
	ids := state.sortedMembers()
	members := make([]protocol.JoinGroupMember, 0, len(ids))
	for _, id := range ids {
		member := state.members[id]
		members = append(members, protocol.JoinGroupMember{
			MemberID: id,
			Metadata: c.encodeSubscription(member.topics),
		})
	}
	return members
}

func (c *GroupCoordinator) assignPartitions(ctx context.Context, state *groupState) map[string][]assignmentTopic {
	topics := c.collectTopicPartitions(ctx, state)
	if len(state.members) == 0 {
		return map[string][]assignmentTopic{}
	}

	memberIDs := state.sortedMembers()
	result := make(map[string]map[string][]int32, len(memberIDs))
	for _, id := range memberIDs {
		result[id] = make(map[string][]int32)
	}

	for topic, partitions := range topics {
		eligible := make([]string, 0, len(memberIDs))
		for _, memberID := range memberIDs {
			if memberSubscribes(state.members[memberID], topic) {
				eligible = append(eligible, memberID)
			}
		}
		if len(eligible) == 0 {
			continue
		}
		for idx, partition := range partitions {
			memberID := eligible[idx%len(eligible)]
			result[memberID][topic] = append(result[memberID][topic], partition)
		}
	}

	assignments := make(map[string][]assignmentTopic, len(result))
	for memberID, topics := range result {
		if len(topics) == 0 {
			assignments[memberID] = nil
			continue
		}
		names := make([]string, 0, len(topics))
		for name := range topics {
			names = append(names, name)
		}
		sort.Strings(names)
		memberAssignments := make([]assignmentTopic, 0, len(names))
		for _, name := range names {
			partitions := topics[name]
			sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
			memberAssignments = append(memberAssignments, assignmentTopic{
				Name:       name,
				Partitions: partitions,
			})
		}
		assignments[memberID] = memberAssignments
	}

	return assignments
}

func memberSubscribes(member *memberState, topic string) bool {
	if member == nil {
		return false
	}
	for _, t := range member.topics {
		if t == topic {
			return true
		}
	}
	return false
}

func (c *GroupCoordinator) collectTopicPartitions(ctx context.Context, state *groupState) map[string][]int32 {
	subscriptions := make([]string, 0)
	seen := make(map[string]struct{})
	for _, member := range state.members {
		for _, topic := range member.topics {
			if _, ok := seen[topic]; !ok {
				seen[topic] = struct{}{}
				subscriptions = append(subscriptions, topic)
			}
		}
	}
	if len(subscriptions) == 0 {
		return map[string][]int32{}
	}
	meta, err := c.store.Metadata(ctx, subscriptions)
	if err != nil || meta == nil {
		result := make(map[string][]int32)
		for _, topic := range subscriptions {
			result[topic] = []int32{0}
		}
		return result
	}
	result := make(map[string][]int32)
	for _, topic := range meta.Topics {
		partitions := make([]int32, 0, len(topic.Partitions))
		for _, p := range topic.Partitions {
			partitions = append(partitions, p.PartitionIndex)
		}
		if len(partitions) == 0 {
			partitions = []int32{0}
		}
		sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
		result[topic.Name] = partitions
	}
	return result
}

func encodeAssignment(topics []assignmentTopic) []byte {
	buf := make([]byte, 0, 16)
	writeInt16 := func(v int16) {
		tmp := make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(v))
		buf = append(buf, tmp...)
	}
	writeInt32 := func(v int32) {
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(v))
		buf = append(buf, tmp...)
	}
	writeInt16(0) // version
	writeInt32(int32(len(topics)))
	for _, topic := range topics {
		writeInt16(int16(len(topic.Name)))
		buf = append(buf, []byte(topic.Name)...)
		writeInt32(int32(len(topic.Partitions)))
		for _, part := range topic.Partitions {
			writeInt32(part)
		}
	}
	writeInt32(0) // user data length
	return buf
}

func (c *GroupCoordinator) cleanupLoop() {
	ticker := time.NewTicker(c.config.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.cleanupGroups()
		case <-c.stopCh:
			return
		}
	}
}

// Stop terminates background cleanup routines.
func (c *GroupCoordinator) Stop() {
	select {
	case <-c.stopCh:
		return
	default:
		close(c.stopCh)
	}
}

func (c *GroupCoordinator) cleanupGroups() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for groupID, state := range c.groups {
		removed := state.removeExpiredMembers(now)
		lostDuringRebalance := state.dropRebalanceLaggers(now)

		if len(state.members) == 0 {
			delete(c.groups, groupID)
			continue
		}
		if removed || lostDuringRebalance {
			state.startRebalance(0)
		}
	}
}

func (s *groupState) ensureLeader() {
	if s.leaderID != "" {
		if _, ok := s.members[s.leaderID]; ok {
			return
		}
	}
	if len(s.members) == 0 {
		s.leaderID = ""
		return
	}
	ids := s.sortedMembers()
	s.leaderID = ids[0]
}

func (s *groupState) sortedMembers() []string {
	ids := make([]string, 0, len(s.members))
	for id := range s.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (s *groupState) startRebalance(timeout time.Duration) {
	if len(s.members) == 0 {
		s.state = groupStateEmpty
		s.assignments = make(map[string][]assignmentTopic)
		s.rebalanceDeadline = time.Time{}
		s.leaderID = ""
		return
	}
	if timeout > 0 {
		s.rebalanceTimeout = timeout
	} else if s.rebalanceTimeout == 0 {
		s.rebalanceTimeout = defaultRebalanceTimeout
	}
	s.generationID++
	s.state = groupStatePreparingRebalance
	s.assignments = make(map[string][]assignmentTopic)
	s.rebalanceDeadline = time.Now().Add(s.rebalanceTimeout)
	s.ensureLeader()
	for _, member := range s.members {
		member.joinGeneration = 0
	}
}

func (s *groupState) bumpRebalanceDeadline(timeout time.Duration) {
	if timeout > 0 {
		s.rebalanceTimeout = timeout
	}
	if s.rebalanceTimeout == 0 {
		s.rebalanceTimeout = defaultRebalanceTimeout
	}
	s.rebalanceDeadline = time.Now().Add(s.rebalanceTimeout)
}

func (s *groupState) completeIfReady() bool {
	if len(s.members) == 0 {
		return false
	}
	for _, member := range s.members {
		if member.joinGeneration != s.generationID {
			return false
		}
	}
	s.state = groupStateCompletingRebalance
	s.rebalanceDeadline = time.Time{}
	return true
}

func (s *groupState) markStable() {
	if s.state != groupStateDead {
		s.state = groupStateStable
		s.rebalanceDeadline = time.Time{}
	}
}

func (s *groupState) removeExpiredMembers(now time.Time) bool {
	changed := false
	for memberID, member := range s.members {
		timeout := member.sessionTimeout
		if timeout == 0 {
			timeout = defaultSessionTimeout
		}
		if now.Sub(member.lastHeartbeat) > timeout {
			delete(s.members, memberID)
			delete(s.assignments, memberID)
			if s.leaderID == memberID {
				s.leaderID = ""
			}
			changed = true
		}
	}
	if len(s.members) == 0 {
		s.state = groupStateEmpty
	}
	return changed
}

func (s *groupState) dropRebalanceLaggers(now time.Time) bool {
	if s.rebalanceDeadline.IsZero() || now.Before(s.rebalanceDeadline) {
		return false
	}
	changed := false
	for memberID, member := range s.members {
		if member.joinGeneration != s.generationID {
			delete(s.members, memberID)
			delete(s.assignments, memberID)
			if s.leaderID == memberID {
				s.leaderID = ""
			}
			changed = true
		}
	}
	if len(s.members) == 0 {
		s.state = groupStateEmpty
	}
	return changed
}
