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

package broker

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
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

const (
	groupStateEmptyStr      = "empty"
	groupStatePreparingStr  = "preparing_rebalance"
	groupStateCompletingStr = "completing_rebalance"
	groupStateStableStr     = "stable"
	groupStateDeadStr       = "dead"
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

func (c *GroupCoordinator) FindCoordinatorResponse(errorCode int16) *kmsg.FindCoordinatorResponse {
	resp := kmsg.NewPtrFindCoordinatorResponse()
	resp.ErrorCode = errorCode
	resp.NodeID = c.broker.NodeID
	resp.Host = c.broker.Host
	resp.Port = c.broker.Port
	return resp
}

func (c *GroupCoordinator) JoinGroup(ctx context.Context, req *kmsg.JoinGroupRequest) (*kmsg.JoinGroupResponse, error) {
	c.mu.Lock()
	state, err := c.ensureGroup(ctx, req.Group)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	state.protocolType = req.ProtocolType
	if len(req.Protocols) > 0 {
		state.protocolName = req.Protocols[0].Name
	}

	timeout := time.Duration(req.RebalanceTimeoutMillis) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultRebalanceTimeout
	}

	memberID := req.MemberID
	member, exists := state.members[memberID]
	if memberID == "" || member == nil {
		memberID = c.newMemberID(req.Group)
		member = &memberState{}
		state.members[memberID] = member
		exists = false
	}

	if req.SessionTimeoutMillis > 0 {
		member.sessionTimeout = time.Duration(req.SessionTimeoutMillis) * time.Millisecond
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

	resp := kmsg.NewPtrJoinGroupResponse()
	resp.Generation = state.generationID
	resp.Protocol = &state.protocolName
	resp.LeaderID = state.leaderID
	resp.MemberID = memberID
	resp.ErrorCode = protocol.REBALANCE_IN_PROGRESS

	if ready && memberID == state.leaderID {
		resp.Members = c.encodeMemberSubscriptions(state)
	} else {
		resp.Members = []kmsg.JoinGroupResponseMember{}
	}

	if ready {
		resp.ErrorCode = protocol.NONE
	}
	if err := c.persistGroupLocked(ctx, req.Group, state); err != nil {
		resp.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
	}
	c.mu.Unlock()
	return resp, nil
}

func (c *GroupCoordinator) SyncGroup(ctx context.Context, req *kmsg.SyncGroupRequest) (*kmsg.SyncGroupResponse, error) {
	c.mu.Lock()
	state, err := c.loadGroupIfMissing(ctx, req.Group)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	mkErrResp := func(code int16) *kmsg.SyncGroupResponse {
		r := kmsg.NewPtrSyncGroupResponse()
		r.ErrorCode = code
		return r
	}

	if state == nil {
		c.mu.Unlock()
		return mkErrResp(protocol.UNKNOWN_MEMBER_ID), nil
	}
	if req.Generation != state.generationID {
		c.mu.Unlock()
		return mkErrResp(protocol.ILLEGAL_GENERATION), nil
	}
	if _, ok := state.members[req.MemberID]; !ok {
		c.mu.Unlock()
		return mkErrResp(protocol.UNKNOWN_MEMBER_ID), nil
	}
	if state.state == groupStatePreparingRebalance {
		c.mu.Unlock()
		return mkErrResp(protocol.REBALANCE_IN_PROGRESS), nil
	}

	if state.state == groupStateCompletingRebalance && len(state.assignments) == 0 {
		if req.MemberID != state.leaderID {
			c.mu.Unlock()
			return mkErrResp(protocol.REBALANCE_IN_PROGRESS), nil
		}
		state.assignments = c.assignPartitions(ctx, state)
		state.markStable()
	}

	assignments := state.assignments[req.MemberID]
	if assignments == nil && state.state != groupStateStable {
		c.mu.Unlock()
		return mkErrResp(protocol.REBALANCE_IN_PROGRESS), nil
	}

	resp := kmsg.NewPtrSyncGroupResponse()
	resp.ErrorCode = protocol.NONE
	if state.protocolType != "" {
		resp.ProtocolType = &state.protocolType
	}
	if state.protocolName != "" {
		resp.Protocol = &state.protocolName
	}
	resp.MemberAssignment = encodeAssignment(assignments)

	if err := c.persistGroupLocked(ctx, req.Group, state); err != nil {
		resp.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
	}
	c.mu.Unlock()
	return resp, nil
}

func (c *GroupCoordinator) Heartbeat(ctx context.Context, req *kmsg.HeartbeatRequest) *kmsg.HeartbeatResponse {
	c.mu.Lock()
	state, err := c.loadGroupIfMissing(ctx, req.Group)

	mkResp := func(code int16) *kmsg.HeartbeatResponse {
		r := kmsg.NewPtrHeartbeatResponse()
		r.ErrorCode = code
		return r
	}

	if err != nil {
		c.mu.Unlock()
		return mkResp(protocol.UNKNOWN_SERVER_ERROR)
	}
	if state == nil {
		c.mu.Unlock()
		return mkResp(protocol.UNKNOWN_MEMBER_ID)
	}
	member := state.members[req.MemberID]
	if member == nil {
		c.mu.Unlock()
		return mkResp(protocol.UNKNOWN_MEMBER_ID)
	}
	if req.Generation != state.generationID {
		c.mu.Unlock()
		return mkResp(protocol.ILLEGAL_GENERATION)
	}
	if state.state != groupStateStable {
		c.mu.Unlock()
		return mkResp(protocol.REBALANCE_IN_PROGRESS)
	}
	member.lastHeartbeat = time.Now()
	resp := mkResp(protocol.NONE)
	if err := c.persistGroupLocked(ctx, req.Group, state); err != nil {
		resp.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
	}
	c.mu.Unlock()
	return resp
}

func (c *GroupCoordinator) LeaveGroup(ctx context.Context, req *kmsg.LeaveGroupRequest) *kmsg.LeaveGroupResponse {
	c.mu.Lock()
	state, err := c.loadGroupIfMissing(ctx, req.Group)

	mkResp := func(code int16) *kmsg.LeaveGroupResponse {
		r := kmsg.NewPtrLeaveGroupResponse()
		r.ErrorCode = code
		return r
	}

	if err != nil {
		c.mu.Unlock()
		return mkResp(protocol.UNKNOWN_SERVER_ERROR)
	}
	if state == nil {
		c.mu.Unlock()
		return mkResp(protocol.UNKNOWN_MEMBER_ID)
	}
	if _, ok := state.members[req.MemberID]; !ok {
		c.mu.Unlock()
		return mkResp(protocol.UNKNOWN_MEMBER_ID)
	}
	delete(state.members, req.MemberID)
	delete(state.assignments, req.MemberID)

	if len(state.members) == 0 {
		delete(c.groups, req.Group)
		resp := mkResp(protocol.NONE)
		if err := c.persistGroupLocked(ctx, req.Group, nil); err != nil {
			resp.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
		}
		c.mu.Unlock()
		return resp
	}
	if state.leaderID == req.MemberID {
		state.leaderID = ""
	}
	state.startRebalance(0)
	resp := mkResp(protocol.NONE)
	if err := c.persistGroupLocked(ctx, req.Group, state); err != nil {
		resp.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
	}
	c.mu.Unlock()
	return resp
}

func (c *GroupCoordinator) OffsetCommit(ctx context.Context, req *kmsg.OffsetCommitRequest) (*kmsg.OffsetCommitResponse, error) {
	c.mu.Lock()
	state, err := c.loadGroupIfMissing(ctx, req.Group)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	groupErr := int16(protocol.NONE)
	if state == nil {
		groupErr = protocol.UNKNOWN_MEMBER_ID
	} else if _, ok := state.members[req.MemberID]; !ok {
		groupErr = protocol.UNKNOWN_MEMBER_ID
	} else if req.Generation != state.generationID {
		groupErr = protocol.ILLEGAL_GENERATION
	}
	c.mu.Unlock()

	resp := kmsg.NewPtrOffsetCommitResponse()
	resp.Topics = make([]kmsg.OffsetCommitResponseTopic, 0, len(req.Topics))
	for _, topic := range req.Topics {
		topicResp := kmsg.NewOffsetCommitResponseTopic()
		topicResp.Topic = topic.Topic
		topicResp.Partitions = make([]kmsg.OffsetCommitResponseTopicPartition, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			code := groupErr
			if code == protocol.NONE {
				meta := ""
				if part.Metadata != nil {
					meta = *part.Metadata
				}
				if err := c.store.CommitConsumerOffset(ctx, req.Group, topic.Topic, part.Partition, part.Offset, meta); err != nil {
					code = protocol.UNKNOWN_SERVER_ERROR
				}
			}
			partResp := kmsg.NewOffsetCommitResponseTopicPartition()
			partResp.Partition = part.Partition
			partResp.ErrorCode = code
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}
	return resp, nil
}

func (c *GroupCoordinator) OffsetFetch(ctx context.Context, req *kmsg.OffsetFetchRequest) (*kmsg.OffsetFetchResponse, error) {
	resp := kmsg.NewPtrOffsetFetchResponse()
	resp.ErrorCode = protocol.NONE
	resp.Topics = make([]kmsg.OffsetFetchResponseTopic, 0, len(req.Topics))
	for _, topic := range req.Topics {
		topicResp := kmsg.NewOffsetFetchResponseTopic()
		topicResp.Topic = topic.Topic
		topicResp.Partitions = make([]kmsg.OffsetFetchResponseTopicPartition, 0, len(topic.Partitions))
		for _, partID := range topic.Partitions {
			offset, metadataStr, err := c.store.FetchConsumerOffset(ctx, req.Group, topic.Topic, partID)
			code := int16(protocol.NONE)
			if err != nil {
				code = protocol.UNKNOWN_SERVER_ERROR
			}
			partResp := kmsg.NewOffsetFetchResponseTopicPartition()
			partResp.Partition = partID
			partResp.Offset = offset
			partResp.LeaderEpoch = -1
			partResp.Metadata = &metadataStr
			partResp.ErrorCode = code
			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}
	return resp, nil
}

func (c *GroupCoordinator) DescribeGroups(ctx context.Context, req *kmsg.DescribeGroupsRequest) (*kmsg.DescribeGroupsResponse, error) {
	resp := kmsg.NewPtrDescribeGroupsResponse()
	resp.Groups = make([]kmsg.DescribeGroupsResponseGroup, 0, len(req.Groups))
	for _, groupID := range req.Groups {
		group, err := c.store.FetchConsumerGroup(ctx, groupID)
		if err != nil {
			g := kmsg.NewDescribeGroupsResponseGroup()
			g.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			g.Group = groupID
			resp.Groups = append(resp.Groups, g)
			continue
		}
		if group == nil {
			g := kmsg.NewDescribeGroupsResponseGroup()
			g.ErrorCode = protocol.GROUP_ID_NOT_FOUND
			g.Group = groupID
			resp.Groups = append(resp.Groups, g)
			continue
		}
		resp.Groups = append(resp.Groups, buildDescribeGroup(group, req.IncludeAuthorizedOperations))
	}
	return resp, nil
}

func (c *GroupCoordinator) ListGroups(ctx context.Context, req *kmsg.ListGroupsRequest) (*kmsg.ListGroupsResponse, error) {
	groups, err := c.store.ListConsumerGroups(ctx)
	if err != nil {
		resp := kmsg.NewPtrListGroupsResponse()
		resp.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
		return resp, nil
	}

	var statesFilter, typesFilter []string
	if len(req.StatesFilter) > 0 {
		statesFilter = req.StatesFilter
	}
	if len(req.TypesFilter) > 0 {
		typesFilter = req.TypesFilter
	}

	resp := kmsg.NewPtrListGroupsResponse()
	resp.ErrorCode = protocol.NONE
	resp.Groups = make([]kmsg.ListGroupsResponseGroup, 0, len(groups))
	for _, group := range groups {
		state := kafkaGroupState(group.GetState())
		if !matchesGroupStateFilter(state, statesFilter) {
			continue
		}
		groupType := "classic"
		if !matchesGroupTypeFilter(groupType, typesFilter) {
			continue
		}
		protocolType := group.GetProtocolType()
		if protocolType == "" {
			protocolType = "consumer"
		}
		entry := kmsg.NewListGroupsResponseGroup()
		entry.Group = group.GetGroupId()
		entry.ProtocolType = protocolType
		entry.GroupState = state
		entry.GroupType = groupType
		resp.Groups = append(resp.Groups, entry)
	}
	return resp, nil
}

func (c *GroupCoordinator) DeleteGroups(ctx context.Context, req *kmsg.DeleteGroupsRequest) (*kmsg.DeleteGroupsResponse, error) {
	resp := kmsg.NewPtrDeleteGroupsResponse()
	resp.Groups = make([]kmsg.DeleteGroupsResponseGroup, 0, len(req.Groups))
	for _, groupID := range req.Groups {
		result := kmsg.NewDeleteGroupsResponseGroup()
		result.Group = groupID
		if strings.TrimSpace(groupID) == "" {
			result.ErrorCode = protocol.INVALID_REQUEST
			resp.Groups = append(resp.Groups, result)
			continue
		}
		group, err := c.store.FetchConsumerGroup(ctx, groupID)
		if err != nil {
			result.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			resp.Groups = append(resp.Groups, result)
			continue
		}
		if group == nil {
			result.ErrorCode = protocol.GROUP_ID_NOT_FOUND
			c.deleteGroupState(groupID)
			resp.Groups = append(resp.Groups, result)
			continue
		}
		if err := c.store.DeleteConsumerGroup(ctx, groupID); err != nil {
			result.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			resp.Groups = append(resp.Groups, result)
			continue
		}
		c.deleteGroupState(groupID)
		result.ErrorCode = protocol.NONE
		resp.Groups = append(resp.Groups, result)
	}
	return resp, nil
}

func (c *GroupCoordinator) deleteGroupState(groupID string) {
	c.mu.Lock()
	delete(c.groups, groupID)
	c.mu.Unlock()
}

func (c *GroupCoordinator) ensureGroup(ctx context.Context, groupID string) (*groupState, error) {
	if state, ok := c.groups[groupID]; ok {
		return state, nil
	}
	state, err := c.loadGroupIfMissing(ctx, groupID)
	if err != nil {
		return nil, err
	}
	if state != nil {
		return state, nil
	}
	state = &groupState{
		members:          make(map[string]*memberState),
		assignments:      make(map[string][]assignmentTopic),
		state:            groupStateEmpty,
		rebalanceTimeout: defaultRebalanceTimeout,
	}
	c.groups[groupID] = state
	return state, nil
}

func (c *GroupCoordinator) loadGroupIfMissing(ctx context.Context, groupID string) (*groupState, error) {
	if state, ok := c.groups[groupID]; ok {
		return state, nil
	}
	group, err := c.store.FetchConsumerGroup(ctx, groupID)
	if err != nil {
		return nil, err
	}
	if group == nil {
		return nil, nil
	}
	state := restoreGroupState(group)
	c.groups[groupID] = state
	return state, nil
}

func (c *GroupCoordinator) persistGroupLocked(ctx context.Context, groupID string, state *groupState) error {
	if state == nil || len(state.members) == 0 {
		return c.store.DeleteConsumerGroup(ctx, groupID)
	}
	group := buildConsumerGroup(groupID, state)
	return c.store.PutConsumerGroup(ctx, group)
}

func buildConsumerGroup(groupID string, state *groupState) *metadatapb.ConsumerGroup {
	group := &metadatapb.ConsumerGroup{
		GroupId:      groupID,
		State:        groupPhaseString(state.state),
		ProtocolType: state.protocolType,
		Protocol:     state.protocolName,
		Leader:       state.leaderID,
		GenerationId: state.generationID,
		Members:      make(map[string]*metadatapb.GroupMember, len(state.members)),
	}
	if state.rebalanceTimeout > 0 {
		group.RebalanceTimeoutMs = int32(state.rebalanceTimeout / time.Millisecond)
	}
	for memberID, member := range state.members {
		pbMember := &metadatapb.GroupMember{
			Subscriptions: append([]string(nil), member.topics...),
		}
		if member.sessionTimeout > 0 {
			pbMember.SessionTimeoutMs = int32(member.sessionTimeout / time.Millisecond)
		}
		if !member.lastHeartbeat.IsZero() {
			pbMember.HeartbeatAt = member.lastHeartbeat.UTC().Format(time.RFC3339Nano)
		}
		if assignments := state.assignments[memberID]; len(assignments) > 0 {
			pbMember.Assignments = make([]*metadatapb.Assignment, 0, len(assignments))
			for _, assignment := range assignments {
				pbMember.Assignments = append(pbMember.Assignments, &metadatapb.Assignment{
					Topic:      assignment.Name,
					Partitions: append([]int32(nil), assignment.Partitions...),
				})
			}
		}
		group.Members[memberID] = pbMember
	}
	return group
}

func buildDescribeGroup(group *metadatapb.ConsumerGroup, includeAuthorized bool) kmsg.DescribeGroupsResponseGroup {
	protocolType := group.GetProtocolType()
	if protocolType == "" {
		protocolType = "consumer"
	}
	protocolName := group.GetProtocol()

	memberIDs := make([]string, 0, len(group.Members))
	for memberID := range group.Members {
		memberIDs = append(memberIDs, memberID)
	}
	sort.Strings(memberIDs)

	members := make([]kmsg.DescribeGroupsResponseGroupMember, 0, len(group.Members))
	for _, memberID := range memberIDs {
		member := group.Members[memberID]
		if member == nil {
			continue
		}
		m := kmsg.NewDescribeGroupsResponseGroupMember()
		m.MemberID = memberID
		m.ClientID = member.ClientId
		m.ClientHost = member.ClientHost
		members = append(members, m)
	}
	authorizedOps := int32(-2147483648)
	if includeAuthorized {
		authorizedOps = 0
	}
	g := kmsg.NewDescribeGroupsResponseGroup()
	g.ErrorCode = protocol.NONE
	g.Group = group.GetGroupId()
	g.State = kafkaGroupState(group.GetState())
	g.ProtocolType = protocolType
	g.Protocol = protocolName
	g.Members = members
	g.AuthorizedOperations = authorizedOps
	return g
}

func restoreGroupState(group *metadatapb.ConsumerGroup) *groupState {
	rebalanceTimeout := defaultRebalanceTimeout
	if group.RebalanceTimeoutMs > 0 {
		rebalanceTimeout = time.Duration(group.RebalanceTimeoutMs) * time.Millisecond
	}
	state := &groupState{
		protocolName:     group.Protocol,
		protocolType:     group.ProtocolType,
		generationID:     group.GenerationId,
		leaderID:         group.Leader,
		state:            parseGroupPhase(group.State),
		members:          make(map[string]*memberState, len(group.Members)),
		assignments:      make(map[string][]assignmentTopic),
		rebalanceTimeout: rebalanceTimeout,
	}
	for memberID, member := range group.Members {
		sessionTimeout := defaultSessionTimeout
		if member.SessionTimeoutMs > 0 {
			sessionTimeout = time.Duration(member.SessionTimeoutMs) * time.Millisecond
		}
		entry := &memberState{
			topics:         append([]string(nil), member.Subscriptions...),
			sessionTimeout: sessionTimeout,
			joinGeneration: group.GenerationId,
		}
		if member.HeartbeatAt != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, member.HeartbeatAt); err == nil {
				entry.lastHeartbeat = parsed
			}
		}
		state.members[memberID] = entry
		if len(member.Assignments) > 0 {
			memberAssignments := make([]assignmentTopic, 0, len(member.Assignments))
			for _, assignment := range member.Assignments {
				memberAssignments = append(memberAssignments, assignmentTopic{
					Name:       assignment.Topic,
					Partitions: append([]int32(nil), assignment.Partitions...),
				})
			}
			state.assignments[memberID] = memberAssignments
		}
	}
	if state.state == groupStatePreparingRebalance || state.state == groupStateCompletingRebalance {
		state.rebalanceDeadline = time.Now().Add(state.rebalanceTimeout)
	}
	state.ensureLeader()
	return state
}

func kafkaGroupState(state string) string {
	switch strings.ToLower(state) {
	case groupStatePreparingStr:
		return "PreparingRebalance"
	case groupStateCompletingStr:
		return "CompletingRebalance"
	case groupStateStableStr:
		return "Stable"
	case groupStateDeadStr:
		return "Dead"
	case groupStateEmptyStr:
		return "Empty"
	default:
		return state
	}
}

func normalizeGroupState(state string) string {
	normalized := strings.ToLower(state)
	normalized = strings.ReplaceAll(normalized, "_", "")
	return normalized
}

func matchesGroupStateFilter(state string, filters []string) bool {
	if len(filters) == 0 {
		return true
	}
	stateKey := normalizeGroupState(state)
	for _, filter := range filters {
		if normalizeGroupState(filter) == stateKey {
			return true
		}
	}
	return false
}

func matchesGroupTypeFilter(groupType string, filters []string) bool {
	if len(filters) == 0 {
		return true
	}
	for _, filter := range filters {
		if strings.EqualFold(filter, groupType) {
			return true
		}
	}
	return false
}

func groupPhaseString(state groupPhase) string {
	switch state {
	case groupStatePreparingRebalance:
		return groupStatePreparingStr
	case groupStateCompletingRebalance:
		return groupStateCompletingStr
	case groupStateStable:
		return groupStateStableStr
	case groupStateDead:
		return groupStateDeadStr
	default:
		return groupStateEmptyStr
	}
}

func parseGroupPhase(state string) groupPhase {
	switch state {
	case groupStatePreparingStr:
		return groupStatePreparingRebalance
	case groupStateCompletingStr:
		return groupStateCompletingRebalance
	case groupStateStableStr:
		return groupStateStable
	case groupStateDeadStr:
		return groupStateDead
	default:
		return groupStateEmpty
	}
}

func (c *GroupCoordinator) newMemberID(group string) string {
	return fmt.Sprintf("%s-%d", group, rand.Int63())
}

func (c *GroupCoordinator) parseSubscriptionTopics(protocols []kmsg.JoinGroupRequestProtocol) []string {
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

func (c *GroupCoordinator) encodeMemberSubscriptions(state *groupState) []kmsg.JoinGroupResponseMember {
	ids := state.sortedMembers()
	members := make([]kmsg.JoinGroupResponseMember, 0, len(ids))
	for _, id := range ids {
		member := state.members[id]
		m := kmsg.NewJoinGroupResponseMember()
		m.MemberID = id
		m.ProtocolMetadata = c.encodeSubscription(member.topics)
		members = append(members, m)
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
			partitions = append(partitions, p.Partition)
		}
		if len(partitions) == 0 {
			partitions = []int32{0}
		}
		sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
		result[*topic.Topic] = partitions
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
	ctx := context.Background()
	for groupID, state := range c.groups {
		removed := state.removeExpiredMembers(now)
		lostDuringRebalance := state.dropRebalanceLaggers(now)

		if len(state.members) == 0 {
			delete(c.groups, groupID)
			_ = c.persistGroupLocked(ctx, groupID, nil)
			continue
		}
		if removed || lostDuringRebalance {
			state.startRebalance(0)
			_ = c.persistGroupLocked(ctx, groupID, state)
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
