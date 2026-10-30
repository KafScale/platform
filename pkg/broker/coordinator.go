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
	"errors"
	"fmt"
	"math/rand"
	"net"
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
	recoveredSessionGrace   = 5 * time.Second
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
	store              metadata.Store
	broker             protocol.MetadataBroker
	config             CoordinatorConfig
	stopCh             chan struct{}
	mu                 sync.Mutex
	groups             map[string]*groupState
	lastRetentionSweep time.Time
}

type CoordinatorConfig struct {
	CleanupInterval time.Duration
	// Zero uses the seven-day default; a negative value disables retention.
	OffsetsRetention       time.Duration
	RetentionCheckInterval time.Duration
	AcquireGroup           func(context.Context, string) error
	ReleaseGroup           func(string)
	// OwnsGroup gates group mutation and cleanup after coordination lease loss.
	// Nil permits standalone coordinators without an external lease manager.
	OwnsGroup func(string) bool
	// GroupDeleted releases external ownership while group operations remain locked.
	GroupDeleted func(string)
}

var defaultCoordinatorConfig = CoordinatorConfig{
	CleanupInterval:        5 * time.Second,
	OffsetsRetention:       7 * 24 * time.Hour,
	RetentionCheckInterval: time.Minute,
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
	createRevision    int64
}

type pendingJoin struct {
	ctx context.Context
}

type memberState struct {
	pendingJoin      *pendingJoin
	clientID         string
	clientHost       string
	protocolMetadata []byte
	memberAssignment []byte
	topics           []string
	sessionTimeout   time.Duration
	lastHeartbeat    time.Time
	expiryGrace      time.Duration
	joinGeneration   int32
}

type assignmentTopic struct {
	Name       string
	Partitions []int32
}

func NewGroupCoordinator(store metadata.Store, broker protocol.MetadataBroker, cfg *CoordinatorConfig) *GroupCoordinator {
	config := defaultCoordinatorConfig
	if cfg != nil {
		if cfg.CleanupInterval > 0 {
			config.CleanupInterval = cfg.CleanupInterval
		}
		if cfg.OffsetsRetention != 0 {
			config.OffsetsRetention = cfg.OffsetsRetention
		}
		if cfg.RetentionCheckInterval > 0 {
			config.RetentionCheckInterval = cfg.RetentionCheckInterval
		}
		config.AcquireGroup, config.ReleaseGroup = cfg.AcquireGroup, cfg.ReleaseGroup
		config.OwnsGroup = cfg.OwnsGroup
		config.GroupDeleted = cfg.GroupDeleted
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
	return c.JoinGroupWithClient(ctx, req, "")
}

// JoinGroupWithClient records display metadata separately from authentication.
func (c *GroupCoordinator) JoinGroupWithClient(ctx context.Context, req *kmsg.JoinGroupRequest, clientID string) (response *kmsg.JoinGroupResponse, resultErr error) {
	c.mu.Lock()
	if err := joinContextErr(ctx); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if !c.ownsGroup(req.Group) {
		c.mu.Unlock()
		resp := kmsg.NewPtrJoinGroupResponse()
		resp.ErrorCode = protocol.NOT_COORDINATOR
		return resp, nil
	}
	state, err := c.ensureGroup(ctx, req.Group)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	memberID := req.MemberID
	member, exists := state.members[memberID]
	if memberID != "" && !exists {
		c.mu.Unlock()
		resp := kmsg.NewPtrJoinGroupResponse()
		resp.MemberID = memberID
		resp.ErrorCode = protocol.UNKNOWN_MEMBER_ID
		return resp, nil
	}
	state.protocolType = req.ProtocolType
	if len(req.Protocols) > 0 {
		state.protocolName = req.Protocols[0].Name
	}

	timeout := time.Duration(req.RebalanceTimeoutMillis) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultRebalanceTimeout
	}

	if memberID == "" {
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
	member.clientID = clientID
	if info := ConnInfoFromContext(ctx); info != nil {
		member.clientHost = info.RemoteAddr
		if host, _, err := net.SplitHostPort(member.clientHost); err == nil {
			member.clientHost = host
		}
	}
	member.protocolMetadata = nil
	member.topics = nil
	for _, offered := range req.Protocols {
		if offered.Name == state.protocolName {
			member.protocolMetadata = append([]byte(nil), offered.Metadata...)
			member.topics = c.parseSubscriptionTopics([]kmsg.JoinGroupRequestProtocol{offered})
			break
		}
	}
	member.lastHeartbeat = time.Now()
	member.expiryGrace = 0

	if len(state.members) == 1 && state.state == groupStateEmpty {
		state.leaderID = memberID
		state.startRebalance(timeout)
	} else if state.state == groupStateStable && !exists {
		state.startRebalance(timeout)
	} else if state.state == groupStateEmpty {
		state.startRebalance(timeout)
	} else if state.state == groupStateCompletingRebalance && !exists {
		state.startRebalance(timeout)
	}

	pending := &pendingJoin{ctx: ctx}
	member.pendingJoin = pending
	// Every return below releases c.mu before this cleanup reacquires it.
	defer func() {
		cleanupErr := c.finishJoin(req.Group, state, memberID, pending)
		if err := joinContextErr(ctx); err != nil {
			response, resultErr = nil, errors.Join(err, cleanupErr)
		}
	}()
	member.joinGeneration = state.generationID
	if state.leaderID == "" {
		state.ensureLeader()
	}

	if err := c.persistGroupLocked(ctx, req.Group, state); err != nil {
		c.mu.Unlock()
		resp := kmsg.NewPtrJoinGroupResponse()
		resp.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
		return resp, nil
	}
	// Keep JoinGroup pending until the known members rejoin. Returning
	// REBALANCE_IN_PROGRESS here makes classic clients restart the join loop.
	// The deadline is fixed when the rebalance starts; retries cannot extend it.
	for state.state == groupStatePreparingRebalance {
		if err := joinContextErr(ctx); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		if state.completeIfReady() {
			break
		}
		if !time.Now().Before(state.rebalanceDeadline) {
			state.dropRebalanceLaggers(time.Now())
			state.ensureLeader()
			state.completeIfReady()
			break
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-protocol.ClientGone(ctx):
			return nil, context.Canceled
		case <-c.stopCh:
			return nil, context.Canceled
		case <-time.After(50 * time.Millisecond):
		}
		c.mu.Lock()
		if err := joinContextErr(ctx); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		if !c.ownsGroup(req.Group) || c.groups[req.Group] != state {
			c.mu.Unlock()
			resp := kmsg.NewPtrJoinGroupResponse()
			resp.ErrorCode = protocol.NOT_COORDINATOR
			return resp, nil
		}
		member = state.members[memberID]
		if member == nil {
			c.mu.Unlock()
			resp := kmsg.NewPtrJoinGroupResponse()
			resp.MemberID = memberID
			resp.ErrorCode = protocol.UNKNOWN_MEMBER_ID
			return resp, nil
		}
		// A session expiry may have started a new generation while this
		// request was pending. It still counts as a join for that generation.
		member.joinGeneration = state.generationID
		member.lastHeartbeat = time.Now()
	}
	ready := state.state == groupStateStable || state.state == groupStateCompletingRebalance

	resp := kmsg.NewPtrJoinGroupResponse()
	resp.Generation = state.generationID
	protocolName := state.protocolName
	resp.Protocol = &protocolName
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

// JoinGroup observes EOF independently from its handler context, so buffered
// and in-flight produces on that connection can still complete.
func joinContextErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-protocol.ClientGone(ctx):
		return context.Canceled
	default:
		return nil
	}
}

// finishJoin removes only the canceled request's membership. A replacement join
// for the same member or a new coordinator must not be changed by old cleanup.
func (c *GroupCoordinator) finishJoin(groupID string, state *groupState, memberID string, pending *pendingJoin) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.groups[groupID] != state || !c.ownsGroup(groupID) {
		return nil
	}
	member := state.members[memberID]
	if member == nil || member.pendingJoin != pending {
		return nil
	}
	member.pendingJoin = nil
	if joinContextErr(pending.ctx) == nil {
		return nil
	}
	delete(state.members, memberID)
	delete(state.assignments, memberID)
	if len(state.members) == 0 || state.state != groupStatePreparingRebalance {
		state.startRebalance(0)
	} else {
		state.ensureLeader()
		state.completeIfReady()
	}
	// The disconnected request's context is canceled; use a bounded independent
	// context so persisted membership is removed as well as the cached member.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(pending.ctx), 5*time.Second)
	defer cancel()
	return c.persistGroupLocked(ctx, groupID, state)
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
		for id, member := range state.members {
			member.memberAssignment = encodeAssignment(state.assignments[id])
		}
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
	state.members[req.MemberID].memberAssignment = append([]byte(nil), resp.MemberAssignment...)

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
	member.expiryGrace = 0
	resp := mkResp(protocol.NONE)
	if err := c.persistGroupLocked(ctx, req.Group, state); err != nil {
		resp.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
	}
	c.mu.Unlock()
	return resp
}

func (c *GroupCoordinator) LeaveGroup(ctx context.Context, req *kmsg.LeaveGroupRequest) *kmsg.LeaveGroupResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	resp := kmsg.NewPtrLeaveGroupResponse()
	state, err := c.loadGroupIfMissing(ctx, req.Group)
	if err != nil {
		resp.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
		return resp
	}
	members := req.Members
	if req.Version < 3 {
		members = []kmsg.LeaveGroupRequestMember{{MemberID: req.MemberID}}
	}
	removed := false
	for _, member := range members {
		result := kmsg.NewLeaveGroupResponseMember()
		result.MemberID, result.InstanceID = member.MemberID, member.InstanceID
		if state == nil || state.members[member.MemberID] == nil {
			result.ErrorCode = protocol.UNKNOWN_MEMBER_ID
		} else {
			delete(state.members, member.MemberID)
			delete(state.assignments, member.MemberID)
			if state.leaderID == member.MemberID {
				state.leaderID = ""
			}
			removed = true
		}
		if req.Version < 3 {
			resp.ErrorCode = result.ErrorCode
		} else {
			resp.Members = append(resp.Members, result)
		}
	}
	if !removed {
		return resp
	}
	// Apply one group transition for the whole batch, including mixed success.
	state.startRebalance(0)
	if err := c.persistGroupLocked(ctx, req.Group, state); err != nil {
		resp.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
	}
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
	createRevision := int64(0)
	simple := req.Generation < 0 && req.MemberID == "" && (state == nil || len(state.members) == 0)
	switch {
	case simple:
	case state == nil:
		groupErr = protocol.UNKNOWN_MEMBER_ID
	case state.members[req.MemberID] == nil:
		groupErr = protocol.UNKNOWN_MEMBER_ID
	case req.Generation != state.generationID:
		groupErr = protocol.ILLEGAL_GENERATION
	default:
		createRevision = state.createRevision
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
				var err error
				if simple {
					err = metadata.CommitSimpleConsumerOffset(ctx, c.store, req.Group, topic.Topic, part.Partition, part.Offset, meta)
				} else {
					err = metadata.CommitConsumerOffsetIfGroupExists(ctx, c.store, req.Group, createRevision, topic.Topic, part.Partition, part.Offset, meta)
				}
				if err != nil {
					code = protocol.UNKNOWN_SERVER_ERROR
					if errors.Is(err, metadata.ErrConsumerGroupDeleted) || errors.Is(err, metadata.ErrConsumerGroupNotEmpty) {
						code = protocol.UNKNOWN_MEMBER_ID
					} else if errors.Is(err, metadata.ErrConsumerGroupChanged) {
						code = protocol.COORDINATOR_NOT_AVAILABLE
					}
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
	if req.Version >= 2 && req.Topics == nil {
		offsets, err := c.store.ListGroupConsumerOffsets(ctx, req.Group)
		if err != nil {
			resp.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			return resp, nil
		}
		sort.Slice(offsets, func(i, j int) bool {
			if offsets[i].Topic != offsets[j].Topic {
				return offsets[i].Topic < offsets[j].Topic
			}
			return offsets[i].Partition < offsets[j].Partition
		})
		for _, offset := range offsets {
			if len(resp.Topics) == 0 || resp.Topics[len(resp.Topics)-1].Topic != offset.Topic {
				topic := kmsg.NewOffsetFetchResponseTopic()
				topic.Topic = offset.Topic
				resp.Topics = append(resp.Topics, topic)
			}
			part := kmsg.NewOffsetFetchResponseTopicPartition()
			part.Partition, part.Offset, part.LeaderEpoch = offset.Partition, offset.Offset, -1
			part.Metadata = kmsg.StringPtr(offset.Metadata)
			topic := &resp.Topics[len(resp.Topics)-1]
			topic.Partitions = append(topic.Partitions, part)
		}
		return resp, nil
	}
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
		group, _, err := c.fetchGroupForAdmin(ctx, groupID)
		if err != nil {
			g := kmsg.NewDescribeGroupsResponseGroup()
			g.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			g.Group = groupID
			resp.Groups = append(resp.Groups, g)
			continue
		}
		if group == nil {
			g := kmsg.NewDescribeGroupsResponseGroup()
			g.Group = groupID
			g.State = "Dead"
			if req.Version >= 6 {
				g.ErrorCode = protocol.GROUP_ID_NOT_FOUND
			}
			resp.Groups = append(resp.Groups, g)
			continue
		}
		resp.Groups = append(resp.Groups, buildDescribeGroup(group, req.IncludeAuthorizedOperations))
	}
	return resp, nil
}

// fetchGroupForAdmin also discovers offsets left without group metadata by older brokers.
func (c *GroupCoordinator) fetchGroupForAdmin(ctx context.Context, groupID string) (*metadatapb.ConsumerGroup, int64, error) {
	group, createRevision, err := metadata.FetchConsumerGroupRevision(ctx, c.store, groupID)
	if err != nil || group != nil {
		return group, createRevision, err
	}
	offsets, err := c.store.ListGroupConsumerOffsets(ctx, groupID)
	if err != nil || len(offsets) == 0 {
		return nil, 0, err
	}
	return emptyConsumerGroup(groupID), 0, nil
}

func emptyConsumerGroup(groupID string) *metadatapb.ConsumerGroup {
	return &metadatapb.ConsumerGroup{GroupId: groupID, State: groupStateEmptyStr, ProtocolType: "consumer"}
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
	// Serialize membership checks and deletion with joins and cleanup.
	c.mu.Lock()
	defer c.mu.Unlock()
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
		if !c.ownsGroup(groupID) {
			result.ErrorCode = protocol.NOT_COORDINATOR
			resp.Groups = append(resp.Groups, result)
			continue
		}
		if state := c.groups[groupID]; state != nil {
			if err := c.expireGroupMembersLocked(ctx, groupID, state, time.Now()); err != nil {
				result.ErrorCode = groupExpiryErrorCode(err)
				resp.Groups = append(resp.Groups, result)
				continue
			}
			if len(state.members) > 0 {
				result.ErrorCode = protocol.NON_EMPTY_GROUP
				resp.Groups = append(resp.Groups, result)
				continue
			}
		}
		group, createRevision, err := c.fetchGroupForAdmin(ctx, groupID)
		if err != nil {
			result.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			resp.Groups = append(resp.Groups, result)
			continue
		}
		if group == nil {
			result.ErrorCode = protocol.GROUP_ID_NOT_FOUND
			delete(c.groups, groupID)
			if c.config.GroupDeleted != nil {
				c.config.GroupDeleted(groupID)
			}
			resp.Groups = append(resp.Groups, result)
			continue
		}
		if len(group.Members) > 0 {
			// A replacement coordinator has not seen this group's sessions yet.
			// Cache it so any surviving members also receive normal background expiry.
			state := restoreGroupState(group)
			state.createRevision = createRevision
			c.groups[groupID] = state
			if err := c.expireGroupMembersLocked(ctx, groupID, state, time.Now()); err != nil {
				result.ErrorCode = groupExpiryErrorCode(err)
				resp.Groups = append(resp.Groups, result)
				continue
			}
			if len(state.members) > 0 {
				result.ErrorCode = protocol.NON_EMPTY_GROUP
				resp.Groups = append(resp.Groups, result)
				continue
			}
		}
		if !c.ownsGroup(groupID) {
			result.ErrorCode = protocol.NOT_COORDINATOR
			resp.Groups = append(resp.Groups, result)
			continue
		}
		if err := metadata.DeleteConsumerGroupAndOffsets(ctx, c.store, groupID); err != nil {
			result.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			if errors.Is(err, metadata.ErrConsumerGroupChanged) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				result.ErrorCode = protocol.COORDINATOR_NOT_AVAILABLE
			}
			resp.Groups = append(resp.Groups, result)
			continue
		}
		delete(c.groups, groupID)
		if c.config.GroupDeleted != nil {
			c.config.GroupDeleted(groupID)
		}
		result.ErrorCode = protocol.NONE
		resp.Groups = append(resp.Groups, result)
	}
	return resp, nil
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
	group, createRevision, err := metadata.FetchConsumerGroupRevision(ctx, c.store, groupID)
	if err != nil {
		return nil, err
	}
	if group == nil {
		return nil, nil
	}
	state := restoreGroupState(group)
	state.createRevision = createRevision
	c.groups[groupID] = state
	return state, nil
}

func (c *GroupCoordinator) persistGroupLocked(ctx context.Context, groupID string, state *groupState) error {
	createRevision, err := metadata.PutConsumerGroupRevision(ctx, c.store, buildConsumerGroup(groupID, state))
	if err != nil {
		return err
	}
	state.createRevision = createRevision
	return nil
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
			ClientId:      member.clientID, ClientHost: member.clientHost,
			ProtocolMetadata: append([]byte(nil), member.protocolMetadata...),
			MemberAssignment: append([]byte(nil), member.memberAssignment...),
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
		m.ProtocolMetadata = append([]byte(nil), member.ProtocolMetadata...)
		if len(m.ProtocolMetadata) == 0 && len(member.Subscriptions) > 0 {
			m.ProtocolMetadata = encodeSubscription(member.Subscriptions)
		}
		m.MemberAssignment = append([]byte(nil), member.MemberAssignment...)
		if len(m.MemberAssignment) == 0 && len(member.Assignments) > 0 {
			assignments := make([]assignmentTopic, 0, len(member.Assignments))
			for _, assignment := range member.Assignments {
				assignments = append(assignments, assignmentTopic{Name: assignment.Topic, Partitions: assignment.Partitions})
			}
			m.MemberAssignment = encodeAssignment(assignments)
		}
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
	now := time.Now()
	for memberID, member := range group.Members {
		sessionTimeout := defaultSessionTimeout
		if member.SessionTimeoutMs > 0 {
			sessionTimeout = time.Duration(member.SessionTimeoutMs) * time.Millisecond
		}
		entry := &memberState{
			topics:   append([]string(nil), member.Subscriptions...),
			clientID: member.ClientId, clientHost: member.ClientHost,
			protocolMetadata: append([]byte(nil), member.ProtocolMetadata...),
			memberAssignment: append([]byte(nil), member.MemberAssignment...),
			sessionTimeout:   sessionTimeout,
			// Older brokers did not persist heartbeats. Give recovered members
			// one local session to reconnect rather than expiring them immediately.
			lastHeartbeat:  now,
			joinGeneration: group.GenerationId,
		}
		if member.HeartbeatAt != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, member.HeartbeatAt); err == nil {
				entry.lastHeartbeat = parsed
				entry.expiryGrace = recoveredSessionGrace
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
	return encodeSubscription(topics)
}

func encodeSubscription(topics []string) []byte {
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
		m.ProtocolMetadata = append([]byte(nil), member.protocolMetadata...)
		if len(m.ProtocolMetadata) == 0 {
			m.ProtocolMetadata = c.encodeSubscription(member.topics)
		}
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

	now := time.Now()
	ctx := context.Background()
	for groupID, state := range c.groups {
		if !c.ownsGroup(groupID) {
			delete(c.groups, groupID)
			continue
		}
		_ = c.expireGroupMembersLocked(ctx, groupID, state, now)
	}
	c.mu.Unlock()
	c.cleanupRetainedGroups(now)
}

func (c *GroupCoordinator) ownsGroup(groupID string) bool {
	return c.config.OwnsGroup == nil || c.config.OwnsGroup(groupID)
}

func groupExpiryErrorCode(err error) int16 {
	if errors.Is(err, metadata.ErrNotOwner) {
		return protocol.NOT_COORDINATOR
	}
	return protocol.COORDINATOR_NOT_AVAILABLE
}

// expireGroupMembersLocked persists a candidate before changing cached state.
// Losing the lease or a failed write must not erase members from local memory.
func (c *GroupCoordinator) expireGroupMembersLocked(ctx context.Context, groupID string, state *groupState, now time.Time) error {
	if !c.ownsGroup(groupID) {
		return metadata.ErrNotOwner
	}
	if len(state.members) == 0 {
		return nil
	}
	candidate := *state
	candidate.members = make(map[string]*memberState, len(state.members))
	for id, member := range state.members {
		memberCopy := *member
		candidate.members[id] = &memberCopy
	}
	candidate.assignments = make(map[string][]assignmentTopic, len(state.assignments))
	for id, assignments := range state.assignments {
		candidate.assignments[id] = assignments
	}
	removed := candidate.removeExpiredMembers(now)
	lostDuringRebalance := candidate.dropRebalanceLaggers(now)
	if removed || lostDuringRebalance {
		// Partial expiry advances the generation even if live members still
		// make DeleteGroups return NON_EMPTY_GROUP, just as background cleanup does.
		if removed || len(candidate.members) == 0 {
			candidate.startRebalance(0)
		} else {
			candidate.ensureLeader()
			candidate.completeIfReady()
		}
		if !c.ownsGroup(groupID) {
			return metadata.ErrNotOwner
		}
		if err := c.persistGroupLocked(ctx, groupID, &candidate); err != nil {
			return err
		}
		*state = candidate
	}
	return nil
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
	s.generationID++
	if len(s.members) == 0 {
		s.protocolName = ""
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
	s.state = groupStatePreparingRebalance
	s.assignments = make(map[string][]assignmentTopic)
	s.rebalanceDeadline = time.Now().Add(s.rebalanceTimeout)
	s.ensureLeader()
	for _, member := range s.members {
		member.joinGeneration = 0
		member.memberAssignment = nil
	}
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
		if now.Sub(member.lastHeartbeat) > timeout+member.expiryGrace {
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
