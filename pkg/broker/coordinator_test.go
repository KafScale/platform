package broker

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/protocol"
)

func TestCoordinatorCleanup(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	coord := NewGroupCoordinator(store, protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 9092}, &CoordinatorConfig{
		CleanupInterval: 10 * time.Millisecond,
	})
	defer coord.Stop()

	joinReq := &protocol.JoinGroupRequest{
		GroupID:            "group-1",
		SessionTimeoutMs:   10,
		RebalanceTimeoutMs: 10,
		MemberID:           "",
		ProtocolType:       "consumer",
		Protocols: []protocol.JoinGroupProtocol{
			{Name: "range", Metadata: encodeSubscriptionMetadata([]string{"orders"})},
		},
	}

	resp, err := coord.JoinGroup(context.Background(), joinReq, 1)
	if err != nil {
		t.Fatalf("JoinGroup: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	coord.cleanupGroups()

	coord.mu.Lock()
	defer coord.mu.Unlock()
	if _, exists := coord.groups["group-1"]; exists {
		t.Fatalf("expected group cleaned up after timeout")
	}
	if resp.MemberID == "" {
		t.Fatalf("expected member id")
	}
}

func TestJoinGroupRebalanceFlow(t *testing.T) {
	coord := newTestCoordinator(t)
	defer coord.Stop()

	leaderReq := newJoinRequest("group-1", "")
	leaderResp := mustJoin(t, coord, leaderReq)
	if leaderResp.MemberID == "" {
		t.Fatalf("expected leader member id")
	}

	syncGeneration(t, coord, "group-1", leaderResp.GenerationID, leaderResp.MemberID)

	// New member triggers rebalance
	followerReq := newJoinRequest("group-1", "")
	followerResp, err := coord.JoinGroup(context.Background(), followerReq, 2)
	if err != nil {
		t.Fatalf("JoinGroup follower: %v", err)
	}
	if followerResp.ErrorCode != protocol.REBALANCE_IN_PROGRESS {
		t.Fatalf("expected rebalance in progress, got %d", followerResp.ErrorCode)
	}
	followerReq.MemberID = followerResp.MemberID

	hb := coord.Heartbeat(context.Background(), &protocol.HeartbeatRequest{
		GroupID:      "group-1",
		GenerationID: leaderResp.GenerationID,
		MemberID:     leaderResp.MemberID,
	}, 3)
	if hb.ErrorCode != protocol.ILLEGAL_GENERATION {
		t.Fatalf("expected heartbeat to fail with illegal generation, got %d", hb.ErrorCode)
	}

	// Existing leader rejoins to finish rebalance
	leaderReq.MemberID = leaderResp.MemberID
	leaderResp2 := mustJoin(t, coord, leaderReq)
	if len(leaderResp2.Members) != 2 {
		t.Fatalf("expected leader to see 2 members, got %d", len(leaderResp2.Members))
	}

	// Follower can now join successfully
	followerResp2 := mustJoin(t, coord, followerReq)
	if followerResp2.ErrorCode != protocol.NONE {
		t.Fatalf("expected follower join success, got %d", followerResp2.ErrorCode)
	}

	syncGeneration(t, coord, "group-1", leaderResp2.GenerationID, leaderResp2.MemberID)
	syncGeneration(t, coord, "group-1", leaderResp2.GenerationID, followerResp2.MemberID)

	hb = coord.Heartbeat(context.Background(), &protocol.HeartbeatRequest{
		GroupID:      "group-1",
		GenerationID: leaderResp2.GenerationID,
		MemberID:     leaderResp2.MemberID,
	}, 4)
	if hb.ErrorCode != protocol.NONE {
		t.Fatalf("expected stable heartbeat, got %d", hb.ErrorCode)
	}
}

func TestLeaveGroupTriggersRebalance(t *testing.T) {
	coord := newTestCoordinator(t)
	defer coord.Stop()

	leaderReq := newJoinRequest("group-1", "")
	leaderResp := mustJoin(t, coord, leaderReq)
	syncGeneration(t, coord, "group-1", leaderResp.GenerationID, leaderResp.MemberID)

	followerReq := newJoinRequest("group-1", "")
	initialFollowerResp, err := coord.JoinGroup(context.Background(), followerReq, 2)
	if err != nil {
		t.Fatalf("initial follower join: %v", err)
	}
	if initialFollowerResp.ErrorCode != protocol.REBALANCE_IN_PROGRESS {
		t.Fatalf("expected rebalance during initial follower join, got %d", initialFollowerResp.ErrorCode)
	}
	followerReq.MemberID = initialFollowerResp.MemberID

	leaderReq.MemberID = leaderResp.MemberID
	leaderResp2 := mustJoin(t, coord, leaderReq)

	followerResp := mustJoin(t, coord, followerReq)

	syncGeneration(t, coord, "group-1", leaderResp2.GenerationID, leaderResp2.MemberID)
	syncGeneration(t, coord, "group-1", leaderResp2.GenerationID, followerResp.MemberID)

	coord.mu.Lock()
	if len(coord.groups["group-1"].members) != 2 {
		t.Fatalf("expected 2 members after join, have %d", len(coord.groups["group-1"].members))
	}
	coord.mu.Unlock()

	leaveResp := coord.LeaveGroup(context.Background(), &protocol.LeaveGroupRequest{
		GroupID:  "group-1",
		MemberID: followerResp.MemberID,
	}, 10)
	if leaveResp.ErrorCode != protocol.NONE {
		t.Fatalf("leave group failed: %d", leaveResp.ErrorCode)
	}

	hb := coord.Heartbeat(context.Background(), &protocol.HeartbeatRequest{
		GroupID:      "group-1",
		GenerationID: leaderResp2.GenerationID,
		MemberID:     leaderResp.MemberID,
	}, 11)
	if hb.ErrorCode != protocol.ILLEGAL_GENERATION {
		t.Fatalf("expected heartbeat to see illegal generation, got %d", hb.ErrorCode)
	}

	leaderReq.MemberID = leaderResp.MemberID
	leaderResp3 := mustJoin(t, coord, leaderReq)
	syncGeneration(t, coord, "group-1", leaderResp3.GenerationID, leaderResp3.MemberID)

	hb = coord.Heartbeat(context.Background(), &protocol.HeartbeatRequest{
		GroupID:      "group-1",
		GenerationID: leaderResp3.GenerationID,
		MemberID:     leaderResp3.MemberID,
	}, 12)
	if hb.ErrorCode != protocol.NONE {
		t.Fatalf("expected stable heartbeat after rejoin, got %d", hb.ErrorCode)
	}
}

func encodeSubscriptionMetadata(topics []string) []byte {
	buf := make([]byte, 0)
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
	writeInt16(0)
	writeInt32(int32(len(topics)))
	for _, topic := range topics {
		writeInt16(int16(len(topic)))
		buf = append(buf, []byte(topic)...)
	}
	writeInt32(0)
	return buf
}

func newTestCoordinator(t *testing.T) *GroupCoordinator {
	t.Helper()
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{
		ControllerID: 1,
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "localhost", Port: 9092},
		},
		Topics: []protocol.MetadataTopic{
			{
				Name: "orders",
				Partitions: []protocol.MetadataPartition{
					{PartitionIndex: 0, LeaderID: 1, ReplicaNodes: []int32{1}, ISRNodes: []int32{1}},
					{PartitionIndex: 1, LeaderID: 1, ReplicaNodes: []int32{1}, ISRNodes: []int32{1}},
				},
			},
		},
	})
	return NewGroupCoordinator(store, protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 9092}, &CoordinatorConfig{
		CleanupInterval: 50 * time.Millisecond,
	})
}

func newJoinRequest(groupID, memberID string) *protocol.JoinGroupRequest {
	return &protocol.JoinGroupRequest{
		GroupID:            groupID,
		SessionTimeoutMs:   10000,
		RebalanceTimeoutMs: 10000,
		MemberID:           memberID,
		ProtocolType:       "consumer",
		Protocols: []protocol.JoinGroupProtocol{
			{Name: "range", Metadata: encodeSubscriptionMetadata([]string{"orders"})},
		},
	}
}

func mustJoin(t *testing.T, coord *GroupCoordinator, req *protocol.JoinGroupRequest) *protocol.JoinGroupResponse {
	t.Helper()
	for attempt := 0; attempt < 5; attempt++ {
		resp, err := coord.JoinGroup(context.Background(), req, int32(attempt+1))
		if err != nil {
			t.Fatalf("JoinGroup attempt %d failed: %v", attempt, err)
		}
		req.MemberID = resp.MemberID
		if resp.ErrorCode == protocol.NONE {
			return resp
		}
		time.Sleep(1 * time.Millisecond)
	}
	t.Fatalf("JoinGroup never stabilized")
	return nil
}

func syncGeneration(t *testing.T, coord *GroupCoordinator, groupID string, generationID int32, memberID string) {
	t.Helper()
	resp, err := coord.SyncGroup(context.Background(), &protocol.SyncGroupRequest{
		GroupID:      groupID,
		GenerationID: generationID,
		MemberID:     memberID,
	}, 99)
	if err != nil {
		t.Fatalf("SyncGroup: %v", err)
	}
	if resp.ErrorCode != protocol.NONE {
		t.Fatalf("sync group failed: %d", resp.ErrorCode)
	}
	if len(resp.Assignment) == 0 {
		t.Fatalf("expected assignment bytes")
	}
}
