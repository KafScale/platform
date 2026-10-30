// Copyright 2026 KafScale team.
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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func disconnectGroupMetadata() metadata.ClusterMetadata {
	return metadata.ClusterMetadata{Topics: []protocol.MetadataTopic{{Topic: kmsg.StringPtr("orders"), Partitions: []protocol.MetadataPartition{{Partition: 0}, {Partition: 1}, {Partition: 2}, {Partition: 3}}}}}
}

func waitForJoiningMember(t *testing.T, c *GroupCoordinator, exclude string) string {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		c.mu.Lock()
		for id := range c.groups["readers"].members {
			if id != exclude {
				c.mu.Unlock()
				return id
			}
		}
		c.mu.Unlock()
		select {
		case <-deadline:
			t.Fatal("member did not enter JoinGroup wait")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func pendingJoinRequest() *kmsg.JoinGroupRequest {
	req := kmsg.NewPtrJoinGroupRequest()
	req.Version, req.Group, req.ProtocolType = 4, "readers", "consumer"
	req.SessionTimeoutMillis, req.RebalanceTimeoutMillis = 30000, 30000
	subscription := kmsg.NewConsumerMemberMetadata()
	subscription.Topics = []string{"orders"}
	req.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: subscription.AppendTo(nil)}}
	return req
}

func assertDisconnectedMemberGone(t *testing.T, c *GroupCoordinator, store metadata.Store, dead, leader string) {
	t.Helper()
	c.mu.Lock()
	_, exists := c.groups["readers"].members[dead]
	c.mu.Unlock()
	if exists {
		t.Fatal("canceled member remains in cached group")
	}
	persisted, err := store.FetchConsumerGroup(context.Background(), "readers")
	if err != nil || len(persisted.Members) != 1 || persisted.Members[leader] == nil {
		t.Fatalf("canceled member remains persisted: %+v, %v", persisted, err)
	}
	joined := joinTestGroup(t, c, leader)
	if joined.ErrorCode != 0 || len(joined.Members) != 1 || joined.Members[0].MemberID != leader {
		t.Fatalf("dead member entered the next generation: %+v", joined)
	}
	synced := syncTestGroup(t, c, leader, joined.Generation, protocol.NONE)
	var assignment kmsg.ConsumerMemberAssignment
	if err := assignment.ReadFrom(synced.MemberAssignment); err != nil || len(assignment.Topics) != 1 || len(assignment.Topics[0].Partitions) != 4 {
		t.Fatalf("survivor did not receive all partitions: %+v, %v", assignment, err)
	}
}

func TestJoinGroupCanceledWaitRemovesMember(t *testing.T) {
	store := metadata.NewInMemoryStore(disconnectGroupMetadata())
	c := newTestCoordinator(t, store)
	leader := joinTestGroup(t, c, "")
	syncTestGroup(t, c, leader.MemberID, leader.Generation, protocol.NONE)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := c.JoinGroup(ctx, pendingJoinRequest()); done <- err }()
	dead := waitForJoiningMember(t, c, leader.MemberID)
	// Waiting in one group must not hold the coordinator's lock over another.
	other := pendingJoinRequest()
	other.Group = "independent"
	independentCtx, independentCancel := context.WithTimeout(context.Background(), time.Second)
	defer independentCancel()
	if response, err := c.JoinGroup(independentCtx, other); err != nil || response.ErrorCode != 0 {
		t.Fatalf("unrelated group blocked: %+v, %v", response, err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled JoinGroup did not return promptly")
	}
	assertDisconnectedMemberGone(t, c, store, dead, leader.MemberID)
}

type disconnectJoinHandler struct{ coordinator *GroupCoordinator }

func (h disconnectJoinHandler) Handle(ctx context.Context, header *protocol.RequestHeader, request kmsg.Request) ([]byte, error) {
	req := request.(*kmsg.JoinGroupRequest)
	response, err := h.coordinator.JoinGroup(ctx, req)
	if err != nil {
		return nil, err
	}
	return protocol.EncodeResponse(header.CorrelationID, header.APIVersion, response), nil
}

func TestServerDisconnectDuringJoinGroup(t *testing.T) {
	store := metadata.NewInMemoryStore(disconnectGroupMetadata())
	c := newTestCoordinator(t, store)
	leader := joinTestGroup(t, c, "")
	syncTestGroup(t, c, leader.MemberID, leader.Generation, protocol.NONE)
	// A real TCP FIN must cancel the handler while it is waiting, without a
	// subsequent request or a write error to drive connection cancellation.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server := &Server{Handler: disconnectJoinHandler{c}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err == nil {
			server.handleConnection(conn)
		}
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	request := pendingJoinRequest()
	frame := kmsg.NewRequestFormatter().AppendRequest(nil, request, 17)
	if _, err := client.Write(frame); err != nil {
		t.Fatal(err)
	}
	dead := waitForJoiningMember(t, c, leader.MemberID)
	client.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("TCP disconnect did not cancel pending JoinGroup")
	}
	assertDisconnectedMemberGone(t, c, store, dead, leader.MemberID)
}

func TestServerPipelinedRequestsPreserveOrder(t *testing.T) {
	serverConn, client := net.Pipe()
	defer client.Close()
	server := &Server{Handler: &testHandler{}}
	done := make(chan struct{})
	go func() { defer close(done); server.handleConnection(serverConn) }()
	// Write a second request before reading the first response. The EOF watcher
	// must queue it rather than consume or reorder its bytes.
	written := make(chan error, 1)
	go func() {
		for _, payload := range [][]byte{buildApiVersionsRequest(), buildMetadataRequest()} {
			if err := protocol.WriteFrame(client, payload); err != nil {
				written <- err
				return
			}
		}
		written <- nil
	}()
	for _, id := range []uint32{42, 5} {
		frame, err := protocol.ReadFrame(client)
		if err != nil {
			t.Fatal(err)
		}
		if got := binary.BigEndian.Uint32(frame.Payload[:4]); got != id {
			t.Fatalf("response order: got %d want %d", got, id)
		}
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	client.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("connection reader did not exit")
	}
}

func TestCanceledJoinDoesNotRemoveReplacement(t *testing.T) {
	store := metadata.NewInMemoryStore(disconnectGroupMetadata())
	c := newTestCoordinator(t, store)
	leader := joinTestGroup(t, c, "")
	syncTestGroup(t, c, leader.MemberID, leader.Generation, protocol.NONE)
	oldCtx, oldCancel := context.WithCancel(context.Background())
	defer oldCancel()
	oldDone := make(chan error, 1)
	go func() { _, err := c.JoinGroup(oldCtx, pendingJoinRequest()); oldDone <- err }()
	memberID := waitForJoiningMember(t, c, leader.MemberID)
	replacementCtx, replacementCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer replacementCancel()
	replacementDone := make(chan error, 1)
	go func() {
		request := pendingJoinRequest()
		request.MemberID = memberID
		response, err := c.JoinGroupWithClient(replacementCtx, request, "replacement")
		if err == nil && (response.ErrorCode != 0 || response.MemberID != memberID) {
			err = errors.New("replacement join failed")
		}
		replacementDone <- err
	}()
	for {
		c.mu.Lock()
		replaced := c.groups["readers"].members[memberID].clientID == "replacement"
		c.mu.Unlock()
		if replaced {
			break
		}
		select {
		case <-replacementCtx.Done():
			t.Fatal("replacement did not enter JoinGroup")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	oldCancel()
	if err := <-oldDone; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	c.mu.Lock()
	_, exists := c.groups["readers"].members[memberID]
	c.mu.Unlock()
	if !exists {
		t.Fatal("old canceled request removed its replacement")
	}
	rejoined := joinTestGroup(t, c, leader.MemberID)
	if rejoined.ErrorCode != 0 || len(rejoined.Members) != 2 {
		t.Fatalf("replacement missing from generation: %+v", rejoined)
	}
	select {
	case err := <-replacementDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-replacementCtx.Done():
		t.Fatal("replacement join did not complete")
	}
}

func TestCanceledJoinAfterLeaseLossDoesNotMutateGroup(t *testing.T) {
	store := metadata.NewInMemoryStore(disconnectGroupMetadata())
	var owned atomic.Bool
	owned.Store(true)
	c := NewGroupCoordinator(store, protocol.MetadataBroker{NodeID: 7}, &CoordinatorConfig{CleanupInterval: time.Hour, OwnsGroup: func(string) bool { return owned.Load() }})
	t.Cleanup(c.Stop)
	leader := joinTestGroup(t, c, "")
	syncTestGroup(t, c, leader.MemberID, leader.Generation, protocol.NONE)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := c.JoinGroup(ctx, pendingJoinRequest()); done <- err }()
	memberID := waitForJoiningMember(t, c, leader.MemberID)
	owned.Store(false)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled join did not return")
	}
	c.mu.Lock()
	_, exists := c.groups["readers"].members[memberID]
	c.mu.Unlock()
	persisted, err := store.FetchConsumerGroup(context.Background(), "readers")
	if !exists || err != nil || persisted.Members[memberID] == nil {
		t.Fatalf("coordinator changed group after lease loss: %+v, %v", persisted, err)
	}
}
