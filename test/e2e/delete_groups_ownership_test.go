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

//go:build e2e

package e2e

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Send directly to a chosen broker so client-side coordinator retries cannot
// hide a wrong-broker response.
func directGroupRequest(t *testing.T, addr string, req kmsg.Request) kmsg.Response {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	header := binary.BigEndian.AppendUint16(nil, uint16(req.Key()))
	header = binary.BigEndian.AppendUint16(header, uint16(req.GetVersion()))
	header = binary.BigEndian.AppendUint32(header, 1)
	header = binary.BigEndian.AppendUint16(header, 0xffff) // null client ID
	if req.IsFlexible() {
		header = append(header, 0)
	}
	if err := protocol.WriteFrame(conn, req.AppendTo(header)); err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	body, ok := protocol.SkipResponseHeader(req.Key(), req.GetVersion(), frame.Payload)
	if !ok {
		t.Fatal("invalid response header")
	}
	resp := req.ResponseKind()
	if err := resp.ReadFrom(body); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestDeleteGroupsNonCoordinatorE2E(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E") != "1" {
		t.Skip("set KAFSCALE_E2E=1 to run integration harness")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	etcd, endpoints := startEmbeddedEtcd(t)
	defer etcd.Close()
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// Publish the operator's two-broker snapshot before starting either process.
	snapshot := metadata.ClusterMetadata{}
	for i, addr := range []string{aAddr, bAddr} {
		host, portString, _ := net.SplitHostPort(addr)
		port, _ := strconv.Atoi(portString)
		snapshot.Brokers = append(snapshot.Brokers, protocol.MetadataBroker{NodeID: int32(i + 1), Host: host, Port: int32(port)})
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.EtcdClient().Put(ctx, "/kafscale/metadata/snapshot", string(encoded)); err != nil {
		t.Fatal(err)
	}

	a, logsA := startBrokerWithEtcd(t, ctx, aAddr, freeAddr(t), freeAddr(t), endpoints, "KAFSCALE_BROKER_ID=1")
	defer stopBroker(t, a)
	waitForBroker(t, logsA, aAddr)
	b, logsB := startBrokerWithEtcd(t, ctx, bAddr, freeAddr(t), freeAddr(t), endpoints, "KAFSCALE_BROKER_ID=2")
	defer stopBroker(t, b)
	waitForBroker(t, logsB, bAddr)
	proxyAddr := freeAddr(t)
	_, proxyPort, _ := net.SplitHostPort(proxyAddr)
	proxyLog, err := os.Create(filepath.Join(t.TempDir(), "proxy.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer proxyLog.Close()
	proxyCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "proxy"))
	configureProcessGroup(proxyCmd)
	proxyCmd.Env = os.Environ()
	for _, entry := range []string{"KAFSCALE_PROXY_ADDR=" + proxyAddr, "KAFSCALE_PROXY_BACKENDS=" + aAddr + "," + bAddr, "KAFSCALE_PROXY_ETCD_ENDPOINTS=" + strings.Join(endpoints, ","), "KAFSCALE_PROXY_ADVERTISED_HOST=127.0.0.1", "KAFSCALE_PROXY_ADVERTISED_PORT=" + proxyPort, "KAFSCALE_PROXY_HEALTH_ADDR=", "KAFSCALE_PROXY_LFS_ENABLED=false", "KAFSCALE_PROXY_IDENTITY_KEY_FILE=", "KAFSCALE_PROXY_TLS_CERT_FILE=", "KAFSCALE_PROXY_TLS_KEY_FILE=", "KAFSCALE_PROXY_TLS_CLIENT_CA_FILE=", "KAFSCALE_PROXY_TLS_PRINCIPALS_FILE="} {
		key, value, _ := strings.Cut(entry, "=")
		proxyCmd.Env = upsertEnv(proxyCmd.Env, key, value)
	}
	var proxyLogs brokerLogBuffer
	proxyWriter := io.MultiWriter(&proxyLogs, proxyLog)
	proxyCmd.Stdout, proxyCmd.Stderr = proxyWriter, proxyWriter
	if err := proxyCmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer stopBroker(t, proxyCmd)
	for deadline := time.Now().Add(20 * time.Second); ; {
		conn, err := net.DialTimeout("tcp", proxyAddr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("proxy startup: %v\n%s", err, proxyLogs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	admin, err := kgo.NewClient(kgo.SeedBrokers(proxyAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	const groupID = "healthy-multibroker"
	join := kmsg.NewPtrJoinGroupRequest()
	join.Group, join.ProtocolType, join.SessionTimeoutMillis = groupID, "consumer", 3000
	join.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range"}}
	joined := directGroupRequest(t, aAddr, join).(*kmsg.JoinGroupResponse)
	if joined.ErrorCode != 0 {
		t.Fatal(joined)
	}
	sync := kmsg.NewPtrSyncGroupRequest()
	sync.Group, sync.MemberID, sync.Generation = groupID, joined.MemberID, joined.Generation
	if resp := directGroupRequest(t, aAddr, sync).(*kmsg.SyncGroupResponse); resp.ErrorCode != 0 {
		t.Fatal(resp)
	}
	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Version, commit.Group, commit.MemberID, commit.Generation = 2, groupID, joined.MemberID, joined.Generation
	commit.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "orders", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 0, Offset: 42}}}}
	committed := directGroupRequest(t, aAddr, commit).(*kmsg.OffsetCommitResponse)
	if len(committed.Topics) != 1 || committed.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatal(committed)
	}
	del := kmsg.NewPtrDeleteGroupsRequest()
	del.Groups = []string{groupID}
	if resp := directGroupRequest(t, bAddr, del).(*kmsg.DeleteGroupsResponse); resp.Groups[0].ErrorCode != protocol.NOT_COORDINATOR {
		t.Fatal(resp)
	}
	find := kmsg.NewPtrFindCoordinatorRequest()
	find.Version, find.CoordinatorKey = 3, groupID
	if resp := directGroupRequest(t, bAddr, find).(*kmsg.FindCoordinatorResponse); resp.ErrorCode != 0 || resp.NodeID != 1 {
		t.Fatalf("wrong coordinator discovery: %+v", resp)
	}
	if resp := directGroupRequest(t, proxyAddr, del).(*kmsg.DeleteGroupsResponse); resp.Groups[0].ErrorCode != protocol.NON_EMPTY_GROUP {
		t.Fatalf("proxy active delete: %+v", resp)
	}
	heartbeat := kmsg.NewPtrHeartbeatRequest()
	heartbeat.Group, heartbeat.MemberID, heartbeat.Generation = groupID, joined.MemberID, joined.Generation
	// Outlast B's five-second cleanup tick and multiple member session timeouts.
	// Only A receives heartbeats; B must never expire a copy of this group.
	until := time.Now().Add(6500 * time.Millisecond)
	for time.Now().Before(until) {
		if resp := directGroupRequest(t, aAddr, heartbeat).(*kmsg.HeartbeatResponse); resp.ErrorCode != 0 {
			t.Fatal(resp)
		}
		time.Sleep(100 * time.Millisecond)
	}
	group, err := store.FetchConsumerGroup(ctx, groupID)
	if err != nil || group == nil || group.State != "stable" || group.GenerationId != joined.Generation || group.Members[joined.MemberID] == nil {
		t.Fatal(group, err)
	}
	offsets, err := store.ListGroupConsumerOffsets(ctx, groupID)
	if err != nil || len(offsets) != 1 || offsets[0].Offset != 42 {
		t.Fatal(offsets, err)
	}
	if resp := directGroupRequest(t, aAddr, del).(*kmsg.DeleteGroupsResponse); resp.Groups[0].ErrorCode != protocol.NON_EMPTY_GROUP {
		t.Fatal(resp)
	}
	// Mix an idle group owned by B, an active group owned by A, and an unknown
	// group in one proxy request. Preserve independent results in request order.
	idleJoin := *join
	idleJoin.Group, idleJoin.SessionTimeoutMillis = "idle-on-b", 60000
	idleMember := directGroupRequest(t, bAddr, &idleJoin).(*kmsg.JoinGroupResponse)
	if idleMember.ErrorCode != 0 {
		t.Fatal(idleMember)
	}
	idleLeave := kmsg.NewPtrLeaveGroupRequest()
	idleLeave.Group, idleLeave.MemberID = idleJoin.Group, idleMember.MemberID
	if resp := directGroupRequest(t, bAddr, idleLeave).(*kmsg.LeaveGroupResponse); resp.ErrorCode != 0 {
		t.Fatal(resp)
	}
	mixed := kmsg.NewPtrDeleteGroupsRequest()
	mixed.Groups = []string{idleJoin.Group, groupID, "never-created"}
	mixedResp, err := mixed.RequestWith(ctx, admin)
	if err != nil || len(mixedResp.Groups) != 3 {
		t.Fatal(mixedResp, err)
	}
	for i, code := range []int16{0, protocol.NON_EMPTY_GROUP, protocol.GROUP_ID_NOT_FOUND} {
		if mixedResp.Groups[i].Group != mixed.Groups[i] || mixedResp.Groups[i].ErrorCode != code {
			t.Fatal(mixedResp)
		}
	}
	leave := kmsg.NewPtrLeaveGroupRequest()
	leave.Group, leave.MemberID = groupID, joined.MemberID
	if resp := directGroupRequest(t, aAddr, leave).(*kmsg.LeaveGroupResponse); resp.ErrorCode != 0 {
		t.Fatal(resp)
	}
	for _, addr := range []string{aAddr, bAddr} {
		list := kmsg.NewPtrListGroupsRequest()
		list.Version, list.StatesFilter = 4, []string{"Empty"}
		listed := directGroupRequest(t, addr, list).(*kmsg.ListGroupsResponse)
		found := false
		for _, g := range listed.Groups {
			found = found || (g.Group == groupID && g.GroupState == "Empty")
		}
		if listed.ErrorCode != 0 || !found {
			t.Fatalf("%s list: %+v", addr, listed)
		}
	}
	for _, addr := range []string{aAddr, proxyAddr} {
		describe := kmsg.NewPtrDescribeGroupsRequest()
		describe.Groups = []string{groupID}
		described := directGroupRequest(t, addr, describe).(*kmsg.DescribeGroupsResponse)
		if described.Groups[0].ErrorCode != 0 || described.Groups[0].State != "Empty" || len(described.Groups[0].Members) != 0 {
			t.Fatalf("%s describe: %+v", addr, described)
		}
	}
	if resp := directGroupRequest(t, proxyAddr, del).(*kmsg.DeleteGroupsResponse); resp.Groups[0].ErrorCode != 0 {
		t.Fatal(resp)
	}
	if offsets, err := store.ListGroupConsumerOffsets(ctx, groupID); err != nil || len(offsets) != 0 {
		t.Fatal("delete retained offsets", offsets, err)
	}
	if group, err := store.FetchConsumerGroup(ctx, groupID); err != nil || group != nil {
		t.Fatal("delete retained group", group, err)
	}
	lease, err := store.EtcdClient().Get(ctx, metadata.GroupLeasePrefix()+"/"+groupID)
	if err != nil || len(lease.Kvs) != 0 {
		t.Fatal("delete retained lease", lease, err)
	}
	// Repeated deletion and immediate recreation on the other broker must not
	// depend on the old coordination lease's TTL or a lucky proxy backend.
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("proxy-delete-%d", i)
		first, second := aAddr, bAddr
		if i%2 != 0 {
			first, second = second, first
		}
		req := *join
		req.Group, req.SessionTimeoutMillis = name, 60000
		member := directGroupRequest(t, first, &req).(*kmsg.JoinGroupResponse)
		if member.ErrorCode != 0 {
			t.Fatal(member)
		}
		leave.Group, leave.MemberID = name, member.MemberID
		if resp := directGroupRequest(t, first, leave).(*kmsg.LeaveGroupResponse); resp.ErrorCode != 0 {
			t.Fatal(resp)
		}
		del.Groups = []string{name}
		deleted, err := del.RequestWith(ctx, admin)
		if err != nil || deleted.Groups[0].ErrorCode != 0 {
			t.Fatalf("proxy delete %d: %+v, %v", i, deleted, err)
		}
		rejoined := directGroupRequest(t, second, &req).(*kmsg.JoinGroupResponse)
		if rejoined.ErrorCode != 0 {
			t.Fatalf("lease not released: %+v", rejoined)
		}
		leave.Group, leave.MemberID = name, rejoined.MemberID
		if resp := directGroupRequest(t, second, leave).(*kmsg.LeaveGroupResponse); resp.ErrorCode != 0 {
			t.Fatal(resp)
		}
		if deleted, err := del.RequestWith(ctx, admin); err != nil || deleted.Groups[0].ErrorCode != 0 {
			t.Fatal(deleted, err)
		}
	}

}
