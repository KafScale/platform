// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package main

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/internal/testutil"
	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type offsetsBroker struct {
	addr     string
	mu       sync.Mutex
	owns     map[int32]bool
	answered map[int32]int
	refused  int
	handoff  map[int32]func()
}

func startOffsetsBroker(t *testing.T, owns ...int32) *offsetsBroker {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &offsetsBroker{addr: listener.Addr().String(), owns: map[int32]bool{}, answered: map[int32]int{}, handoff: map[int32]func(){}}
	for _, part := range owns {
		b.owns[part] = true
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
			go b.serve(c)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
	})
	return b
}

func (b *offsetsBroker) own(partition int32) {
	b.mu.Lock()
	b.owns[partition] = true
	b.mu.Unlock()
}

func (b *offsetsBroker) leads(partition int32) bool {
	b.mu.Lock()
	handoff := b.handoff[partition]
	if handoff != nil {
		delete(b.handoff, partition)
		delete(b.owns, partition)
	}
	b.mu.Unlock()
	if handoff != nil {
		handoff()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.owns[partition] {
		b.refused++
		return false
	}
	b.answered[partition]++
	return true
}

func (b *offsetsBroker) serve(c net.Conn) {
	defer func() { _ = c.Close() }()
	for {
		frame, err := protocol.ReadFrame(c)
		if err != nil {
			return
		}
		header, parsed, err := protocol.ParseRequest(frame.Payload)
		if err != nil {
			return
		}
		var resp kmsg.Response
		switch req := parsed.(type) {
		case *kmsg.ListOffsetsRequest:
			lo := kmsg.NewPtrListOffsetsResponse()
			for _, topic := range req.Topics {
				rt := kmsg.NewListOffsetsResponseTopic()
				rt.Topic = topic.Topic
				for _, part := range topic.Partitions {
					rp := kmsg.NewListOffsetsResponseTopicPartition()
					rp.Partition = part.Partition
					if !b.leads(part.Partition) {
						rp.ErrorCode = protocol.NOT_LEADER_OR_FOLLOWER
					} else {
						rp.Offset = 0
						if part.Timestamp == -1 {
							rp.Offset = int64(100 + part.Partition)
						}
					}
					rt.Partitions = append(rt.Partitions, rp)
				}
				lo.Topics = append(lo.Topics, rt)
			}
			resp = lo
		case *kmsg.OffsetForLeaderEpochRequest:
			oe := kmsg.NewPtrOffsetForLeaderEpochResponse()
			for _, topic := range req.Topics {
				rt := kmsg.NewOffsetForLeaderEpochResponseTopic()
				rt.Topic = topic.Topic
				for _, part := range topic.Partitions {
					rp := kmsg.NewOffsetForLeaderEpochResponseTopicPartition()
					rp.Partition = part.Partition
					if !b.leads(part.Partition) {
						rp.ErrorCode = protocol.NOT_LEADER_OR_FOLLOWER
					} else {
						rp.LeaderEpoch = part.LeaderEpoch
						rp.EndOffset = int64(200 + part.Partition)
					}
					rt.Partitions = append(rt.Partitions, rp)
				}
				oe.Topics = append(oe.Topics, rt)
			}
			resp = oe
		default:
			return
		}
		if err := protocol.WriteFrame(c, protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp)); err != nil {
			return
		}
	}
}

type offsetsRoutingSetup struct {
	brokers []*offsetsBroker
	etcd    *clientv3.Client
	router  *metadata.PartitionRouter
	client  net.Conn
}

func startOffsetsRouting(t *testing.T, ctx context.Context, topic string) *offsetsRoutingSetup {
	t.Helper()
	endpoints := testutil.StartEmbeddedEtcd(t)
	brokers := []*offsetsBroker{startOffsetsBroker(t, 0, 2), startOffsetsBroker(t, 1, 3)}
	partitions := []protocol.MetadataPartition{{Partition: 0, Leader: 0}, {Partition: 1, Leader: 1}, {Partition: 2, Leader: 0}, {Partition: 3, Leader: 1}}
	putProxyTestSnapshot(t, endpoints, metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{brokerAt(t, 0, brokers[0].addr), brokerAt(t, 1, brokers[1].addr)},
		Topics:  []protocol.MetadataTopic{{Topic: kmsg.StringPtr(topic), Partitions: partitions}},
	})
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	for _, part := range partitions {
		if _, err := cli.Put(ctx, metadata.PartitionLeasePrefix()+"/"+topic+"/"+strconv.Itoa(int(part.Partition)), strconv.Itoa(int(part.Leader))); err != nil {
			t.Fatal(err)
		}
	}
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	p := lfsTestProxy(store, time.Minute)
	if p.router, err = metadata.NewPartitionRouter(ctx, store.EtcdClient(), p.logger); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.router.Stop)
	p.refreshMetadataCache(ctx)
	p.setReady(true)

	server, client := net.Pipe()
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	done := make(chan struct{})
	go func() { defer close(done); p.handleConnection(ctx, server) }()
	t.Cleanup(func() {
		_ = client.Close()
		<-done
	})
	return &offsetsRoutingSetup{brokers: brokers, etcd: cli, router: p.router, client: client}
}

func (s *offsetsRoutingSetup) roundTrip(t *testing.T, req kmsg.Request, correlationID int32, resp kmsg.Response) {
	t.Helper()
	if _, err := s.client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, req, correlationID)); err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.ReadFrame(s.client)
	if err != nil {
		t.Fatal(err)
	}
	body, ok := protocol.SkipResponseHeader(req.Key(), req.GetVersion(), frame.Payload)
	if !ok {
		t.Fatal("invalid response header")
	}
	resp.SetVersion(req.GetVersion())
	if err := resp.ReadFrom(body); err != nil {
		t.Fatal(err)
	}
}

func listOffsetsRequest(topic string, timestamp int64, order []int32) *kmsg.ListOffsetsRequest {
	req := kmsg.NewPtrListOffsetsRequest()
	req.Version = 4
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	for _, part := range order {
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Partition = part
		rp.Timestamp = timestamp
		rt.Partitions = append(rt.Partitions, rp)
	}
	req.Topics = append(req.Topics, rt)
	return req
}

func (s *offsetsRoutingSetup) checkOwnersOnly(t *testing.T, want int) {
	t.Helper()
	for i, b := range s.brokers {
		b.mu.Lock()
		if b.refused != 0 {
			t.Errorf("broker %d refused %d partitions it does not own", i, b.refused)
		}
		for part := range b.owns {
			if b.answered[part] != want {
				t.Errorf("broker %d answered partition %d %d times, want %d", i, part, b.answered[part], want)
			}
		}
		b.mu.Unlock()
	}
}

func TestProxyRoutesListOffsetsToPartitionOwners(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := startOffsetsRouting(t, ctx, "lg6")

	order := []int32{3, 0, 2, 1}
	for i, timestamp := range []int64{-2, -1} {
		resp := kmsg.NewPtrListOffsetsResponse()
		s.roundTrip(t, listOffsetsRequest("lg6", timestamp, order), int32(i+1), resp)
		if len(resp.Topics) != 1 || resp.Topics[0].Topic != "lg6" || len(resp.Topics[0].Partitions) != len(order) {
			t.Fatalf("timestamp %d: unexpected response %+v", timestamp, resp.Topics)
		}
		for j, got := range resp.Topics[0].Partitions {
			want := int64(0)
			if timestamp == -1 {
				want = int64(100 + order[j])
			}
			if got.Partition != order[j] || got.ErrorCode != 0 || got.Offset != want {
				t.Fatalf("timestamp %d: partition %d got %+v, want offset %d", timestamp, order[j], got, want)
			}
		}
	}
	s.checkOwnersOnly(t, 2)
}

func TestProxyRoutesOffsetForLeaderEpochToPartitionOwners(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := startOffsetsRouting(t, ctx, "lg7")

	order := []int32{3, 0, 2, 1}
	req := kmsg.NewPtrOffsetForLeaderEpochRequest()
	req.Version = 3
	req.ReplicaID = -1
	rt := kmsg.NewOffsetForLeaderEpochRequestTopic()
	rt.Topic = "lg7"
	for _, part := range order {
		rp := kmsg.NewOffsetForLeaderEpochRequestTopicPartition()
		rp.Partition = part
		rp.CurrentLeaderEpoch = 5
		rp.LeaderEpoch = 5
		rt.Partitions = append(rt.Partitions, rp)
	}
	req.Topics = append(req.Topics, rt)

	resp := kmsg.NewPtrOffsetForLeaderEpochResponse()
	s.roundTrip(t, req, 1, resp)
	if len(resp.Topics) != 1 || resp.Topics[0].Topic != "lg7" || len(resp.Topics[0].Partitions) != len(order) {
		t.Fatalf("unexpected response %+v", resp.Topics)
	}
	for j, got := range resp.Topics[0].Partitions {
		if got.Partition != order[j] || got.ErrorCode != 0 || got.LeaderEpoch != 5 || got.EndOffset != int64(200+order[j]) {
			t.Fatalf("partition %d got %+v, want end offset %d", order[j], got, 200+order[j])
		}
	}
	s.checkOwnersOnly(t, 1)
}

func TestProxyListOffsetsFollowsOwnerThatMovesMidRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := startOffsetsRouting(t, ctx, "lg8")
	s.router.Stop()
	oldOwner, newOwner := s.brokers[1], s.brokers[0]
	oldOwner.handoff[1] = func() {
		if _, err := s.etcd.Put(ctx, metadata.PartitionLeasePrefix()+"/lg8/1", "0"); err != nil {
			t.Error(err)
		}
		newOwner.own(1)
	}

	resp := kmsg.NewPtrListOffsetsResponse()
	s.roundTrip(t, listOffsetsRequest("lg8", -1, []int32{0, 1}), 1, resp)
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 2 {
		t.Fatalf("unexpected response %+v", resp.Topics)
	}
	for j, got := range resp.Topics[0].Partitions {
		if got.Partition != int32(j) || got.ErrorCode != 0 || got.Offset != int64(100+j) {
			t.Fatalf("partition %d got %+v, want offset %d", j, got, 100+j)
		}
	}

	oldOwner.mu.Lock()
	defer oldOwner.mu.Unlock()
	newOwner.mu.Lock()
	defer newOwner.mu.Unlock()
	if oldOwner.refused != 1 || oldOwner.answered[1] != 0 {
		t.Errorf("old owner refused %d and answered partition 1 %d times, want 1 and 0", oldOwner.refused, oldOwner.answered[1])
	}
	if newOwner.refused != 0 || newOwner.answered[0] != 1 || newOwner.answered[1] != 1 {
		t.Errorf("new owner refused %d and answered %v, want 0 refused and partitions 0 and 1 once", newOwner.refused, newOwner.answered)
	}
}
