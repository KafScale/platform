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

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type offsetsBroker struct {
	addr     string
	owns     map[int32]bool
	mu       sync.Mutex
	answered map[int32]int
	refused  int
}

func startOffsetsBroker(t *testing.T, owns ...int32) *offsetsBroker {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &offsetsBroker{addr: listener.Addr().String(), owns: map[int32]bool{}, answered: map[int32]int{}}
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

func (b *offsetsBroker) serve(c net.Conn) {
	defer c.Close()
	for {
		frame, err := protocol.ReadFrame(c)
		if err != nil {
			return
		}
		header, parsed, err := protocol.ParseRequest(frame.Payload)
		if err != nil {
			return
		}
		req, ok := parsed.(*kmsg.ListOffsetsRequest)
		if !ok {
			return
		}
		resp := kmsg.NewPtrListOffsetsResponse()
		b.mu.Lock()
		for _, topic := range req.Topics {
			rt := kmsg.NewListOffsetsResponseTopic()
			rt.Topic = topic.Topic
			for _, part := range topic.Partitions {
				rp := kmsg.NewListOffsetsResponseTopicPartition()
				rp.Partition = part.Partition
				if !b.owns[part.Partition] {
					b.refused++
					rp.ErrorCode = protocol.NOT_LEADER_OR_FOLLOWER
				} else {
					b.answered[part.Partition]++
					rp.Offset = 0
					if part.Timestamp == -1 {
						rp.Offset = int64(100 + part.Partition)
					}
				}
				rt.Partitions = append(rt.Partitions, rp)
			}
			resp.Topics = append(resp.Topics, rt)
		}
		b.mu.Unlock()
		if err := protocol.WriteFrame(c, protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp)); err != nil {
			return
		}
	}
}

func TestProxyRoutesListOffsetsToPartitionOwners(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	endpoints := testutil.StartEmbeddedEtcd(t)
	brokers := []*offsetsBroker{startOffsetsBroker(t, 0, 2), startOffsetsBroker(t, 1, 3)}
	partitions := []protocol.MetadataPartition{{Partition: 0, Leader: 0}, {Partition: 1, Leader: 1}, {Partition: 2, Leader: 0}, {Partition: 3, Leader: 1}}
	putProxyTestSnapshot(t, endpoints, metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{brokerAt(t, 0, brokers[0].addr), brokerAt(t, 1, brokers[1].addr)},
		Topics:  []protocol.MetadataTopic{{Topic: kmsg.StringPtr("lg6"), Partitions: partitions}},
	})
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	for _, part := range partitions {
		if _, err := cli.Put(ctx, metadata.PartitionLeasePrefix()+"/lg6/"+strconv.Itoa(int(part.Partition)), strconv.Itoa(int(part.Leader))); err != nil {
			t.Fatal(err)
		}
	}
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	p := lfsTestProxy(store, time.Minute)
	if p.router, err = metadata.NewPartitionRouter(ctx, store.EtcdClient(), p.logger); err != nil {
		t.Fatal(err)
	}
	defer p.router.Stop()
	p.refreshMetadataCache(ctx)
	p.setReady(true)

	server, client := net.Pipe()
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	done := make(chan struct{})
	go func() { defer close(done); p.handleConnection(ctx, server) }()

	order := []int32{3, 0, 2, 1}
	for i, timestamp := range []int64{-2, -1} {
		req := kmsg.NewPtrListOffsetsRequest()
		req.Version = 4
		req.ReplicaID = -1
		topic := kmsg.NewListOffsetsRequestTopic()
		topic.Topic = "lg6"
		for _, part := range order {
			rp := kmsg.NewListOffsetsRequestTopicPartition()
			rp.Partition = part
			rp.Timestamp = timestamp
			topic.Partitions = append(topic.Partitions, rp)
		}
		req.Topics = append(req.Topics, topic)
		if _, err := client.Write(kmsg.NewRequestFormatter().AppendRequest(nil, req, int32(i+1))); err != nil {
			t.Fatal(err)
		}
		frame, err := protocol.ReadFrame(client)
		if err != nil {
			t.Fatal(err)
		}
		body, ok := protocol.SkipResponseHeader(protocol.APIKeyListOffsets, req.Version, frame.Payload)
		if !ok {
			t.Fatal("invalid response header")
		}
		resp := kmsg.NewPtrListOffsetsResponse()
		resp.SetVersion(req.Version)
		if err := resp.ReadFrom(body); err != nil {
			t.Fatal(err)
		}
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
	_ = client.Close()
	<-done

	for i, b := range brokers {
		b.mu.Lock()
		if b.refused != 0 {
			t.Errorf("broker %d refused %d partitions it does not own", i, b.refused)
		}
		for part := range b.owns {
			if b.answered[part] != 2 {
				t.Errorf("broker %d answered partition %d %d times, want 2", i, part, b.answered[part])
			}
		}
		b.mu.Unlock()
	}
}
