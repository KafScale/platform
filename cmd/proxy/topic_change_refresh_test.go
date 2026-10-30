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

package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestTopicChangesRefreshListedTopics(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()
	brokers := []protocol.MetadataBroker{{NodeID: 0, Host: "broker-0", Port: 9092}}
	putProxyTestSnapshot(t, endpoints, metadata.ClusterMetadata{Brokers: brokers})
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	cases := []struct {
		name   string
		topics []protocol.MetadataTopic
		req    kmsg.Request
	}{
		{"create", []protocol.MetadataTopic{{Topic: kmsg.StringPtr("orders"), Partitions: []protocol.MetadataPartition{{Partition: 0}}}}, &kmsg.CreateTopicsRequest{Version: 2}},
		{"partitions", []protocol.MetadataTopic{{Topic: kmsg.StringPtr("orders"), Partitions: []protocol.MetadataPartition{{Partition: 0}, {Partition: 1}}}}, &kmsg.CreatePartitionsRequest{Version: 1}},
		{"delete", nil, &kmsg.DeleteTopicsRequest{Version: 2}},
	}
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for _, tc := range cases {
			frame, err := protocol.ReadFrame(conn)
			if err != nil {
				return
			}
			header, _, err := protocol.ParseRequestHeader(frame.Payload)
			if err != nil {
				return
			}
			putProxyTestSnapshot(t, endpoints, metadata.ClusterMetadata{Brokers: brokers, Topics: tc.topics})
			resp := tc.req.ResponseKind()
			resp.SetVersion(header.APIVersion)
			if protocol.WriteFrame(conn, protocol.EncodeResponse(header.CorrelationID, header.APIVersion, resp)) != nil {
				return
			}
		}
	}()
	p := &proxy{store: store, backends: []string{listener.Addr().String()}, backendRetries: 1, dialTimeout: time.Second, advertisedHost: "entry", advertisedPort: 29092, cacheTTL: time.Minute, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	p.setReady(true)
	server, client := net.Pipe()
	defer client.Close()
	go p.handleConnection(ctx, server)
	formatter := kmsg.NewRequestFormatter(kmsg.FormatterClientID("client"))
	correlation := int32(0)
	roundTrip := func(req kmsg.Request) []byte {
		t.Helper()
		correlation++
		_ = client.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := client.Write(formatter.AppendRequest(nil, req, correlation)); err != nil {
			t.Fatal(err)
		}
		frame, err := protocol.ReadFrame(client)
		if err != nil {
			t.Fatal(err)
		}
		return frame.Payload
	}
	for _, tc := range cases {
		roundTrip(tc.req)
		payload := roundTrip(&kmsg.MetadataRequest{Version: 12})
		listed := kmsg.NewPtrMetadataResponse()
		listed.Version = 12
		if err := listed.ReadFrom(payload[5:]); err != nil {
			t.Fatal(err)
		}
		partitions := -1
		for _, topic := range listed.Topics {
			if topic.Topic != nil && *topic.Topic == "orders" {
				partitions = len(topic.Partitions)
			}
		}
		want := -1
		if len(tc.topics) > 0 {
			want = len(tc.topics[0].Partitions)
		}
		if partitions != want || len(listed.Brokers) != 1 || listed.Brokers[0].Host != "entry" {
			t.Fatalf("%s: orders partitions %d want %d, brokers %+v", tc.name, partitions, want, listed.Brokers)
		}
	}
}
