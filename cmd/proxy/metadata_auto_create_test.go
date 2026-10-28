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

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestPlainMetadataMissingTopicsReachBroker(t *testing.T) {
	for _, version := range []int16{1, 4, 9, 12} {
		for _, flag := range []bool{false, true} {
			for _, enabled := range []bool{false, true} {
				t.Run(fmt.Sprintf("v%d/flag=%t/auto=%t", version, flag, enabled), func(t *testing.T) {
					store := metadata.NewInMemoryStore(metadata.ClusterMetadata{Topics: []protocol.MetadataTopic{{Topic: kmsg.StringPtr("existing")}}})
					p := &proxy{store: store, backends: []string{"broker"}, advertisedHost: "entry", advertisedPort: 29092, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
					pool := newConnPool(time.Second)
					defer pool.Close()
					seen := make(chan *kmsg.MetadataRequest, 1)
					backendErr := make(chan error, 1)
					pool.dial = func(ctx context.Context, _ string) (net.Conn, error) {
						client, server := net.Pipe()
						_ = server.SetDeadline(time.Now().Add(5 * time.Second))
						go func() {
							defer server.Close()
							frame, err := protocol.ReadFrame(server)
							if err != nil {
								backendErr <- err
								return
							}
							h, request, err := protocol.ParseRequest(frame.Payload)
							if err != nil {
								backendErr <- err
								return
							}
							req := request.(*kmsg.MetadataRequest)
							seen <- req
							resp := kmsg.NewPtrMetadataResponse()
							resp.Version = version
							resp.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 7, Host: "internal", Port: 9092}}
							for _, topic := range req.Topics {
								result := kmsg.NewMetadataResponseTopic()
								result.Topic = topic.Topic
								if *topic.Topic == "new" && !enabled {
									result.ErrorCode = protocol.UNKNOWN_TOPIC_OR_PARTITION
								} else {
									result.Partitions = []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 7}, {Partition: 1, Leader: 7}, {Partition: 2, Leader: 7}}
								}
								resp.Topics = append(resp.Topics, result)
							}
							backendErr <- protocol.WriteFrame(server, protocol.EncodeResponse(h.CorrelationID, version, resp))
						}()
						return client, nil
					}
					req := kmsg.NewPtrMetadataRequest()
					req.Version = version
					req.AllowAutoTopicCreation = flag
					req.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr("existing")}, {Topic: kmsg.StringPtr("new")}}
					request := kmsg.NewRequestFormatter(kmsg.FormatterClientID("client")).AppendRequest(nil, req, 23)[4:]
					header, _, _ := protocol.ParseRequestHeader(request)
					response, err := p.handleMetadata(context.Background(), header, request, pool)
					if err != nil {
						t.Fatal(err)
					}
					observed := <-seen
					if observed.Version != version || (version >= 4 && observed.AllowAutoTopicCreation != flag) || len(observed.Topics) != 2 {
						t.Fatal("request changed", observed)
					}
					if err := <-backendErr; err != nil {
						t.Fatal(err)
					}
					decoded := kmsg.NewPtrMetadataResponse()
					decoded.Version = version
					offset := 4
					if version >= 9 {
						offset++
					}
					if err := decoded.ReadFrom(response[offset:]); err != nil {
						t.Fatal(err)
					}
					if len(decoded.Topics) != 2 || decoded.Brokers[0].Host != "entry" || decoded.Brokers[0].Port != 29092 {
						t.Fatal(decoded)
					}
					topic := decoded.Topics[1]
					if enabled {
						if topic.ErrorCode != 0 || len(topic.Partitions) != 3 || topic.Partitions[0].Leader != 0 {
							t.Fatal(topic)
						}
					} else if topic.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
						t.Fatal(topic)
					}
					if len(pool.conns) != 1 {
						t.Fatal("backend connection was not pooled")
					}
				})
			}
		}
	}
}

func TestPlainMetadataKeepsStorePathForKnownTopicsAndIDs(t *testing.T) {
	id := metadata.TopicIDForName("known")
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{Topics: []protocol.MetadataTopic{{Topic: kmsg.StringPtr("known"), TopicID: id}}})
	p := &proxy{store: store, advertisedHost: "entry", advertisedPort: 29092}
	for _, topics := range [][]kmsg.MetadataRequestTopic{nil, {}, {{Topic: kmsg.StringPtr("known")}}, {{TopicID: id}}, {{TopicID: [16]byte{99}}}} {
		req := kmsg.NewPtrMetadataRequest()
		req.Version = 12
		req.Topics = topics
		request := kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)[4:]
		header, _, _ := protocol.ParseRequestHeader(request)
		if _, err := p.handleMetadata(context.Background(), header, request, nil); err != nil {
			t.Fatal("metadata unexpectedly required a broker", err)
		}
	}
}
