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
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProxyAdvertisesLibrdkafkaFetchCompatibility(t *testing.T) {
	p := &proxy{apiVersions: generateProxyApiVersions()}
	wire, err := p.handleApiVersions(&protocol.RequestHeader{APIVersion: 3})
	if err != nil {
		t.Fatal(err)
	}
	// ApiVersions always uses response header v0, even with a flexible body.
	resp := kmsg.NewPtrApiVersionsResponse()
	resp.Version = 3
	if err := resp.ReadFrom(wire[4:]); err != nil {
		t.Fatal(err)
	}
	for _, api := range resp.ApiKeys {
		if api.ApiKey == protocol.APIKeyFetch {
			if api.MinVersion != 4 || api.MaxVersion != 13 {
				t.Fatalf("Fetch range %d..%d excludes librdkafka's v4 probe", api.MinVersion, api.MaxVersion)
			}
			return
		}
	}
	t.Fatal("Fetch missing from proxy ApiVersions")
}

func TestProxyForwardsFetchCompatibilityVersions(t *testing.T) {
	addr, _ := fallbackBackend(t)
	p := &proxy{backends: []string{addr}, backendRetries: 1, dialTimeout: time.Second, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	pool := newConnPool(time.Second)
	pool.dial = p.dialBackend
	defer pool.Close()
	for version := int16(4); version <= 12; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			req := kmsg.NewPtrFetchRequest()
			req.Version = version
			req.Topics = []kmsg.FetchRequestTopic{{Topic: "orders", Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 0, PartitionMaxBytes: 1024}}}}
			header := &protocol.RequestHeader{APIKey: protocol.APIKeyFetch, APIVersion: version}
			wire, err := p.handleFetchRouting(context.Background(), header, encodeFetchRequest(header, req), pool)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := parseFetchResponse(wire, version)
			if err != nil || len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 || resp.Topics[0].Partitions[0].ErrorCode != 0 {
				t.Fatal(resp, err)
			}
		})
	}
}

func TestClassicGroupApiProbeCompatibility(t *testing.T) {
	probes := map[int16]int16{protocol.APIKeyFindCoordinator: 0, protocol.APIKeyJoinGroup: 0, protocol.APIKeySyncGroup: 0, protocol.APIKeyHeartbeat: 0, protocol.APIKeyLeaveGroup: 0, protocol.APIKeyOffsetCommit: 1, protocol.APIKeyOffsetFetch: 1}
	for _, api := range generateProxyApiVersions() {
		if probe, ok := probes[api.ApiKey]; ok {
			if api.MinVersion > probe || api.MaxVersion < probe {
				t.Fatalf("API %d range %d..%d excludes librdkafka probe %d", api.ApiKey, api.MinVersion, api.MaxVersion, probe)
			}
			delete(probes, api.ApiKey)
		}
	}
	if len(probes) > 0 {
		t.Fatal("missing classic group APIs", probes)
	}
}
