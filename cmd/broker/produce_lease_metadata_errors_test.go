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
	"testing"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func leaseMetadataErrorBrokers(t *testing.T) (*metadata.EtcdStore, *handler, *handler) {
	t.Helper()
	store := newEtcdTopicStore(t, context.Background())
	a := newTestHandler(store)
	b := newHandler(store, storage.NewMemoryS3Client(), protocol.MetadataBroker{NodeID: 2}, testLogger())
	for _, h := range []*handler{a, b} {
		t.Cleanup(h.coordinator.Stop)
		t.Cleanup(h.leaseManager.ReleaseAll)
		h.autoCreateTopics = true
	}
	return store, a, b
}
func TestProduceInvalidTopicNameAcrossLeaseOwners(t *testing.T) {
	store, a, b := leaseMetadataErrorBrokers(t)
	for _, name := range invalidTopicNames {
		t.Run(name, func(t *testing.T) {
			for _, h := range []*handler{a, b} {
				raw := producePromptly(t, h, unknownPartitionProduce(name, 0, -1, 0))
				p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
				if p.ErrorCode != protocol.INVALID_TOPIC_EXCEPTION {
					t.Fatalf("broker %d: got %d, want INVALID_TOPIC_EXCEPTION", h.brokerInfo.NodeID, p.ErrorCode)
				}
			}
			if topicExists(t, context.Background(), store, name) {
				t.Fatal("invalid topic was created")
			}
		})
	}
	if len(a.logs) != 0 || len(b.logs) != 0 {
		t.Fatal("invalid requests initialized partition logs")
	}
}

type topicErrorLeaseStore struct {
	*metadata.EtcdStore
	code int16
}

func (s *topicErrorLeaseStore) Metadata(ctx context.Context, topics []string) (*metadata.ClusterMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	meta := &metadata.ClusterMetadata{}
	for _, name := range topics {
		meta.Topics = append(meta.Topics, protocol.MetadataTopic{Topic: kmsg.StringPtr(name), ErrorCode: s.code})
	}
	return meta, nil
}
func TestProducePreservesTopicMetadataErrorsAcrossLeaseOwners(t *testing.T) {
	store, a, b := leaseMetadataErrorBrokers(t)
	// The first broker holds the lease for a partition absent from metadata.
	raw := producePromptly(t, a, unknownPartitionProduce("orders", 7, -1, 0))
	if p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]; p.ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatal(p.ErrorCode)
	}
	for _, code := range []int16{protocol.UNKNOWN_TOPIC_OR_PARTITION, protocol.INVALID_TOPIC_EXCEPTION, protocol.TOPIC_AUTHORIZATION_FAILED, protocol.LEADER_NOT_AVAILABLE, protocol.REQUEST_TIMED_OUT, protocol.UNKNOWN_SERVER_ERROR} {
		t.Run(fmt.Sprintf("code=%d", code), func(t *testing.T) {
			b.store = &topicErrorLeaseStore{EtcdStore: store, code: code}
			raw := producePromptly(t, b, unknownPartitionProduce("orders", 7, -1, 0))
			p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
			if p.ErrorCode != code {
				t.Fatalf("topic metadata error %d became %d", code, p.ErrorCode)
			}
		})
	}
}
