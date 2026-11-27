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
	"reflect"
	"strings"
	"testing"

	"github.com/KafScale/platform/v2/internal/testutil"
	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestFreshBrokerMetadataHasNoTopics(t *testing.T) {
	for _, backend := range []string{"memory", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			t.Setenv("KAFSCALE_ETCD_ENDPOINTS", "")
			t.Setenv("KAFSCALE_ACL_ENABLED", "false")
			t.Setenv("KAFSCALE_AUTO_CREATE_TOPICS", "false")
			if backend == "etcd" {
				t.Setenv("KAFSCALE_ETCD_ENDPOINTS", strings.Join(testutil.StartEmbeddedEtcd(t), ","))
			}
			ctx := context.Background()
			info := protocol.MetadataBroker{NodeID: 7, Host: "localhost", Port: 19092}
			store := buildStore(ctx, info, testLogger())
			if backend == "etcd" {
				etcd, ok := store.(*metadata.EtcdStore)
				if !ok {
					t.Fatalf("expected etcd store, got %T", store)
				}
				t.Cleanup(func() { _ = etcd.Close() })
			}
			h := newTestHandler(store)
			t.Cleanup(h.coordinator.Stop)
			lookup := func(names ...string) *kmsg.MetadataResponse {
				t.Helper()
				req := kmsg.NewPtrMetadataRequest()
				req.Version = 12
				req.AllowAutoTopicCreation = false
				for _, name := range names {
					req.Topics = append(req.Topics, kmsg.MetadataRequestTopic{Topic: kmsg.StringPtr(name)})
				}
				data, err := h.Handle(ctx, &protocol.RequestHeader{APIKey: protocol.APIKeyMetadata, APIVersion: 12}, req)
				if err != nil {
					t.Fatal(err)
				}
				return decodeKmsgResponse(t, 12, data, kmsg.NewPtrMetadataResponse)
			}
			listed := lookup()
			if len(listed.Topics) != 0 || !reflect.DeepEqual(listed.Brokers, []protocol.MetadataBroker{info}) || listed.ControllerID != info.NodeID || listed.ClusterID == nil {
				t.Fatalf("fresh broker metadata: %+v", listed)
			}
			for _, name := range []string{"orders", "other"} {
				resp := lookup(name)
				if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION || len(resp.Topics[0].Partitions) != 0 {
					t.Fatalf("missing topic %q: %+v", name, resp.Topics)
				}
			}
			if etcd, ok := store.(*metadata.EtcdStore); ok {
				keys, err := etcd.EtcdClient().Get(ctx, "/kafscale/topics/", clientv3.WithPrefix())
				if err != nil || len(keys.Kvs) != 0 {
					t.Fatalf("metadata probe persisted topic keys: %+v, %v", keys, err)
				}
			}
			create := kmsg.NewPtrCreateTopicsRequest()
			create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: "orders", NumPartitions: 6, ReplicationFactor: 1}}
			data, err := h.Handle(ctx, &protocol.RequestHeader{APIKey: protocol.APIKeyCreateTopics}, create)
			if err != nil {
				t.Fatal(err)
			}
			created := decodeKmsgResponse(t, 0, data, kmsg.NewPtrCreateTopicsResponse)
			if len(created.Topics) != 1 || created.Topics[0].ErrorCode != 0 {
				t.Fatalf("explicit create: %+v", created.Topics)
			}
			assertOrders := func(resp *kmsg.MetadataResponse) {
				t.Helper()
				if len(resp.Topics) != 1 || resp.Topics[0].Topic == nil || *resp.Topics[0].Topic != "orders" || resp.Topics[0].ErrorCode != 0 || len(resp.Topics[0].Partitions) != 6 {
					t.Fatalf("explicitly created orders: %+v", resp.Topics)
				}
			}
			assertOrders(lookup("orders"))
			assertOrders(lookup())
			if backend == "etcd" {
				reloaded := buildStore(ctx, info, testLogger()).(*metadata.EtcdStore)
				defer func() { _ = reloaded.Close() }()
				meta, err := reloaded.Metadata(ctx, []string{"orders"})
				if err != nil || len(meta.Topics) != 1 || len(meta.Topics[0].Partitions) != 6 {
					t.Fatalf("persisted metadata: %+v, %v", meta, err)
				}
			}
		})
	}
}
