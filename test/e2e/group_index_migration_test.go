// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestConsumerGroupIndexMigrationE2E(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E") != "1" {
		t.Skip("set KAFSCALE_E2E=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	etcd, endpoints := startEmbeddedEtcd(t)
	defer etcd.Close()
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cli := store.EtcdClient()
	old := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	offset, _ := json.Marshal(map[string]any{"offset": 42, "metadata": "checkpoint", "committed_at": old})
	groups := map[string]*metadatapb.ConsumerGroup{
		"migrate-expired":     {GroupId: "migrate-expired", State: "empty", EmptySince: old},
		"migrate-live":        {GroupId: "migrate-live", State: "stable", Members: map[string]*metadatapb.GroupMember{"live": {HeartbeatAt: time.Now().UTC().Format(time.RFC3339Nano), SessionTimeoutMs: 600000}}},
		"migrate-offset-only": nil,
	}
	for id, group := range groups {
		for p := int32(0); p < 3; p++ {
			if _, err := cli.Put(ctx, metadata.ConsumerOffsetKey(id, "orders", p), string(offset)); err != nil {
				t.Fatal(err)
			}
		}
		if group == nil {
			continue
		}
		encoded, err := metadata.EncodeConsumerGroup(group)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cli.Put(ctx, metadata.ConsumerGroupKey(id), string(encoded)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cli.Put(ctx, metadata.ConsumerGroupIndexKey("migrate-deleted-by-old-broker"), ""); err != nil {
		t.Fatal(err)
	}
	index := func() string {
		resp, err := cli.Get(ctx, metadata.ConsumerGroupIndexKey(""), clientv3.WithPrefix(), clientv3.WithKeysOnly())
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]string, 0, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			ids = append(ids, strings.TrimPrefix(string(kv.Key), metadata.ConsumerGroupIndexKey("")))
		}
		return strings.Join(ids, ",")
	}

	addr := freeAddr(t)
	command, logs := startBrokerWithEtcd(t, ctx, addr, freeAddr(t), freeAddr(t), endpoints, "KAFSCALE_OFFSETS_RETENTION_MINUTES=1")
	defer stopBroker(t, command)
	waitForBroker(t, logs, addr)
	for deadline := time.Now().Add(15 * time.Second); index() != "migrate-live,migrate-offset-only"; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("index after broker sweep: %q\n%s", index(), logs.String())
		}
	}
	if group, err := store.FetchConsumerGroup(ctx, "migrate-expired"); err != nil || group != nil {
		t.Fatalf("expired group survived: %+v, %v", group, err)
	}
	if offsets, err := store.ListGroupConsumerOffsets(ctx, "migrate-expired"); err != nil || len(offsets) != 0 {
		t.Fatalf("expired offsets survived: %d, %v", len(offsets), err)
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	list := kmsg.NewPtrListGroupsRequest()
	list.Version = 5
	listed, err := list.RequestWith(ctx, client)
	if err != nil || listed.ErrorCode != 0 {
		t.Fatalf("list groups: %+v, %v", listed, err)
	}
	var names []string
	for _, group := range listed.Groups {
		names = append(names, group.Group)
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "migrate-live,migrate-offset-only" {
		t.Fatalf("listed groups: %v", names)
	}
	for _, id := range []string{"migrate-live", "migrate-offset-only"} {
		fetch := kmsg.NewPtrOffsetFetchRequest()
		fetch.Version, fetch.Group = 5, id
		topic := kmsg.NewOffsetFetchRequestTopic()
		topic.Topic, topic.Partitions = "orders", []int32{0, 1, 2}
		fetch.Topics = append(fetch.Topics, topic)
		fetched, err := fetch.RequestWith(ctx, client)
		if err != nil || fetched.ErrorCode != 0 || len(fetched.Topics) != 1 || len(fetched.Topics[0].Partitions) != 3 {
			t.Fatalf("offset fetch %s: %+v, %v", id, fetched, err)
		}
		for _, p := range fetched.Topics[0].Partitions {
			if p.ErrorCode != 0 || p.Offset != 42 {
				t.Fatalf("offset fetch %s partition %d: offset %d error %d", id, p.Partition, p.Offset, p.ErrorCode)
			}
		}
	}
}
