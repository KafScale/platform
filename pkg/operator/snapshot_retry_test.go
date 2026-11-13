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

package operator

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type snapshotRetryKV struct {
	clientv3.KV
	beforeCommit func()
}

func (k snapshotRetryKV) Txn(ctx context.Context) clientv3.Txn {
	return &snapshotRetryTxn{Txn: k.KV.Txn(ctx), beforeCommit: k.beforeCommit}
}

type snapshotRetryTxn struct {
	clientv3.Txn
	beforeCommit func()
}

func (t *snapshotRetryTxn) If(cmps ...clientv3.Cmp) clientv3.Txn { t.Txn = t.Txn.If(cmps...); return t }
func (t *snapshotRetryTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.Txn = t.Txn.Then(ops...)
	return t
}
func (t *snapshotRetryTxn) Commit() (*clientv3.TxnResponse, error) {
	t.beforeCommit()
	return t.Txn.Commit()
}

func TestPublishSnapshotRetryDoesNotResurrectDeletedTopic(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	desired := metadata.ClusterMetadata{Brokers: []protocol.MetadataBroker{{NodeID: 0, Host: "broker-0", Port: 9092}}, Topics: []protocol.MetadataTopic{{Topic: kmsg.StringPtr("managed"), Partitions: []protocol.MetadataPartition{{Partition: 0}}}}}
	initial := desired
	initial.Topics = append(append([]protocol.MetadataTopic(nil), desired.Topics...), protocol.MetadataTopic{Topic: kmsg.StringPtr("auto"), Partitions: []protocol.MetadataPartition{{Partition: 0}}})
	if err := PublishMetadataSnapshot(ctx, endpoints, initial); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var once sync.Once
	factory := func(cfg clientv3.Config) (*clientv3.Client, error) {
		cli, err := clientv3.New(cfg)
		if err != nil {
			return nil, err
		}
		cli.KV = snapshotRetryKV{KV: cli.KV, beforeCommit: func() {
			once.Do(func() {
				if err := store.DeleteTopic(ctx, "auto"); err != nil {
					t.Error(err)
				}
			})
		}}
		return cli, nil
	}
	if err := publishMetadataSnapshot(ctx, endpoints, desired, factory); err != nil {
		t.Fatal(err)
	}
	resp, err := store.EtcdClient().Get(ctx, "/kafscale/metadata/snapshot")
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatal(err)
	}
	var saved metadata.ClusterMetadata
	if err := json.Unmarshal(resp.Kvs[0].Value, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.Topics) != 1 || *saved.Topics[0].Topic != "managed" {
		t.Fatalf("CAS retry resurrected a retired topic: %+v", saved.Topics)
	}
}

func TestPublishSnapshotPreservesUnreadableSnapshot(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if _, err := cli.Put(ctx, "/kafscale/metadata/snapshot", "broken"); err != nil {
		t.Fatal(err)
	}
	if err := PublishMetadataSnapshot(ctx, endpoints, metadata.ClusterMetadata{}); err == nil {
		t.Fatal("unreadable snapshot was replaced")
	}
	resp, err := cli.Get(ctx, "/kafscale/metadata/snapshot")
	if err != nil || len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "broken" {
		t.Fatal("failed read erased snapshot", err)
	}
}
