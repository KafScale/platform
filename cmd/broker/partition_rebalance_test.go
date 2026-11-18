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
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

func rebalanceTestLeases(t *testing.T, endpoints []string, brokerID string) *metadata.PartitionLeaseManager {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return metadata.NewPartitionLeaseManager(cli, metadata.PartitionLeaseConfig{BrokerID: brokerID, LeaseTTLSeconds: 10, Logger: slog.Default()})
}

func rebalanceTestHandler(t *testing.T, endpoints []string, flushOnAck bool) *handler {
	t.Helper()
	meta := metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 0, Host: "broker-0", Port: 9092}, {NodeID: 1, Host: "broker-1", Port: 9092}},
		Topics: []protocol.MetadataTopic{{
			Topic:      kmsg.StringPtr("orders"),
			Partitions: []protocol.MetadataPartition{{Partition: 0, Leader: 0}, {Partition: 1, Leader: 1}, {Partition: 2, Leader: 0}, {Partition: 3, Leader: 1}},
		}},
	}
	h := &handler{
		store:        metadata.NewInMemoryStore(meta),
		leaseManager: rebalanceTestLeases(t, endpoints, "0"),
		brokerInfo:   protocol.MetadataBroker{NodeID: 0, Host: "broker-0", Port: 9092},
		flushOnAck:   flushOnAck,
		logger:       testLogger(),
	}
	for p := int32(0); p < 4; p++ {
		if err := h.leaseManager.Acquire(context.Background(), "orders", p); err != nil {
			t.Fatalf("acquire orders/%d: %v", p, err)
		}
	}
	return h
}

func TestRebalanceHandsPartitionsToListeningPreferredBroker(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	h := rebalanceTestHandler(t, endpoints, true)
	ctx := context.Background()
	var dialed []string
	listening := func(_ context.Context, addr string) bool {
		dialed = append(dialed, addr)
		return addr == "broker-1:9092"
	}
	if got := h.rebalancePartitions(ctx, listening, time.Minute); got != 2 {
		t.Fatalf("released = %d, want 2", got)
	}
	if len(dialed) != 1 {
		t.Fatalf("dialed %v, want one check of broker 1", dialed)
	}
	for _, p := range []int32{0, 2} {
		if !h.leaseManager.Owns("orders", p) {
			t.Fatalf("broker 0 gave up orders/%d it is preferred for", p)
		}
	}
	other := rebalanceTestLeases(t, endpoints, "1")
	for _, p := range []int32{1, 3} {
		if h.leaseManager.Owns("orders", p) {
			t.Fatalf("broker 0 still owns orders/%d", p)
		}
		if err := h.leaseManager.Acquire(ctx, "orders", p); !errors.Is(err, metadata.ErrNotOwner) {
			t.Fatalf("broker 0 took orders/%d back during the hold: %v", p, err)
		}
		if err := other.Acquire(ctx, "orders", p); err != nil {
			t.Fatalf("broker 1 acquire orders/%d: %v", p, err)
		}
	}
	if got := h.rebalancePartitions(ctx, listening, time.Minute); got != 0 {
		t.Fatalf("second pass released = %d, want 0", got)
	}
}

func TestRebalanceKeepsPartitionsWhenPreferredBrokerIsDown(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	h := rebalanceTestHandler(t, endpoints, true)
	if got := h.rebalancePartitions(context.Background(), func(context.Context, string) bool { return false }, time.Minute); got != 0 {
		t.Fatalf("released = %d, want 0", got)
	}
	for p := int32(0); p < 4; p++ {
		if !h.leaseManager.Owns("orders", p) {
			t.Fatalf("broker 0 lost orders/%d", p)
		}
	}
}

func TestRebalanceOffWithoutSyncFlush(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	h := rebalanceTestHandler(t, endpoints, false)
	if got := h.rebalancePartitions(context.Background(), func(context.Context, string) bool { return true }, time.Minute); got != 0 {
		t.Fatalf("released = %d with KAFSCALE_PRODUCE_SYNC_FLUSH=false, want 0", got)
	}
}
