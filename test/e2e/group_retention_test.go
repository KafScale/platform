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

//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
)

func TestConsumerGroupRetentionE2E(t *testing.T) {
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
	old := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	for _, id := range []string{"retention-expired", "retention-live", "retention-offset-only"} {
		offset, _ := json.Marshal(map[string]any{"offset": 42, "metadata": "checkpoint", "committed_at": old})
		if _, err := store.EtcdClient().Put(ctx, fmt.Sprintf("%s/%s/offsets/orders/0", metadata.ConsumerGroupPrefix(), id), string(offset)); err != nil {
			t.Fatal(err)
		}
		if id == "retention-offset-only" {
			continue
		}
		group := &metadatapb.ConsumerGroup{GroupId: id, State: "empty", EmptySince: old}
		if id == "retention-live" {
			group.State = "stable"
			group.Members = map[string]*metadatapb.GroupMember{"live": {HeartbeatAt: time.Now().UTC().Format(time.RFC3339Nano), SessionTimeoutMs: 30000}}
		}
		if err := store.PutConsumerGroup(ctx, group); err != nil {
			t.Fatal(err)
		}
	}
	addr := freeAddr(t)
	command, logs := startBrokerWithEtcd(t, ctx, addr, freeAddr(t), freeAddr(t), endpoints, "KAFSCALE_OFFSETS_RETENTION_MINUTES=1")
	defer stopBroker(t, command)
	waitForBroker(t, logs, addr)
	for deadline := time.Now().Add(12 * time.Second); ; {
		expired, err := store.FetchConsumerGroup(ctx, "retention-expired")
		if err != nil {
			t.Fatal(err)
		}
		legacy, legacyErr := store.FetchConsumerGroup(ctx, "retention-offset-only")
		if legacyErr != nil {
			t.Fatal(legacyErr)
		}
		if expired == nil && legacy != nil && legacy.EmptySince != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expired group was not removed by broker sweep: %+v\n%s", expired, logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if offsets, err := store.ListGroupConsumerOffsets(ctx, "retention-expired"); err != nil || len(offsets) != 0 {
		t.Fatalf("expired offsets survived sweep: %+v, %v", offsets, err)
	}
	for _, id := range []string{"retention-live", "retention-offset-only"} {
		group, err := store.FetchConsumerGroup(ctx, id)
		if err != nil || group == nil {
			t.Fatalf("retention removed protected %s: %+v, %v", id, group, err)
		}
		if id == "retention-live" && len(group.Members) != 1 {
			t.Fatal("active member expired")
		}
		if id == "retention-offset-only" && group.EmptySince == "" {
			t.Fatal("legacy first-seen clock was not persisted")
		}
		if offset, _, err := store.FetchConsumerOffset(ctx, id, "orders", 0); err != nil || offset != 42 {
			t.Fatalf("protected %s lost offset: %d, %v", id, offset, err)
		}
	}
}
