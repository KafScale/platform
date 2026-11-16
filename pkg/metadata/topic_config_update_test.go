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

package metadata

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/encoding/protowire"
)

// Catch both the legacy config PUT and the snapshot transaction used by a
// revision-aware writer, immediately before either can publish its config.
type topicConfigWriteHookKV struct{ *snapshotHookKV }

func (k topicConfigWriteHookKV) Put(ctx context.Context, key, value string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	if key == TopicConfigKey("existing") {
		k.once.Do(k.hook)
	}
	return k.KV.Put(ctx, key, value, opts...)
}

func TestDelayedTopicConfigWriteCannotUndoExpansion(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "create", true: "update"}[existing], func(t *testing.T) {
			ctx := context.Background()
			endpoints := testutil.StartEmbeddedEtcd(t)
			expander := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
			writeSnapshotFixture(t, expander.client, snapshotFixture())
			cfg, err := expander.FetchTopicConfig(ctx, "existing")
			if err != nil {
				t.Fatal(err)
			}
			if existing {
				if err := expander.UpdateTopicConfig(ctx, cfg); err != nil {
					t.Fatal(err)
				}
			}
			cfg.RetentionMs = 60000
			cfg.ReplicationFactor = 2
			cfg.Config["cleanup.policy"] = "compact"
			writer := snapshotMutationStore(t, expander, topicConfigWriteHookKV{&snapshotHookKV{KV: expander.client, hook: func() {
				if err := expander.CreatePartitions(ctx, "existing", 8); err != nil {
					t.Fatal(err)
				}
			}}})
			if err := writer.UpdateTopicConfig(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			got, err := expander.FetchTopicConfig(ctx, "existing")
			if err != nil || got.Partitions != 8 || got.RetentionMs != 60000 || got.ReplicationFactor != 2 || got.Config["cleanup.policy"] != "compact" {
				t.Fatalf("delayed config write undid expansion or lost settings: %+v, %v", got, err)
			}
		})
	}
}

func TestMemoryTopicConfigUpdatePreservesExpandedCount(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore(snapshotFixture())
	cfg, err := store.FetchTopicConfig(ctx, "existing")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreatePartitions(ctx, "existing", 8); err != nil {
		t.Fatal(err)
	}
	cfg.RetentionMs = 60000
	if err := store.UpdateTopicConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := store.FetchTopicConfig(ctx, "existing")
	if err != nil || got.Partitions != 8 || got.RetentionMs != 60000 || cfg.Partitions != 4 {
		t.Fatalf("stale input changed topology or was mutated: config=%+v input=%+v err=%v", got, cfg, err)
	}
}

func TestTopicConfigUpdateFencesRecreatedTopic(t *testing.T) {
	ctx := context.Background()
	endpoints := testutil.StartEmbeddedEtcd(t)
	base := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
	writeSnapshotFixture(t, base.client, snapshotFixture())
	cfg, err := base.FetchTopicConfig(ctx, "existing")
	if err != nil {
		t.Fatal(err)
	}
	cfg.RetentionMs = 60000
	writer := snapshotMutationStore(t, base, topicConfigWriteHookKV{&snapshotHookKV{KV: base.client, hook: func() {
		if err := base.RetireTopic(ctx, "existing", "default"); err != nil {
			t.Fatal(err)
		}
		if _, err := base.CreateTopic(ctx, TopicSpec{Name: "existing", NumPartitions: 2, ReplicationFactor: 1}); err != nil {
			t.Fatal(err)
		}
	}}})
	if err := writer.UpdateTopicConfig(ctx, cfg); !errors.Is(err, ErrTopicGenerationChanged) {
		t.Fatalf("stale update crossed topic generations: %v", err)
	}
	got, err := base.FetchTopicConfig(ctx, "existing")
	if err != nil || got.Partitions != 2 || got.RetentionMs != -1 {
		t.Fatalf("recreated topic was modified: %+v, %v", got, err)
	}
}

type cancelAfterGenerationReadKV struct {
	clientv3.KV
	cancel context.CancelFunc
}

func (k cancelAfterGenerationReadKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	result, err := k.KV.Get(ctx, key, opts...)
	if err == nil && key == TopicGenerationKey("existing") {
		k.cancel()
	}
	return result, err
}

func TestTopicConfigUpdateFailureDoesNotPublish(t *testing.T) {
	for _, phase := range []string{"read", "generation", "cancel", "encode", "commit", "unknown-topic"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			endpoints := testutil.StartEmbeddedEtcd(t)
			base := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
			writeSnapshotFixture(t, base.client, snapshotFixture())
			initial, err := base.FetchTopicConfig(ctx, "existing")
			if err != nil {
				t.Fatal(err)
			}
			initial.RetentionMs = 50000
			if err := base.UpdateTopicConfig(ctx, initial); err != nil {
				t.Fatal(err)
			}
			cfg := cloneTopicConfig(initial)
			cfg.RetentionMs = 60000
			opCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			var kv clientv3.KV = base.client
			switch phase {
			case "read":
				kv = snapshotFailKV{KV: kv, failRead: true}
			case "generation":
				if _, err := base.client.Put(ctx, TopicGenerationKey("existing"), "invalid generation"); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				kv = cancelAfterGenerationReadKV{KV: kv, cancel: cancel}
			case "encode":
				cfg.CreatedAt = string([]byte{0xff})
			case "commit":
				kv = snapshotFailKV{KV: kv, failWrite: true}
			case "unknown-topic":
				cfg.Name = "missing"
			}
			writer := snapshotMutationStore(t, base, kv)
			if err := writer.metadata.UpdateTopicConfig(ctx, initial); err != nil {
				t.Fatal(err)
			}
			if err := writer.UpdateTopicConfig(opCtx, cfg); err == nil {
				t.Fatal("expected config update failure")
			}
			stored, err := base.FetchTopicConfig(ctx, "existing")
			if err != nil || stored.RetentionMs != 50000 {
				t.Fatalf("failed update reached etcd: %+v, %v", stored, err)
			}
			cached, err := writer.metadata.FetchTopicConfig(ctx, "existing")
			if err != nil || cached.RetentionMs != 50000 {
				t.Fatalf("failed update reached local cache: %+v, %v", cached, err)
			}
		})
	}
}

func TestEtcdTopicConfigUpdateRejectsInvalidInput(t *testing.T) {
	store := &EtcdStore{}
	for _, cfg := range []*metadatapb.TopicConfig{nil, {}} {
		if err := store.UpdateTopicConfig(context.Background(), cfg); !errors.Is(err, ErrInvalidTopic) {
			t.Fatalf("invalid config accepted: %v", err)
		}
	}
}

func TestTopicConfigUpdatePreservesUnknownFields(t *testing.T) {
	ctx := context.Background()
	endpoints := testutil.StartEmbeddedEtcd(t)
	store := newTestEtcdStore(t, ctx, snapshotFixture(), endpoints)
	writeSnapshotFixture(t, store.client, snapshotFixture())
	cfg, err := store.FetchTopicConfig(ctx, "existing")
	if err != nil {
		t.Fatal(err)
	}
	future := protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 7)
	cfg.ProtoReflect().SetUnknown(future)
	if err := store.UpdateTopicConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	stored, err := store.FetchTopicConfig(ctx, "existing")
	if err != nil || !bytes.Equal(stored.ProtoReflect().GetUnknown(), future) {
		t.Fatalf("config update dropped unknown protobuf fields: %+v, %v", stored, err)
	}
	cached, err := store.metadata.FetchTopicConfig(ctx, "existing")
	if err != nil || !bytes.Equal(cached.ProtoReflect().GetUnknown(), future) {
		t.Fatalf("config cache dropped unknown protobuf fields: %+v, %v", cached, err)
	}
}
