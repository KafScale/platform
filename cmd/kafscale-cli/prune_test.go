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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type pruneFixture struct {
	ctx   context.Context
	store *metadata.EtcdStore
	s3    *storage.MemoryS3Client
}

func newPruneFixture(t *testing.T, topics ...string) *pruneFixture {
	t.Helper()
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()
	initial := metadata.ClusterMetadata{
		Brokers:      []protocol.MetadataBroker{{NodeID: 1, Host: "broker-0", Port: 9092}},
		ControllerID: 1,
	}
	for _, name := range topics {
		initial.Topics = append(initial.Topics, protocol.MetadataTopic{Topic: kmsg.StringPtr(name)})
	}
	store := newTestEtcdStore(t, ctx, initial, endpoints)
	return &pruneFixture{ctx: ctx, store: store, s3: storage.NewMemoryS3Client()}
}

func (f *pruneFixture) createTopic(t *testing.T, name string) {
	t.Helper()
	if _, err := f.store.CreateTopic(f.ctx, metadata.TopicSpec{Name: name, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
}

func (f *pruneFixture) retire(t *testing.T, name string) {
	t.Helper()
	if err := f.store.RetireTopic(f.ctx, name, "default"); err != nil {
		t.Fatal(err)
	}
}

func (f *pruneFixture) put(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if err := f.s3.UploadSegment(f.ctx, key, []byte("data")); err != nil {
			t.Fatal(err)
		}
	}
}

func (f *pruneFixture) exists(key string) bool {
	_, err := f.s3.DownloadSegment(f.ctx, key, nil)
	return err == nil
}

func (f *pruneFixture) records(t *testing.T) []string {
	t.Helper()
	resp, err := f.store.EtcdClient().Get(f.ctx, metadata.DeletedTopicPrefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
	}
	return keys
}

func (f *pruneFixture) run(t *testing.T, cfg pruneConfig) (string, error) {
	t.Helper()
	if cfg.Now.IsZero() {
		cfg.Now = time.Now()
	}
	var out bytes.Buffer
	err := executePrune(f.ctx, &out, cfg, f.s3, f.store)
	return out.String(), err
}

func siblingFixture(t *testing.T) *pruneFixture {
	f := newPruneFixture(t)
	f.createTopic(t, "orders")
	f.createTopic(t, "orders-x")
	f.retire(t, "orders")
	f.createTopic(t, "orders")
	f.retire(t, "orders")
	f.createTopic(t, "orders")
	f.put(t,
		"default/orders/0/segment-00000000000000000000.kfs",
		"default/orders@1/0/segment-00000000000000000000.kfs",
		"default/orders@1/0/segment-00000000000000000001.kfs",
		"default/orders@2/0/segment-00000000000000000000.kfs",
		"default/orders@10/0/segment-00000000000000000000.kfs",
		"default/orders-x/0/segment-00000000000000000000.kfs",
	)
	return f
}

func TestPruneDryRunDeletesNothing(t *testing.T) {
	f := siblingFixture(t)
	out, err := f.run(t, pruneConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"prune (dry run, nothing deleted)",
		"topic=orders generation=0 prefix=default/orders/ objects=1 bytes=4",
		"topic=orders generation=1 prefix=default/orders@1/ objects=2 bytes=8",
		"total incarnations=2 objects=3 bytes=12",
		"dry run: rerun with --force to delete",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
	if !f.exists("default/orders/0/segment-00000000000000000000.kfs") || !f.exists("default/orders@1/0/segment-00000000000000000001.kfs") || len(f.records(t)) != 2 {
		t.Fatal("dry run deleted data")
	}
}

func TestPruneForceDeletesOnlyDeletedPrefixes(t *testing.T) {
	f := siblingFixture(t)
	out, err := f.run(t, pruneConfig{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "prune (deleting)") || !strings.Contains(out, "pruned topic=orders generation=1") || strings.Contains(out, "dry run:") {
		t.Fatalf("unexpected output:\n%s", out)
	}
	for _, key := range []string{"default/orders/0/segment-00000000000000000000.kfs", "default/orders@1/0/segment-00000000000000000000.kfs", "default/orders@1/0/segment-00000000000000000001.kfs"} {
		if f.exists(key) {
			t.Fatalf("%s not deleted", key)
		}
	}
	for _, key := range []string{"default/orders@2/0/segment-00000000000000000000.kfs", "default/orders@10/0/segment-00000000000000000000.kfs", "default/orders-x/0/segment-00000000000000000000.kfs"} {
		if !f.exists(key) {
			t.Fatalf("%s was deleted", key)
		}
	}
	if got := f.records(t); len(got) != 0 {
		t.Fatalf("records left: %v", got)
	}
}

func (f *pruneFixture) putRecord(t *testing.T, record metadata.DeletedTopic) {
	t.Helper()
	body, _ := json.Marshal(record)
	if _, err := f.store.EtcdClient().Put(f.ctx, metadata.DeletedTopicKey(record.Name, record.Generation), string(body)); err != nil {
		t.Fatal(err)
	}
}

func TestPruneRefusesLiveData(t *testing.T) {
	f := newPruneFixture(t, "legacy@1")
	f.createTopic(t, "orders")
	f.retire(t, "orders")
	f.createTopic(t, "orders")
	f.put(t, "default/orders@1/0/segment-00000000000000000000.kfs", "default/legacy@1/0/segment-00000000000000000000.kfs")
	now := time.Now().UTC().Format(time.RFC3339)
	f.putRecord(t, metadata.DeletedTopic{Name: "orders", Generation: 1, Prefix: "default/orders@1/", DeletedAt: now})
	f.putRecord(t, metadata.DeletedTopic{Name: "legacy", Generation: 1, Prefix: "default/legacy@1/", DeletedAt: now})
	if _, err := f.store.EtcdClient().Put(f.ctx, metadata.TopicGenerationKey("legacy"), "2"); err != nil {
		t.Fatal(err)
	}
	out, err := f.run(t, pruneConfig{Force: true, Topic: "orders"})
	if err == nil || !strings.Contains(err.Error(), "current generation") {
		t.Fatalf("expected current generation refusal, got %v\n%s", err, out)
	}
	out, err = f.run(t, pruneConfig{Force: true, Topic: "legacy"})
	if err == nil || !strings.Contains(err.Error(), "live topic legacy@1") {
		t.Fatalf("expected live topic refusal, got %v\n%s", err, out)
	}
	if !f.exists("default/orders@1/0/segment-00000000000000000000.kfs") || !f.exists("default/legacy@1/0/segment-00000000000000000000.kfs") || len(f.records(t)) != 2 {
		t.Fatalf("live data was deleted: %v %v %v", f.exists("default/orders@1/0/segment-00000000000000000000.kfs"), f.exists("default/legacy@1/0/segment-00000000000000000000.kfs"), f.records(t))
	}
}

func TestPruneRejectsMismatchedPrefix(t *testing.T) {
	f := newPruneFixture(t)
	for _, prefix := range []string{"default/orders@1", "default/orders@10/", "default/orders/", "/orders@1/", "a/b/orders@1/"} {
		f.putRecord(t, metadata.DeletedTopic{Name: "orders", Generation: 1, Prefix: prefix, DeletedAt: "2026-01-01T00:00:00Z"})
		if _, err := f.run(t, pruneConfig{}); err == nil || !strings.Contains(err.Error(), "does not match") {
			t.Fatalf("prefix %q: expected rejection, got %v", prefix, err)
		}
	}
}

func TestPruneFilters(t *testing.T) {
	f := newPruneFixture(t)
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	f.putRecord(t, metadata.DeletedTopic{Name: "orders", Generation: 0, Prefix: "default/orders/", DeletedAt: now.Add(-10 * 24 * time.Hour).Format(time.RFC3339)})
	f.putRecord(t, metadata.DeletedTopic{Name: "orders", Generation: 1, Prefix: "default/orders@1/", DeletedAt: now.Add(-time.Hour).Format(time.RFC3339)})
	f.putRecord(t, metadata.DeletedTopic{Name: "orders-x", Generation: 0, Prefix: "default/orders-x/", DeletedAt: now.Add(-10 * 24 * time.Hour).Format(time.RFC3339)})
	out, err := f.run(t, pruneConfig{Topic: "orders", Now: now})
	if err != nil || !strings.Contains(out, "total incarnations=2") || strings.Contains(out, "orders-x") {
		t.Fatalf("topic filter: %v\n%s", err, out)
	}
	out, err = f.run(t, pruneConfig{OlderThan: 7 * 24 * time.Hour, Now: now})
	if err != nil || !strings.Contains(out, "total incarnations=2") || strings.Contains(out, "generation=1") {
		t.Fatalf("older-than filter: %v\n%s", err, out)
	}
	out, err = f.run(t, pruneConfig{Topic: "orders", OlderThan: 7 * 24 * time.Hour, Now: now})
	if err != nil || !strings.Contains(out, "total incarnations=1") {
		t.Fatalf("both filters: %v\n%s", err, out)
	}
	f.putRecord(t, metadata.DeletedTopic{Name: "bad", Generation: 0, Prefix: "default/bad/", DeletedAt: "yesterday"})
	if _, err := f.run(t, pruneConfig{OlderThan: time.Hour, Now: now}); err == nil {
		t.Fatal("expected deleted_at parse error")
	}
}

type flakyDeleteS3 struct {
	*storage.MemoryS3Client
	failKey string
}

func (f *flakyDeleteS3) DeleteSegment(ctx context.Context, key string) error {
	if key == f.failKey {
		return errors.New("boom")
	}
	return f.MemoryS3Client.DeleteSegment(ctx, key)
}

func TestPrunePartialFailureThenRerun(t *testing.T) {
	f := newPruneFixture(t)
	f.createTopic(t, "orders")
	f.retire(t, "orders")
	f.put(t, "default/orders/0/segment-00000000000000000000.kfs", "default/orders/0/segment-00000000000000000001.kfs")
	if err := f.s3.UploadIndex(f.ctx, "default/orders/0/segment-00000000000000000000.index", []byte("i")); err != nil {
		t.Fatal(err)
	}
	flaky := &flakyDeleteS3{MemoryS3Client: f.s3, failKey: "default/orders/0/segment-00000000000000000001.kfs"}
	var out bytes.Buffer
	err := executePrune(f.ctx, &out, pruneConfig{Force: true, Now: time.Now()}, flaky, f.store)
	if err == nil || !strings.Contains(err.Error(), "rerun to finish") {
		t.Fatalf("expected partial failure, got %v", err)
	}
	if !f.exists("default/orders/0/segment-00000000000000000001.kfs") || len(f.records(t)) != 1 {
		t.Fatal("record removed before S3 delete finished")
	}
	got, err := f.run(t, pruneConfig{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "pruned topic=orders generation=0") || f.exists("default/orders/0/segment-00000000000000000001.kfs") || len(f.records(t)) != 0 {
		t.Fatalf("rerun did not finish:\n%s", got)
	}
	if got, err := f.run(t, pruneConfig{Force: true}); err != nil || !strings.Contains(got, "total incarnations=0") {
		t.Fatalf("second rerun: %v\n%s", err, got)
	}
}

func TestPruneListError(t *testing.T) {
	f := newPruneFixture(t)
	f.putRecord(t, metadata.DeletedTopic{Name: "orders", Generation: 0, Prefix: "default/orders/", DeletedAt: "2026-01-01T00:00:00Z"})
	var out bytes.Buffer
	failing := &failingListS3{MemoryS3Client: f.s3, err: errors.New("list down")}
	if err := executePrune(f.ctx, &out, pruneConfig{}, failing, f.store); err == nil || !strings.Contains(err.Error(), "list down") || !strings.Contains(err.Error(), "rerun to finish") {
		t.Fatalf("expected list error, got %v", err)
	}
	if _, err := f.store.EtcdClient().Put(f.ctx, metadata.DeletedTopicKey("orders", 0), "{"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.run(t, pruneConfig{}); err == nil {
		t.Fatal("expected decode error")
	}
	if _, err := f.store.EtcdClient().Put(f.ctx, metadata.DeletedTopicKey("orders", 0), `{"name":"other","generation":0}`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.run(t, pruneConfig{}); err == nil || !strings.Contains(err.Error(), "does not match its key") {
		t.Fatalf("expected key mismatch, got %v", err)
	}
}

func TestParseAge(t *testing.T) {
	cases := map[string]time.Duration{"": 0, "7d": 7 * 24 * time.Hour, "24h": 24 * time.Hour, " 90m ": 90 * time.Minute}
	for raw, want := range cases {
		got, err := parseAge(raw)
		if err != nil || got != want {
			t.Fatalf("parseAge(%q) = %v, %v", raw, got, err)
		}
	}
	for _, raw := range []string{"xd", "-1d", "-1h", "soon"} {
		if _, err := parseAge(raw); err == nil {
			t.Fatalf("parseAge(%q) accepted", raw)
		}
	}
}

func TestRunPruneCommand(t *testing.T) {
	f := newPruneFixture(t)
	f.createTopic(t, "orders")
	f.retire(t, "orders")
	f.put(t, "default/orders/0/segment-00000000000000000000.kfs")
	prevStore, prevS3 := newEtcdStore, newMemoryS3
	t.Cleanup(func() { newEtcdStore, newMemoryS3 = prevStore, prevS3 })
	t.Setenv("KAFSCALE_USE_MEMORY_S3", "true")
	newMemoryS3 = func() storage.S3Client { return f.s3 }
	newEtcdStore = func(ctx context.Context, snapshot metadata.ClusterMetadata, cfg metadata.EtcdStoreConfig) (*metadata.EtcdStore, error) {
		return metadata.NewEtcdStore(ctx, snapshot, metadata.EtcdStoreConfig{Endpoints: f.store.EtcdClient().Endpoints()})
	}
	var stdout, stderr bytes.Buffer
	if err := run(f.ctx, []string{"prune", "--topic", "orders", "--older-than", "0d"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "dry run") || !f.exists("default/orders/0/segment-00000000000000000000.kfs") {
		t.Fatalf("dry run output:\n%s", stdout.String())
	}
	stdout.Reset()
	if err := run(f.ctx, []string{"prune", "--force"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if f.exists("default/orders/0/segment-00000000000000000000.kfs") || len(f.records(t)) != 0 {
		t.Fatalf("force did not prune:\n%s", stdout.String())
	}
	if err := run(f.ctx, []string{"prune", "--older-than", "later"}, &stdout, &stderr); err == nil {
		t.Fatal("expected --older-than error")
	}
	if err := run(f.ctx, []string{"prune", "--bogus"}, &stdout, &stderr); err == nil {
		t.Fatal("expected flag error")
	}
	newEtcdStore = func(context.Context, metadata.ClusterMetadata, metadata.EtcdStoreConfig) (*metadata.EtcdStore, error) {
		return nil, errors.New("etcd down")
	}
	if err := run(f.ctx, []string{"prune"}, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "etcd down") {
		t.Fatalf("expected etcd error, got %v", err)
	}
}

func TestPruneSkipsLFSAndUnknownKeys(t *testing.T) {
	f := newPruneFixture(t)
	f.createTopic(t, "orders")
	f.retire(t, "orders")
	f.createTopic(t, "orders")
	f.retire(t, "orders")
	f.createTopic(t, "orders")
	f.put(t,
		"default/orders/0/segment-00000000000000000000.kfs",
		"default/orders/lfs/2026/10/30/obj-live-gen2",
		"default/orders/0/notes.txt",
		"default/orders/0/segment-1.kfs",
		"default/orders/x/segment-00000000000000000000.kfs",
	)
	out, err := f.run(t, pruneConfig{Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "topic=orders generation=0 prefix=default/orders/ objects=1 bytes=4") {
		t.Fatalf("dry run counted non segment keys:\n%s", out)
	}
	if _, err := f.run(t, pruneConfig{Topic: "orders", Force: true}); err != nil {
		t.Fatal(err)
	}
	if f.exists("default/orders/0/segment-00000000000000000000.kfs") {
		t.Fatal("segment not deleted")
	}
	for _, key := range []string{"default/orders/lfs/2026/10/30/obj-live-gen2", "default/orders/0/notes.txt", "default/orders/0/segment-1.kfs", "default/orders/x/segment-00000000000000000000.kfs"} {
		if !f.exists(key) {
			t.Fatalf("%s was deleted", key)
		}
	}
}

type prefixListFailS3 struct {
	*storage.MemoryS3Client
	failPrefix string
}

func (f *prefixListFailS3) ListSegments(ctx context.Context, prefix string) ([]storage.S3Object, error) {
	if prefix == f.failPrefix {
		return nil, errors.New("list down")
	}
	return f.MemoryS3Client.ListSegments(ctx, prefix)
}

func TestPruneListFailureContinues(t *testing.T) {
	f := newPruneFixture(t)
	f.createTopic(t, "orders")
	f.createTopic(t, "payments")
	f.retire(t, "orders")
	f.retire(t, "payments")
	f.put(t, "default/orders/0/segment-00000000000000000000.kfs", "default/payments/0/segment-00000000000000000000.kfs")
	flaky := &prefixListFailS3{MemoryS3Client: f.s3, failPrefix: "default/orders/"}
	var out bytes.Buffer
	err := executePrune(f.ctx, &out, pruneConfig{Force: true, Now: time.Now()}, flaky, f.store)
	if err == nil || !strings.Contains(err.Error(), "rerun to finish") || !strings.Contains(err.Error(), "list down") {
		t.Fatalf("expected rerun error, got %v", err)
	}
	if f.exists("default/payments/0/segment-00000000000000000000.kfs") || !f.exists("default/orders/0/segment-00000000000000000000.kfs") {
		t.Fatalf("expected payments pruned and orders kept:\n%s", out.String())
	}
	if got, err := f.run(t, pruneConfig{Force: true}); err != nil || f.exists("default/orders/0/segment-00000000000000000000.kfs") {
		t.Fatalf("rerun: %v\n%s", err, got)
	}
}

type checkedS3 struct {
	*storage.MemoryS3Client
	err error
}

func (c *checkedS3) CheckBucket(context.Context) error { return c.err }

func TestPruneFailsWhenBucketCheckFails(t *testing.T) {
	f := newPruneFixture(t)
	var out bytes.Buffer
	err := executePrune(f.ctx, &out, pruneConfig{}, &checkedS3{MemoryS3Client: f.s3, err: errors.New("bucket b does not exist")}, f.store)
	if err == nil || !strings.Contains(err.Error(), "does not exist") || out.Len() != 0 {
		t.Fatalf("expected bucket error before any output, got %v\n%s", err, out.String())
	}
	if err := executePrune(f.ctx, &out, pruneConfig{}, &checkedS3{MemoryS3Client: f.s3}, f.store); err != nil {
		t.Fatal(err)
	}
}
