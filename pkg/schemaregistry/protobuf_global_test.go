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

package schemaregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/KafScale/platform/pkg/storage"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProtoGlobalReusePreservesStoredIdentity(t *testing.T) {
	for _, offload := range []bool{false, true} {
		t.Run(fmt.Sprint(offload), func(t *testing.T) {
			ctx := context.Background()
			cfg := Config{}
			s3 := storage.NewMemoryS3Client()
			if offload {
				cfg.S3 = s3
				cfg.Namespace = "test"
				cfg.InlineLimit = 1
			}
			h := newHarness(t, nil, cfg)
			text := protoHead + "message N {} message M { .a.N child = 1; }"
			descriptor := protoDescriptorBase64(t, text, nil, nil)
			id := h.register("first", protoSchemaBody(text))
			before, err := h.cli.Get(ctx, idKey(id))
			if err != nil {
				t.Fatal(err)
			}
			var marker []byte
			if offload {
				marker, err = s3.DownloadIndex(ctx, h.reg.idMarkerKey(id))
				if err != nil {
					t.Fatal(err)
				}
			}
			// Restart against records written without a descriptor alias, as on upgrade.
			restarted := newHarness(t, h.cli.Endpoints(), cfg)
			if got := restarted.register("second", protoSchemaBody(descriptor)); got != id {
				t.Fatalf("restart reused %d, want %d", got, id)
			}
			after, err := h.cli.Get(ctx, idKey(id))
			if err != nil || !bytes.Equal(before.Kvs[0].Value, after.Kvs[0].Value) {
				t.Fatal("stored identity changed", err)
			}
			p, err := h.reg.prepare(ctx, RegisterRequest{SchemaType: schemaTypeProtobuf, Schema: descriptor})
			if err != nil {
				t.Fatal(err)
			}
			alias, err := h.cli.Get(ctx, p.protoAliasKey())
			if err != nil || len(alias.Kvs) != 1 || string(alias.Kvs[0].Value) != fmt.Sprint(id) {
				t.Fatalf("hash alias missing: %v %v", alias, err)
			}
			if got := restarted.register("third", protoSchemaBody(descriptor)); got != id {
				t.Fatal("alias did not reuse id")
			}
			counter, err := h.cli.Get(ctx, counterKey())
			if err != nil || string(counter.Kvs[0].Value) != fmt.Sprint(id) {
				t.Fatal("id counter changed", err)
			}
			if offload {
				after, err := s3.DownloadIndex(ctx, h.reg.idMarkerKey(id))
				if err != nil || !bytes.Equal(marker, after) {
					t.Fatal("S3 marker identity changed", err)
				}
			}
		})
	}
}

func TestProtoGlobalReusePreservesRawTextAndReferenceBoundaries(t *testing.T) {
	h := newHarness(t, nil, Config{})
	text := protoHead + "message N {} message M { .a.N child = 1; }"
	qualified := strings.Replace(text, ".a.N child", "N child", 1)
	first := h.register("raw-a", protoSchemaBody(text))
	descriptor := protoDescriptorBase64(t, text, nil, nil)
	if got := h.register("bridge", protoSchemaBody(descriptor)); got != first {
		t.Fatal("descriptor bridge failed")
	}
	printed, _, err := protoFromDescriptor(descriptor, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := h.register("printed-text", protoSchemaBody(printed)); got == first {
		t.Fatal("descriptor alias changed text-only id assignment")
	}

	if got := h.register("raw-b", protoSchemaBody(qualified)); got == first {
		t.Fatal("text-only schemas were normalized without normalize=true")
	}
	changed := protoDescriptorBase64(t, protoHead+"message N {} message M { N child = 2; }", nil, nil)
	if got := h.register("different", protoSchemaBody(changed)); got == first {
		t.Fatal("different descriptor reused id")
	}
	common := protoHead + "message Common { string value = 1; }"
	h.register("ref-a", protoSchemaBody(common))
	h.register("ref-b", protoSchemaBody(common))
	parent := protoHead + `import "common.proto"; message Parent { Common common = 1; }`
	a := Reference{Name: "common.proto", Subject: "ref-a", Version: 1}
	b := Reference{Name: "common.proto", Subject: "ref-b", Version: 1}
	desc := protoDescriptorBase64(t, parent, []resolvedReference{{Name: a.Name, Text: common}}, nil)
	original := h.register("parent-a", protoSchemaBody(parent, a))
	if got := h.register("parent-b", protoSchemaBody(desc, b)); got == original {
		t.Fatal("different reference subjects reused an id")
	}
}

func TestProtoGlobalMatchPagesAndPreservesFirstImportedID(t *testing.T) {
	h := newHarness(t, nil, Config{})
	ctx := context.Background()
	// Membership keys also share the ids/ prefix. Force multiple pages without
	// creating additional schemas or reading schema text from object storage.
	ops := make([]clientv3.Op, 70)
	for i := range ops {
		ops[i] = clientv3.OpPut(idSubjectsPrefix(1)+fmt.Sprint(i), "")
	}
	if _, err := h.cli.Txn(ctx).Then(ops...).Commit(); err != nil {
		t.Fatal(err)
	}
	text := protoHead + "message N {} message M { .a.N child = 1; }"
	h.ok("PUT", "/mode", map[string]string{"mode": "IMPORT"}, nil)
	for _, id := range []int64{90, 12} {
		body := protoSchemaBody(text)
		body["id"], body["version"] = id, 1
		if got := h.register(fmt.Sprint(id), body); got != id {
			t.Fatal(got)
		}
	}
	h.ok("PUT", "/mode", map[string]string{"mode": "READWRITE"}, nil)
	// Raw hash lookup already returns the first imported ID. The descriptor
	// fallback must use creation order as well, rather than key or numeric order.
	descriptor := protoDescriptorBase64(t, text, nil, nil)
	if got := h.register("descriptor", protoSchemaBody(descriptor)); got != 90 {
		t.Fatalf("reused %d, want first imported id 90", got)
	}
	// Include an Avro record and malformed id metadata in error-path coverage.
	h.register("avro", schema(userV1))
	if _, err := h.cli.Put(ctx, idKey(999), "broken"); err != nil {
		t.Fatal(err)
	}
	p, err := h.reg.prepare(ctx, RegisterRequest{SchemaType: schemaTypeProtobuf, Schema: descriptor})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.reg.protoGlobalMatch(ctx, p); err == nil {
		t.Fatal("corrupt schema metadata ignored")
	}
	if _, err := h.cli.Delete(ctx, idKey(999)); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := h.reg.protoGlobalMatch(canceled, p); err == nil {
		t.Fatal("canceled lookup accepted")
	}
}

func TestProtoConcurrentCrossSubjectRegistration(t *testing.T) {
	h := newHarness(t, nil, Config{})
	other := newHarness(t, h.cli.Endpoints(), Config{})
	text := protoHead + "message N {} message M { .a.N child = 1; }"
	descriptor := protoDescriptorBase64(t, text, nil, nil)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	for _, reg := range []*Registry{h.reg, other.reg} {
		var once sync.Once
		reg.beforeCommit = func() { once.Do(func() { entered <- struct{}{}; <-release }) }
	}
	type result struct {
		id  int64
		err error
	}
	out := make(chan result, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i, reg := range []*Registry{h.reg, other.reg} {
		input := []string{text, descriptor}[i]
		go func(reg *Registry, subject, input string) {
			v, err := reg.Register(ctx, subject, RegisterRequest{SchemaType: schemaTypeProtobuf, Schema: input})
			var id int64
			if v != nil {
				id = v.Schema.ID
			}
			out <- result{id, err}
		}(reg, fmt.Sprint(i), input)
	}
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("writers did not reach commit")
		}
	}
	unblock.Do(func() { close(release) })
	first, second := <-out, <-out
	if first.err != nil || second.err != nil || first.id != second.id {
		t.Fatalf("concurrent ids: %+v %+v", first, second)
	}
}

func TestProtoGlobalLookupFailureDoesNotAllocateID(t *testing.T) {
	h := newHarness(t, nil, Config{})
	ctx := context.Background()
	text := protoHead + "message M {}"

	p, err := h.reg.prepare(ctx, RegisterRequest{SchemaType: schemaTypeProtobuf, Schema: text})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.cli.Put(ctx, p.protoAliasKey(), "invalid-id"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.reg.Register(ctx, "bad-alias", RegisterRequest{SchemaType: schemaTypeProtobuf, Schema: text}); err == nil {
		t.Fatal("corrupt descriptor alias accepted")
	}
	if _, err := h.cli.Delete(ctx, p.protoAliasKey()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cli.Put(ctx, idKey(999), "broken"); err != nil {
		t.Fatal(err)
	}
	_, err = h.reg.Register(ctx, "new", RegisterRequest{SchemaType: schemaTypeProtobuf, Schema: text})
	if err == nil {
		t.Fatal("registration ignored metadata read failure")
	}
	counter, err := h.cli.Get(ctx, counterKey())
	if err != nil || len(counter.Kvs) != 0 {
		t.Fatal("failed match allocated an id", err)
	}
}

type protoPagingKV struct {
	clientv3.KV
	t     *testing.T
	calls int
}

func (k *protoPagingKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	k.calls++
	want := int64(10)
	if k.calls == 1 {
		want = 0
	}
	if got := clientv3.OpGet(key, opts...).Rev(); got != want {
		k.t.Errorf("page %d read revision %d, want fixed revision %d", k.calls, got, want)
	}
	kv := &mvccpb.KeyValue{Key: []byte(idKey(int64(k.calls))), CreateRevision: int64(k.calls), ModRevision: int64(k.calls)}
	record := schemaRecord{SchemaType: schemaTypeAvro}
	if k.calls == 3 {
		record = schemaRecord{SchemaType: schemaTypeProtobuf, Hash: "old", Desc: "same"}
	}
	kv.Value, _ = json.Marshal(record)
	return &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: int64(k.calls * 10)}, Kvs: []*mvccpb.KeyValue{kv}, More: k.calls < 3}, nil
}
func TestProtoGlobalMatchPinsFirstRevision(t *testing.T) {
	h := newHarness(t, nil, Config{})
	fake := &protoPagingKV{t: t}
	h.reg.cli = &clientv3.Client{KV: fake}
	p := &prepared{schemaType: schemaTypeProtobuf, hash: "new", desc: "same", fromDesc: true}
	_, id, err := h.reg.protoGlobalMatch(context.Background(), p)
	if err != nil || id != 3 || fake.calls != 3 {
		t.Fatalf("paged match: id=%d calls=%d err=%v", id, fake.calls, err)
	}
}

func TestProtoAliasReuseIgnoresUnrelatedIDAllocations(t *testing.T) {
	h := newHarness(t, nil, Config{})
	ctx := context.Background()
	text := protoHead + "message N {} message M { .a.N child = 1; }"
	first := h.register("text", protoSchemaBody(text))
	descriptor := protoDescriptorBase64(t, text, nil, nil)
	commits := 0
	h.reg.beforeCommit = func() {
		commits++
		if commits == 1 {
			if _, err := h.cli.Put(ctx, counterKey(), "1000"); err != nil {
				t.Error(err)
			}
		}
	}
	if got := h.register("descriptor", protoSchemaBody(descriptor)); got != first {
		t.Fatal(got)
	}
	if commits != 1 {
		t.Fatalf("unrelated id allocation forced %d alias commit attempts", commits)
	}
}

func TestProtoAliasReuseFencesConcurrentNativeHash(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprint(cached), func(t *testing.T) {
			h := newHarness(t, nil, Config{})
			other := newHarness(t, h.cli.Endpoints(), Config{})
			text := protoHead + "message N {} message M { .a.N child = 1; }"
			first := h.register("text", protoSchemaBody(text))
			descriptor := protoDescriptorBase64(t, text, nil, nil)
			if cached {
				if got := h.register("bridge", protoSchemaBody(descriptor)); got != first {
					t.Fatal(got)
				}
			}
			printed, _, err := protoFromDescriptor(descriptor, nil)
			if err != nil {
				t.Fatal(err)
			}
			var once sync.Once
			var native int64
			h.reg.beforeCommit = func() { once.Do(func() { native = other.register("native", protoSchemaBody(printed)) }) }
			if got := h.register("racing-descriptor", protoSchemaBody(descriptor)); got != native {
				t.Fatalf("alias won over concurrently created native hash: %d, want %d", got, native)
			}
		})
	}
}
