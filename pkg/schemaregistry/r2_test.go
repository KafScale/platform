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
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/storage"
)

func TestReRegisterExistingInReadOnlyAndImport(t *testing.T) {
	for _, mode := range []string{"READONLY", "IMPORT"} {
		t.Run(mode, func(t *testing.T) {
			h := newHarness(t, nil, Config{})
			id := h.register("s", schema(userV1))
			h.ok("PUT", "/mode/s?force=true", map[string]string{"mode": mode}, nil)
			var got struct {
				ID      int64
				Version int
			}
			h.ok("POST", "/subjects/s/versions", schema(userV1), &got)
			if got.ID != id || got.Version != 1 {
				t.Fatalf("%s re-register gave %+v, want id %d version 1", mode, got, id)
			}
			h.fail("POST", "/subjects/s/versions", schema(userV2), 422, 42205)
		})
	}
}

func TestGUIDMatchesConfluent(t *testing.T) {
	h := newHarness(t, nil, Config{})
	var reg struct{ GUID string }
	h.ok("POST", "/subjects/s/versions", schema(userV1), &reg)
	if reg.GUID != "b4233e53-d42c-78fc-f52b-daf039258374" {
		t.Fatalf("guid %s", reg.GUID)
	}
	a := `{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}`
	b := `{"type":"record","name":"B","fields":[{"name":"a","type":"A"}]}`
	h.register("guidr2-a", schema(a))
	var ref struct{ GUID string }
	h.ok("POST", "/subjects/guidr2-b/versions", map[string]any{"schema": b, "references": []Reference{{Name: "A", Subject: "guidr2-a", Version: 1}}}, &ref)
	if ref.GUID != "7bb655a1-13f4-e8dd-0337-5aef7c1fa99e" {
		t.Fatalf("reference guid %s", ref.GUID)
	}
	var byGUID map[string]any
	h.ok("GET", "/schemas/guids/"+reg.GUID, nil, &byGUID)
	if byGUID["schema"] != userV1 || byGUID["guid"] != reg.GUID || byGUID["deleted"] != false || byGUID["schemaType"] != "AVRO" {
		t.Fatalf("by guid %v", byGUID)
	}
	for _, path := range []string{"/schemas/ids/1", "/subjects/s/versions/1"} {
		var v map[string]any
		h.ok("GET", path, nil, &v)
		if v["guid"] != reg.GUID {
			t.Fatalf("%s guid %v", path, v["guid"])
		}
	}
	var lookup map[string]any
	h.ok("POST", "/subjects/s", schema(userV1), &lookup)
	if lookup["guid"] != reg.GUID {
		t.Fatalf("lookup guid %v", lookup["guid"])
	}
	h.fail("GET", "/schemas/guids/00000000-0000-0000-0000-000000000000", nil, 404, 40403)
	h.fail("GET", "/schemas/guids/nope", nil, 404, 40403)
	h.ok("DELETE", "/subjects/s", nil, nil)
	h.ok("GET", "/schemas/guids/"+reg.GUID, nil, &byGUID)
	if byGUID["deleted"] != true {
		t.Fatalf("soft deleted by guid %v", byGUID)
	}
}

func TestSchemaTooLarge(t *testing.T) {
	h := newHarness(t, nil, Config{})
	doc := strings.Repeat(" ", MaxSchemaBytes) + userV1
	h.fail("POST", "/subjects/s/versions", schema(doc), 422, 42209)
	h.fail("POST", "/subjects/s/versions", schema(strings.Repeat(`\"`, 2<<20)), 400, 400)
}

func dumpRegistry(t *testing.T, cli *clientv3.Client) map[string]string {
	t.Helper()
	resp, err := cli.Get(context.Background(), keyPrefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		out[string(kv.Key)] = string(kv.Value)
	}
	return out
}

func TestRestoredEtcdNeverReissuesIDs(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	s3 := storage.NewMemoryS3Client()
	cfg := Config{S3: s3, Namespace: "prod"}
	a := newHarness(t, endpoints, cfg)
	b := newHarness(t, endpoints, cfg)
	a.ok("PUT", "/config", map[string]string{"compatibility": "NONE"}, nil)
	a.register("s", schema(userV1))
	snapshot := dumpRegistry(t, a.cli)
	a.register("s", schema(userV2))
	a.register("s", schema(userV3))
	for _, id := range []string{"2", "3"} {
		b.ok("GET", "/schemas/ids/"+id, nil, nil)
	}
	restored := newClient(t, testutil.StartEmbeddedEtcd(t))
	for k, v := range snapshot {
		if _, err := restored.Put(context.Background(), k, v); err != nil {
			t.Fatal(err)
		}
	}
	a.reg.cli, b.reg.cli = restored, restored
	id := a.register("t", schema(address))
	if id <= 3 {
		t.Fatalf("restored etcd reissued id %d", id)
	}
	_, data := b.do("GET", fmt.Sprintf("/schemas/ids/%d/schema", id), nil)
	if string(data) != address {
		t.Fatalf("pod b id %d gave %s", id, data)
	}
	for _, old := range []string{"2", "3"} {
		if _, data := b.do("GET", "/schemas/ids/"+old+"/schema", nil); string(data) == address {
			t.Fatalf("pod b id %s serves the new schema", old)
		}
	}
	a.ok("PUT", "/mode/imp", map[string]string{"mode": "IMPORT"}, nil)
	a.fail("POST", "/subjects/imp/versions", map[string]any{"schema": person, "references": []Reference{{Name: "example.Address", Subject: "t", Version: 1}}, "id": 2}, 422, 42205)
	a.ok("POST", "/subjects/imp/versions", map[string]any{"schema": userV2, "id": 2}, nil)
}

type failingS3 struct {
	*storage.MemoryS3Client
	fail bool
}

func (f *failingS3) DownloadIndex(ctx context.Context, key string) ([]byte, error) {
	if f.fail {
		return nil, errors.New("s3 down")
	}
	return f.MemoryS3Client.DownloadIndex(ctx, key)
}

func TestRegisterFailsWhenIDMarkersUnavailable(t *testing.T) {
	s3 := &failingS3{MemoryS3Client: storage.NewMemoryS3Client()}
	h := newHarness(t, nil, Config{S3: s3, Namespace: "prod"})
	h.register("s", schema(userV1))
	s3.fail = true
	h.fail("POST", "/subjects/t/versions", schema(address), 500, 50001)
	h.ok("POST", "/subjects/s/versions", schema(userV1), nil)
	s3.fail = false
	if id := h.register("t", schema(address)); id != 2 {
		t.Fatalf("id %d after S3 recovered", id)
	}
}

func TestCacheResetsOnClusterOrTermChange(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	h.ok("GET", "/schemas/ids/1", nil, nil)
	r := h.reg
	base := r.CacheResets()
	r.mu.RLock()
	cluster, term, rev := r.clusterID, r.raftTerm, r.revision
	r.mu.RUnlock()
	r.observe(&etcdserverpb.ResponseHeader{ClusterId: cluster, RaftTerm: term, Revision: rev + 10})
	if r.CacheResets() != base {
		t.Fatal("reset without a change")
	}
	r.observe(&etcdserverpb.ResponseHeader{ClusterId: cluster + 1, RaftTerm: term, Revision: rev + 20})
	if r.CacheResets() != base+1 {
		t.Fatal("no reset on cluster id change")
	}
	r.observe(&etcdserverpb.ResponseHeader{ClusterId: cluster + 1, RaftTerm: term + 5, Revision: rev + 30})
	r.observe(&etcdserverpb.ResponseHeader{ClusterId: cluster + 1, RaftTerm: term + 1, Revision: rev + 40})
	if r.CacheResets() != base+2 {
		t.Fatal("no reset on lower raft term")
	}
}

func TestCacheBoundedByBytes(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.reg.cacheLimit = 2 * len(userV2)
	h.ok("PUT", "/config", map[string]string{"compatibility": "NONE"}, nil)
	texts := []string{userV1, userV2, userV3, address}
	for i, text := range texts {
		h.register(fmt.Sprintf("s%d", i), schema(text))
	}
	for round := 0; round < 2; round++ {
		for i, text := range texts {
			_, data := h.do("GET", fmt.Sprintf("/schemas/ids/%d/schema", i+1), nil)
			if string(data) != text {
				t.Fatalf("id %d gave %s", i+1, data)
			}
			if n := h.reg.CacheBytes(); n > h.reg.cacheLimit {
				t.Fatalf("cache holds %d bytes, limit %d", n, h.reg.cacheLimit)
			}
		}
	}
	h.reg.cacheLimit = 1
	h.ok("GET", "/schemas/ids/1", nil, nil)
}
