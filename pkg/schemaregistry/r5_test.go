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
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func readState(r *Registry) (uint64, uint64, int64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.clusterID, r.raftTerm, r.revision
}

func TestOutOfOrderRevisionDoesNotReset(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	h.ok("GET", "/schemas/ids/1", nil, nil)
	r := h.reg
	base := r.CacheResets()
	cluster, term, rev := readState(r)
	early := r.mark()
	late := r.mark()
	r.observe(&late, &etcdserverpb.ResponseHeader{ClusterId: cluster, RaftTerm: term, Revision: rev + 20})
	r.observe(&early, &etcdserverpb.ResponseHeader{ClusterId: cluster, RaftTerm: term, Revision: rev + 10})
	if r.CacheResets() != base {
		t.Fatal("reset on a late reply")
	}
	if _, _, got := readState(r); got != rev+20 {
		t.Fatalf("revision %d, want %d", got, rev+20)
	}
	h.ok("GET", "/schemas/ids/1", nil, nil)
}

func TestLowerRevisionAfterCompletedReadResets(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	r := h.reg
	base := r.CacheResets()
	cluster, term, rev := readState(r)
	r.observe(markOf(r), &etcdserverpb.ResponseHeader{ClusterId: cluster, RaftTerm: term, Revision: rev + 20})
	r.observe(markOf(r), &etcdserverpb.ResponseHeader{ClusterId: cluster, RaftTerm: term, Revision: rev + 5})
	if r.CacheResets() != base+1 {
		t.Fatal("no reset on a lower revision")
	}
}

func TestOutOfOrderRaftTermDoesNotReset(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	r := h.reg
	base := r.CacheResets()
	cluster, term, rev := readState(r)
	early := r.mark()
	late := r.mark()
	r.observe(&late, &etcdserverpb.ResponseHeader{ClusterId: cluster, RaftTerm: term + 2, Revision: rev + 20})
	r.observe(&early, &etcdserverpb.ResponseHeader{ClusterId: cluster, RaftTerm: term + 1, Revision: rev + 20})
	if r.CacheResets() != base {
		t.Fatal("reset on a late reply from an older term")
	}
	if _, got, _ := readState(r); got != term+2 {
		t.Fatalf("raft term %d, want %d", got, term+2)
	}
}

func TestLowerRaftTermAfterCompletedReadResets(t *testing.T) {
	h := newHarness(t, nil, Config{})
	h.register("s", schema(userV1))
	r := h.reg
	base := r.CacheResets()
	cluster, term, rev := readState(r)
	r.observe(markOf(r), &etcdserverpb.ResponseHeader{ClusterId: cluster, RaftTerm: term + 2, Revision: rev + 20})
	r.observe(markOf(r), &etcdserverpb.ResponseHeader{ClusterId: cluster, RaftTerm: term + 1, Revision: rev + 30})
	if r.CacheResets() != base+1 {
		t.Fatal("no reset on a lower raft term")
	}
}

func TestSoftDeletedLatestReRegisterAddsVersion(t *testing.T) {
	h := newHarness(t, nil, Config{})
	var first, again map[string]any
	h.register("s", schema(userV1))
	h.ok("POST", "/subjects/s/versions", schema(userV2), &first)
	h.ok("DELETE", "/subjects/s/versions/2", nil, nil)
	h.ok("POST", "/subjects/s/versions", schema(userV2), &again)
	if first["id"] != again["id"] {
		t.Fatalf("id %v, want %v", again["id"], first["id"])
	}
	var vs []int
	h.ok("GET", "/subjects/s/versions", nil, &vs)
	if len(vs) != 2 || vs[0] != 1 || vs[1] != 3 {
		t.Fatalf("versions %v, want [1 3]", vs)
	}
}
