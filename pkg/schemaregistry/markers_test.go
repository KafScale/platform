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
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/storage"
)

type ignoringS3 struct {
	*storage.MemoryS3Client
}

func (s *ignoringS3) UploadIndexIfAbsent(ctx context.Context, key string, body []byte) error {
	return s.UploadIndex(ctx, key, body)
}

type conflictS3 struct {
	*storage.MemoryS3Client
	mu        sync.Mutex
	conflicts map[string]int
	calls     map[string]int
}

func (s *conflictS3) UploadIndexIfAbsent(ctx context.Context, key string, body []byte) error {
	s.mu.Lock()
	s.calls[key]++
	if s.conflicts[key] > 0 {
		s.conflicts[key]--
		s.mu.Unlock()
		return storage.ErrConditionalConflict
	}
	s.mu.Unlock()
	return s.MemoryS3Client.UploadIndexIfAbsent(ctx, key, body)
}

type recordingS3 struct {
	*storage.MemoryS3Client
	mu      sync.Mutex
	puts    []string
	deletes []string
}

func (s *recordingS3) UploadIndexIfAbsent(ctx context.Context, key string, body []byte) error {
	s.mu.Lock()
	s.puts = append(s.puts, key)
	s.mu.Unlock()
	return s.MemoryS3Client.UploadIndexIfAbsent(ctx, key, body)
}

func (s *recordingS3) DeleteIndex(ctx context.Context, key string) error {
	s.mu.Lock()
	s.deletes = append(s.deletes, key)
	s.mu.Unlock()
	return s.MemoryS3Client.DeleteIndex(ctx, key)
}

func record(name string) string {
	return fmt.Sprintf(`{"type":"record","name":"%s","fields":[{"name":"f","type":"string"}]}`, name)
}

func writerFor(t *testing.T, r *Registry, text string) *recordWriter {
	t.Helper()
	p, err := r.prepare(context.Background(), RegisterRequest{Schema: text})
	if err != nil {
		t.Fatal(err)
	}
	return &recordWriter{r: r, p: p}
}

func registryOn(t *testing.T, s3 storage.S3Client) *Registry {
	t.Helper()
	reg, err := New(Config{Client: newClient(t, testutil.StartEmbeddedEtcd(t)), S3: s3, Namespace: "prod", InlineLimit: DefaultInlineLimit})
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestClaimLoserNeverOverwritesWinner(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		s3     storage.S3Client
		fenced bool
	}{
		{"conditional", storage.NewMemoryS3Client(), true},
		{"condition removed", &ignoringS3{storage.NewMemoryS3Client()}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := registryOn(t, tc.s3), registryOn(t, tc.s3)
			wa, wb := writerFor(t, a, record("A")), writerFor(t, b, record("B"))
			ida, err := wa.claim(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			idb, err := wb.claim(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			marker, err := tc.s3.DownloadIndex(ctx, a.idMarkerKey(ida))
			if err != nil {
				t.Fatal(err)
			}
			intact := ida != idb && string(marker) == wa.p.hash
			if intact != tc.fenced {
				t.Fatalf("ids %d and %d, marker %d holds winner hash %v", ida, idb, ida, string(marker) == wa.p.hash)
			}
		})
	}
}

func TestRacingRegistriesGetDistinctIDs(t *testing.T) {
	s3 := storage.NewMemoryS3Client()
	a := newHarness(t, nil, Config{S3: s3, Namespace: "prod"})
	b := newHarness(t, nil, Config{S3: s3, Namespace: "prod"})
	const n = 20
	type result struct {
		h    *harness
		id   int64
		text string
	}
	results := make(chan result, 2*n)
	var wg sync.WaitGroup
	for hi, h := range []*harness{a, b} {
		for i := range n {
			wg.Add(1)
			go func(h *harness, subject, text string) {
				defer wg.Done()
				status, data := h.do("POST", "/subjects/"+subject+"/versions", schema(text))
				var out struct{ ID int64 }
				if status != 200 || json.Unmarshal(data, &out) != nil {
					t.Errorf("register %s: %d %s", subject, status, data)
					return
				}
				results <- result{h, out.ID, text}
			}(h, fmt.Sprintf("s%d-%d", hi, i), record(fmt.Sprintf("R%d_%d", hi, i)))
		}
	}
	wg.Wait()
	close(results)
	seen := map[int64]string{}
	for res := range results {
		if prev, ok := seen[res.id]; ok {
			t.Fatalf("id %d issued for %s and %s", res.id, prev, res.text)
		}
		seen[res.id] = res.text
		marker, err := s3.DownloadIndex(context.Background(), res.h.reg.idMarkerKey(res.id))
		if err != nil {
			t.Fatal(err)
		}
		if want := writerFor(t, res.h.reg, res.text).p.hash; string(marker) != want {
			t.Fatalf("marker for id %d holds %s, want %s", res.id, marker, want)
		}
		if _, data := res.h.do("GET", fmt.Sprintf("/schemas/ids/%d/schema", res.id), nil); string(data) != res.text {
			t.Fatalf("id %d serves %s, want %s", res.id, data, res.text)
		}
	}
	if len(seen) != 2*n {
		t.Fatalf("%d ids for %d registrations", len(seen), 2*n)
	}
	for id := range seen {
		if id > 8*n {
			t.Fatalf("id %d for %d registrations", id, 2*n)
		}
	}
}

func TestClaimRetriesConditionalConflict(t *testing.T) {
	ctx := context.Background()
	newS3 := func(conflicts int) *conflictS3 {
		return &conflictS3{MemoryS3Client: storage.NewMemoryS3Client(), conflicts: map[string]int{}, calls: map[string]int{}}
	}
	s3 := newS3(0)
	r := registryOn(t, s3)
	s3.conflicts[r.idMarkerKey(2)] = maxConflictRetry
	if id, err := writerFor(t, r, record("A")).claim(ctx, 1); err != nil || id != 2 {
		t.Fatalf("claim after transient conflicts: id %d err %v", id, err)
	}
	if got := s3.calls[r.idMarkerKey(2)]; got != maxConflictRetry+1 {
		t.Fatalf("%d puts for id 2", got)
	}
	s3.conflicts[r.idMarkerKey(3)] = maxConflictRetry + 1
	if id, err := writerFor(t, r, record("B")).claim(ctx, 2); err != nil || id != 4 {
		t.Fatalf("claim after persistent conflict: id %d err %v", id, err)
	}
	if _, err := s3.DownloadIndex(ctx, r.idMarkerKey(3)); err == nil {
		t.Fatal("id 3 marker written despite conflict")
	}
	s3.conflicts[r.idMarkerKey(9)] = maxConflictRetry + 1
	w := writerFor(t, r, record("C"))
	if err := w.claimImport(ctx, 9); err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("import with persistent conflict: %v", err)
	}
	if err := w.claimImport(ctx, 9); err != nil {
		t.Fatalf("import retry: %v", err)
	}
}

func TestCheckIDMarkers(t *testing.T) {
	ctx := context.Background()
	rec := &recordingS3{MemoryS3Client: storage.NewMemoryS3Client()}
	if err := registryOn(t, rec).CheckIDMarkers(ctx); err != nil {
		t.Fatal(err)
	}
	if len(rec.puts) != 2 || rec.puts[0] != rec.puts[1] || !strings.HasPrefix(rec.puts[0], "prod/@schemas/probe/") {
		t.Fatalf("probe puts %v", rec.puts)
	}
	if len(rec.deletes) != 1 || rec.deletes[0] != rec.puts[0] {
		t.Fatalf("probe deletes %v", rec.deletes)
	}
	if _, err := rec.DownloadIndex(ctx, rec.puts[0]); err == nil {
		t.Fatal("probe key left behind")
	}
	if err := registryOn(t, &ignoringS3{storage.NewMemoryS3Client()}).CheckIDMarkers(ctx); err == nil || !strings.Contains(err.Error(), "ignores the condition") {
		t.Fatalf("ignoring backend: %v", err)
	}
	failing := &failingS3{MemoryS3Client: storage.NewMemoryS3Client(), fail: true}
	if err := registryOn(t, failing).CheckIDMarkers(ctx); err == nil {
		t.Fatal("unreachable backend passed the probe")
	}
	if _, err := New(Config{Client: newClient(t, testutil.StartEmbeddedEtcd(t)), S3: plainS3{storage.NewMemoryS3Client()}, Namespace: "prod"}); err == nil {
		t.Fatal("client without conditional writes accepted")
	}
	if err := (&Registry{}).CheckIDMarkers(ctx); err != nil {
		t.Fatal(err)
	}
}

type plainS3 struct {
	storage.S3Client
}

func TestOrphanMarkerSkipsOnlyItsID(t *testing.T) {
	s3 := storage.NewMemoryS3Client()
	h := newHarness(t, nil, Config{S3: s3, Namespace: "prod"})
	h.ok("PUT", "/config", map[string]string{"compatibility": "NONE"}, nil)
	bumped := false
	h.reg.beforeCommit = func() {
		if !bumped {
			bumped = true
			if _, err := h.cli.Put(context.Background(), counterKey(), "5"); err != nil {
				t.Error(err)
			}
		}
	}
	if id := h.register("s", schema(userV1)); id != 6 {
		t.Fatalf("first id %d", id)
	}
	orphan, err := s3.DownloadIndex(context.Background(), h.reg.idMarkerKey(1))
	if err != nil {
		t.Fatalf("orphan marker for id 1: %v", err)
	}
	if want := writerFor(t, h.reg, userV1).p.hash; string(orphan) != want {
		t.Fatalf("orphan holds %s", orphan)
	}
	h.ok("PUT", "/mode/imp", map[string]string{"mode": "IMPORT"}, nil)
	h.ok("POST", "/subjects/imp/versions", map[string]any{"schema": address, "id": 2}, nil)
	_, data := h.do("POST", "/subjects/imp/versions", map[string]any{"schema": userV2, "id": 1})
	if !strings.Contains(string(data), "42205") || !strings.Contains(string(data), "prod/@schemas/ids/00000000000000000001") {
		t.Fatalf("import over orphan: %s", data)
	}
	if err := s3.DeleteIndex(context.Background(), h.reg.idMarkerKey(1)); err != nil {
		t.Fatal(err)
	}
	h.ok("POST", "/subjects/imp/versions", map[string]any{"schema": userV2, "id": 1}, nil)
	if _, data := h.do("GET", "/schemas/ids/1/schema", nil); string(data) != userV2 {
		t.Fatalf("id 1 serves %s", data)
	}
	if _, data := h.do("GET", "/schemas/ids/6/schema", nil); string(data) != userV1 {
		t.Fatalf("id 6 serves %s", data)
	}
}

func TestClaimStaysWithinInt32(t *testing.T) {
	ctx := context.Background()
	s3 := storage.NewMemoryS3Client()
	r := registryOn(t, s3)
	if id, err := writerFor(t, r, record("A")).claim(ctx, math.MaxInt32-1); err != nil || id != math.MaxInt32 {
		t.Fatalf("id %d err %v", id, err)
	}
	if id, err := writerFor(t, r, record("B")).claim(ctx, math.MaxInt32-1); err == nil {
		t.Fatalf("claimed id %d above the int32 range", id)
	}
	for i := int64(1); i <= linearClaimProbes+4; i++ {
		if err := s3.UploadIndexIfAbsent(ctx, r.idMarkerKey(i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	id, err := writerFor(t, r, record("C")).claim(ctx, 0)
	if err != nil || id <= linearClaimProbes+4 || id > 2*(linearClaimProbes+4)+64 {
		t.Fatalf("claim past occupied ids: id %d err %v", id, err)
	}
}
