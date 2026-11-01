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
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

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

type undeletableS3 struct {
	*storage.MemoryS3Client
}

func (s *undeletableS3) DeleteIndex(ctx context.Context, key string) error {
	return errors.New("AccessDenied")
}

func TestCheckIDMarkersWarnsOnFailedDelete(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	if err := registryOn(t, &undeletableS3{storage.NewMemoryS3Client()}).CheckIDMarkers(context.Background()); err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); !strings.Contains(out, "delete S3 probe object prod/@schemas/probe/") || !strings.Contains(out, "AccessDenied") || !strings.Contains(out, "s3:DeleteObject") {
		t.Fatalf("log %q", out)
	}
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
		if id > 64*n {
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

func markerIDs(t *testing.T, r *Registry, s3 storage.S3Client, upTo int64) []int64 {
	t.Helper()
	var out []int64
	for id := int64(1); id <= upTo; id++ {
		if _, err := s3.DownloadIndex(context.Background(), r.idMarkerKey(id)); err == nil {
			out = append(out, id)
		}
	}
	return out
}

func TestRejectedRegistersClaimNoIDs(t *testing.T) {
	s3 := storage.NewMemoryS3Client()
	h := newHarness(t, nil, Config{S3: s3, Namespace: "prod"})
	h.register("s", schema(userV1))
	for range 20 {
		h.fail("POST", "/subjects/s/versions", schema(userV3), 409, 40901)
	}
	h.ok("PUT", "/mode/imp", map[string]string{"mode": "IMPORT"}, nil)
	h.ok("POST", "/subjects/imp/versions", map[string]any{"schema": address, "id": 40, "version": 1}, nil)
	for range 5 {
		h.fail("POST", "/subjects/imp/versions", map[string]any{"schema": userV3, "id": 41, "version": 1}, 422, 42205)
	}
	if got := markerIDs(t, h.reg, s3, 100); !slices.Equal(got, []int64{1, 40}) {
		t.Fatalf("markers after rejections %v, want [1 40]", got)
	}
	if id := h.register("s", schema(userV2)); id != 41 {
		t.Fatalf("compatible register after rejections got id %d, want 41", id)
	}
	if got := markerIDs(t, h.reg, s3, 100); !slices.Equal(got, []int64{1, 40, 41}) {
		t.Fatalf("markers %v, want [1 40 41]", got)
	}
}

func TestFenceRetryReusesClaimedID(t *testing.T) {
	s3 := storage.NewMemoryS3Client()
	h := newHarness(t, nil, Config{S3: s3, Namespace: "prod"})
	h.register("s", schema(userV1))
	bumps := 0
	h.reg.beforeCommit = func() {
		if bumps < 3 {
			bumps++
			if _, err := h.cli.Put(context.Background(), configKey("s"), CompatBackward); err != nil {
				t.Error(err)
			}
		}
	}
	if id := h.register("s", schema(userV2)); id != 2 {
		t.Fatalf("id %d after fence retries, want 2", id)
	}
	if bumps != 3 {
		t.Fatalf("%d fence failures, want 3", bumps)
	}
	if got := markerIDs(t, h.reg, s3, 20); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("markers %v, want [1 2]", got)
	}
}

func TestConcurrentRegistersInOneInstanceLeaveNoOrphans(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	s3 := storage.NewMemoryS3Client()
	regs := make([]*Registry, 1)
	for i := range regs {
		reg, err := New(Config{Client: newClient(t, endpoints), S3: s3, Namespace: "prod", InlineLimit: DefaultInlineLimit})
		if err != nil {
			t.Fatal(err)
		}
		regs[i] = reg
	}
	if err := regs[0].SetCompatibility(context.Background(), "", CompatNone); err != nil {
		t.Fatal(err)
	}
	const workers, perWorker = 8, 25
	var wg sync.WaitGroup
	var mu sync.Mutex
	var maxID int64
	for g := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWorker {
				out, err := regs[g%len(regs)].Register(context.Background(), fmt.Sprintf("s%d", g), RegisterRequest{Schema: record(fmt.Sprintf("W%d_%d", g, i))})
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				maxID = max(maxID, out.Schema.ID)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	markers := markerIDs(t, regs[0], s3, maxID+64)
	orphans := len(markers) - workers*perWorker
	t.Logf("%d registers, %d markers, %d orphans, max id %d", workers*perWorker, len(markers), orphans, maxID)
	if orphans != 0 {
		t.Fatalf("%d orphan markers for %d registers", orphans, workers*perWorker)
	}
}

type stallingS3 struct {
	*storage.MemoryS3Client
	mu      sync.Mutex
	stall   chan struct{}
	stalled chan struct{}
}

func (s *stallingS3) UploadIndexIfAbsent(ctx context.Context, key string, body []byte) error {
	s.mu.Lock()
	stall := s.stall
	s.mu.Unlock()
	if stall != nil {
		select {
		case s.stalled <- struct{}{}:
		default:
		}
		select {
		case <-stall:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.MemoryS3Client.UploadIndexIfAbsent(ctx, key, body)
}

func stalledWrite(t *testing.T) (*Registry, func()) {
	t.Helper()
	s3 := &stallingS3{MemoryS3Client: storage.NewMemoryS3Client()}
	reg := registryOn(t, s3)
	if err := reg.SetCompatibility(context.Background(), "", CompatNone); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Register(context.Background(), "existing", RegisterRequest{Schema: record("E")}); err != nil {
		t.Fatal(err)
	}
	stall := make(chan struct{})
	s3.mu.Lock()
	s3.stall = stall
	s3.stalled = make(chan struct{}, 1)
	s3.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = reg.Register(ctx, "slow", RegisterRequest{Schema: record("S")})
	}()
	select {
	case <-s3.stalled:
	case <-time.After(5 * time.Second):
		t.Fatal("write never reached S3")
	}
	return reg, func() {
		close(stall)
		<-done
	}
}

func TestExistingSchemaNotBlockedByStalledWrite(t *testing.T) {
	reg, release := stalledWrite(t)
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	out, err := reg.Register(ctx, "existing", RegisterRequest{Schema: record("E")})
	if err != nil || out.Schema.ID != 1 {
		t.Fatalf("re-register of an existing schema during a stalled write: %v after %v", err, time.Since(start))
	}
	if _, err := reg.Lookup(ctx, "existing", RegisterRequest{Schema: record("E")}, false); err != nil {
		t.Fatalf("lookup during a stalled write: %v", err)
	}
}

func TestWriteLockWaiterHonorsDeadline(t *testing.T) {
	reg, release := stalledWrite(t)
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := reg.Register(ctx, "new", RegisterRequest{Schema: record("N")})
	took := time.Since(start)
	var regErr *Error
	if !errors.As(err, &regErr) || regErr.Code != 50002 {
		t.Fatalf("waiter got %v, want 50002", err)
	}
	if took > time.Second {
		t.Fatalf("waiter returned after %v with a 300ms deadline", took)
	}
}

func TestWaiterReturnsSchemaRegisteredWhileWaiting(t *testing.T) {
	reg, release := stalledWrite(t)
	type res struct {
		id, version int64
		err         error
	}
	got := make(chan res, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, err := reg.Register(ctx, "slow", RegisterRequest{Schema: record("S")})
		if err != nil {
			got <- res{err: err}
			return
		}
		got <- res{id: out.Schema.ID, version: int64(out.Version)}
	}()
	time.Sleep(100 * time.Millisecond)
	release()
	r := <-got
	if r.err != nil || r.id != 2 || r.version != 1 {
		t.Fatalf("waiter got id %d version %d err %v, want id 2 version 1", r.id, r.version, r.err)
	}
	versions, err := reg.Versions(context.Background(), "slow", false)
	if err != nil || len(versions) != 1 {
		t.Fatalf("versions %v err %v", versions, err)
	}
}
