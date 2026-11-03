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

package schemaregistry

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/storage"
)

func TestImportReclaimWithCounterAlreadyPast(t *testing.T) {
	s3 := storage.NewMemoryS3Client()
	ep := testutil.StartEmbeddedEtcd(t)
	a := newHarness(t, ep, Config{S3: s3, Namespace: "prod"})
	b := newHarness(t, ep, Config{S3: s3, Namespace: "prod"})
	imp := newHarness(t, ep, Config{S3: s3, Namespace: "prod"})
	imp.ok("PUT", "/mode/imp", map[string]string{"mode": "IMPORT"}, nil)
	held, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	a.reg.beforeCommit = func() { once.Do(func() { close(held); <-resume }) }
	done := make(chan int64, 1)
	go func() { done <- a.register("a", schema(userV1)) }()
	<-held
	idB := b.register("b", schema(userV2))
	status, body := imp.do("POST", "/subjects/imp/versions", map[string]any{"schema": address, "id": 1, "version": 1})
	close(resume)
	idA := <-done
	t.Logf("B=%d import=%d %s A=%d", idB, status, body, idA)
	if status != 200 || idA == 1 || idA <= idB {
		t.Fatalf("B=%d import=%d A=%d", idB, status, idA)
	}
	for _, id := range []int64{1, idB, idA} {
		if got, want := markerBody(t, imp, s3, id), etcdHash(t, imp, id); got != want {
			t.Fatalf("id %d marker %q etcd %q", id, got, want)
		}
	}
}

type holdIfMatchS3 struct {
	*storage.MemoryS3Client
	hold func()
}

func (s *holdIfMatchS3) UploadIndexIfMatch(ctx context.Context, key string, data []byte, etag string) error {
	if s.hold != nil {
		h := s.hold
		s.hold = nil
		h()
	}
	return s.MemoryS3Client.UploadIndexIfMatch(ctx, key, data, etag)
}

func TestLosingImportLeavesMarkerOfWinner(t *testing.T) {
	mem := storage.NewMemoryS3Client()
	ep := testutil.StartEmbeddedEtcd(t)
	a := newHarness(t, ep, Config{S3: mem, Namespace: "prod"})
	bs3 := &holdIfMatchS3{MemoryS3Client: mem}
	b := newHarness(t, ep, Config{S3: bs3, Namespace: "prod"})
	if err := mem.UploadIndex(context.Background(), a.reg.idMarkerKey(7), []byte("orphan-hash")); err != nil {
		t.Fatal(err)
	}
	a.ok("PUT", "/mode", map[string]string{"mode": "IMPORT"}, nil)
	aHeld, aGo, bHeld, bGo := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	a.reg.beforeCommit = func() {
		if markerBody(t, a, mem, 7) != "orphan-hash" {
			once.Do(func() { close(aHeld); <-aGo })
		}
	}
	aRes := make(chan int, 1)
	go func() {
		s, _ := a.do("POST", "/subjects/a/versions", map[string]any{"schema": userV1, "id": 7, "version": 1})
		aRes <- s
	}()
	<-aHeld
	bs3.hold = func() { close(bHeld); <-bGo }
	bRes := make(chan int, 1)
	go func() {
		s, _ := b.do("POST", "/subjects/b/versions", map[string]any{"schema": address, "id": 7, "version": 1})
		bRes <- s
	}()
	<-bHeld
	close(aGo)
	sa := <-aRes
	close(bGo)
	sb := <-bRes
	got, want := markerBody(t, a, mem, 7), etcdHash(t, a, 7)
	t.Logf("A=%d B=%d marker=%s etcd=%s", sa, sb, got, want)
	if got != want {
		t.Fatalf("live id 7: marker %q, etcd holds %q", got, want)
	}
}

func TestTwoLosingImportsLeaveMarkerOfWinner(t *testing.T) {
	mem := storage.NewMemoryS3Client()
	ep := testutil.StartEmbeddedEtcd(t)
	a := newHarness(t, ep, Config{S3: mem, Namespace: "prod"})
	b := newHarness(t, ep, Config{S3: mem, Namespace: "prod"})
	cs3 := &holdIfMatchS3{MemoryS3Client: mem}
	c := newHarness(t, ep, Config{S3: cs3, Namespace: "prod"})
	if err := mem.UploadIndex(context.Background(), a.reg.idMarkerKey(7), []byte("orphan-hash")); err != nil {
		t.Fatal(err)
	}
	a.ok("PUT", "/mode", map[string]string{"mode": "IMPORT"}, nil)
	var buf syncBuffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)
	hold := func(h *harness, skip int, held, release chan struct{}) {
		var once sync.Once
		h.reg.beforeCommit = func() {
			if skip > 0 {
				skip--
				return
			}
			once.Do(func() { close(held); <-release })
		}
	}
	aHeld, aGo, bHeld, bGo, cHeld, cGo, cPut, cPutGo := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	hold(a, 1, aHeld, aGo)
	hold(b, 0, bHeld, bGo)
	hold(c, 0, cHeld, cGo)
	post := func(h *harness, subject, text string, out chan<- int) {
		s, _ := h.do("POST", "/subjects/"+subject+"/versions", map[string]any{"schema": text, "id": 7, "version": 1})
		out <- s
	}
	aRes, bRes, cRes := make(chan int, 1), make(chan int, 1), make(chan int, 1)
	go post(a, "a", userV1, aRes)
	<-aHeld
	go post(b, "b", address, bRes)
	<-bHeld
	cs3.hold = func() { close(cPut); <-cPutGo }
	go post(c, "c", userV2, cRes)
	<-cPut
	close(cPutGo)
	<-cHeld
	close(aGo)
	sa := <-aRes
	close(bGo)
	sb := <-bRes
	close(cGo)
	sc := <-cRes
	got, want := markerBody(t, a, mem, 7), etcdHash(t, a, 7)
	t.Logf("A=%d B=%d C=%d marker=%s etcd=%s", sa, sb, sc, got, want)
	if sa != 200 || sb != 422 || sc != 422 {
		t.Fatalf("A=%d B=%d C=%d", sa, sb, sc)
	}
	if got != want {
		t.Fatalf("live id 7: marker %q, etcd holds %q", got, want)
	}
	if n := strings.Count(buf.String(), "reclaimed orphaned schema id marker"); n != 1 {
		t.Fatalf("%d reclaim log lines, want 1:\n%s", n, buf.String())
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
