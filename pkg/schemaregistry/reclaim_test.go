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
	"strings"
	"sync"
	"testing"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/storage"
)

type ifMatchIgnoringS3 struct {
	*storage.MemoryS3Client
}

func (s *ifMatchIgnoringS3) UploadIndexIfMatch(ctx context.Context, key string, body []byte, etag string) error {
	return s.UploadIndex(ctx, key, body)
}

type racingS3 struct {
	*storage.MemoryS3Client
	race func()
}

func (s *racingS3) DownloadIndexObject(ctx context.Context, key string) (storage.IndexObject, error) {
	obj, err := s.MemoryS3Client.DownloadIndexObject(ctx, key)
	if s.race != nil {
		s.race()
	}
	return obj, err
}

func orphanHarness(t *testing.T, s3 storage.S3Client) *harness {
	h := newHarness(t, nil, Config{S3: s3, Namespace: "prod"})
	if err := s3.UploadIndex(context.Background(), h.reg.idMarkerKey(7), []byte("orphan-hash")); err != nil {
		t.Fatal(err)
	}
	h.ok("PUT", "/mode", map[string]string{"mode": "IMPORT"}, nil)
	return h
}

func markerBody(t *testing.T, h *harness, s3 storage.S3Client, id int64) string {
	data, err := s3.DownloadIndex(context.Background(), h.reg.idMarkerKey(id))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestImportReclaimsOrphanedIDMarker(t *testing.T) {
	s3 := storage.NewMemoryS3Client()
	h := orphanHarness(t, s3)
	var out struct {
		ID int64 `json:"id"`
	}
	h.ok("POST", "/subjects/s/versions", map[string]any{"schema": userV1, "id": 7, "version": 1}, &out)
	if out.ID != 7 || h.reg.counter != 7 {
		t.Fatalf("import got %v", out)
	}
	if got := markerBody(t, h, s3, 7); got == "orphan-hash" {
		t.Fatal("marker not rewritten")
	}
	h.ok("POST", "/subjects/s/versions", map[string]any{"schema": userV1, "id": 7, "version": 1}, &out)
}

func TestImportDoesNotReclaimMarkerWithEtcdRecord(t *testing.T) {
	s3 := storage.NewMemoryS3Client()
	h := newHarness(t, nil, Config{S3: s3, Namespace: "prod"})
	h.ok("PUT", "/mode", map[string]string{"mode": "IMPORT"}, nil)
	h.ok("POST", "/subjects/a/versions", map[string]any{"schema": address, "id": 7, "version": 1}, nil)
	before := markerBody(t, h, s3, 7)
	h.fail("POST", "/subjects/s/versions", map[string]any{"schema": userV1, "id": 7, "version": 1}, 422, 42205)
	if got := markerBody(t, h, s3, 7); got != before {
		t.Fatalf("marker changed from %q to %q", before, got)
	}
}

func TestImportReclaimLosesRace(t *testing.T) {
	s3 := &racingS3{MemoryS3Client: storage.NewMemoryS3Client()}
	h := orphanHarness(t, s3)
	key := h.reg.idMarkerKey(7)
	s3.race = func() {
		s3.race = nil
		if err := s3.UploadIndex(context.Background(), key, []byte("other-hash")); err != nil {
			t.Error(err)
		}
	}
	h.fail("POST", "/subjects/s/versions", map[string]any{"schema": userV1, "id": 7, "version": 1}, 422, 42205)
	if got := markerBody(t, h, s3, 7); got != "other-hash" {
		t.Fatalf("marker %q", got)
	}
}

func TestCheckIDMarkersProbesIfMatch(t *testing.T) {
	ctx := context.Background()
	if err := registryOn(t, storage.NewMemoryS3Client()).CheckIDMarkers(ctx); err != nil {
		t.Fatal(err)
	}
	err := registryOn(t, &ifMatchIgnoringS3{storage.NewMemoryS3Client()}).CheckIDMarkers(ctx)
	if err == nil || !strings.Contains(err.Error(), "stale If-Match") {
		t.Fatalf("If-Match ignoring backend: %v", err)
	}
}

func etcdHash(t *testing.T, h *harness, id int64) string {
	resp, err := h.cli.Get(context.Background(), idKey(id))
	if err != nil || len(resp.Kvs) == 0 {
		t.Fatalf("id %d: %v %v", id, resp, err)
	}
	var rec schemaRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		t.Fatal(err)
	}
	return rec.Hash
}

func TestImportReclaimFencesInFlightRegister(t *testing.T) {
	s3 := storage.NewMemoryS3Client()
	endpoints := testutil.StartEmbeddedEtcd(t)
	live := newHarness(t, endpoints, Config{S3: s3, Namespace: "prod"})
	imp := newHarness(t, endpoints, Config{S3: s3, Namespace: "prod"})
	imp.ok("PUT", "/mode/imp", map[string]string{"mode": "IMPORT"}, nil)
	reserved, resume := make(chan struct{}), make(chan struct{})
	var held sync.Once
	live.reg.beforeCommit = func() {
		held.Do(func() {
			close(reserved)
			<-resume
		})
	}
	done := make(chan int64, 1)
	go func() { done <- live.register("live", schema(userV1)) }()
	<-reserved
	liveMarker := markerBody(t, imp, s3, 1)
	var liveID int64
	var released sync.Once
	release := func() {
		released.Do(func() {
			close(resume)
			liveID = <-done
		})
	}
	imp.reg.beforeCommit = func() {
		if markerBody(t, imp, s3, 1) != liveMarker {
			release()
		}
	}
	status, body := imp.do("POST", "/subjects/imp/versions", map[string]any{"schema": address, "id": 1, "version": 1})
	release()
	if status != 200 || liveID == 1 {
		t.Fatalf("import %d %s, register got id %d", status, body, liveID)
	}
	for _, id := range []int64{1, liveID} {
		if got, want := markerBody(t, imp, s3, id), etcdHash(t, imp, id); got != want {
			t.Fatalf("id %d marker %q, etcd holds %q", id, got, want)
		}
	}
}

func TestConcurrentImportsLeaveMarkerOfWinner(t *testing.T) {
	s3 := storage.NewMemoryS3Client()
	endpoints := testutil.StartEmbeddedEtcd(t)
	first := newHarness(t, endpoints, Config{S3: s3, Namespace: "prod"})
	second := newHarness(t, endpoints, Config{S3: s3, Namespace: "prod"})
	if err := s3.UploadIndex(context.Background(), first.reg.idMarkerKey(7), []byte("orphan-hash")); err != nil {
		t.Fatal(err)
	}
	first.ok("PUT", "/mode", map[string]string{"mode": "IMPORT"}, nil)
	firstMarker, secondMarker := "", ""
	started, secondHeld, firstDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var startOnce, holdOnce sync.Once
	first.reg.beforeCommit = func() {
		if m := markerBody(t, first, s3, 7); m != "orphan-hash" && firstMarker == "" {
			firstMarker = m
			startOnce.Do(func() {
				close(started)
				<-secondHeld
			})
		}
	}
	second.reg.beforeCommit = func() {
		if m := markerBody(t, second, s3, 7); m != "orphan-hash" && m != firstMarker {
			holdOnce.Do(func() {
				secondMarker = m
				close(secondHeld)
				<-firstDone
			})
		}
	}
	result := make(chan int, 1)
	go func() {
		<-started
		status, _ := second.do("POST", "/subjects/b/versions", map[string]any{"schema": address, "id": 7, "version": 1})
		result <- status
	}()
	status, body := first.do("POST", "/subjects/a/versions", map[string]any{"schema": userV1, "id": 7, "version": 1})
	close(firstDone)
	if status != 200 {
		t.Fatalf("first import %d %s", status, body)
	}
	if got := <-result; got != 422 {
		t.Fatalf("second import %d", got)
	}
	if secondMarker == "" {
		t.Fatal("second import never replaced the marker")
	}
	if got, want := markerBody(t, first, s3, 7), etcdHash(t, first, 7); got != want {
		t.Fatalf("marker %q, etcd holds %q", got, want)
	}
}

func TestImportReclaimChecksEtcdAfterReadingMarker(t *testing.T) {
	s3 := &racingS3{MemoryS3Client: storage.NewMemoryS3Client()}
	h := orphanHarness(t, s3)
	rec, err := json.Marshal(schemaRecord{SchemaType: schemaTypeAvro, Hash: "other-hash", Norm: "other-hash", Schema: address})
	if err != nil {
		t.Fatal(err)
	}
	s3.race = func() {
		s3.race = nil
		if _, err := h.cli.Put(context.Background(), idKey(7), string(rec)); err != nil {
			t.Error(err)
		}
		if _, err := h.cli.Put(context.Background(), counterKey(), "7"); err != nil {
			t.Error(err)
		}
	}
	h.fail("POST", "/subjects/s/versions", map[string]any{"schema": userV1, "id": 7, "version": 1}, 422, 42205)
	if got := markerBody(t, h, s3, 7); got != "orphan-hash" {
		t.Fatalf("marker %q", got)
	}
}
