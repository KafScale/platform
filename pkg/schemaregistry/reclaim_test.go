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
	"strings"
	"testing"
	"time"

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

const reclaimTimeout = 5 * time.Second

func orphanHarness(t *testing.T, s3 storage.S3Client, timeout time.Duration) *harness {
	h := newHarness(t, nil, Config{S3: s3, Namespace: "prod", RequestTimeout: timeout})
	if err := s3.UploadIndex(context.Background(), h.reg.idMarkerKey(7), []byte("orphan-hash")); err != nil {
		t.Fatal(err)
	}
	h.ok("PUT", "/mode", map[string]string{"mode": "IMPORT"}, nil)
	return h
}

func age(h *harness, d time.Duration) {
	h.reg.now = func() time.Time { return time.Now().Add(d) }
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
	h := orphanHarness(t, s3, reclaimTimeout)
	age(h, 2*reclaimTimeout+time.Second)
	var out struct {
		ID int64 `json:"id"`
	}
	h.ok("POST", "/subjects/s/versions", map[string]any{"schema": userV1, "id": 7, "version": 1}, &out)
	if out.ID != 7 {
		t.Fatalf("import got %v", out)
	}
	if got := markerBody(t, h, s3, 7); got == "orphan-hash" {
		t.Fatal("marker not rewritten")
	}
	h.ok("POST", "/subjects/s/versions", map[string]any{"schema": userV1, "id": 7, "version": 1}, &out)
}

func TestImportKeepsRecentIDMarker(t *testing.T) {
	s3 := storage.NewMemoryS3Client()
	h := orphanHarness(t, s3, reclaimTimeout)
	age(h, 2*reclaimTimeout-time.Second)
	h.fail("POST", "/subjects/s/versions", map[string]any{"schema": userV1, "id": 7, "version": 1}, 422, 42205)
	if got := markerBody(t, h, s3, 7); got != "orphan-hash" {
		t.Fatalf("marker %q", got)
	}
}

func TestImportWithoutTimeoutNeverReclaims(t *testing.T) {
	s3 := storage.NewMemoryS3Client()
	h := orphanHarness(t, s3, 0)
	age(h, 24*time.Hour)
	h.fail("POST", "/subjects/s/versions", map[string]any{"schema": userV1, "id": 7, "version": 1}, 422, 42205)
}

func TestImportDoesNotReclaimMarkerWithEtcdRecord(t *testing.T) {
	s3 := storage.NewMemoryS3Client()
	h := newHarness(t, nil, Config{S3: s3, Namespace: "prod", RequestTimeout: reclaimTimeout})
	h.ok("PUT", "/mode", map[string]string{"mode": "IMPORT"}, nil)
	h.ok("POST", "/subjects/a/versions", map[string]any{"schema": address, "id": 7, "version": 1}, nil)
	before := markerBody(t, h, s3, 7)
	age(h, 24*time.Hour)
	h.fail("POST", "/subjects/s/versions", map[string]any{"schema": userV1, "id": 7, "version": 1}, 422, 42205)
	if got := markerBody(t, h, s3, 7); got != before {
		t.Fatalf("marker changed from %q to %q", before, got)
	}
}

func TestImportReclaimLosesRace(t *testing.T) {
	s3 := &racingS3{MemoryS3Client: storage.NewMemoryS3Client()}
	h := orphanHarness(t, s3, reclaimTimeout)
	age(h, 2*reclaimTimeout+time.Second)
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
