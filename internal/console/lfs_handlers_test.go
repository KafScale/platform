// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProcessEventAggregatesTopicStats(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{Enabled: true}, nil)
	topic := "video-uploads"

	handlers.ProcessEvent(LFSEvent{
		EventType: "upload_completed",
		Topic:     topic,
		S3Key:     "default/video-uploads/lfs/obj-1",
		Size:      1024,
		Timestamp: "2026-02-05T10:00:00Z",
	})
	handlers.ProcessEvent(LFSEvent{
		EventType: "download_requested",
		Topic:     topic,
		Timestamp: "2026-02-05T10:01:00Z",
	})
	handlers.ProcessEvent(LFSEvent{
		EventType: "upload_failed",
		Topic:     topic,
		Timestamp: "2026-02-05T10:02:00Z",
		ErrorCode: "kafka_produce_failed",
	})
	handlers.ProcessEvent(LFSEvent{
		EventType: "orphan_detected",
		Topic:     topic,
		Timestamp: "2026-02-05T10:03:00Z",
		S3Key:     "default/video-uploads/lfs/obj-2",
	})

	handlers.mu.RLock()
	stats := handlers.topicStats[topic]
	handlers.mu.RUnlock()

	if stats == nil {
		t.Fatalf("expected topic stats to exist")
	}
	if !stats.HasLFS {
		t.Fatalf("expected HasLFS to be true")
	}
	if stats.ObjectCount != 1 {
		t.Fatalf("expected object_count=1, got %d", stats.ObjectCount)
	}
	if stats.TotalBytes != 1024 {
		t.Fatalf("expected total_bytes=1024, got %d", stats.TotalBytes)
	}
	if stats.Uploads24h != 1 {
		t.Fatalf("expected uploads_24h=1, got %d", stats.Uploads24h)
	}
	if stats.Downloads24h != 1 {
		t.Fatalf("expected downloads_24h=1, got %d", stats.Downloads24h)
	}
	if stats.Errors24h != 1 {
		t.Fatalf("expected errors_24h=1, got %d", stats.Errors24h)
	}
	if stats.Orphans != 1 {
		t.Fatalf("expected orphans=1, got %d", stats.Orphans)
	}
	if stats.LastEvent == "" {
		t.Fatalf("expected last_event to be set")
	}
}

func TestProcessEventCircularBuffer(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{Enabled: true}, nil)
	// Fill buffer past maxRecentEvents
	for i := 0; i < maxRecentEvents+50; i++ {
		handlers.ProcessEvent(LFSEvent{
			EventType: "upload_started",
			Topic:     "test",
			Timestamp: "2026-02-05T10:00:00Z",
		})
	}
	handlers.mu.RLock()
	defer handlers.mu.RUnlock()
	if len(handlers.events) != maxRecentEvents {
		t.Fatalf("expected %d events, got %d", maxRecentEvents, len(handlers.events))
	}
}

func TestProcessEventDownloadCompleted(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{Enabled: true}, nil)
	handlers.ProcessEvent(LFSEvent{
		EventType: "upload_completed",
		Topic:     "t1",
		S3Key:     "key1",
		Size:      100,
		Timestamp: "2026-02-05T10:00:00Z",
	})
	handlers.ProcessEvent(LFSEvent{
		EventType: "download_completed",
		Topic:     "t1",
		Timestamp: "2026-02-05T10:01:00Z",
	})
	handlers.mu.RLock()
	stats := handlers.topicStats["t1"]
	handlers.mu.RUnlock()
	if stats.LastDownload != "2026-02-05T10:01:00Z" {
		t.Fatalf("expected last_download set")
	}
}

func TestHandleStatusHTTP(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{Enabled: true, S3Bucket: "my-bucket", TrackerTopic: "__lfs"}, nil)
	handlers.ProcessEvent(LFSEvent{
		EventType: "upload_completed",
		Topic:     "t1",
		S3Key:     "key1",
		Size:      100,
		Timestamp: "2026-02-05T10:00:00Z",
	})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/lfs/status", nil)
	handlers.HandleStatus(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status: %d", w.Code)
	}
	var resp LFSStatusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.Enabled {
		t.Fatal("expected enabled")
	}
	if resp.S3Bucket != "my-bucket" {
		t.Fatalf("bucket: %q", resp.S3Bucket)
	}
	if resp.Stats.TotalObjects != 1 {
		t.Fatalf("total_objects: %d", resp.Stats.TotalObjects)
	}
}

func TestLFSHandleStatusMethodNotAllowed(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/ui/api/lfs/status", nil)
	handlers.HandleStatus(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleObjectsHTTP(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{Enabled: true}, nil)
	handlers.ProcessEvent(LFSEvent{
		EventType: "upload_completed",
		Topic:     "t1",
		S3Key:     "key1",
		Size:      100,
		Timestamp: "2026-02-05T10:00:00Z",
	})
	handlers.ProcessEvent(LFSEvent{
		EventType: "upload_completed",
		Topic:     "t2",
		S3Key:     "key2",
		Size:      200,
		Timestamp: "2026-02-05T10:01:00Z",
	})

	// Without filter
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/lfs/objects", nil)
	handlers.HandleObjects(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status: %d", w.Code)
	}
	var resp LFSObjectsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TotalCount != 2 {
		t.Fatalf("total: %d", resp.TotalCount)
	}

	// With topic filter
	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest(http.MethodGet, "/ui/api/lfs/objects?topic=t1&limit=10", nil)
	handlers.HandleObjects(w2, r2)
	var resp2 LFSObjectsResponse
	if err := json.NewDecoder(w2.Body).Decode(&resp2); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp2.Objects) != 1 {
		t.Fatalf("filtered objects: %d", len(resp2.Objects))
	}
}

func TestHandleObjectsMethodNotAllowed(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/ui/api/lfs/objects", nil)
	handlers.HandleObjects(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleTopicsHTTP(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{Enabled: true}, nil)
	handlers.ProcessEvent(LFSEvent{EventType: "upload_completed", Topic: "t1", S3Key: "k1", Size: 50, Timestamp: "2026-01-01T00:00:00Z"})
	handlers.ProcessEvent(LFSEvent{EventType: "upload_completed", Topic: "t2", S3Key: "k2", Size: 100, Timestamp: "2026-01-01T00:00:00Z"})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/lfs/topics", nil)
	handlers.HandleTopics(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status: %d", w.Code)
	}
	var resp LFSTopicsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Topics) != 2 {
		t.Fatalf("topics: %d", len(resp.Topics))
	}
}

func TestHandleTopicsMethodNotAllowed(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/ui/api/lfs/topics", nil)
	handlers.HandleTopics(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleTopicDetailHTTP(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{Enabled: true}, nil)
	handlers.ProcessEvent(LFSEvent{EventType: "upload_completed", Topic: "orders", S3Key: "k1", Size: 50, Timestamp: "2026-01-01T00:00:00Z"})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/lfs/topics/orders", nil)
	handlers.HandleTopicDetail(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status: %d", w.Code)
	}
	var resp LFSTopicDetailResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Topic.Name != "orders" {
		t.Fatalf("topic name: %q", resp.Topic.Name)
	}
}

func TestHandleTopicDetailNotFound(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{Enabled: true}, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/lfs/topics/nonexistent", nil)
	handlers.HandleTopicDetail(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandleTopicDetailEmptyName(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{Enabled: true}, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/lfs/topics/", nil)
	handlers.HandleTopicDetail(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleTopicDetailMethodNotAllowed(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/ui/api/lfs/topics/x", nil)
	handlers.HandleTopicDetail(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleOrphansHTTP(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{Enabled: true}, nil)
	handlers.ProcessEvent(LFSEvent{
		EventType: "orphan_detected",
		Topic:     "t1",
		S3Key:     "orphan-key",
		Timestamp: "2026-01-01T00:00:00Z",
		ErrorCode: "no_kafka_ref",
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/lfs/orphans", nil)
	handlers.HandleOrphans(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status: %d", w.Code)
	}
	var resp LFSOrphansResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 1 {
		t.Fatalf("count: %d", resp.Count)
	}
}

func TestHandleOrphansMethodNotAllowed(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/ui/api/lfs/orphans", nil)
	handlers.HandleOrphans(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleS3BrowseNoClient(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/lfs/s3/browse", nil)
	handlers.HandleS3Browse(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestHandleS3BrowseMethodNotAllowed(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/ui/api/lfs/s3/browse", nil)
	handlers.HandleS3Browse(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleS3PresignNoClient(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/ui/api/lfs/s3/presign", strings.NewReader(`{"s3_key":"test"}`))
	handlers.HandleS3Presign(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestHandleS3PresignMethodNotAllowed(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/lfs/s3/presign", nil)
	handlers.HandleS3Presign(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleS3PresignInvalidBody(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	handlers.s3Client = &LFSS3Client{logger: handlers.logger}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/ui/api/lfs/s3/presign", strings.NewReader(`invalid`))
	handlers.HandleS3Presign(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleS3PresignMissingKey(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	handlers.s3Client = &LFSS3Client{logger: handlers.logger}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/ui/api/lfs/s3/presign", strings.NewReader(`{"s3_key":""}`))
	handlers.HandleS3Presign(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleEventsSSE(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	handlers.ProcessEvent(LFSEvent{EventType: "upload_completed", Topic: "t1", S3Key: "k1", Size: 100, Timestamp: "2026-01-01T00:00:00Z"})

	// Create a flushing recorder
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlers.HandleEvents(w, r)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"?types=upload_completed", nil)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content-type: %q", resp.Header.Get("Content-Type"))
	}
	buf := make([]byte, 1024)
	n, _ := resp.Body.Read(buf)
	if n == 0 {
		t.Fatal("expected SSE data")
	}
	if !strings.Contains(string(buf[:n]), "data:") {
		t.Fatalf("expected SSE data prefix: %s", buf[:n])
	}
}

func TestHandleEventsMethodNotAllowed(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/ui/api/lfs/events", nil)
	handlers.HandleEvents(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestResetStats(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{Enabled: true}, nil)
	handlers.ProcessEvent(LFSEvent{EventType: "upload_completed", Topic: "t1", S3Key: "k1", Size: 100, Timestamp: "2026-01-01T00:00:00Z"})
	handlers.ProcessEvent(LFSEvent{EventType: "upload_failed", Topic: "t1", Timestamp: "2026-01-01T00:01:00Z"})
	handlers.ProcessEvent(LFSEvent{EventType: "download_requested", Topic: "t1", Timestamp: "2026-01-01T00:02:00Z"})

	handlers.ResetStats()

	handlers.mu.RLock()
	defer handlers.mu.RUnlock()
	if handlers.stats.Uploads24h != 0 {
		t.Fatalf("uploads_24h: %d", handlers.stats.Uploads24h)
	}
	if handlers.stats.Errors24h != 0 {
		t.Fatalf("errors_24h: %d", handlers.stats.Errors24h)
	}
	if handlers.stats.Downloads24h != 0 {
		t.Fatalf("downloads_24h: %d", handlers.stats.Downloads24h)
	}
	// Total objects should NOT be reset
	if handlers.stats.TotalObjects != 1 {
		t.Fatalf("total_objects should persist: %d", handlers.stats.TotalObjects)
	}
	ts := handlers.topicStats["t1"]
	if ts.Uploads24h != 0 || ts.Errors24h != 0 {
		t.Fatalf("topic stats not reset: uploads=%d errors=%d", ts.Uploads24h, ts.Errors24h)
	}
}

func TestSetConsumerAndS3Client(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	if handlers.consumer != nil {
		t.Fatal("expected nil consumer")
	}
	if handlers.s3Client != nil {
		t.Fatal("expected nil s3Client")
	}
	handlers.SetConsumer(nil)
	handlers.SetS3Client(nil)
}

func TestNewLFSHandlersDefaults(t *testing.T) {
	h := NewLFSHandlers(LFSConfig{Enabled: true, S3Bucket: "b"}, nil)
	if h.logger == nil {
		t.Fatal("expected default logger")
	}
	if h.objects == nil || h.topicStats == nil || h.orphans == nil {
		t.Fatal("expected maps initialized")
	}
}

func TestUpdateTopicStatsNilSafe(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	// Should not panic
	handlers.updateTopicStats(nil, 100, "2026-01-01T00:00:00Z")
}

func TestHandleObjectsWithLimit(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	for i := 0; i < 10; i++ {
		handlers.ProcessEvent(LFSEvent{
			EventType: "upload_completed",
			Topic:     "t1",
			S3Key:     "key-" + strings.Repeat("x", i+1),
			Size:      int64(100 * (i + 1)),
			Timestamp: "2026-01-01T00:00:00Z",
		})
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/lfs/objects?limit=3", nil)
	handlers.HandleObjects(w, r)
	var resp LFSObjectsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Objects) > 3 {
		t.Fatalf("expected at most 3 objects, got %d", len(resp.Objects))
	}
}

func TestHandleObjectsInvalidLimit(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	handlers.ProcessEvent(LFSEvent{EventType: "upload_completed", Topic: "t1", S3Key: "k1", Size: 100, Timestamp: "2026-01-01T00:00:00Z"})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ui/api/lfs/objects?limit=abc", nil)
	handlers.HandleObjects(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestUpdateTopicStatsFirstObject(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	stats := &LFSTopicStats{Name: "t1"}
	handlers.updateTopicStats(stats, 100, "2026-01-01T00:00:00Z")
	if stats.FirstObject != "2026-01-01T00:00:00Z" {
		t.Fatalf("first object: %q", stats.FirstObject)
	}
	handlers.updateTopicStats(stats, 200, "2026-01-02T00:00:00Z")
	if stats.FirstObject != "2026-01-01T00:00:00Z" {
		t.Fatalf("first object should not change: %q", stats.FirstObject)
	}
	if stats.LastObject != "2026-01-02T00:00:00Z" {
		t.Fatalf("last object: %q", stats.LastObject)
	}
	if stats.AvgObjectSize != 150 {
		t.Fatalf("avg object size: %d", stats.AvgObjectSize)
	}
}
