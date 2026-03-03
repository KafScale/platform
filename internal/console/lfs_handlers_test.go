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
	"testing"
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
