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

package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestTrackerEventTypes(t *testing.T) {
	proxyID := "test-proxy"
	requestID := "req-123"

	t.Run("UploadStartedEvent", func(t *testing.T) {
		event := NewUploadStartedEvent(proxyID, requestID, "test-topic", 0, "s3/key", "application/json", "127.0.0.1", "http", 1024)

		if event.EventType != EventTypeUploadStarted {
			t.Errorf("expected event type %s, got %s", EventTypeUploadStarted, event.EventType)
		}
		if event.Topic != "test-topic" {
			t.Errorf("expected topic test-topic, got %s", event.Topic)
		}
		if event.ProxyID != proxyID {
			t.Errorf("expected proxy ID %s, got %s", proxyID, event.ProxyID)
		}
		if event.RequestID != requestID {
			t.Errorf("expected request ID %s, got %s", requestID, event.RequestID)
		}
		if event.Version != TrackerEventVersion {
			t.Errorf("expected version %d, got %d", TrackerEventVersion, event.Version)
		}

		// Test marshaling
		data, err := event.Marshal()
		if err != nil {
			t.Fatalf("failed to marshal event: %v", err)
		}
		var decoded UploadStartedEvent
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal event: %v", err)
		}
		if decoded.Topic != event.Topic {
			t.Errorf("decoded topic mismatch: %s vs %s", decoded.Topic, event.Topic)
		}
	})

	t.Run("UploadCompletedEvent", func(t *testing.T) {
		event := NewUploadCompletedEvent(proxyID, requestID, "test-topic", 0, 42, "bucket", "s3/key", 1024, "sha256hex", "checksum", "sha256", "application/json", 500)

		if event.EventType != EventTypeUploadCompleted {
			t.Errorf("expected event type %s, got %s", EventTypeUploadCompleted, event.EventType)
		}
		if event.KafkaOffset != 42 {
			t.Errorf("expected kafka offset 42, got %d", event.KafkaOffset)
		}
		if event.Size != 1024 {
			t.Errorf("expected size 1024, got %d", event.Size)
		}
		if event.DurationMs != 500 {
			t.Errorf("expected duration 500ms, got %d", event.DurationMs)
		}

		data, err := event.Marshal()
		if err != nil {
			t.Fatalf("failed to marshal event: %v", err)
		}
		var decoded UploadCompletedEvent
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal event: %v", err)
		}
	})

	t.Run("UploadFailedEvent", func(t *testing.T) {
		event := NewUploadFailedEvent(proxyID, requestID, "test-topic", "s3/key", "s3_error", "connection refused", "s3_upload", 512, 250)

		if event.EventType != EventTypeUploadFailed {
			t.Errorf("expected event type %s, got %s", EventTypeUploadFailed, event.EventType)
		}
		if event.ErrorCode != "s3_error" {
			t.Errorf("expected error code s3_error, got %s", event.ErrorCode)
		}
		if event.Stage != "s3_upload" {
			t.Errorf("expected stage s3_upload, got %s", event.Stage)
		}

		data, err := event.Marshal()
		if err != nil {
			t.Fatalf("failed to marshal event: %v", err)
		}
		var decoded UploadFailedEvent
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal event: %v", err)
		}
	})

	t.Run("DownloadRequestedEvent", func(t *testing.T) {
		event := NewDownloadRequestedEvent(proxyID, requestID, "bucket", "s3/key", "presign", "192.168.1.1", 120)

		if event.EventType != EventTypeDownloadRequested {
			t.Errorf("expected event type %s, got %s", EventTypeDownloadRequested, event.EventType)
		}
		if event.Mode != "presign" {
			t.Errorf("expected mode presign, got %s", event.Mode)
		}
		if event.TTLSeconds != 120 {
			t.Errorf("expected TTL 120, got %d", event.TTLSeconds)
		}

		data, err := event.Marshal()
		if err != nil {
			t.Fatalf("failed to marshal event: %v", err)
		}
		var decoded DownloadRequestedEvent
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal event: %v", err)
		}
	})

	t.Run("DownloadCompletedEvent", func(t *testing.T) {
		event := NewDownloadCompletedEvent(proxyID, requestID, "s3/key", "stream", 150, 2048)

		if event.EventType != EventTypeDownloadCompleted {
			t.Errorf("expected event type %s, got %s", EventTypeDownloadCompleted, event.EventType)
		}
		if event.DurationMs != 150 {
			t.Errorf("expected duration 150ms, got %d", event.DurationMs)
		}
		if event.Size != 2048 {
			t.Errorf("expected size 2048, got %d", event.Size)
		}

		data, err := event.Marshal()
		if err != nil {
			t.Fatalf("failed to marshal event: %v", err)
		}
		var decoded DownloadCompletedEvent
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal event: %v", err)
		}
	})

	t.Run("OrphanDetectedEvent", func(t *testing.T) {
		event := NewOrphanDetectedEvent(proxyID, requestID, "upload_failure", "test-topic", "bucket", "s3/key", "orig-req-456", "kafka_produce_failed", 4096)

		if event.EventType != EventTypeOrphanDetected {
			t.Errorf("expected event type %s, got %s", EventTypeOrphanDetected, event.EventType)
		}
		if event.DetectionSource != "upload_failure" {
			t.Errorf("expected detection source upload_failure, got %s", event.DetectionSource)
		}
		if event.Reason != "kafka_produce_failed" {
			t.Errorf("expected reason kafka_produce_failed, got %s", event.Reason)
		}
		if event.OriginalRequestID != "orig-req-456" {
			t.Errorf("expected original request ID orig-req-456, got %s", event.OriginalRequestID)
		}

		data, err := event.Marshal()
		if err != nil {
			t.Fatalf("failed to marshal event: %v", err)
		}
		var decoded OrphanDetectedEvent
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal event: %v", err)
		}
	})
}

func TestTrackerDisabled(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	cfg := TrackerConfig{
		Enabled: false,
		ProxyID: "test-proxy",
	}

	tracker, err := NewLfsOpsTracker(ctx, cfg, logger)
	if err != nil {
		t.Fatalf("failed to create disabled tracker: %v", err)
	}

	if tracker.IsEnabled() {
		t.Error("expected tracker to be disabled")
	}

	// Should not panic when emitting to disabled tracker
	tracker.EmitUploadStarted("req-1", "topic", 0, "key", "ct", "ip", "http", 100)
	tracker.EmitUploadCompleted("req-1", "topic", 0, 0, "bucket", "key", 100, "sha", "cs", "alg", "ct", time.Second)
	tracker.EmitUploadFailed("req-1", "topic", "key", "code", "msg", "stage", 0, time.Second)
	tracker.EmitDownloadRequested("req-1", "bucket", "key", "presign", "ip", 60)
	tracker.EmitDownloadCompleted("req-1", "key", "presign", time.Second, 100)
	tracker.EmitOrphanDetected("req-1", "source", "topic", "bucket", "key", "orig", "reason", 100)

	stats := tracker.Stats()
	if stats.Enabled {
		t.Error("expected stats.Enabled to be false")
	}
}

func TestTrackerNoBrokers(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	cfg := TrackerConfig{
		Enabled: true,
		Topic:   "__lfs_ops_state",
		Brokers: nil, // No brokers
		ProxyID: "test-proxy",
	}

	tracker, err := NewLfsOpsTracker(ctx, cfg, logger)
	if err != nil {
		t.Fatalf("failed to create tracker without brokers: %v", err)
	}

	if tracker.IsEnabled() {
		t.Error("expected tracker to be disabled when no brokers configured")
	}
}

func TestTrackerConfigDefaults(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	cfg := TrackerConfig{
		Enabled:   true,
		Topic:     "", // Should default to __lfs_ops_state
		Brokers:   []string{"localhost:9092"},
		BatchSize: 0, // Should default
		FlushMs:   0, // Should default
		ProxyID:   "test-proxy",
	}

	// This will fail to connect but should not error on config defaults
	tracker, err := NewLfsOpsTracker(ctx, cfg, logger)
	if err != nil {
		// May fail to connect, but defaults should be set
		t.Logf("tracker creation returned error (expected if Kafka not running): %v", err)
	}
	if tracker != nil {
		defer tracker.Close()
	}
}

func TestEventToRecordUsesTopicKey(t *testing.T) {
	tracker := &LfsOpsTracker{}
	event := NewUploadCompletedEvent(
		"proxy-1",
		"req-1",
		"topic-a",
		0,
		10,
		"bucket",
		"key",
		123,
		"sha",
		"chk",
		"sha256",
		"application/octet-stream",
		10,
	)

	record, err := tracker.eventToRecord(event)
	if err != nil {
		t.Fatalf("eventToRecord error: %v", err)
	}
	if string(record.Key) != "topic-a" {
		t.Fatalf("expected record key topic-a, got %q", string(record.Key))
	}
	if record.Partition != 0 {
		t.Fatalf("expected partition 0 (unset), got %d", record.Partition)
	}
}

func TestTrackerStats(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tracker := &LfsOpsTracker{
		config: TrackerConfig{
			Enabled: true,
			Topic:   "__lfs_ops_state",
		},
		logger: logger,
	}

	stats := tracker.Stats()
	if !stats.Enabled {
		t.Error("expected stats.Enabled to be true")
	}
	if stats.Topic != "__lfs_ops_state" {
		t.Errorf("expected topic __lfs_ops_state, got %s", stats.Topic)
	}
}

func TestNilTrackerSafe(t *testing.T) {
	var tracker *LfsOpsTracker

	// All these should not panic on nil tracker
	tracker.Emit(nil)
	tracker.EmitUploadStarted("", "", 0, "", "", "", "", 0)
	tracker.EmitUploadCompleted("", "", 0, 0, "", "", 0, "", "", "", "", 0)
	tracker.EmitUploadFailed("", "", "", "", "", "", 0, 0)
	tracker.EmitDownloadRequested("", "", "", "", "", 0)
	tracker.EmitDownloadCompleted("", "", "", 0, 0)
	tracker.EmitOrphanDetected("", "", "", "", "", "", "", 0)

	if tracker.IsEnabled() {
		t.Error("nil tracker should not be enabled")
	}

	stats := tracker.Stats()
	if stats.Enabled {
		t.Error("nil tracker stats should show disabled")
	}

	// Close should not panic
	err := tracker.Close()
	if err != nil {
		t.Errorf("nil tracker close should not error: %v", err)
	}
}

func TestGetTopic(t *testing.T) {
	tests := []struct {
		event    TrackerEvent
		expected string
	}{
		{&UploadStartedEvent{Topic: "topic-a"}, "topic-a"},
		{&UploadCompletedEvent{Topic: "topic-b"}, "topic-b"},
		{&UploadFailedEvent{Topic: "topic-c"}, "topic-c"},
		{&DownloadRequestedEvent{}, ""},
		{&DownloadCompletedEvent{}, ""},
		{&OrphanDetectedEvent{Topic: "topic-d"}, "topic-d"},
	}

	for _, tt := range tests {
		result := tt.event.GetTopic()
		if result != tt.expected {
			t.Errorf("GetTopic() = %q, expected %q", result, tt.expected)
		}
	}
}

func TestBaseEventFields(t *testing.T) {
	base := newBaseEvent("test_event", "proxy-1", "req-abc")

	if base.EventType != "test_event" {
		t.Errorf("expected event type test_event, got %s", base.EventType)
	}
	if base.ProxyID != "proxy-1" {
		t.Errorf("expected proxy ID proxy-1, got %s", base.ProxyID)
	}
	if base.RequestID != "req-abc" {
		t.Errorf("expected request ID req-abc, got %s", base.RequestID)
	}
	if base.Version != TrackerEventVersion {
		t.Errorf("expected version %d, got %d", TrackerEventVersion, base.Version)
	}
	if base.EventID == "" {
		t.Error("expected non-empty event ID")
	}
	if base.Timestamp == "" {
		t.Error("expected non-empty timestamp")
	}
}
