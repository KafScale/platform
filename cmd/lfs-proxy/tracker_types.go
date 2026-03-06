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
	"encoding/json"
	"time"
)

// Event types for LFS operations tracking.
const (
	EventTypeUploadStarted      = "upload_started"
	EventTypeUploadCompleted    = "upload_completed"
	EventTypeUploadFailed       = "upload_failed"
	EventTypeDownloadRequested  = "download_requested"
	EventTypeDownloadCompleted  = "download_completed"
	EventTypeOrphanDetected     = "orphan_detected"
)

// TrackerEventVersion is the current schema version for tracker events.
const TrackerEventVersion = 1

// BaseEvent contains common fields for all tracker events.
type BaseEvent struct {
	EventType   string `json:"event_type"`
	EventID     string `json:"event_id"`
	Timestamp   string `json:"timestamp"`
	ProxyID     string `json:"proxy_id"`
	RequestID   string `json:"request_id"`
	Version     int    `json:"version"`
}

// UploadStartedEvent is emitted when an upload operation begins.
type UploadStartedEvent struct {
	BaseEvent
	Topic        string `json:"topic"`
	Partition    int32  `json:"partition"`
	S3Key        string `json:"s3_key"`
	ContentType  string `json:"content_type,omitempty"`
	ExpectedSize int64  `json:"expected_size,omitempty"`
	ClientIP     string `json:"client_ip,omitempty"`
	APIType      string `json:"api_type"` // "http" or "kafka"
}

// UploadCompletedEvent is emitted when an upload operation succeeds.
type UploadCompletedEvent struct {
	BaseEvent
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	KafkaOffset int64  `json:"kafka_offset,omitempty"`
	S3Bucket    string `json:"s3_bucket"`
	S3Key       string `json:"s3_key"`
	Size        int64  `json:"size"`
	SHA256      string `json:"sha256"`
	Checksum    string `json:"checksum,omitempty"`
	ChecksumAlg string `json:"checksum_alg,omitempty"`
	DurationMs  int64  `json:"duration_ms"`
	ContentType string `json:"content_type,omitempty"`
}

// UploadFailedEvent is emitted when an upload operation fails.
type UploadFailedEvent struct {
	BaseEvent
	Topic        string `json:"topic"`
	S3Key        string `json:"s3_key,omitempty"`
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
	Stage        string `json:"stage"` // "validation", "s3_upload", "kafka_produce"
	SizeUploaded int64  `json:"size_uploaded,omitempty"`
	DurationMs   int64  `json:"duration_ms"`
}

// DownloadRequestedEvent is emitted when a download operation is requested.
type DownloadRequestedEvent struct {
	BaseEvent
	S3Bucket   string `json:"s3_bucket"`
	S3Key      string `json:"s3_key"`
	Mode       string `json:"mode"` // "presign" or "stream"
	ClientIP   string `json:"client_ip,omitempty"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

// DownloadCompletedEvent is emitted when a download operation completes.
type DownloadCompletedEvent struct {
	BaseEvent
	S3Key      string `json:"s3_key"`
	Mode       string `json:"mode"`
	DurationMs int64  `json:"duration_ms"`
	Size       int64  `json:"size,omitempty"`
}

// OrphanDetectedEvent is emitted when an orphaned S3 object is detected.
type OrphanDetectedEvent struct {
	BaseEvent
	DetectionSource   string `json:"detection_source"` // "upload_failure", "reconciliation"
	Topic             string `json:"topic"`
	S3Bucket          string `json:"s3_bucket"`
	S3Key             string `json:"s3_key"`
	Size              int64  `json:"size,omitempty"`
	OriginalRequestID string `json:"original_request_id,omitempty"`
	Reason            string `json:"reason"` // "kafka_produce_failed", "checksum_mismatch", etc.
}

// TrackerEvent is a union type that can hold any tracker event.
type TrackerEvent interface {
	GetEventType() string
	GetTopic() string
	Marshal() ([]byte, error)
}

// GetEventType returns the event type.
func (e *BaseEvent) GetEventType() string {
	return e.EventType
}

// GetTopic returns the topic for partitioning.
func (e *UploadStartedEvent) GetTopic() string { return e.Topic }
func (e *UploadCompletedEvent) GetTopic() string { return e.Topic }
func (e *UploadFailedEvent) GetTopic() string { return e.Topic }
func (e *DownloadRequestedEvent) GetTopic() string { return "" }
func (e *DownloadCompletedEvent) GetTopic() string { return "" }
func (e *OrphanDetectedEvent) GetTopic() string { return e.Topic }

// Marshal serializes the event to JSON.
func (e *UploadStartedEvent) Marshal() ([]byte, error) { return json.Marshal(e) }
func (e *UploadCompletedEvent) Marshal() ([]byte, error) { return json.Marshal(e) }
func (e *UploadFailedEvent) Marshal() ([]byte, error) { return json.Marshal(e) }
func (e *DownloadRequestedEvent) Marshal() ([]byte, error) { return json.Marshal(e) }
func (e *DownloadCompletedEvent) Marshal() ([]byte, error) { return json.Marshal(e) }
func (e *OrphanDetectedEvent) Marshal() ([]byte, error) { return json.Marshal(e) }

// newBaseEvent creates a new base event with common fields.
func newBaseEvent(eventType, proxyID, requestID string) BaseEvent {
	return BaseEvent{
		EventType: eventType,
		EventID:   newUUID(),
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		ProxyID:   proxyID,
		RequestID: requestID,
		Version:   TrackerEventVersion,
	}
}

// NewUploadStartedEvent creates a new upload started event.
func NewUploadStartedEvent(proxyID, requestID, topic string, partition int32, s3Key, contentType, clientIP, apiType string, expectedSize int64) *UploadStartedEvent {
	return &UploadStartedEvent{
		BaseEvent:    newBaseEvent(EventTypeUploadStarted, proxyID, requestID),
		Topic:        topic,
		Partition:    partition,
		S3Key:        s3Key,
		ContentType:  contentType,
		ExpectedSize: expectedSize,
		ClientIP:     clientIP,
		APIType:      apiType,
	}
}

// NewUploadCompletedEvent creates a new upload completed event.
func NewUploadCompletedEvent(proxyID, requestID, topic string, partition int32, kafkaOffset int64, s3Bucket, s3Key string, size int64, sha256, checksum, checksumAlg, contentType string, durationMs int64) *UploadCompletedEvent {
	return &UploadCompletedEvent{
		BaseEvent:   newBaseEvent(EventTypeUploadCompleted, proxyID, requestID),
		Topic:       topic,
		Partition:   partition,
		KafkaOffset: kafkaOffset,
		S3Bucket:    s3Bucket,
		S3Key:       s3Key,
		Size:        size,
		SHA256:      sha256,
		Checksum:    checksum,
		ChecksumAlg: checksumAlg,
		DurationMs:  durationMs,
		ContentType: contentType,
	}
}

// NewUploadFailedEvent creates a new upload failed event.
func NewUploadFailedEvent(proxyID, requestID, topic, s3Key, errorCode, errorMessage, stage string, sizeUploaded, durationMs int64) *UploadFailedEvent {
	return &UploadFailedEvent{
		BaseEvent:    newBaseEvent(EventTypeUploadFailed, proxyID, requestID),
		Topic:        topic,
		S3Key:        s3Key,
		ErrorCode:    errorCode,
		ErrorMessage: errorMessage,
		Stage:        stage,
		SizeUploaded: sizeUploaded,
		DurationMs:   durationMs,
	}
}

// NewDownloadRequestedEvent creates a new download requested event.
func NewDownloadRequestedEvent(proxyID, requestID, s3Bucket, s3Key, mode, clientIP string, ttlSeconds int) *DownloadRequestedEvent {
	return &DownloadRequestedEvent{
		BaseEvent:  newBaseEvent(EventTypeDownloadRequested, proxyID, requestID),
		S3Bucket:   s3Bucket,
		S3Key:      s3Key,
		Mode:       mode,
		ClientIP:   clientIP,
		TTLSeconds: ttlSeconds,
	}
}

// NewDownloadCompletedEvent creates a new download completed event.
func NewDownloadCompletedEvent(proxyID, requestID, s3Key, mode string, durationMs, size int64) *DownloadCompletedEvent {
	return &DownloadCompletedEvent{
		BaseEvent:  newBaseEvent(EventTypeDownloadCompleted, proxyID, requestID),
		S3Key:      s3Key,
		Mode:       mode,
		DurationMs: durationMs,
		Size:       size,
	}
}

// NewOrphanDetectedEvent creates a new orphan detected event.
func NewOrphanDetectedEvent(proxyID, requestID, detectionSource, topic, s3Bucket, s3Key, originalRequestID, reason string, size int64) *OrphanDetectedEvent {
	return &OrphanDetectedEvent{
		BaseEvent:         newBaseEvent(EventTypeOrphanDetected, proxyID, requestID),
		DetectionSource:   detectionSource,
		Topic:             topic,
		S3Bucket:          s3Bucket,
		S3Key:             s3Key,
		Size:              size,
		OriginalRequestID: originalRequestID,
		Reason:            reason,
	}
}
