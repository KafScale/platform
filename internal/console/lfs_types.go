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

// LFSStatusResponse represents the response for /ui/api/lfs/status
type LFSStatusResponse struct {
	Enabled         bool              `json:"enabled"`
	ProxyCount      int               `json:"proxy_count"`
	S3Bucket        string            `json:"s3_bucket"`
	TopicsWithLFS   []string          `json:"topics_with_lfs"`
	Stats           LFSStats          `json:"stats"`
	TrackerTopic    string            `json:"tracker_topic"`
	TrackerEnabled  bool              `json:"tracker_enabled"`
	ConsumerStatus  LFSConsumerStatus `json:"consumer_status"`
}

// LFSConsumerStatus represents the tracker consumer health.
type LFSConsumerStatus struct {
	Connected   bool   `json:"connected"`
	LastError   string `json:"last_error,omitempty"`
	LastErrorAt string `json:"last_error_at,omitempty"`
	LastPollAt  string `json:"last_poll_at,omitempty"`
}

// LFSStats represents aggregate LFS statistics
type LFSStats struct {
	TotalObjects   int64   `json:"total_objects"`
	TotalBytes     int64   `json:"total_bytes"`
	Uploads24h     int64   `json:"uploads_24h"`
	Downloads24h   int64   `json:"downloads_24h"`
	Errors24h      int64   `json:"errors_24h"`
	OrphansPending int64   `json:"orphans_pending"`
	AvgUploadMs    float64 `json:"avg_upload_ms"`
	AvgDownloadMs  float64 `json:"avg_download_ms"`
}

// LFSObject represents an LFS object in the browser
type LFSObject struct {
	S3Key       string `json:"s3_key"`
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	KafkaOffset int64  `json:"kafka_offset,omitempty"`
	Size        int64  `json:"size"`
	SHA256      string `json:"sha256"`
	ContentType string `json:"content_type,omitempty"`
	CreatedAt   string `json:"created_at"`
	ProxyID     string `json:"proxy_id,omitempty"`
}

// LFSObjectsResponse represents the response for /ui/api/lfs/objects
type LFSObjectsResponse struct {
	Objects    []LFSObject `json:"objects"`
	NextCursor string      `json:"next_cursor,omitempty"`
	TotalCount int64       `json:"total_count"`
}

// LFSTopicStats represents per-topic LFS statistics
type LFSTopicStats struct {
	Name          string `json:"name"`
	HasLFS        bool   `json:"has_lfs"`
	ObjectCount   int64  `json:"object_count"`
	TotalBytes    int64  `json:"total_bytes"`
	AvgObjectSize int64  `json:"avg_object_size"`
	Uploads24h    int64  `json:"uploads_24h"`
	Downloads24h  int64  `json:"downloads_24h"`
	Errors24h     int64  `json:"errors_24h"`
	Orphans       int64  `json:"orphans_detected"`
	FirstObject   string `json:"first_object,omitempty"`
	LastObject    string `json:"last_object,omitempty"`
	LastUpload    string `json:"last_upload,omitempty"`
	LastDownload  string `json:"last_download,omitempty"`
	LastError     string `json:"last_error,omitempty"`
	LastEvent     string `json:"last_event,omitempty"`
}

// LFSTopicsResponse represents the response for /ui/api/lfs/topics
type LFSTopicsResponse struct {
	Topics []LFSTopicStats `json:"topics"`
}

// LFSTopicDetailResponse represents a single topic detail response
type LFSTopicDetailResponse struct {
	Topic  LFSTopicStats `json:"topic"`
	Events []LFSEvent    `json:"events,omitempty"`
}

// LFSEvent represents a tracker event
type LFSEvent struct {
	EventType   string `json:"event_type"`
	EventID     string `json:"event_id"`
	Timestamp   string `json:"timestamp"`
	ProxyID     string `json:"proxy_id"`
	RequestID   string `json:"request_id"`
	Topic       string `json:"topic,omitempty"`
	S3Key       string `json:"s3_key,omitempty"`
	Size        int64  `json:"size,omitempty"`
	DurationMs  int64  `json:"duration_ms,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
	Mode        string `json:"mode,omitempty"`
}

// LFSOrphan represents an orphaned S3 object
type LFSOrphan struct {
	S3Key      string `json:"s3_key"`
	S3Bucket   string `json:"s3_bucket"`
	Topic      string `json:"topic"`
	Size       int64  `json:"size"`
	DetectedAt string `json:"detected_at"`
	Reason     string `json:"reason"`
	AgeHours   int    `json:"age_hours"`
}

// LFSOrphansResponse represents the response for /ui/api/lfs/orphans
type LFSOrphansResponse struct {
	Orphans   []LFSOrphan `json:"orphans"`
	TotalSize int64       `json:"total_size"`
	Count     int         `json:"count"`
}

// S3Object represents an object in S3 browser
type S3Object struct {
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified"`
	ETag         string `json:"etag,omitempty"`
}

// S3BrowseResponse represents the response for /ui/api/lfs/s3/browse
type S3BrowseResponse struct {
	Objects        []S3Object `json:"objects"`
	CommonPrefixes []string   `json:"common_prefixes"`
	IsTruncated    bool       `json:"is_truncated"`
}

// S3PresignRequest represents the request for /ui/api/lfs/s3/presign
type S3PresignRequest struct {
	S3Key      string `json:"s3_key"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

// S3PresignResponse represents the response for /ui/api/lfs/s3/presign
type S3PresignResponse struct {
	URL       string `json:"url"`
	ExpiresAt string `json:"expires_at"`
}

// LFSConfig holds configuration for LFS console features
type LFSConfig struct {
	Enabled       bool
	TrackerTopic  string
	KafkaBrokers  []string
	S3Bucket      string
	S3Region      string
	S3Endpoint    string
	S3AccessKey   string
	S3SecretKey   string
	PresignTTL    int // seconds
}
