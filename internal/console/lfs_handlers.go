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
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// LFSHandlers provides HTTP handlers for LFS admin APIs
type LFSHandlers struct {
	config   LFSConfig
	consumer *LFSConsumer
	s3Client *LFSS3Client
	logger   *log.Logger

	// In-memory state (populated from tracker events)
	mu           sync.RWMutex
	objects      map[string]*LFSObject     // key: s3_key
	topicStats   map[string]*LFSTopicStats // key: topic name
	orphans      map[string]*LFSOrphan     // key: s3_key
	events       []LFSEvent                // circular buffer of recent events
	stats        LFSStats
	lastEventIdx int
}

const maxRecentEvents = 1000

// NewLFSHandlers creates a new LFS handlers instance
func NewLFSHandlers(cfg LFSConfig, logger *log.Logger) *LFSHandlers {
	if logger == nil {
		logger = log.Default()
	}
	h := &LFSHandlers{
		config:     cfg,
		logger:     logger,
		objects:    make(map[string]*LFSObject),
		topicStats: make(map[string]*LFSTopicStats),
		orphans:    make(map[string]*LFSOrphan),
		events:     make([]LFSEvent, 0, maxRecentEvents),
	}
	return h
}

// SetConsumer sets the LFS tracker consumer
func (h *LFSHandlers) SetConsumer(consumer *LFSConsumer) {
	h.consumer = consumer
}

// SetS3Client sets the S3 client for browsing
func (h *LFSHandlers) SetS3Client(client *LFSS3Client) {
	h.s3Client = client
}

// ProcessEvent handles an incoming tracker event
func (h *LFSHandlers) ProcessEvent(event LFSEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Add to recent events
	if len(h.events) < maxRecentEvents {
		h.events = append(h.events, event)
	} else {
		h.events[h.lastEventIdx] = event
		h.lastEventIdx = (h.lastEventIdx + 1) % maxRecentEvents
	}

	var topicStats *LFSTopicStats
	if event.Topic != "" {
		topicStats = h.getOrCreateTopicStats(event.Topic)
		topicStats.HasLFS = true
		topicStats.LastEvent = event.Timestamp
	}

	// Update stats based on event type
	switch event.EventType {
	case "upload_started":
		// No stat updates on start

	case "upload_completed":
		h.stats.TotalObjects++
		h.stats.TotalBytes += event.Size
		h.stats.Uploads24h++
		if topicStats != nil {
			h.updateTopicStats(topicStats, event.Size, event.Timestamp)
			topicStats.Uploads24h++
			topicStats.LastUpload = event.Timestamp
		}

		// Add object to map
		obj := &LFSObject{
			S3Key:       event.S3Key,
			Topic:       event.Topic,
			Size:        event.Size,
			CreatedAt:   event.Timestamp,
			ProxyID:     event.ProxyID,
		}
		h.objects[event.S3Key] = obj

	case "upload_failed":
		h.stats.Errors24h++
		if topicStats != nil {
			topicStats.Errors24h++
			topicStats.LastError = event.Timestamp
		}

	case "download_requested":
		h.stats.Downloads24h++
		if topicStats != nil {
			topicStats.Downloads24h++
			topicStats.LastDownload = event.Timestamp
		}

	case "download_completed":
		// Track download metrics
		if topicStats != nil {
			topicStats.LastDownload = event.Timestamp
		}

	case "orphan_detected":
		h.stats.OrphansPending++
		if topicStats != nil {
			topicStats.Orphans++
		}
		h.orphans[event.S3Key] = &LFSOrphan{
			S3Key:      event.S3Key,
			Topic:      event.Topic,
			DetectedAt: event.Timestamp,
			Reason:     event.ErrorCode,
		}
	}
}

func (h *LFSHandlers) getOrCreateTopicStats(topic string) *LFSTopicStats {
	stats, exists := h.topicStats[topic]
	if !exists {
		stats = &LFSTopicStats{Name: topic}
		h.topicStats[topic] = stats
	}
	return stats
}

func (h *LFSHandlers) updateTopicStats(stats *LFSTopicStats, size int64, timestamp string) {
	if stats == nil {
		return
	}
	stats.ObjectCount++
	stats.TotalBytes += size
	if stats.ObjectCount > 0 {
		stats.AvgObjectSize = stats.TotalBytes / stats.ObjectCount
	}
	stats.LastObject = timestamp
	if stats.FirstObject == "" {
		stats.FirstObject = stats.LastObject
	}
}

// HandleStatus handles GET /ui/api/lfs/status
func (h *LFSHandlers) HandleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h.mu.RLock()
	topics := make([]string, 0, len(h.topicStats))
	for topic, stats := range h.topicStats {
		if stats.HasLFS {
			topics = append(topics, topic)
		}
	}
	stats := h.stats
	h.mu.RUnlock()

	resp := LFSStatusResponse{
		Enabled:        h.config.Enabled,
		ProxyCount:     1, // TODO: Get from metrics
		S3Bucket:       h.config.S3Bucket,
		TopicsWithLFS:  topics,
		Stats:          stats,
		TrackerTopic:   h.config.TrackerTopic,
		TrackerEnabled: h.config.Enabled,
	}
	if h.consumer != nil {
		resp.ConsumerStatus = h.consumer.Status()
	}

	writeJSON(w, resp)
}

// HandleObjects handles GET /ui/api/lfs/objects
func (h *LFSHandlers) HandleObjects(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters
	topic := r.URL.Query().Get("topic")
	limitStr := r.URL.Query().Get("limit")
	cursor := r.URL.Query().Get("cursor")
	_ = cursor // TODO: Implement pagination

	limit := 50
	if limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 && parsed <= 200 {
			limit = parsed
		}
	}

	h.mu.RLock()
	objects := make([]LFSObject, 0, limit)
	count := 0
	for _, obj := range h.objects {
		if topic != "" && obj.Topic != topic {
			continue
		}
		if count >= limit {
			break
		}
		objects = append(objects, *obj)
		count++
	}
	total := int64(len(h.objects))
	h.mu.RUnlock()

	resp := LFSObjectsResponse{
		Objects:    objects,
		TotalCount: total,
	}

	writeJSON(w, resp)
}

// HandleTopics handles GET /ui/api/lfs/topics
func (h *LFSHandlers) HandleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h.mu.RLock()
	topics := make([]LFSTopicStats, 0, len(h.topicStats))
	for _, stats := range h.topicStats {
		topics = append(topics, *stats)
	}
	h.mu.RUnlock()

	resp := LFSTopicsResponse{
		Topics: topics,
	}

	writeJSON(w, resp)
}

// HandleTopicDetail handles GET /ui/api/lfs/topics/{name}
func (h *LFSHandlers) HandleTopicDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/ui/api/lfs/topics/")
	if name == "" {
		http.Error(w, "topic name required", http.StatusBadRequest)
		return
	}

	h.mu.RLock()
	stats, ok := h.topicStats[name]
	if !ok {
		h.mu.RUnlock()
		http.Error(w, "topic not found", http.StatusNotFound)
		return
	}
	events := make([]LFSEvent, 0, len(h.events))
	for _, event := range h.events {
		if event.Topic == name {
			events = append(events, event)
		}
	}
	h.mu.RUnlock()

	resp := LFSTopicDetailResponse{
		Topic:  *stats,
		Events: events,
	}
	writeJSON(w, resp)
}

// HandleEvents handles GET /ui/api/lfs/events (SSE)
func (h *LFSHandlers) HandleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Parse filter
	typesFilter := r.URL.Query().Get("types")
	var allowedTypes map[string]bool
	if typesFilter != "" {
		allowedTypes = make(map[string]bool)
		for _, t := range strings.Split(typesFilter, ",") {
			allowedTypes[strings.TrimSpace(t)] = true
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Send existing events first
	h.mu.RLock()
	for _, event := range h.events {
		if allowedTypes != nil && !allowedTypes[event.EventType] {
			continue
		}
		data, _ := json.Marshal(event)
		_, _ = w.Write([]byte("data: "))
		_, _ = w.Write(data)
		_, _ = w.Write([]byte("\n\n"))
	}
	h.mu.RUnlock()
	flusher.Flush()

	// Keep connection open for new events
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Send keepalive
			_, _ = w.Write([]byte(": keepalive\n\n"))
			flusher.Flush()
		}
	}
}

// HandleOrphans handles GET /ui/api/lfs/orphans
func (h *LFSHandlers) HandleOrphans(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h.mu.RLock()
	orphans := make([]LFSOrphan, 0, len(h.orphans))
	var totalSize int64
	for _, orphan := range h.orphans {
		orphans = append(orphans, *orphan)
		totalSize += orphan.Size
	}
	h.mu.RUnlock()

	resp := LFSOrphansResponse{
		Orphans:   orphans,
		TotalSize: totalSize,
		Count:     len(orphans),
	}

	writeJSON(w, resp)
}

// HandleS3Browse handles GET /ui/api/lfs/s3/browse
func (h *LFSHandlers) HandleS3Browse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h.s3Client == nil {
		http.Error(w, "s3 client not configured", http.StatusServiceUnavailable)
		return
	}

	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	if delimiter == "" {
		delimiter = "/"
	}
	maxKeysStr := r.URL.Query().Get("max_keys")
	maxKeys := 100
	if maxKeysStr != "" {
		if parsed, err := strconv.Atoi(maxKeysStr); err == nil && parsed > 0 && parsed <= 1000 {
			maxKeys = parsed
		}
	}

	objects, prefixes, truncated, err := h.s3Client.ListObjects(r.Context(), prefix, delimiter, maxKeys)
	if err != nil {
		h.logger.Printf("s3 list error: %v", err)
		http.Error(w, "s3 list failed", http.StatusBadGateway)
		return
	}

	resp := S3BrowseResponse{
		Objects:        objects,
		CommonPrefixes: prefixes,
		IsTruncated:    truncated,
	}

	writeJSON(w, resp)
}

// HandleS3Presign handles POST /ui/api/lfs/s3/presign
func (h *LFSHandlers) HandleS3Presign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h.s3Client == nil {
		http.Error(w, "s3 client not configured", http.StatusServiceUnavailable)
		return
	}

	var req S3PresignRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.S3Key == "" {
		http.Error(w, "s3_key required", http.StatusBadRequest)
		return
	}

	ttl := h.config.PresignTTL
	if ttl <= 0 {
		ttl = 300 // default 5 minutes
	}
	if req.TTLSeconds > 0 && req.TTLSeconds < ttl {
		ttl = req.TTLSeconds
	}

	url, err := h.s3Client.PresignGetObject(r.Context(), req.S3Key, time.Duration(ttl)*time.Second)
	if err != nil {
		h.logger.Printf("s3 presign error: %v", err)
		http.Error(w, "s3 presign failed", http.StatusBadGateway)
		return
	}

	resp := S3PresignResponse{
		URL:       url,
		ExpiresAt: time.Now().UTC().Add(time.Duration(ttl) * time.Second).Format(time.RFC3339),
	}

	writeJSON(w, resp)
}

// ResetStats resets the 24h rolling statistics (call periodically)
func (h *LFSHandlers) ResetStats() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.stats.Uploads24h = 0
	h.stats.Downloads24h = 0
	h.stats.Errors24h = 0

	for _, ts := range h.topicStats {
		ts.Uploads24h = 0
		ts.Errors24h = 0
	}
}
