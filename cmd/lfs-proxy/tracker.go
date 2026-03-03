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
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultTrackerTopic       = "__lfs_ops_state"
	defaultTrackerBatchSize   = 100
	defaultTrackerFlushMs     = 100
	defaultTrackerChanSize    = 10000
	defaultTrackerPartitions  = 3
	defaultTrackerReplication = 1
)

// TrackerConfig holds configuration for the LFS operations tracker.
type TrackerConfig struct {
	Enabled           bool
	Topic             string
	Brokers           []string
	BatchSize         int
	FlushMs           int
	ProxyID           string
	EnsureTopic       bool
	Partitions        int
	ReplicationFactor int
}

// LfsOpsTracker tracks LFS operations by emitting events to a Kafka topic.
type LfsOpsTracker struct {
	config  TrackerConfig
	client  *kgo.Client
	logger  *slog.Logger
	eventCh chan TrackerEvent
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc

	// Circuit breaker state
	circuitOpen      uint32
	failures         uint32
	lastSuccess      int64
	failureThreshold uint32
	resetTimeout     time.Duration

	// Metrics
	eventsEmitted uint64
	eventsDropped uint64
	batchesSent   uint64
}

// NewLfsOpsTracker creates a new tracker instance.
func NewLfsOpsTracker(ctx context.Context, cfg TrackerConfig, logger *slog.Logger) (*LfsOpsTracker, error) {
	if !cfg.Enabled {
		logger.Info("lfs ops tracker disabled")
		return &LfsOpsTracker{config: cfg, logger: logger}, nil
	}

	if cfg.Topic == "" {
		cfg.Topic = defaultTrackerTopic
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultTrackerBatchSize
	}
	if cfg.FlushMs <= 0 {
		cfg.FlushMs = defaultTrackerFlushMs
	}
	if cfg.Partitions <= 0 {
		cfg.Partitions = defaultTrackerPartitions
	}
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = defaultTrackerReplication
	}
	if len(cfg.Brokers) == 0 {
		logger.Warn("lfs ops tracker: no brokers configured, tracker disabled")
		return &LfsOpsTracker{config: cfg, logger: logger}, nil
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.ProducerBatchMaxBytes(1024 * 1024), // 1MB max batch
		kgo.ProducerLinger(time.Duration(cfg.FlushMs) * time.Millisecond),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(), // Not required for tracking events
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	if cfg.EnsureTopic {
		if err := ensureTrackerTopic(ctx, client, cfg, logger); err != nil {
			logger.Warn("lfs ops tracker: ensure topic failed", "topic", cfg.Topic, "error", err)
		}
	}

	trackerCtx, cancel := context.WithCancel(ctx)
	t := &LfsOpsTracker{
		config:           cfg,
		client:           client,
		logger:           logger,
		eventCh:          make(chan TrackerEvent, defaultTrackerChanSize),
		ctx:              trackerCtx,
		cancel:           cancel,
		failureThreshold: 5,
		resetTimeout:     30 * time.Second,
	}

	t.wg.Add(1)
	go t.runBatcher()

	logger.Info("lfs ops tracker started", "topic", cfg.Topic, "brokers", cfg.Brokers)
	return t, nil
}

// Emit sends a tracker event to the channel for async processing.
func (t *LfsOpsTracker) Emit(event TrackerEvent) {
	if t == nil || !t.config.Enabled || t.client == nil {
		return
	}

	// Check circuit breaker
	if atomic.LoadUint32(&t.circuitOpen) == 1 {
		// Check if we should try to reset
		if time.Now().UnixNano()-atomic.LoadInt64(&t.lastSuccess) > t.resetTimeout.Nanoseconds() {
			atomic.StoreUint32(&t.circuitOpen, 0)
			atomic.StoreUint32(&t.failures, 0)
			t.logger.Info("lfs ops tracker: circuit breaker reset")
		} else {
			atomic.AddUint64(&t.eventsDropped, 1)
			return
		}
	}

	select {
	case t.eventCh <- event:
		atomic.AddUint64(&t.eventsEmitted, 1)
	default:
		// Channel full, drop the event
		atomic.AddUint64(&t.eventsDropped, 1)
		t.logger.Debug("lfs ops tracker: event dropped, channel full")
	}
}

// runBatcher processes events from the channel and sends them in batches.
func (t *LfsOpsTracker) runBatcher() {
	defer t.wg.Done()

	batch := make([]*kgo.Record, 0, t.config.BatchSize)
	flushInterval := time.Duration(t.config.FlushMs) * time.Millisecond
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		// Produce batch
		results := t.client.ProduceSync(t.ctx, batch...)
		hasError := false
		for _, result := range results {
			if result.Err != nil {
				hasError = true
				t.logger.Warn("lfs ops tracker: produce failed", "error", result.Err)
			}
		}

		if hasError {
			failures := atomic.AddUint32(&t.failures, 1)
			if failures >= t.failureThreshold {
				atomic.StoreUint32(&t.circuitOpen, 1)
				t.logger.Warn("lfs ops tracker: circuit breaker opened", "failures", failures)
			}
		} else {
			atomic.StoreUint32(&t.failures, 0)
			atomic.StoreInt64(&t.lastSuccess, time.Now().UnixNano())
			atomic.AddUint64(&t.batchesSent, 1)
		}

		batch = batch[:0]
	}

	for {
		select {
		case <-t.ctx.Done():
			flush()
			return

		case event := <-t.eventCh:
			record, err := t.eventToRecord(event)
			if err != nil {
				t.logger.Warn("lfs ops tracker: failed to serialize event", "error", err, "type", event.GetEventType())
				continue
			}
			batch = append(batch, record)
			if len(batch) >= t.config.BatchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

// eventToRecord converts a TrackerEvent to a Kafka record.
func (t *LfsOpsTracker) eventToRecord(event TrackerEvent) (*kgo.Record, error) {
	value, err := event.Marshal()
	if err != nil {
		return nil, err
	}

	return &kgo.Record{
		Key:   []byte(event.GetTopic()),
		Value: value,
	}, nil
}

func ensureTrackerTopic(ctx context.Context, client *kgo.Client, cfg TrackerConfig, logger *slog.Logger) error {
	admin := kadm.NewClient(client)
	responses, err := admin.CreateTopics(ctx, int32(cfg.Partitions), int16(cfg.ReplicationFactor), nil, cfg.Topic)
	if err != nil {
		return err
	}
	resp, ok := responses[cfg.Topic]
	if !ok {
		return errors.New("tracker topic response missing")
	}
	if resp.Err == nil || errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		logger.Info("lfs ops tracker topic ready", "topic", cfg.Topic, "partitions", cfg.Partitions, "replication", cfg.ReplicationFactor)
		return nil
	}
	return resp.Err
}

// Close gracefully shuts down the tracker.
func (t *LfsOpsTracker) Close() error {
	if t == nil || t.client == nil {
		return nil
	}

	t.cancel()
	t.wg.Wait()
	t.client.Close()

	t.logger.Info("lfs ops tracker closed",
		"events_emitted", atomic.LoadUint64(&t.eventsEmitted),
		"events_dropped", atomic.LoadUint64(&t.eventsDropped),
		"batches_sent", atomic.LoadUint64(&t.batchesSent),
	)
	return nil
}

// Stats returns tracker statistics.
func (t *LfsOpsTracker) Stats() TrackerStats {
	if t == nil {
		return TrackerStats{}
	}
	return TrackerStats{
		Enabled:       t.config.Enabled,
		Topic:         t.config.Topic,
		EventsEmitted: atomic.LoadUint64(&t.eventsEmitted),
		EventsDropped: atomic.LoadUint64(&t.eventsDropped),
		BatchesSent:   atomic.LoadUint64(&t.batchesSent),
		CircuitOpen:   atomic.LoadUint32(&t.circuitOpen) == 1,
	}
}

// TrackerStats holds statistics about the tracker.
type TrackerStats struct {
	Enabled       bool   `json:"enabled"`
	Topic         string `json:"topic"`
	EventsEmitted uint64 `json:"events_emitted"`
	EventsDropped uint64 `json:"events_dropped"`
	BatchesSent   uint64 `json:"batches_sent"`
	CircuitOpen   bool   `json:"circuit_open"`
}

// IsEnabled returns true if the tracker is enabled and ready.
func (t *LfsOpsTracker) IsEnabled() bool {
	return t != nil && t.config.Enabled && t.client != nil
}

// EmitUploadStarted emits an upload started event.
func (t *LfsOpsTracker) EmitUploadStarted(requestID, topic string, partition int32, s3Key, contentType, clientIP, apiType string, expectedSize int64) {
	if !t.IsEnabled() {
		return
	}
	event := NewUploadStartedEvent(t.config.ProxyID, requestID, topic, partition, s3Key, contentType, clientIP, apiType, expectedSize)
	t.Emit(event)
}

// EmitUploadCompleted emits an upload completed event.
func (t *LfsOpsTracker) EmitUploadCompleted(requestID, topic string, partition int32, kafkaOffset int64, s3Bucket, s3Key string, size int64, sha256, checksum, checksumAlg, contentType string, duration time.Duration) {
	if !t.IsEnabled() {
		return
	}
	event := NewUploadCompletedEvent(t.config.ProxyID, requestID, topic, partition, kafkaOffset, s3Bucket, s3Key, size, sha256, checksum, checksumAlg, contentType, duration.Milliseconds())
	t.Emit(event)
}

// EmitUploadFailed emits an upload failed event.
func (t *LfsOpsTracker) EmitUploadFailed(requestID, topic, s3Key, errorCode, errorMessage, stage string, sizeUploaded int64, duration time.Duration) {
	if !t.IsEnabled() {
		return
	}
	event := NewUploadFailedEvent(t.config.ProxyID, requestID, topic, s3Key, errorCode, errorMessage, stage, sizeUploaded, duration.Milliseconds())
	t.Emit(event)
}

// EmitDownloadRequested emits a download requested event.
func (t *LfsOpsTracker) EmitDownloadRequested(requestID, s3Bucket, s3Key, mode, clientIP string, ttlSeconds int) {
	if !t.IsEnabled() {
		return
	}
	event := NewDownloadRequestedEvent(t.config.ProxyID, requestID, s3Bucket, s3Key, mode, clientIP, ttlSeconds)
	t.Emit(event)
}

// EmitDownloadCompleted emits a download completed event.
func (t *LfsOpsTracker) EmitDownloadCompleted(requestID, s3Key, mode string, duration time.Duration, size int64) {
	if !t.IsEnabled() {
		return
	}
	event := NewDownloadCompletedEvent(t.config.ProxyID, requestID, s3Key, mode, duration.Milliseconds(), size)
	t.Emit(event)
}

// EmitOrphanDetected emits an orphan detected event.
func (t *LfsOpsTracker) EmitOrphanDetected(requestID, detectionSource, topic, s3Bucket, s3Key, originalRequestID, reason string, size int64) {
	if !t.IsEnabled() {
		return
	}
	event := NewOrphanDetectedEvent(t.config.ProxyID, requestID, detectionSource, topic, s3Bucket, s3Key, originalRequestID, reason, size)
	t.Emit(event)
}
